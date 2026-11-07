import asyncio
import logging
import os
import signal
import traceback
from contextlib import suppress
from typing import Optional

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from cryptography.fernet import Fernet, InvalidToken
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("multi-user-forwarder")

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
ENCRYPTION_KEY = os.environ["ENCRYPTION_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
PORT = int(os.getenv("PORT", "8000"))

fernet = Fernet(ENCRYPTION_KEY.encode())
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

db: Optional[asyncpg.Pool] = None
clients: dict[int, Client] = {}
client_locks: dict[int, asyncio.Lock] = {}
tasks: dict[int, asyncio.Task] = {}
task_locks: dict[int, asyncio.Lock] = {}
pending_connect: set[int] = set()
shutdown_event = asyncio.Event()


def encrypt(value: str) -> str:
    return fernet.encrypt(value.encode()).decode()


def decrypt(value: str) -> str:
    return fernet.decrypt(value.encode()).decode()


def get_client_lock(user_id: int) -> asyncio.Lock:
    return client_locks.setdefault(user_id, asyncio.Lock())


def get_task_lock(user_id: int) -> asyncio.Lock:
    return task_locks.setdefault(user_id, asyncio.Lock())


async def init_db():
    global db
    db = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=60,
        server_settings={"application_name": "multi-user-telegram-forwarder"},
    )
    async with db.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            telegram_user_id BIGINT PRIMARY KEY,
            session_ciphertext TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS user_settings (
            telegram_user_id BIGINT PRIMARY KEY REFERENCES user_sessions(telegram_user_id) ON DELETE CASCADE,
            caption TEXT NOT NULL DEFAULT '',
            disabled_types JSONB NOT NULL DEFAULT '[]'::jsonb
        );
        CREATE TABLE IF NOT EXISTS forwarding_tasks (
            telegram_user_id BIGINT PRIMARY KEY REFERENCES user_sessions(telegram_user_id) ON DELETE CASCADE,
            source_chat_id BIGINT NOT NULL,
            destination_chat_id BIGINT NOT NULL,
            start_message_id BIGINT NOT NULL,
            end_message_id BIGINT NOT NULL,
            next_message_id BIGINT NOT NULL,
            success_count BIGINT NOT NULL DEFAULT 0,
            failed_count BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'queued',
            caption TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)


async def save_task(user_id: int, **values):
    assert db
    allowed = {
        "next_message_id", "success_count", "failed_count", "status",
        "caption", "updated_at"
    }
    fields = [(k, v) for k, v in values.items() if k in allowed]
    if not fields:
        return
    assignments = ", ".join(f"{k} = ${i+2}" for i, (k, _) in enumerate(fields))
    args = [user_id] + [v for _, v in fields]
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE forwarding_tasks SET {assignments}, updated_at = NOW() WHERE telegram_user_id = $1",
            *args,
        )


async def get_user_client(user_id: int) -> Optional[Client]:
    if user_id in clients:
        return clients[user_id]
    assert db
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT session_ciphertext FROM user_sessions WHERE telegram_user_id=$1",
            user_id,
        )
    if not row:
        return None
    try:
        session_string = decrypt(row["session_ciphertext"])
    except (InvalidToken, ValueError):
        log.exception("Could not decrypt session for user %s", user_id)
        return None

    async with get_client_lock(user_id):
        if user_id in clients:
            return clients[user_id]
        client = Client(
            name=f"user_{user_id}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
        )
        await client.start()
        clients[user_id] = client
        log.info("Started Telegram user client for %s", user_id)
        return client


async def connect_user_session(user_id: int, session_string: str):
    client = Client(
        name=f"user_{user_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
    )
    await client.start()
    me = await client.get_me()
    assert db
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_sessions(telegram_user_id, session_ciphertext)
            VALUES($1, $2)
            ON CONFLICT(telegram_user_id) DO UPDATE
            SET session_ciphertext=EXCLUDED.session_ciphertext, updated_at=NOW()
            """,
            user_id, encrypt(session_string),
        )
    old = clients.get(user_id)
    if old:
        with suppress(Exception):
            await old.stop()
    clients[user_id] = client
    return me


async def run_forwarding(user_id: int, row):
    assert db
    source = int(row["source_chat_id"])
    destination = int(row["destination_chat_id"])
    current = int(row["next_message_id"])
    end = int(row["end_message_id"])
    success = int(row["success_count"])
    failed = int(row["failed_count"])
    caption = row["caption"] or ""
    disabled_types = set()
    async with db.acquire() as conn:
        settings = await conn.fetchrow(
            "SELECT caption, disabled_types FROM user_settings WHERE telegram_user_id=$1",
            user_id,
        )
    if settings:
        caption = settings["caption"] or caption
        disabled_types = set(settings["disabled_types"] or [])

    try:
        client = await get_user_client(user_id)
        if not client:
            await save_task(user_id, status="waiting_for_session")
            return

        await save_task(user_id, status="running")
        while current <= end and not shutdown_event.is_set():
            try:
                message = await client.get_messages(source, current)
                if not message or getattr(message, "empty", False) or getattr(message, "service", False):
                    current += 1
                    await save_task(user_id, next_message_id=current)
                    continue

                media_type = None
                if message.text:
                    media_type = "text"
                elif message.photo:
                    media_type = "photo"
                elif message.video:
                    media_type = "video"
                elif message.animation:
                    media_type = "gif"
                elif message.audio:
                    media_type = "audio"
                elif message.voice:
                    media_type = "voice"
                elif message.document:
                    media_type = "file"
                if media_type in disabled_types:
                    current += 1
                    await save_task(user_id, next_message_id=current)
                    continue

                while True:
                    try:
                        await message.copy(destination, caption=caption or None)
                        success += 1
                        break
                    except FloodWait as exc:
                        wait_seconds = int(getattr(exc, "value", 30)) + 1
                        log.warning("FloodWait user=%s wait=%s", user_id, wait_seconds)
                        await asyncio.sleep(wait_seconds)
                    except RPCError:
                        failed += 1
                        log.exception("Telegram error user=%s message=%s", user_id, current)
                        break

                current += 1
                await save_task(
                    user_id,
                    next_message_id=current,
                    success_count=success,
                    failed_count=failed,
                )
                await asyncio.sleep(1.05)
            except FloodWait as exc:
                await asyncio.sleep(int(getattr(exc, "value", 30)) + 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                failed += 1
                log.exception("Forwarding failure user=%s message=%s", user_id, current)
                current += 1
                await save_task(
                    user_id, next_message_id=current, failed_count=failed
                )

        if shutdown_event.is_set():
            await save_task(user_id, status="queued")
        else:
            await save_task(user_id, status="completed")
            log.info("Forwarding completed for %s", user_id)
    except asyncio.CancelledError:
        await save_task(user_id, status="cancelled")
        raise
    except Exception:
        log.exception("Forwarding task crashed for %s", user_id)
        await save_task(user_id, status="error")


async def launch_saved_task(user_id: int):
    if user_id in tasks and not tasks[user_id].done():
        return False
    assert db
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM forwarding_tasks WHERE telegram_user_id=$1",
            user_id,
        )
    if not row:
        return False
    if row["status"] in ("completed", "cancelled"):
        return False
    tasks[user_id] = asyncio.create_task(run_forwarding(user_id, row))
    return True


@dp.message(CommandStart())
async def start_handler(message: Message):
    await message.answer(
        "Welcome to Multi-User Forwarder.\\n\\n"
        "Each Telegram user connects their own user session.\\n"
        "/connect - connect your Telegram user session\\n"
        "/disconnect - remove your saved session\\n"
        "/forward SOURCE DEST START_ID END_ID - start forwarding\\n"
        "/status - view your forwarding status\\n"
        "/cancel - cancel your forwarding task\\n"
        "/caption TEXT - set forwarding caption\\n"
        "/dcaption - clear caption\\n"
        "/filter TYPE - toggle media type\\n"
        "/filters - show media filters\\n"
        "/help - show commands"
    )


@dp.message(Command("help"))
async def help_handler(message: Message):
    await start_handler(message)


@dp.message(Command("connect"))
async def connect_handler(message: Message):
    if not message.from_user:
        return
    pending_connect.add(message.from_user.id)
    await message.answer(
        "Send your Pyrogram String Session in your next message.\\n"
        "The session will be encrypted before it is stored in PostgreSQL. "
        "Do not share it with anyone else."
    )


@dp.message(Command("disconnect"))
async def disconnect_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    task = tasks.get(uid)
    if task and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    client = clients.pop(uid, None)
    if client:
        with suppress(Exception):
            await client.stop()
    assert db
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM user_sessions WHERE telegram_user_id=$1", uid)
    await message.answer("Your saved session and forwarding task data have been deleted.")



@dp.message(Command("caption"))
async def caption_handler(message: Message):
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        assert db
        async with db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT caption FROM user_settings WHERE telegram_user_id=$1",
                message.from_user.id,
            )
        return await message.answer(f"Current caption: {row['caption'] if row else '(empty)'}")
    assert db
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_settings(telegram_user_id, caption)
            VALUES($1,$2)
            ON CONFLICT(telegram_user_id) DO UPDATE SET caption=EXCLUDED.caption
            """,
            message.from_user.id, parts[1],
        )
    await message.answer("Caption saved for your forwarding tasks.")


@dp.message(Command("dcaption"))
async def delete_caption_handler(message: Message):
    if not message.from_user:
        return
    assert db
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_settings(telegram_user_id, caption)
            VALUES($1,'')
            ON CONFLICT(telegram_user_id) DO UPDATE SET caption=''
            """,
            message.from_user.id,
        )
    await message.answer("Your forwarding caption was cleared.")


@dp.message(Command("filter"))
async def filter_handler(message: Message):
    if not message.from_user:
        return
    parts = (message.text or "").split()
    valid = {"text", "photo", "video", "gif", "audio", "voice", "file"}
    if len(parts) != 2 or parts[1].lower() not in valid:
        return await message.answer("Usage: /filter text|photo|video|gif|audio|voice|file")
    kind = parts[1].lower()
    assert db
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT disabled_types FROM user_settings WHERE telegram_user_id=$1",
            message.from_user.id,
        )
        disabled = set(row["disabled_types"] or []) if row else set()
        if kind in disabled:
            disabled.remove(kind)
            state = "enabled"
        else:
            disabled.add(kind)
            state = "disabled"
        await conn.execute(
            """
            INSERT INTO user_settings(telegram_user_id, disabled_types)
            VALUES($1,$2::jsonb)
            ON CONFLICT(telegram_user_id) DO UPDATE SET disabled_types=EXCLUDED.disabled_types
            """,
            message.from_user.id, __import__("json").dumps(sorted(disabled)),
        )
    await message.answer(f"{kind} forwarding {state}.")


@dp.message(Command("filters"))
async def filters_handler(message: Message):
    if not message.from_user:
        return
    assert db
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT disabled_types FROM user_settings WHERE telegram_user_id=$1",
            message.from_user.id,
        )
    disabled = set(row["disabled_types"] or []) if row else set()
    types = ["text", "photo", "video", "gif", "audio", "voice", "file"]
    await message.answer("\\n".join(
        f"{kind}: {'OFF' if kind in disabled else 'ON'}" for kind in types
    ))


@dp.message(Command("forward"))
async def forward_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    parts = (message.text or "").split()
    if len(parts) != 5:
        return await message.answer(
            "Usage:\\n/forward SOURCE_CHAT_ID DESTINATION_CHAT_ID START_ID END_ID\\n"
            "Example: /forward -1001234567890 -1009876543210 1 500"
        )
    try:
        source, destination, start_id, end_id = map(int, parts[1:])
        if start_id < 1 or end_id < start_id:
            raise ValueError
    except ValueError:
        return await message.answer("Invalid chat/message IDs or range.")

    if uid not in clients:
        try:
            client = await get_user_client(uid)
            if not client:
                return await message.answer("Connect your user session first with /connect.")
        except Exception as exc:
            log.exception("Client reconnect failed for %s", uid)
            return await message.answer(f"Could not connect your session: {type(exc).__name__}")

    async with get_task_lock(uid):
        running = tasks.get(uid)
        if running and not running.done():
            return await message.answer("You already have a forwarding task running. Use /status or /cancel.")
        assert db
        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO forwarding_tasks(
                    telegram_user_id, source_chat_id, destination_chat_id,
                    start_message_id, end_message_id, next_message_id,
                    success_count, failed_count, status, caption
                ) VALUES($1,$2,$3,$4,$5,$4,0,0,'queued','')
                ON CONFLICT(telegram_user_id) DO UPDATE SET
                    source_chat_id=EXCLUDED.source_chat_id,
                    destination_chat_id=EXCLUDED.destination_chat_id,
                    start_message_id=EXCLUDED.start_message_id,
                    end_message_id=EXCLUDED.end_message_id,
                    next_message_id=EXCLUDED.next_message_id,
                    success_count=0, failed_count=0, status='queued', caption=''
                """,
                uid, source, destination, start_id, end_id,
            )
        await launch_saved_task(uid)
    await message.answer("Forwarding task started. Use /status to check progress.")


@dp.message(Command("status"))
async def status_handler(message: Message):
    if not message.from_user:
        return
    assert db
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM forwarding_tasks WHERE telegram_user_id=$1",
            message.from_user.id,
        )
    if not row:
        return await message.answer("No forwarding task found.")
    await message.answer(
        f"Status: {row['status']}\\n"
        f"Next message ID: {row['next_message_id']} / {row['end_message_id']}\\n"
        f"Success: {row['success_count']}\\nFailed: {row['failed_count']}"
    )


@dp.message(Command("cancel"))
async def cancel_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    task = tasks.get(uid)
    if not task or task.done():
        return await message.answer("No active forwarding task.")
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    await message.answer("Your forwarding task has been cancelled.")


@dp.message(F.text)
async def session_message_handler(message: Message):
    if not message.from_user or message.from_user.id not in pending_connect:
        return
    uid = message.from_user.id
    session_string = (message.text or "").strip()
    pending_connect.discard(uid)
    # Best-effort removal of the message containing the sensitive session string.
    with suppress(Exception):
        await message.delete()
    if not session_string or len(session_string) < 40:
        return await bot.send_message(uid, "That does not look like a valid Pyrogram session string. Try /connect again.")
    try:
        me = await connect_user_session(uid, session_string)
        await bot.send_message(
            uid,
            f"Session connected successfully.\\nAccount: {me.first_name or 'Telegram user'}"
        )
        # Resume a task left queued by a previous restart.
        await launch_saved_task(uid)
    except Exception as exc:
        log.exception("Session connect failed for user %s", uid)
        await bot.send_message(uid, f"Session connection failed: {type(exc).__name__}. Check the string and try /connect again.")


async def health(request):
    return web.json_response({"status": "ok", "service": "multi-user-telegram-forwarder"})


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    return runner


async def restore_clients_and_tasks():
    assert db
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_user_id FROM user_sessions")
    for row in rows:
        uid = int(row["telegram_user_id"])
        try:
            await get_user_client(uid)
        except Exception:
            log.exception("Could not restore user client %s", uid)
    async with db.acquire() as conn:
        pending = await conn.fetch(
            "SELECT telegram_user_id FROM forwarding_tasks WHERE status IN ('queued','running','waiting_for_session')"
        )
    for row in pending:
        uid = int(row["telegram_user_id"])
        try:
            await launch_saved_task(uid)
        except Exception:
            log.exception("Could not restore forwarding task for %s", uid)


async def main():
    await init_db()
    health_runner = await start_health_server()
    await restore_clients_and_tasks()
    polling = asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    log.info("Bot started; listening on port %s", PORT)
    try:
        await polling
    finally:
        shutdown_event.set()
        polling.cancel()
        with suppress(asyncio.CancelledError):
            await polling
        active = [t for t in tasks.values() if not t.done()]
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        for client in list(clients.values()):
            with suppress(Exception):
                await client.stop()
        with suppress(Exception):
            await bot.session.close()
        await health_runner.cleanup()
        if db:
            await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
