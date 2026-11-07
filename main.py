import asyncio
import logging
import os
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("multi-user-forwarder")

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
DATABASE_URL = os.environ["DATABASE_URL"]
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_forwarder")
PORT = int(os.getenv("PORT", "8000"))

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

mongo_client: Optional[AsyncMongoClient] = None
db = None
users_col = None
settings_col = None
tasks_col = None

clients: dict[int, Client] = {}
client_locks: dict[int, asyncio.Lock] = {}
tasks: dict[int, asyncio.Task] = {}
task_locks: dict[int, asyncio.Lock] = {}
pending_connect: set[int] = set()
shutdown_event = asyncio.Event()


def now_utc():
    return datetime.now(timezone.utc)


def get_client_lock(user_id: int) -> asyncio.Lock:
    return client_locks.setdefault(user_id, asyncio.Lock())


def get_task_lock(user_id: int) -> asyncio.Lock:
    return task_locks.setdefault(user_id, asyncio.Lock())


async def init_db():
    global mongo_client, db, users_col, settings_col, tasks_col
    mongo_client = AsyncMongoClient(
        DATABASE_URL,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
        appname="multi-user-telegram-forwarder",
    )
    await mongo_client.admin.command("ping")
    db = mongo_client[MONGO_DB_NAME]
    users_col = db["user_sessions"]
    settings_col = db["user_settings"]
    tasks_col = db["forwarding_tasks"]

    await tasks_col.create_index("status")
    log.info("Connected to MongoDB database '%s'", MONGO_DB_NAME)


async def save_task(user_id: int, **values):
    if tasks_col is None:
        return
    allowed = {
        "next_message_id", "success_count", "failed_count", "status",
        "caption", "updated_at"
    }
    update = {k: v for k, v in values.items() if k in allowed}
    if not update:
        return
    update["updated_at"] = now_utc()
    await tasks_col.update_one({"_id": user_id}, {"$set": update})


async def get_user_client(user_id: int) -> Optional[Client]:
    if user_id in clients:
        return clients[user_id]
    if users_col is None:
        return None

    async with get_client_lock(user_id):
        if user_id in clients:
            return clients[user_id]

        row = await users_col.find_one({"_id": user_id})
        if not row:
            return None

        session_string = row.get("session_string")
        if not session_string:
            return None

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

    assert users_col is not None
    await users_col.update_one(
        {"_id": user_id},
        {
            "$set": {
                "session_string": session_string,
                "updated_at": now_utc(),
            },
            "$setOnInsert": {"created_at": now_utc()},
        },
        upsert=True,
    )

    old = clients.get(user_id)
    if old:
        with suppress(Exception):
            await old.stop()
    clients[user_id] = client
    return me


async def run_forwarding(user_id: int, row: dict):
    source = int(row["source_chat_id"])
    destination = int(row["destination_chat_id"])
    current = int(row["next_message_id"])
    end = int(row["end_message_id"])
    success = int(row.get("success_count", 0))
    failed = int(row.get("failed_count", 0))
    caption = row.get("caption", "") or ""

    settings = await settings_col.find_one({"_id": user_id}) if settings_col is not None else None
    disabled_types = set(settings.get("disabled_types", [])) if settings else set()
    if settings and settings.get("caption"):
        caption = settings["caption"]

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
    running = tasks.get(user_id)
    if running and not running.done():
        return False

    row = await tasks_col.find_one({"_id": user_id}) if tasks_col is not None else None
    if not row or row.get("status") in ("completed", "cancelled"):
        return False

    tasks[user_id] = asyncio.create_task(run_forwarding(user_id, row))
    return True


@dp.message(CommandStart())
async def start_handler(message: Message):
    await message.answer(
        "Welcome to Multi-User Forwarder.\n\n"
        "Each Telegram user connects their own user session.\n"
        "/connect - connect your Telegram user session\n"
        "/disconnect - remove your saved session\n"
        "/forward SOURCE DEST START_ID END_ID - start forwarding\n"
        "/status - view your forwarding status\n"
        "/cancel - cancel your forwarding task\n"
        "/caption TEXT - set forwarding caption\n"
        "/dcaption - clear caption\n"
        "/filter TYPE - toggle media type\n"
        "/filters - show media filters\n"
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
        "Send your Pyrogram String Session in your next message.\n"
        "The session will be stored in MongoDB without application-level encryption. "
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

    await users_col.delete_one({"_id": uid})
    await settings_col.delete_one({"_id": uid})
    await tasks_col.delete_one({"_id": uid})
    await message.answer("Your saved session and forwarding task data have been deleted.")


@dp.message(Command("caption"))
async def caption_handler(message: Message):
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        row = await settings_col.find_one({"_id": message.from_user.id})
        return await message.answer(
            f"Current caption: {row.get('caption', '') if row else '(empty)'}"
        )

    await settings_col.update_one(
        {"_id": message.from_user.id},
        {
            "$set": {"caption": parts[1], "updated_at": now_utc()},
            "$setOnInsert": {"disabled_types": []},
        },
        upsert=True,
    )
    await message.answer("Caption saved for your forwarding tasks.")


@dp.message(Command("dcaption"))
async def delete_caption_handler(message: Message):
    if not message.from_user:
        return
    await settings_col.update_one(
        {"_id": message.from_user.id},
        {
            "$set": {"caption": "", "updated_at": now_utc()},
            "$setOnInsert": {"disabled_types": []},
        },
        upsert=True,
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

    uid = message.from_user.id
    kind = parts[1].lower()
    row = await settings_col.find_one({"_id": uid})
    disabled = set(row.get("disabled_types", [])) if row else set()
    if kind in disabled:
        disabled.remove(kind)
        state = "enabled"
    else:
        disabled.add(kind)
        state = "disabled"

    await settings_col.update_one(
        {"_id": uid},
        {
            "$set": {
                "disabled_types": sorted(disabled),
                "updated_at": now_utc(),
            },
            "$setOnInsert": {"caption": ""},
        },
        upsert=True,
    )
    await message.answer(f"{kind} forwarding {state}.")


@dp.message(Command("filters"))
async def filters_handler(message: Message):
    if not message.from_user:
        return
    row = await settings_col.find_one({"_id": message.from_user.id})
    disabled = set(row.get("disabled_types", [])) if row else set()
    types = ["text", "photo", "video", "gif", "audio", "voice", "file"]
    await message.answer("\n".join(
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
            "Usage:\n/forward SOURCE_CHAT_ID DESTINATION_CHAT_ID START_ID END_ID\n"
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
            return await message.answer(
                "You already have a forwarding task running. Use /status or /cancel."
            )

        await tasks_col.update_one(
            {"_id": uid},
            {
                "$set": {
                    "source_chat_id": source,
                    "destination_chat_id": destination,
                    "start_message_id": start_id,
                    "end_message_id": end_id,
                    "next_message_id": start_id,
                    "success_count": 0,
                    "failed_count": 0,
                    "status": "queued",
                    "caption": "",
                    "updated_at": now_utc(),
                },
                "$setOnInsert": {"created_at": now_utc()},
            },
            upsert=True,
        )
        await launch_saved_task(uid)

    await message.answer("Forwarding task started. Use /status to check progress.")


@dp.message(Command("status"))
async def status_handler(message: Message):
    if not message.from_user:
        return
    row = await tasks_col.find_one({"_id": message.from_user.id})
    if not row:
        return await message.answer("No forwarding task found.")

    await message.answer(
        f"Status: {row.get('status', 'unknown')}\n"
        f"Next message ID: {row.get('next_message_id', 0)} / {row.get('end_message_id', 0)}\n"
        f"Success: {row.get('success_count', 0)}\n"
        f"Failed: {row.get('failed_count', 0)}"
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
        return await bot.send_message(
            uid,
            "That does not look like a valid Pyrogram session string. Try /connect again."
        )

    try:
        me = await connect_user_session(uid, session_string)
        await bot.send_message(
            uid,
            f"Session connected successfully.\nAccount: {me.first_name or 'Telegram user'}"
        )
        await launch_saved_task(uid)
    except Exception as exc:
        log.exception("Session connect failed for user %s", uid)
        await bot.send_message(
            uid,
            f"Session connection failed: {type(exc).__name__}. Check the string and try /connect again."
        )


async def health(request):
    return web.json_response({
        "status": "ok",
        "service": "multi-user-telegram-forwarder",
        "database": "mongodb",
    })


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
    async for row in users_col.find({}, {"_id": 1}):
        uid = int(row["_id"])
        try:
            await get_user_client(uid)
        except Exception:
            log.exception("Could not restore user client %s", uid)

    async for row in tasks_col.find(
        {"status": {"$in": ["queued", "running", "waiting_for_session"]}},
        {"_id": 1},
    ):
        uid = int(row["_id"])
        try:
            await launch_saved_task(uid)
        except Exception:
            log.exception("Could not restore forwarding task for %s", uid)


async def main():
    await init_db()
    health_runner = await start_health_server()
    await restore_clients_and_tasks()

    polling = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    )
    log.info("Bot started; listening on port %s", PORT)

    try:
        await polling
    finally:
        shutdown_event.set()
        polling.cancel()
        with suppress(asyncio.CancelledError):
            await polling

        active = [task for task in tasks.values() if not task.done()]
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

        if mongo_client is not None:
            await mongo_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
