import asyncio
import gc
import logging
import os
import sys
from pathlib import Path
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional

import psutil

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand, Message
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
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
# Maximum number of forwarding jobs allowed to run at the same time.
# Keep this at 2 for a 512 MB Koyeb instance.
MAX_CONCURRENT_FORWARDING = max(1, int(os.getenv("MAX_CONCURRENT_FORWARDING", "2")))
MIN_FREE_RAM_MB = max(40, int(os.getenv("MIN_FREE_RAM_MB", "75")))
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

mongo_client: Optional[AsyncMongoClient] = None
db = None
users_col = None
settings_col = None
tasks_col = None
history_col = None
blocked_col = None

clients: dict[int, Client] = {}
client_locks: dict[int, asyncio.Lock] = {}
tasks: dict[int, asyncio.Task] = {}
task_locks: dict[int, asyncio.Lock] = {}
speed_delays: dict[int, float] = {}  # Cached per-user delay; avoids a DB read per message.
caption_cache: dict[int, str] = {}  # Live per-user caption; changes apply to active tasks.
PROGRESS_SAVE_EVERY = 10  # Batch progress writes to reduce MongoDB overhead.
live_progress: dict[int, dict] = {}
status_monitors: dict[int, asyncio.Task] = {}
pause_events: dict[int, asyncio.Event] = {}
userbot_handler_clients: dict[int, Client] = {}
pending_connect: set[int] = set()
pending_session_generation: dict[int, dict] = {}  # Per-user OTP/password login flow
shutdown_event = asyncio.Event()
restart_requested = False
forwarding_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FORWARDING)
APP_STARTED_MONOTONIC = __import__('time').monotonic()


async def setup_bot_commands():
    """Publish the complete command list in Telegram's menu button."""
    commands = [
        BotCommand(command="start", description="Open the main menu"),
        BotCommand(command="help", description="Show all commands and help"),
        BotCommand(command="connect", description="Connect your Telegram session"),
        BotCommand(command="generate_session", description="Generate your own Telegram session"),
        BotCommand(command="cancel_session", description="Cancel session generation"),
        BotCommand(command="disconnect", description="Remove your saved session"),
        BotCommand(command="forward", description="Start a forwarding task"),
        BotCommand(command="status", description="Check forwarding progress"),
        BotCommand(command="cancel", description="Stop the active forwarding task"),
        BotCommand(command="pause", description="Pause your forwarding task"),
        BotCommand(command="resume", description="Resume your forwarding task"),
        BotCommand(command="retry", description="Retry failed messages"),
        BotCommand(command="history", description="View recent task history"),
        BotCommand(command="admin", description="Admin controls"),
        BotCommand(command="restart", description="Restart the bot (admin only)"),
        BotCommand(command="speed", description="Set or view forwarding speed"),
        BotCommand(command="caption", description="Set or view forwarding caption"),
        BotCommand(command="dcaption", description="Clear forwarding caption"),
        BotCommand(command="filter", description="Enable or disable a media type"),
        BotCommand(command="filters", description="View media filter settings"),
    ]
    await bot.set_my_commands(commands)
    log.info("Telegram command menu configured with %d commands", len(commands))


def now_utc():
    return datetime.now(timezone.utc)


def get_client_lock(user_id: int) -> asyncio.Lock:
    return client_locks.setdefault(user_id, asyncio.Lock())


def get_task_lock(user_id: int) -> asyncio.Lock:
    return task_locks.setdefault(user_id, asyncio.Lock())


async def init_db():
    global mongo_client, db, users_col, settings_col, tasks_col, history_col, blocked_col
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
    history_col = db["task_history"]
    blocked_col = db["blocked_users"]

    await tasks_col.create_index("status")
    await history_col.create_index([("user_id", 1), ("archived_at", -1)])
    log.info("Connected to MongoDB database '%s'", MONGO_DB_NAME)
    log.info("Maximum simultaneous forwarding tasks: %s", MAX_CONCURRENT_FORWARDING)


async def save_task(user_id: int, **values):
    if tasks_col is None:
        return
    allowed = {
        "next_message_id", "success_count", "failed_count", "skipped_count", "status",
        "caption", "updated_at", "paused", "failed_message_ids", "sent_message_ids", "task_id", "retry_only"
    }
    update = {k: v for k, v in values.items() if k in allowed}
    if not update:
        return
    update["updated_at"] = now_utc()
    await tasks_col.update_one({"_id": user_id}, {"$set": update})



def container_memory_percent() -> float:
    """Return cgroup/container RAM usage percent, falling back to psutil."""
    try:
        current = int(Path("/sys/fs/cgroup/memory.current").read_text().strip())
        limit = int(Path("/sys/fs/cgroup/memory.max").read_text().strip())
        if limit > 0 and limit < (1 << 60):
            return current / limit * 100
    except Exception:
        pass
    try:
        # cgroup v1 compatibility
        current = int(Path("/sys/fs/cgroup/memory/memory.usage_in_bytes").read_text().strip())
        limit = int(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").read_text().strip())
        if limit > 0 and limit < (1 << 60):
            return current / limit * 100
    except Exception:
        pass
    try:
        return float(psutil.virtual_memory().percent)
    except Exception:
        return 0.0


def container_memory_available_mb() -> float:
    """Best-effort available memory inside the container."""
    try:
        current = int(Path("/sys/fs/cgroup/memory.current").read_text().strip())
        limit = int(Path("/sys/fs/cgroup/memory.max").read_text().strip())
        if limit > 0 and limit < (1 << 60):
            return max(0, limit - current) / (1024 * 1024)
    except Exception:
        pass
    try:
        current = int(Path("/sys/fs/cgroup/memory/memory.usage_in_bytes").read_text().strip())
        limit = int(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").read_text().strip())
        if limit > 0 and limit < (1 << 60):
            return max(0, limit - current) / (1024 * 1024)
    except Exception:
        pass
    return psutil.virtual_memory().available / (1024 * 1024)


def container_memory_usage_mb() -> tuple[float, float]:
    """Return container memory used/limit in MB, preferring cgroup metrics."""
    try:
        current = int(Path("/sys/fs/cgroup/memory.current").read_text().strip())
        limit = int(Path("/sys/fs/cgroup/memory.max").read_text().strip())
        if 0 < limit < (1 << 60):
            return current / (1024 * 1024), limit / (1024 * 1024)
    except Exception:
        pass
    try:
        current = int(Path("/sys/fs/cgroup/memory/memory.usage_in_bytes").read_text().strip())
        limit = int(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").read_text().strip())
        if 0 < limit < (1 << 60):
            return current / (1024 * 1024), limit / (1024 * 1024)
    except Exception:
        pass
    vm = psutil.virtual_memory()
    return (vm.total - vm.available) / (1024 * 1024), vm.total / (1024 * 1024)


async def is_blocked(user_id: int) -> bool:
    if blocked_col is None:
        return False
    return await blocked_col.find_one({"_id": user_id}) is not None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def register_userbot_commands(client: Client, user_id: int):
    """Enable the legacy .id and .f commands in each connected user's Telegram account."""
    if userbot_handler_clients.get(user_id) is client:
        return

    async def id_handler(_, message):
        try:
            if message.reply_to_message:
                response = (
                    f"Chat ID: `{message.chat.id}`\n"
                    f"Msg ID: `{message.reply_to_message.id}`"
                )
            else:
                response = f"Chat ID: `{message.chat.id}`"
            await message.reply_text(response)
        except Exception:
            log.exception(".id command failed for user %s", user_id)

    async def f_handler(_, message):
        parts = (message.text or "").split()
        if len(parts) != 4:
            return await message.reply_text(
                "Usage: `.f SOURCE_CHAT_ID START_ID END_ID`\n"
                "Example: `.f -1001234567890 1 500`\n\n"
                "Send this command in the destination chat."
            )

        try:
            source_chat_id, start_id, end_id = map(int, parts[1:])
            if start_id < 1 or end_id < start_id:
                raise ValueError
        except ValueError:
            return await message.reply_text("Invalid IDs. Check the source chat ID and message range.")

        destination_chat_id = int(message.chat.id)
        async with get_task_lock(user_id):
            running = tasks.get(user_id)
            if running and not running.done():
                return await message.reply_text("A forwarding task is already running. Use /status or /cancel in the main bot.")

            previous = await tasks_col.find_one({"_id": user_id})
            if previous and previous.get("status") in {"completed", "cancelled", "error"}:
                previous.pop("_id", None)
                previous["user_id"] = user_id
                previous["archived_at"] = now_utc()
                await history_col.insert_one(previous)
            await tasks_col.update_one(
                {"_id": user_id},
                {
                    "$set": {
                        "task_id": f"{user_id}-{int(now_utc().timestamp())}",
                        "sent_message_ids": [], "failed_message_ids": [], "retry_only": False, "paused": False,
                        "source_chat_id": source_chat_id,
                        "destination_chat_id": destination_chat_id,
                        "start_message_id": start_id,
                        "end_message_id": end_id,
                        "next_message_id": start_id,
                        "success_count": 0,
                        "failed_count": 0,
                        "skipped_count": 0,
                        "status": "queued",
                        "caption": "",
                        "updated_at": now_utc(),
                    },
                    "$setOnInsert": {"created_at": now_utc()},
                },
                upsert=True,
            )
            await launch_saved_task(user_id)
        await message.reply_text(
            f"Forwarding started.\nSource: `{source_chat_id}`\n"
            f"Destination: `{destination_chat_id}`\n"
            f"Message IDs: `{start_id}` to `{end_id}`\n"
            "Check progress with /status in the main bot."
        )

    client.add_handler(MessageHandler(id_handler, filters.me & filters.command("id", prefixes=".")))
    client.add_handler(MessageHandler(f_handler, filters.me & filters.regex(r"^\.f(?:\s|$)")))
    userbot_handler_clients[user_id] = client


async def get_user_client(user_id: int) -> Optional[Client]:
    existing = clients.get(user_id)
    if existing is not None and existing.is_connected:
        return existing
    if existing is not None:
        with suppress(Exception):
            await existing.stop()
        clients.pop(user_id, None)
        userbot_handler_clients.pop(user_id, None)
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
            no_updates=False,
        )
        await client.start()
        register_userbot_commands(client, user_id)
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
        no_updates=False,
    )
    await client.start()
    register_userbot_commands(client, user_id)
    try:
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
    except Exception:
        with suppress(Exception):
            await client.stop()
        raise

    old = clients.get(user_id)
    if old:
        with suppress(Exception):
            await old.stop()
    clients[user_id] = client
    return me


async def warm_chat_peers(client: Client, chat_ids: set[int]) -> set[int]:
    """Load chat access hashes into this user's Pyrogram session cache."""
    found: set[int] = set()
    async for dialog in client.get_dialogs():
        chat = getattr(dialog, "chat", None)
        if chat is None:
            continue
        chat_id = int(chat.id)
        if chat_id in chat_ids:
            found.add(chat_id)
            if found == chat_ids:
                break
    return found


async def run_forwarding(user_id: int, row: dict):
    source = int(row["source_chat_id"])
    destination = int(row["destination_chat_id"])
    current = int(row["next_message_id"])
    end = int(row["end_message_id"])
    success = int(row.get("success_count", 0))
    failed = int(row.get("failed_count", 0))
    skipped = int(row.get("skipped_count", 0))
    failed_ids = set(int(x) for x in row.get("failed_message_ids", []))
    sent_ids = set(int(x) for x in row.get("sent_message_ids", []))
    retry_only = bool(row.get("retry_only", False))
    if retry_only and failed_ids:
        current = min(failed_ids)

    pause_event = pause_events.setdefault(user_id, asyncio.Event())
    if row.get("paused"):
        pause_event.clear()
    else:
        pause_event.set()

    settings = await settings_col.find_one(
        {"_id": user_id},
        {"disabled_types": 1, "caption": 1, "speed_delay": 1},
    ) if settings_col is not None else None

    disabled_types = set(settings.get("disabled_types", [])) if settings else set()
    caption = (settings.get("caption") if settings else None) or row.get("caption", "") or ""
    caption_cache[user_id] = caption
    speed_delays[user_id] = max(
        0.5, min(float(settings.get("speed_delay", 1.05)) if settings else 1.05, 30.0)
    )

    processed_since_checkpoint = 0

    async def checkpoint(force: bool = False):
        nonlocal processed_since_checkpoint
        live_progress[user_id] = {
            "next_message_id": current,
            "success_count": success,
            "failed_count": failed,
            "skipped_count": skipped,
            "status": "running",
        }
        if force or processed_since_checkpoint >= PROGRESS_SAVE_EVERY:
            await save_task(
                user_id,
                next_message_id=current,
                success_count=success,
                failed_count=failed,
                skipped_count=skipped,
            )
            processed_since_checkpoint = 0

    try:
        client = await get_user_client(user_id)
        if not client:
            await save_task(user_id, status="waiting_for_session")
            return

        # Load access hashes once before processing the range.
        required_chats = {source, destination}
        cached_chats = await warm_chat_peers(client, required_chats)
        missing_chats = required_chats - cached_chats
        if missing_chats:
            log.error(
                "User %s cannot access chat(s) %s; account must join both chats.",
                user_id, sorted(missing_chats),
            )
            await save_task(user_id, status="chat_not_found")
            return

        await save_task(user_id, status="running")

        while current <= end and not shutdown_event.is_set():
            if not pause_event.is_set():
                await save_task(user_id, status="paused", next_message_id=current)
                live_progress[user_id] = {"next_message_id": current, "success_count": success, "failed_count": failed, "skipped_count": skipped, "status": "paused"}
                await pause_event.wait()
                if shutdown_event.is_set():
                    break
                await save_task(user_id, status="running")

            # Protect a 512 MB instance: hold the queue slot but wait if the
            # container is close to its memory limit. cgroup values are preferred.
            while (container_memory_percent() >= 88 or container_memory_available_mb() < MIN_FREE_RAM_MB) and not shutdown_event.is_set():
                await save_task(user_id, status="waiting_for_memory", next_message_id=current)
                await asyncio.sleep(8)
            try:
                message = await client.get_messages(source, current)

                if not message or getattr(message, "empty", False) or getattr(message, "service", False):
                    skipped += 1
                    current += 1
                    processed_since_checkpoint += 1
                    await checkpoint()
                    continue

                media_type = (
                    "text" if message.text else
                    "photo" if message.photo else
                    "video" if message.video else
                    "gif" if message.animation else
                    "audio" if message.audio else
                    "voice" if message.voice else
                    "file" if message.document else
                    None
                )

                if media_type in disabled_types:
                    skipped += 1
                    current += 1
                    processed_since_checkpoint += 1
                    await checkpoint()
                    continue

                if current in sent_ids:
                    skipped += 1
                    current += 1
                    processed_since_checkpoint += 1
                    await checkpoint()
                    continue

                while True:
                    try:
                        await message.copy(destination, caption=(caption_cache.get(user_id, caption) or None))
                        success += 1
                        sent_ids.add(current)
                        failed_ids.discard(current)
                        await tasks_col.update_one({"_id": user_id}, {"$addToSet": {"sent_message_ids": current}, "$pull": {"failed_message_ids": current}})
                        break
                    except FloodWait as exc:
                        wait_seconds = int(getattr(exc, "value", 30)) + 1
                        log.warning("FloodWait user=%s wait=%s", user_id, wait_seconds)
                        await asyncio.sleep(wait_seconds)
                    except RPCError:
                        failed += 1
                        failed_ids.add(current)
                        await tasks_col.update_one({"_id": user_id}, {"$addToSet": {"failed_message_ids": current}})
                        log.exception("Telegram copy failed user=%s message=%s", user_id, current)
                        break

                current += 1
                processed_since_checkpoint += 1
                await checkpoint()

                # Speed changes are reflected immediately by /speed without a DB
                # query on every message. Telegram FloodWait is always respected.
                await asyncio.sleep(speed_delays.get(user_id, 1.05))

            except FloodWait as exc:
                wait_seconds = int(getattr(exc, "value", 30)) + 1
                log.warning("FloodWait while reading user=%s wait=%s", user_id, wait_seconds)
                await asyncio.sleep(wait_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                # If the MTProto connection dropped, reconnect once and retry
                # this message without marking it failed.
                active_client = clients.get(user_id)
                if active_client is None or not active_client.is_connected:
                    with suppress(Exception):
                        if active_client:
                            await active_client.stop()
                    clients.pop(user_id, None)
                    userbot_handler_clients.pop(user_id, None)
                    try:
                        client = await get_user_client(user_id)
                        if client:
                            await asyncio.sleep(2)
                            continue
                    except Exception:
                        log.exception("Automatic reconnect failed for user %s", user_id)
                failed += 1
                failed_ids.add(current)
                await tasks_col.update_one({"_id": user_id}, {"$addToSet": {"failed_message_ids": current}})
                log.exception("Forwarding failure user=%s message=%s", user_id, current)
                current += 1
                processed_since_checkpoint += 1
                await checkpoint()

        await checkpoint(force=True)
        if shutdown_event.is_set():
            live_progress[user_id] = {"next_message_id": current, "success_count": success, "failed_count": failed, "skipped_count": skipped, "status": "queued"}
            await save_task(user_id, status="queued")
        else:
            live_progress[user_id] = {"next_message_id": current, "success_count": success, "failed_count": failed, "skipped_count": skipped, "status": "completed"}
            await save_task(user_id, status="completed", retry_only=False)
            log.info(
                "Forwarding completed user=%s success=%s failed=%s",
                user_id, success, failed,
            )

    except asyncio.CancelledError:
        shutdown = shutdown_event.is_set()
        final_status = "queued" if shutdown else "cancelled"
        live_progress[user_id] = {"next_message_id": current, "success_count": success, "failed_count": failed, "skipped_count": skipped, "status": final_status}
        await save_task(
            user_id,
            next_message_id=current,
            success_count=success,
            failed_count=failed,
            skipped_count=skipped,
            status=final_status,
        )
        raise
    except Exception:
        log.exception("Forwarding task crashed for %s", user_id)
        live_progress[user_id] = {"next_message_id": current, "success_count": success, "failed_count": failed, "skipped_count": skipped, "status": "error"}
        await save_task(
            user_id,
            next_message_id=current,
            success_count=success,
            failed_count=failed,
            skipped_count=skipped,
            status="error",
        )
    finally:
        # Release per-task objects; keep logged-in client alive for .id/.f commands.
        with suppress(Exception):
            del message
        with suppress(Exception):
            del sent_ids
        with suppress(Exception):
            del failed_ids
        with suppress(Exception):
            del pause_event
        gc.collect()
        # Return free heap pages to the Linux container when glibc supports it.
        with suppress(Exception):
            import ctypes
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        log.info("Task memory cleanup completed for user %s", user_id)


async def run_queued_forwarding(user_id: int):
    """Wait for a global worker slot, then run this user's forwarding job."""
    try:
        # Keep queued tasks visible in the dashboard while they wait for a slot.
        await save_task(user_id, status="queued")
        live = live_progress.get(user_id, {})
        live_progress[user_id] = {
            **live,
            "status": "queued",
        }

        async with forwarding_semaphore:
            if shutdown_event.is_set():
                await save_task(user_id, status="queued")
                return

            row = await tasks_col.find_one({"_id": user_id}) if tasks_col is not None else None
            if not row or row.get("status") in ("completed", "cancelled"):
                return

            await run_forwarding(user_id, row)
    except asyncio.CancelledError:
        live = live_progress.get(user_id, {})
        final_status = "queued" if shutdown_event.is_set() else "cancelled"
        live_progress[user_id] = {**live, "status": final_status}
        await save_task(user_id, status=final_status)
        raise


async def launch_saved_task(user_id: int):
    running = tasks.get(user_id)
    if running and not running.done():
        return False

    row = await tasks_col.find_one({"_id": user_id}) if tasks_col is not None else None
    if not row or row.get("status") in ("completed", "cancelled"):
        return False

    tasks[user_id] = asyncio.create_task(run_queued_forwarding(user_id))
    return True


@dp.message(CommandStart())
async def start_handler(message: Message):
    if message.from_user and await is_blocked(message.from_user.id):
        return await message.answer("Your access to this bot has been disabled by the administrator.")
    menu_text = (
        "<b>✦  M U L T I  •  F O R W A R D E R</b>\n"
        "<i>Powerful. Simple. Made for Telegram.</i>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>◈ ACCOUNT</b>\n"
        "• <code>/connect</code> — Connect your Telegram session\n"
        "• <code>/generate_session</code> — Generate a session and save it to Saved Messages\n"
        "• <code>/disconnect</code> — Remove your saved session\n\n"
        "<b>◈ FORWARDING</b>\n"
        "• <code>/forward SOURCE DEST START END</code> — Start a task\n"
        "• <code>/status</code> — View task progress\n"
        "• <code>/cancel</code> — Stop your active task\n"
        "• <code>/pause</code> / <code>/resume</code> — Pause or resume\n"
        "• <code>/retry</code> — Retry failed messages\n"
        "• <code>/history</code> — Recent task history\n"
        "• Maximum active forwarding tasks are controlled by the server; extra tasks wait in queue.\n"
        "• <code>/speed [seconds]</code> — Set forwarding delay\n\n"
        "<b>◈ LEGACY USERBOT COMMANDS</b>\n"
        "• <code>.id</code> — Send inside the source bot chat to get its Chat ID\n"
        "• Reply to a post with <code>.id</code> to get that Message ID\n"
        "• <code>.f SOURCE_CHAT_ID START_ID END_ID</code> — Send in the destination chat\n\n"
        "<b>◈ CUSTOMIZATION</b>\n"
        "• <code>/caption TEXT</code> — Set a custom caption\n"
        "• <code>/dcaption</code> — Clear your caption\n"
        "• <code>/filter TYPE</code> — Toggle a media filter\n"
        "• <code>/filters</code> — View all media filters\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "<b>⚡ QUICK START</b>\n"
        "1. Connect your account with <code>/connect</code>\n"
        "2. Set your preferred delay with <code>/speed 1</code>\n"
        "3. Or use .id in the source chat, then .f in the destination chat\n\n"
        "<i>Need help? Send /help</i>"
    )
    await message.answer(menu_text, parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("help"))
async def help_handler(message: Message):
    await start_handler(message)


@dp.message(Command("connect"))
async def connect_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    if await is_blocked(uid):
        return await message.answer("Your access is blocked.")

    # After a Koyeb restart, let the user reconnect their already-saved session
    # without having to generate and paste a new string session.
    saved = await users_col.find_one({"_id": uid}, {"session_string": 1})
    if saved and saved.get("session_string"):
        try:
            client = await get_user_client(uid)
            if client:
                me = await client.get_me()
                return await message.answer(
                    f"Saved session reconnected successfully.\nAccount: {me.first_name or 'Telegram user'}"
                )
        except Exception as exc:
            log.exception("Saved session reconnect failed for %s", uid)
            await message.answer(f"Saved session could not reconnect: {type(exc).__name__}. Send a fresh session below.")

    pending_connect.add(uid)
    await message.answer(
        "Send your Pyrogram String Session in your next message.\n"
        "The session will be stored in MongoDB without application-level encryption. "
        "Do not share it with anyone else."
    )


@dp.message(Command("generate_session"))
async def generate_session_handler(message: Message):
    """Start a per-user Telegram login and export its Pyrogram string session."""
    if not message.from_user:
        return
    uid = message.from_user.id
    if message.chat.type != "private":
        return await message.answer("For security, use /generate_session in this bot's private chat.")
    if await is_blocked(uid):
        return await message.answer("Your access is blocked.")
    if uid in pending_session_generation:
        return await message.answer("A session-generation flow is already active. Send /cancel_session to stop it.")

    auth_client = Client(
        name=f"session_generator_{uid}", api_id=API_ID, api_hash=API_HASH,
        in_memory=True, no_updates=True,
    )
    try:
        await auth_client.connect()
    except Exception as exc:
        with suppress(Exception):
            await auth_client.disconnect()
        log.exception("Could not initialize session generator for %s", uid)
        return await message.answer(f"Could not start Telegram login: {type(exc).__name__}. Try again later.")

    pending_session_generation[uid] = {"client": auth_client, "stage": "phone"}
    await message.answer(
        "<b>Generate your personal Telegram session</b>\n\n"
        "Send your phone number with country code (example: <code>+919876543210</code>).\n"
        "You will receive Telegram's login code. Never share that code with anyone.\n\n"
        "Send /cancel_session to cancel.", parse_mode="HTML"
    )


@dp.message(Command("cancel_session"))
async def cancel_session_generation(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    flow = pending_session_generation.pop(uid, None)
    if flow:
        with suppress(Exception):
            await flow["client"].disconnect()
        return await message.answer("Session generation cancelled. No new session was saved.")
    await message.answer("There is no active session-generation request.")


async def handle_session_generation_input(message: Message, uid: int) -> bool:
    """Consume phone, OTP, and optional 2FA password messages for this user."""
    flow = pending_session_generation.get(uid)
    if not flow:
        return False

    # Delete OTP/password/phone messages where Telegram permits it.
    text_value = (message.text or "").strip()
    with suppress(Exception):
        await message.delete()

    auth_client = flow["client"]
    try:
        if flow["stage"] == "phone":
            phone = text_value.replace(" ", "")
            if not phone.startswith("+") or not phone[1:].isdigit() or not 8 <= len(phone[1:]) <= 15:
                await bot.send_message(uid, "Invalid phone number. Send it in international format, e.g. <code>+919876543210</code>.", parse_mode="HTML")
                return True
            sent_code = await auth_client.send_code(phone)
            flow.update({"stage": "code", "phone": phone, "phone_code_hash": sent_code.phone_code_hash})
            await bot.send_message(uid, "Telegram login code sent. Send the code here (not in any group).")
            return True

        if flow["stage"] == "code":
            code = "".join(ch for ch in text_value if ch.isdigit())
            if not 4 <= len(code) <= 8:
                await bot.send_message(uid, "That code format looks incorrect. Send the Telegram login code again.")
                return True
            try:
                await auth_client.sign_in(
                    phone_number=flow["phone"],
                    phone_code_hash=flow["phone_code_hash"],
                    phone_code=code,
                )
            except Exception as exc:
                if type(exc).__name__ == "SessionPasswordNeeded":
                    flow["stage"] = "password"
                    await bot.send_message(uid, "Two-Step Verification is enabled. Send your Telegram 2-Step Verification password.")
                    return True
                raise

        elif flow["stage"] == "password":
            await auth_client.check_password(text_value)

        session_string = await auth_client.export_session_string()
        me = await auth_client.get_me()
        # Deliver only to the authenticated account's own Saved Messages.
        await auth_client.send_message(
            "me",
            "<b>Your new Pyrogram String Session</b>\n\n"
            "<code>" + session_string + "</code>\n\n"
            "Keep this private. Anyone with this string may access your Telegram account.",
            parse_mode="HTML",
        )
        await auth_client.disconnect()
        pending_session_generation.pop(uid, None)

        # Attach this generated session to the user's existing multi-user bot profile.
        await connect_user_session(uid, session_string)
        await bot.send_message(
            uid,
            f"<b>Session generated successfully.</b>\nAccount: {me.first_name or 'Telegram user'}\n\n"
            "Your session was sent to your Telegram Saved Messages and connected to this bot.",
            parse_mode="HTML",
        )
        await launch_saved_task(uid)
        return True

    except Exception as exc:
        log.warning("Session generation failed for user %s: %s", uid, type(exc).__name__)
        pending_session_generation.pop(uid, None)
        with suppress(Exception):
            await auth_client.disconnect()
        await bot.send_message(
            uid,
            f"Session generation failed: <code>{type(exc).__name__}</code>.\n"
            "Start again with /generate_session. If Telegram reports an expired code, request a new one.",
            parse_mode="HTML",
        )
        return True


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

    pause_events.pop(uid, None)
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
    caption_cache[message.from_user.id] = parts[1]
    await message.answer("Caption updated. It will apply to the next messages in active forwarding tasks.")


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
    caption_cache[message.from_user.id] = ""
    await message.answer("Caption cleared. The change will apply to the next messages in active forwarding tasks.")


@dp.message(Command("speed", "forwardingspeed"))
async def speed_handler(message: Message):
    if not message.from_user:
        return

    uid = message.from_user.id
    parts = (message.text or "").split(maxsplit=1)

    if len(parts) == 1:
        row = await settings_col.find_one({"_id": uid}, {"speed_delay": 1})
        delay = float(row.get("speed_delay", 1.05)) if row else 1.05
        return await message.answer(
            f"Current forwarding delay: {delay:g} seconds per message.\n"
            "Set with /speed 2 (allowed range: 0.5 to 30 seconds)."
        )

    try:
        delay = float(parts[1])
        if not 0.5 <= delay <= 30:
            raise ValueError
    except ValueError:
        return await message.answer(
            "Invalid speed delay. Use a number from 0.5 to 30 seconds.\n"
            "Example: /speed 1 or /speed 3"
        )

    # Update the in-process cache first so a running task sees this immediately.
    speed_delays[uid] = delay
    await settings_col.update_one(
        {"_id": uid},
        {
            "$set": {"speed_delay": delay, "updated_at": now_utc()},
            "$setOnInsert": {"caption": "", "disabled_types": []},
        },
        upsert=True,
    )
    await message.answer(
        f"Forwarding delay set to {delay:g} seconds per message. "
        "The change applies to the next forwarded message."
    )


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
    if await is_blocked(uid):
        return await message.answer("Your access is blocked.")
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

        previous = await tasks_col.find_one({"_id": uid})
        if previous and previous.get("status") in {"completed", "cancelled", "error"}:
            previous.pop("_id", None)
            previous["user_id"] = uid
            previous["archived_at"] = now_utc()
            await history_col.insert_one(previous)
        await tasks_col.update_one(
            {"_id": uid},
            {
                "$set": {
                    "task_id": f"{uid}-{int(now_utc().timestamp())}",
                    "sent_message_ids": [], "failed_message_ids": [], "retry_only": False, "paused": False,
                    "source_chat_id": source,
                    "destination_chat_id": destination,
                    "start_message_id": start_id,
                    "end_message_id": end_id,
                    "next_message_id": start_id,
                    "success_count": 0,
                    "failed_count": 0,
                    "skipped_count": 0,
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


async def build_status_dashboard(uid: int):
    row = await tasks_col.find_one({"_id": uid})
    settings = await settings_col.find_one({"_id": uid}, {"speed_delay": 1}) if settings_col is not None else None
    delay = float(settings.get("speed_delay", 1.05)) if settings else 1.05

    try:
        cpu = await asyncio.to_thread(psutil.Process().cpu_percent, 0.1)
        used_mb, limit_mb = container_memory_usage_mb()
        ram_percent = (used_mb / limit_mb * 100) if limit_mb else 0.0
        process_rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        disk = psutil.disk_usage("/")
        uptime_seconds = max(0, int(__import__("time").monotonic() - APP_STARTED_MONOTONIC))
        days, rem = divmod(uptime_seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, seconds = divmod(rem, 60)
        uptime = (f"{days}d " if days else "") + f"{hours}h {minutes}m {seconds}s"
        system_stats = (
            f"▣ <b>SERVER STATS</b>\n"
            f"├ CPU  · {cpu:.1f}% (process)\n"
            f"├ RAM  · {ram_percent:.1f}% ({used_mb:.0f}/{limit_mb:.0f} MB container)\n"
            f"├ APP  · {process_rss_mb:.0f} MB process RAM\n"
            f"├ DISK · {disk.percent:.1f}% ({disk.used / (1024**3):.1f}/{disk.total / (1024**3):.1f} GB filesystem)\n"
            f"└ UP   · {uptime}"
        )
    except Exception:
        log.exception("Could not collect server metrics")
        system_stats = "▣ <b>SERVER STATS</b>\nMetrics temporarily unavailable."

    live = live_progress.get(uid, {})
    db_status = str((row or {}).get("status", "")).lower()
    live_status = str(live.get("status", db_status)).lower()
    active_statuses = {"queued", "running", "waiting_for_session", "waiting_for_memory", "paused"}
    is_active = bool(row) and (db_status in active_statuses or live_status in active_statuses)

    if not is_active:
        return (
            "✦ <b>M U L T I  ·  F O R W A R D E R</b>\\n"
            "<i>Live personal status dashboard</i>\\n"
            "━━━━━━━━━━━━━━━━━━━━\\n\\n"
            "◉ <b>FORWARDING STATUS</b>\\n"
            "├ Status · <b>IDLE</b>\\n"
            "├ Active Tasks · 0\\n"
            "└ No forwarding task is running.\\n\\n"
            f"⚡ <b>FORWARDING SPEED</b>\\n└ {delay:g}s delay per message\\n\\n"
            f"{system_stats}\\n\\n"
            "<i>Use /forward to start · /history to view completed tasks</i>",
            "idle",
        )

    status = live_status.upper().replace("_", " ")
    start_id = int(row.get("start_message_id", 0))
    end_id = int(row.get("end_message_id", 0))
    next_id = int(live.get("next_message_id", row.get("next_message_id", start_id)))
    total = max(0, end_id - start_id + 1)
    processed = min(total, max(0, next_id - start_id))
    remaining = max(0, total - processed)
    percent = (processed / total * 100) if total else 0
    filled = min(20, max(0, int(percent // 5)))
    bar = "▰" * filled + "▱" * (20 - filled)
    success = int(live.get("success_count", row.get("success_count", 0)))
    failed = int(live.get("failed_count", row.get("failed_count", 0)))
    skipped = int(live.get("skipped_count", row.get("skipped_count", 0)))

    dashboard = (
        "✦ <b>M U L T I  ·  F O R W A R D E R</b>\n"
        "<i>Live personal status dashboard</i>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "◉ <b>FORWARDING TASK</b>\n"
        f"├ Status · <b>{status}</b>\n"
        f"├ Progress · {bar} <b>{percent:.1f}%</b>\n"
        f"├ Messages · {processed:,}/{total:,}\n"
        f"├ Remaining · {remaining:,}\n"
        f"├ Success · <b>{success:,}</b>\n"
        f"├ Failed · <b>{failed:,}</b>\n"
        f"├ Skipped · <b>{skipped:,}</b>\n"
        f"└ Remaining · <b>{remaining:,}</b>\n\n"
        f"⚡ <b>FORWARDING SPEED</b>\n└ {delay:g}s delay per message\n\n"
        f"{system_stats}\n\n"
        f"<i>Auto-refresh: every 2 seconds · Queue limit: {MAX_CONCURRENT_FORWARDING} workers</i>"
    )
    return dashboard, status.lower()


async def refresh_status_message(chat_id: int, message_id: int, uid: int):
    last_text = None
    try:
        while True:
            text, status = await build_status_dashboard(uid)
            if text != last_text:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        parse_mode="HTML",
                    )
                    last_text = text
                except Exception as exc:
                    if "message is not modified" not in str(exc).lower():
                        log.info("Live status refresh stopped for user %s: %s", uid, type(exc).__name__)
                        break
            if status not in {"queued", "running", "waiting for session", "waiting for memory", "paused"}:
                break
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Live status monitor failed for user %s", uid)
    finally:
        if status_monitors.get(uid) is asyncio.current_task():
            status_monitors.pop(uid, None)


@dp.message(Command("status"))
async def status_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    dashboard, status = await build_status_dashboard(uid)
    sent = await message.answer(dashboard, parse_mode="HTML")

    old_monitor = status_monitors.get(uid)
    if old_monitor and not old_monitor.done():
        old_monitor.cancel()
    if status in {"queued", "running", "waiting for session"}:
        status_monitors[uid] = asyncio.create_task(
            refresh_status_message(sent.chat.id, sent.message_id, uid)
        )


@dp.message(Command("cancel"))
async def cancel_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    task = tasks.get(uid)
    if not task or task.done():
        row = await tasks_col.find_one({"_id": uid})
        if not row or row.get("status") not in {"queued", "paused", "waiting_for_memory"}:
            return await message.answer("No active forwarding task.")
    if task and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    await save_task(uid, status="cancelled", paused=False)
    await message.answer("Your forwarding task has been cancelled.")


@dp.message(Command("pause"))
async def pause_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    row = await tasks_col.find_one({"_id": uid})
    if not row or row.get("status") not in {"running", "queued", "waiting_for_memory"}:
        return await message.answer("No running/queued task to pause.")
    pause_events.setdefault(uid, asyncio.Event()).clear()
    await save_task(uid, status="paused", paused=True)
    await message.answer("Task paused. Use /resume to continue.")


@dp.message(Command("resume"))
async def resume_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    row = await tasks_col.find_one({"_id": uid})
    if not row or row.get("status") not in {"paused", "queued", "waiting_for_memory"}:
        return await message.answer("No paused task found.")
    pause_events.setdefault(uid, asyncio.Event()).set()
    await save_task(uid, paused=False, status="queued")
    await launch_saved_task(uid)
    await message.answer("Task resumed / placed in queue.")


@dp.message(Command("retry"))
async def retry_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    row = await tasks_col.find_one({"_id": uid})
    failed_ids = sorted(set(row.get("failed_message_ids", []))) if row else []
    if not failed_ids:
        return await message.answer("No failed message IDs to retry.")
    task = tasks.get(uid)
    if task and not task.done():
        return await message.answer("A task is already active. Cancel or wait before retrying.")
    await tasks_col.update_one({"_id": uid}, {"$set": {"next_message_id": min(failed_ids), "retry_only": True, "status": "queued", "paused": False, "failed_count": 0}})
    pause_events.setdefault(uid, asyncio.Event()).set()
    await launch_saved_task(uid)
    await message.answer(f"Retry queued for {len(failed_ids)} failed message(s). Already-sent messages are skipped.")


@dp.message(Command("history"))
async def history_handler(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    rows = await history_col.find({"user_id": uid}).sort("archived_at", -1).limit(5).to_list(length=5)
    current = await tasks_col.find_one({"_id": uid})
    lines = ["<b>Recent forwarding history</b>"]
    if current:
        lines.append(f"Current: {current.get('status', 'unknown')} · success {current.get('success_count', 0)} · failed {current.get('failed_count', 0)}")
    for r in rows:
        lines.append(f"• {r.get('status', 'unknown')} · success {r.get('success_count', 0)} · failed {r.get('failed_count', 0)} · {r.get('archived_at', '')}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("admin"))
async def admin_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return await message.answer("Admin only.")
    total_users = await users_col.count_documents({})
    blocked = await blocked_col.count_documents({})
    active = await tasks_col.count_documents({"status": {"$in": ["running", "waiting_for_memory"]}})
    queued = await tasks_col.count_documents({"status": {"$in": ["queued", "paused"]}})
    await message.answer(
        "<b>Admin panel</b>\n"
        f"Connected sessions: {total_users}\nBlocked users: {blocked}\n"
        f"Active tasks: {active}/{MAX_CONCURRENT_FORWARDING}\nQueued/paused: {queued}\n"
        "Admin commands: /restart, /block USER_ID, /unblock USER_ID, /admincancel USER_ID",
        parse_mode="HTML",
    )


@dp.message(Command("restart"))
async def restart_handler(message: Message):
    """Gracefully stop workers and replace the process; admin only."""
    global restart_requested
    if not message.from_user or not is_admin(message.from_user.id):
        return await message.answer("Admin only.")
    if restart_requested:
        return await message.answer("Restart is already in progress.")
    restart_requested = True
    await message.answer(
        "♻️ Restart requested. Active forwarding tasks will be checkpointed and returned to the queue. "
        "The service will reconnect automatically if the hosting platform allows process replacement."
    )
    # Stop polling so main() can run its normal graceful-shutdown path.
    await dp.stop_polling()


@dp.message(Command("block"))
async def block_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return await message.answer("Admin only.")
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Usage: /block USER_ID")
    target = int(parts[1])
    await blocked_col.update_one({"_id": target}, {"$set": {"blocked_at": now_utc()}}, upsert=True)
    task = tasks.get(target)
    if task and not task.done():
        task.cancel()
    await message.answer(f"User {target} blocked.")


@dp.message(Command("unblock"))
async def unblock_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return await message.answer("Admin only.")
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Usage: /unblock USER_ID")
    target = int(parts[1])
    await blocked_col.delete_one({"_id": target})
    await message.answer(f"User {target} unblocked.")


@dp.message(Command("admincancel"))
async def admincancel_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return await message.answer("Admin only.")
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Usage: /admincancel USER_ID")
    target = int(parts[1])
    task = tasks.get(target)
    if task and not task.done():
        task.cancel()
    await save_task(target, status="cancelled", paused=False)
    await message.answer(f"Task for user {target} cancelled (if active).")


@dp.message(F.text)
async def session_message_handler(message: Message):
    if not message.from_user:
        return

    uid = message.from_user.id
    if uid in pending_session_generation:
        await handle_session_generation_input(message, uid)
        return
    if uid not in pending_connect:
        return
    if await is_blocked(uid):
        pending_connect.discard(uid)
        return await message.answer("Your access is blocked.")
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
    # Do not eagerly connect every saved user session on a 512 MB instance.
    # Sessions remain in MongoDB and clients start lazily when a task runs.
    async for row in tasks_col.find(
        {"status": {"$in": ["queued", "running", "waiting_for_session", "waiting_for_memory"]}},
        {"_id": 1},
    ):
        uid = int(row["_id"])
        try:
            await launch_saved_task(uid)
        except Exception:
            log.exception("Could not restore forwarding task for %s", uid)


async def main():
    await init_db()
    await setup_bot_commands()
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

        for flow in list(pending_session_generation.values()):
            with suppress(Exception):
                await flow["client"].disconnect()
        pending_session_generation.clear()

        with suppress(Exception):
            await bot.session.close()
        await health_runner.cleanup()

        if mongo_client is not None:
            await mongo_client.close()

    if restart_requested:
        log.warning("Replacing process after administrator restart request")
        os.execv(sys.executable, [sys.executable, *sys.argv])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
