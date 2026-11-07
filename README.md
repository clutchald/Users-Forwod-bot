Multi-User Telegram Forwarder — Koyeb
A single Telegram bot where multiple Telegram users can connect their own Pyrogram user sessions. Each Telegram user has an isolated session, settings, and forwarding task.
Multi-user and queue behavior
Each Telegram user connects one Telegram account with /connect.
Multiple users can connect independently (for example, 5–6 users).
Only MAX_CONCURRENT_FORWARDING forwarding tasks run at the same time. The default is 2, suitable as a cautious starting point for a 512 MB instance.
Additional users' tasks stay in the queue and start automatically when a worker slot becomes free.
A user can have one forwarding task at a time.
Run only one Koyeb replica; the task limit is local to this service process.
Saved sessions are kept in MongoDB and connected lazily when a task needs them; the bot does not eagerly start every user session at process boot. The number of connected sessions and safe simultaneous forwarding capacity depend on RAM, CPU, media sizes, and Telegram API behavior. Two concurrent tasks on 512 MB are a starting configuration, not a guarantee. Monitor memory and logs; increase instance memory or lower the concurrency if the process approaches its memory limit.
Commands
/start or /help — instructions
/connect — connect your own Pyrogram String Session
/disconnect — remove your saved session and task data
/forward SOURCE_CHAT_ID DESTINATION_CHAT_ID START_ID END_ID — start a forwarding task
/status — show your own task progress
/cancel — cancel your own task
/pause and /resume — pause/resume your task
/retry — retry failed message IDs; already-sent IDs are skipped
/history — show recent archived task summaries
/admin — admin statistics (requires ADMIN_IDS)
/block USER_ID, /unblock USER_ID, /admincancel USER_ID — admin controls
/speed [seconds] — set/view per-message delay
/caption TEXT — set a custom caption
/dcaption — clear your caption
/filter TYPE — toggle a media type (text, photo, video, gif, audio, voice, file)
/filters — show media filter settings
The connected Telegram account must be able to access the source and destination chats. The legacy .id command returns the chat ID in the current chat; replying to a post with .id also returns that message ID. The legacy .f SOURCE_CHAT_ID START_ID END_ID command starts forwarding to the chat where .f is sent.
Environment variables
Required:
BOT_TOKEN — central bot token from BotFather
API_ID — Telegram API ID from https://my.telegram.org
API_HASH — Telegram API hash
DATABASE_URL — MongoDB Atlas connection URI
Optional:
MONGO_DB_NAME — database name (default: telegram_forwarder)
MAX_CONCURRENT_FORWARDING — maximum simultaneous forwarding jobs (default: 2; keep at 2 for the current 512 MB instance)
MIN_FREE_RAM_MB — reserved memory target for queue planning (default: 75 MB)
ADMIN_IDS — comma-separated numeric Telegram user IDs allowed to use admin commands
LOG_LEVEL — logging level (default: INFO)
PORT — Koyeb web-service port (normally supplied by Koyeb)
Koyeb deployment
Upload the contents of this folder to the root of your GitHub repository.
Create or update a Koyeb Web Service from that repository.
Build command: pip install -r requirements.txt
Run command: python main.py
Add the required environment variables above.
Set health check path to /health if requested.
Keep the service at one replica when using Telegram long polling.
For a 512 MB instance, leave MAX_CONCURRENT_FORWARDING=2. If memory usage becomes high, reduce it to 1. Only raise it after testing on a larger instance.
Security and limitations
A Pyrogram session string grants access to its Telegram account. Never share it publicly.
Session strings are stored in MongoDB as plaintext in this implementation. Restrict MongoDB network access and credentials carefully.
Forwarding uses message copying, not Telegram's native forward attribution.
Telegram FloodWait responses are respected. Telegram rate limits and account restrictions still apply.
Use this bot only with accounts and content you are authorized to access and copy; do not use it for spam or unsolicited bulk messaging.
Added operational controls
/pause and /resume control a user's current task.
/retry retries the recorded failed message IDs. Successfully copied IDs are stored and skipped on retry to reduce duplicate copies.
/history displays recent archived task summaries (the latest task is shown separately).
RAM guard checks container cgroup memory when available and pauses workers while memory pressure is high.
User sessions are loaded on demand rather than all being started at boot; after a restart, /connect can reconnect a previously saved session.
Admin commands are enabled only for numeric IDs listed in ADMIN_IDS.
Note: this service uses one active task per Telegram user and a process-local worker semaphore. Run one Koyeb replica; multiple replicas would need a distributed queue/lock to enforce a global worker limit. Resume/retry behavior should be load-tested with your own Telegram accounts and chats before production use.
