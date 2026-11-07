# Multi-User Telegram Forwarder — Koyeb

A single-service Telegram bot: one central bot token, many independent Telegram user sessions, encrypted PostgreSQL session storage, and restart recovery for forwarding tasks.

## Important behavior

- Each Telegram user has an isolated user client and one independent forwarding task.
- User session strings are encrypted with Fernet before being stored in PostgreSQL.
- PostgreSQL stores sessions and forwarding progress. The local filesystem is not used for permanent data.
- On service startup, saved sessions are reconnected and queued/running jobs resume from `next_message_id`.
- A Koyeb restart can interrupt the current message operation; progress is checkpointed after each message. A message being processed at the instant of a crash may be attempted again.
- This project uses Pyrogram String Sessions. Users must only connect accounts they own or are authorized to operate. Telegram API limits and account restrictions still apply.

## Commands

- `/start` or `/help` — instructions
- `/connect` — send your Pyrogram String Session in the next message
- `/disconnect` — stop your task and remove your saved session
- `/forward SOURCE_CHAT_ID DESTINATION_CHAT_ID START_ID END_ID` — start forwarding
- `/status` — show your own task progress
- `/cancel` — cancel your own active task
- `/caption TEXT` — set your forwarding caption
- `/dcaption` — clear your caption
- `/filter TYPE` — toggle a media type (`text`, `photo`, `video`, `gif`, `audio`, `voice`, `file`)
- `/filters` — show your media filter settings

Example:

`/forward -1001234567890 -1009876543210 1 500`

The connected user account must have access to both chats. For private channels/groups, the user account must be a member and have permission to read the source and post to the destination.

## Environment variables

Required:

- `BOT_TOKEN` — token from BotFather for the central bot
- `API_ID` — Telegram API ID from https://my.telegram.org
- `API_HASH` — Telegram API hash from https://my.telegram.org
- `ENCRYPTION_KEY` — Fernet key; generate once and keep it unchanged
- `DATABASE_URL` — PostgreSQL connection URL

Koyeb provides `PORT` automatically in web services. The app listens on `0.0.0.0:$PORT`.

Generate an encryption key locally:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Keep the same key permanently. If you change or lose it, existing encrypted sessions cannot be decrypted.

## Koyeb deployment — exact steps

### 1. Create PostgreSQL

Use a managed PostgreSQL provider such as Neon, Supabase, or another PostgreSQL host.

1. Create a PostgreSQL database.
2. Copy its connection URL.
3. Ensure SSL is enabled if required by your provider. Many providers supply a URL containing `sslmode=require`.
4. Do not use SQLite or a local Koyeb filesystem for persistent sessions.

### 2. Upload this project to GitHub

1. Download/extract this project ZIP.
2. Create a new GitHub repository, for example `multi-user-telegram-forwarder`.
3. Upload the contents of this folder to the repository root (the `main.py`, `requirements.txt`, `runtime.txt`, `Procfile`, and `README.md` files should be at the root).
4. Commit and push to GitHub.

### 3. Create a Koyeb service

1. Sign in to https://app.koyeb.com.
2. Choose **Create App** → **Create Service**.
3. Select **GitHub** as the deployment source.
4. Connect/authorize GitHub if prompted.
5. Select your repository and branch (usually `main`).
6. Choose **Buildpack** as the builder.
7. Set the build command to:

   `pip install -r requirements.txt`

8. Set the run command / start command to:

   `python main.py`

9. Choose a Web Service so Koyeb provides `PORT` and health checks can reach the app.
10. Set the exposed port to the value supplied by Koyeb / `PORT` (the app binds to `0.0.0.0:$PORT`).
11. Set the health check path to `/health` if the Koyeb UI asks for one.
12. Choose an instance size with enough memory for the central bot and the connected user clients. More simultaneous accounts/tasks require more memory and CPU.

### 4. Add environment variables in Koyeb

In the service's **Environment variables and files** section, add:

| Key | Value |
|---|---|
| `BOT_TOKEN` | Your BotFather bot token |
| `API_ID` | Your Telegram API ID (digits) |
| `API_HASH` | Your Telegram API hash |
| `ENCRYPTION_KEY` | A generated Fernet key |
| `DATABASE_URL` | Your managed PostgreSQL URL |

Optional:

| Key | Value |
|---|---|
| `LOG_LEVEL` | `INFO` |
| `PORT` | Usually supplied by Koyeb; do not override unless required |

Do not commit `.env`, tokens, API hashes, session strings, or the encryption key to GitHub.

### 5. Deploy

1. Save the service settings.
2. Click **Deploy**.
3. Wait for the build and deployment to finish.
4. Open the service logs. A successful startup should show `Bot started; listening on port ...`.
5. Open the Koyeb service URL followed by `/health`. It should return JSON with `"status":"ok"`.
6. Open your Telegram central bot and send `/start`.
7. Send `/connect`, then send your own Pyrogram String Session.
8. Start a job with `/forward SOURCE DEST START END`.

## Exact Koyeb start command

```bash
python main.py
```

## Operational notes and limitations

- Deploy only one running replica of this polling bot. Telegram long polling should not be run by multiple replicas with the same bot token.
- PostgreSQL is the persistent source of truth. Koyeb instance disk is not used for permanent user data.
- Keep `ENCRYPTION_KEY` backed up securely and unchanged.
- A session string is equivalent to account access. Never share it or paste it into public chats. The bot attempts to delete the message containing the string after receiving it, but deletion is best-effort; use a private chat with your bot.
- This starter implementation supports one active forwarding job per Telegram user at a time. Different users can forward concurrently.
- Forwarding is implemented as copying messages, not Telegram's native forward attribution.
- The app uses message-by-message checkpoints for restart recovery. Telegram FloodWait responses are respected.
- Do not use this bot for spam, unsolicited bulk messaging, or content you do not have permission to copy.
