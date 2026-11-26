# YouTube Telegram Downloader Bot (100% Free Serverless)

Serverless YouTube video & playlist downloader bot for Telegram powered by **Cloudflare Workers** + **GitHub Actions** + **yt-dlp** + **Telethon (MTProto up to 2 GB)**.

## Architecture
1. **Cloudflare Worker (`worker/src/index.js`)**: Receives Telegram webhook messages 24/7 with zero cold start, verifies your `ALLOWED_USER_IDS`, displays quality selection buttons (video, playlist, or range like `1-10`), and triggers GitHub Actions via `workflow_dispatch`.
2. **GitHub Actions (`.github/workflows/download.yml`)**: Spins up a free Ubuntu runner (`4 vCPU`, `16 GB RAM`, up to ~6 hours/job) with `Deno`, `ffmpeg`, `bgutil-ytdlp-pot-provider`, `yt-dlp`, and `Telethon` (`cryptg` accelerated) to download and upload each item sequentially while deleting temp files immediately.

---

## Step-by-Step Setup (No Credit Card Required)

### 1. Gather Telegram Credentials
1. Create a bot via `@BotFather` on Telegram and copy `TELEGRAM_BOT_TOKEN`.
2. Go to `https://my.telegram.org` -> **API development tools** -> create an app and copy `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`.
3. Get your numeric Telegram User ID from `@userinfobot` (or send `/myid` to your bot after step 4).

### 2. Create a Private GitHub Repository
1. Create a new **Private** repository on GitHub (e.g., `yt-tg-bot`) and push this project to the `main` branch.
2. Go to **Settings -> Secrets and variables -> Actions -> New repository secret** and add:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `YOUTUBE_COOKIES` *(recommended)*: Export `cookies.txt` from a **secondary/burner** YouTube account in an incognito browser tab using a Netscape cookies exporter extension, then paste the contents (raw text or Base64) here.
3. Create a GitHub Fine-Grained Personal Access Token:
   - **GitHub -> Settings -> Developer settings -> Personal access tokens -> Fine-grained tokens -> Generate new token**
   - Repository access: **Only select repositories** -> select `yt-tg-bot`
   - Repository permissions: **Actions -> Read and write**
   - Copy the token (`GITHUB_TOKEN`).

### 3. Deploy the Cloudflare Worker
You can deploy either from the **Cloudflare Dashboard** (zero CLI needed) or via `wrangler`:

#### Option A: Cloudflare Dashboard (Browser Only)
1. Sign up at `https://dash.cloudflare.com` (free, no credit card).
2. Go to **Workers & Pages -> Create -> Create Worker -> Deploy**.
3. Click **Edit code**, paste the contents of `worker/src/index.js`, and click **Deploy**.
4. Go to **Settings -> Variables and Secrets** and add:
   - `TELEGRAM_BOT_TOKEN` *(Secret)*
   - `TELEGRAM_WEBHOOK_SECRET` *(Secret — any random password string, e.g. `mySecretKey98765`)*
   - `GITHUB_TOKEN` *(Secret — Fine-Grained PAT from step 2)*
   - `GITHUB_OWNER` *(Text — your GitHub username)*
   - `GITHUB_REPO` *(Text — e.g. `yt-tg-bot`)*
   - `GITHUB_BRANCH` *(Text — `main`)*
   - `ALLOWED_USER_IDS` *(Text — your Telegram numeric ID, comma-separated for multiple users)*

#### Option B: Wrangler CLI
```bash
cd worker
npm install
npx wrangler login
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put TELEGRAM_WEBHOOK_SECRET
npx wrangler secret put GITHUB_TOKEN
npx wrangler secret put GITHUB_OWNER
npx wrangler secret put GITHUB_REPO
npx wrangler secret put ALLOWED_USER_IDS
npx wrangler deploy
```

### 4. Register the Telegram Webhook (One Click)
Open this URL in your browser (replace placeholders):
```text
https://<your-worker-name>.<your-subdomain>.workers.dev/setup?secret=<TELEGRAM_WEBHOOK_SECRET>
```
You should see `"ok": true` in the JSON response.

---

## Usage on Telegram
- Send any YouTube video or Shorts link -> pick `1080p`, `720p`, `480p`, `360p`, or `MP3`.
- Send any YouTube playlist link -> pick quality -> videos download and upload sequentially.
- Download a specific range from a playlist by appending the range after the URL:
  ```text
  https://www.youtube.com/playlist?list=PLxxxx 1-10
  ```
