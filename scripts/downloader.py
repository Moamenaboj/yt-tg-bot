#!/usr/bin/env python3
"""
YouTube Video & Playlist Downloader -> Telegram (up to 2 GB via Telethon MTProto).
Designed to run inside GitHub Actions triggered by a Cloudflare Worker.
"""

import asyncio
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Optional

from PIL import Image
from telethon import TelegramClient
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.sessions import MemorySession
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
)
import yt_dlp

# Safe ceiling just below Telegram's 2000 MiB bot MTProto upload limit
MAX_TG_FILE_BYTES = 1_980_000_000
QUALITY_FALLBACK_ORDER = ["1080", "720", "480", "360"]
VALID_QUALITIES = {"1080", "720", "480", "360", "mp3"}


def env_str(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def env_int(name: str, default: int = 0) -> int:
    val = env_str(name)
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def format_bytes(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} TB"


def sanitize_filename(name: str, max_len: int = 80) -> str:
    cleaned = re.sub(r'[\\/*?:"<>|\r\n\t]+', " ", name or "video").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = "video"
    return cleaned[:max_len].strip()


def truncate_text(text: str, max_len: int = 1000) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def prepare_cookies_file(raw_secret: str) -> Optional[Path]:
    """Accepts either raw Netscape cookies.txt text or Base64-encoded cookies.txt."""
    content = (raw_secret or "").strip()
    if not content:
        return None

    decoded_text = content
    # If it doesn't look like tab-separated Netscape cookies, try base64 decoding
    if "\t" not in content and "# Netscape" not in content and "# HTTP Cookie" not in content:
        try:
            raw_bytes = base64.b64decode(content, validate=True)
            candidate = raw_bytes.decode("utf-8")
            if "\t" in candidate or "youtube.com" in candidate:
                decoded_text = candidate
        except Exception:
            pass

    decoded_text = decoded_text.replace("\r\n", "\n")
    fd, path_str = tempfile.mkstemp(prefix="yt_cookies_", suffix=".txt")
    os.chmod(path_str, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(decoded_text)
        if not decoded_text.endswith("\n"):
            fh.write("\n")
    return Path(path_str)


class StatusTracker:
    """Rate-limited Telegram status message updater."""

    def __init__(
        self,
        client: TelegramClient,
        chat_id: int,
        message_id: int = 0,
        min_interval: float = 4.5,
    ):
        self.client = client
        self.chat_id = chat_id
        self.message_id = message_id
        self.min_interval = min_interval
        self._last_sent_at = 0.0
        self._blocked_until = 0.0
        self._last_text = ""
        self._lock = asyncio.Lock()

    async def update(self, text: str, force: bool = False) -> None:
        text = truncate_text(text, 4000)
        now = time.monotonic()
        if not force:
            if text == self._last_text:
                return
            if now < self._blocked_until:
                return
            if (now - self._last_sent_at) < self.min_interval:
                return

        async with self._lock:
            now = time.monotonic()
            if not force and (
                text == self._last_text
                or now < self._blocked_until
                or (now - self._last_sent_at) < self.min_interval
            ):
                return

            try:
                if self.message_id:
                    await self.client.edit_message(
                        self.chat_id,
                        self.message_id,
                        text,
                        link_preview=False,
                    )
                else:
                    msg = await self.client.send_message(
                        self.chat_id,
                        text,
                        link_preview=False,
                    )
                    self.message_id = msg.id
                self._last_text = text
                self._last_sent_at = time.monotonic()
            except MessageNotModifiedError:
                self._last_text = text
            except FloodWaitError as exc:
                self._blocked_until = time.monotonic() + exc.seconds + 2
                if force:
                    await asyncio.sleep(exc.seconds + 2)
                    try:
                        if self.message_id:
                            await self.client.edit_message(
                                self.chat_id,
                                self.message_id,
                                text,
                                link_preview=False,
                            )
                        else:
                            msg = await self.client.send_message(
                                self.chat_id,
                                text,
                                link_preview=False,
                            )
                            self.message_id = msg.id
                        self._last_text = text
                        self._last_sent_at = time.monotonic()
                    except Exception:
                        pass
            except Exception as exc:
                print(f"[warn] status update failed: {exc}", file=sys.stderr)


def build_base_ydl_opts(cookies_path: Optional[Path]) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "retries": 5,
        "fragment_retries": 5,
        "file_access_retries": 3,
        "socket_timeout": 30,
        "js_runtimes": {"deno": {}},
    }
    if cookies_path and cookies_path.exists():
        opts["cookiefile"] = str(cookies_path)
    return opts


def extract_playlist_entries(
    url: str,
    playlist_items: str,
    max_videos: int,
    cookies_path: Optional[Path],
) -> tuple[str, list[dict[str, str]], bool]:
    """
    Returns (playlist_title, list_of_items[{url, title}], was_capped).
    Uses flat extraction so no video is downloaded during discovery.
    """
    opts = build_base_ydl_opts(cookies_path)
    opts.update(
        {
            "extract_flat": "in_playlist",
            "skip_download": True,
            "ignoreerrors": True,
            "noplaylist": False,
        }
    )

    if playlist_items:
        opts["playlist_items"] = playlist_items
    else:
        # Request 1 extra item to detect whether playlist exceeds max_videos
        opts["playlist_items"] = f"1-{max_videos + 1}"

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    if not info:
        raise RuntimeError("تعذر قراءة بيانات الرابط من يوتيوب.")

    entries = info.get("entries")
    if entries is None:
        # Single video fallback
        return (
            info.get("title") or "YouTube Video",
            [{"url": info.get("webpage_url") or url, "title": info.get("title") or ""}],
            False,
        )

    playlist_title = info.get("title") or "YouTube Playlist"
    items: list[dict[str, str]] = []
    for entry in entries:
        if not entry:
            continue
        title = (entry.get("title") or "").strip()
        if title in ("[Deleted video]", "[Private video]"):
            continue
        entry_url = entry.get("webpage_url") or entry.get("url") or ""
        entry_id = entry.get("id") or ""
        if not entry_url.startswith("http"):
            if entry_id:
                entry_url = f"https://www.youtube.com/watch?v={entry_id}"
            else:
                continue
        items.append({"url": entry_url, "title": title})

    was_capped = False
    if not playlist_items and len(items) > max_videos:
        items = items[:max_videos]
        was_capped = True

    return playlist_title, items, was_capped


def download_single_sync(
    url: str,
    quality: str,
    work_dir: Path,
    cookies_path: Optional[Path],
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
) -> tuple[Path, dict[str, Any]]:
    opts = build_base_ydl_opts(cookies_path)
    opts.update(
        {
            "noplaylist": True,
            "outtmpl": str(work_dir / "%(id)s.%(ext)s"),
            "writethumbnail": True,
            "overwrites": True,
        }
    )
    if on_progress:
        opts["progress_hooks"] = [on_progress]

    if quality == "mp3":
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ]
    else:
        q = int(quality)
        opts["format"] = (
            f"bestvideo[height<={q}][vcodec^=avc1]+bestaudio[acodec^=mp4a]/"
            f"bestvideo[height<={q}][ext=mp4]+bestaudio[ext=m4a]/"
            f"bestvideo[height<={q}]+bestaudio/"
            f"best[height<={q}]/best"
        )
        opts["merge_output_format"] = "mp4"
        opts["postprocessor_args"] = {"merger": ["-movflags", "+faststart"]}

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)

    if not info:
        raise RuntimeError("فشل استخراج بيانات الفيديو.")

    # Resolve final postprocessed file path
    media_path: Optional[Path] = None
    requested = info.get("requested_downloads") or []
    for item in requested:
        fp = item.get("filepath")
        if fp and Path(fp).is_file():
            media_path = Path(fp)
            break

    if media_path is None:
        ext_priority = [".mp3"] if quality == "mp3" else [".mp4", ".mkv", ".webm", ".m4a"]
        candidates = [
            p
            for p in work_dir.iterdir()
            if p.is_file()
            and p.suffix.lower() in ext_priority
            and not p.name.endswith(".part")
        ]
        if candidates:
            candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
            media_path = candidates[0]

    if media_path is None or not media_path.is_file():
        raise RuntimeError("لم يتم العثور على الملف بعد التحميل.")

    return media_path, info


def prepare_thumbnail(work_dir: Path) -> Optional[Path]:
    """Finds downloaded thumbnail and converts it to a <=320px JPEG for Telegram."""
    image_exts = {".jpg", ".jpeg", ".png", ".webp"}
    thumbs = [
        p for p in work_dir.iterdir() if p.is_file() and p.suffix.lower() in image_exts
    ]
    if not thumbs:
        return None

    out_path = work_dir / "_tg_thumb.jpg"
    try:
        with Image.open(thumbs[0]) as img:
            rgb = img.convert("RGB")
            rgb.thumbnail((320, 320))
            rgb.save(out_path, format="JPEG", quality=85, optimize=True)
        return out_path
    except Exception as exc:
        print(f"[warn] thumbnail conversion failed: {exc}", file=sys.stderr)
        return None


def probe_media(file_path: Path, info: dict[str, Any]) -> tuple[int, int, int]:
    """Returns (duration_sec, width, height) using ffprobe with yt-dlp info fallback."""
    duration = int(info.get("duration") or 0)
    width = int(info.get("width") or 0)
    height = int(info.get("height") or 0)

    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout:
            data = json.loads(proc.stdout)
            fmt_dur = data.get("format", {}).get("duration")
            if fmt_dur:
                duration = int(float(fmt_dur))
            for stream in data.get("streams", []):
                if stream.get("codec_type") == "video":
                    w = int(stream.get("width") or 0)
                    h = int(stream.get("height") or 0)
                    # Check rotation metadata (important for vertical Shorts)
                    rotation = 0
                    tags = stream.get("tags") or {}
                    if "rotate" in tags:
                        try:
                            rotation = int(tags["rotate"])
                        except ValueError:
                            rotation = 0
                    for sd in stream.get("side_data_list") or []:
                        if "rotation" in sd:
                            try:
                                rotation = int(sd["rotation"])
                            except ValueError:
                                pass
                    if abs(rotation) in (90, 270):
                        w, h = h, w
                    if w > 0 and h > 0:
                        width, height = w, h
                    break
    except Exception as exc:
        print(f"[warn] ffprobe failed: {exc}", file=sys.stderr)

    return duration, width, height


async def download_with_size_fallback(
    url: str,
    requested_quality: str,
    work_dir: Path,
    cookies_path: Optional[Path],
    status: StatusTracker,
    header: str,
) -> tuple[Path, dict[str, Any], str]:
    """Downloads media; if video exceeds 1.98 GB, automatically steps down quality."""
    loop = asyncio.get_running_loop()

    if requested_quality == "mp3":
        qualities_to_try = ["mp3"]
    else:
        start_idx = (
            QUALITY_FALLBACK_ORDER.index(requested_quality)
            if requested_quality in QUALITY_FALLBACK_ORDER
            else 1
        )
        qualities_to_try = QUALITY_FALLBACK_ORDER[start_idx:]

    last_size = 0
    for q in qualities_to_try:
        # Clean previous attempt artifacts inside work_dir
        for child in work_dir.iterdir():
            if child.is_file():
                child.unlink(missing_ok=True)

        last_hook_ts = 0.0

        def progress_hook(d: dict[str, Any]) -> None:
            nonlocal last_hook_ts
            if d.get("status") != "downloading":
                return
            now = time.monotonic()
            if now - last_hook_ts < 4.5:
                return
            last_hook_ts = now
            downloaded = d.get("downloaded_bytes") or 0
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            speed = d.get("speed") or 0
            if total > 0:
                pct = min(100.0, (downloaded / total) * 100.0)
                line = f"⬇️ التحميل ({q}): {pct:.0f}% ({format_bytes(downloaded)} / {format_bytes(total)})"
            else:
                line = f"⬇️ التحميل ({q}): {format_bytes(downloaded)}"
            if speed:
                line += f" • {format_bytes(speed)}/s"
            asyncio.run_coroutine_threadsafe(
                status.update(f"{header}\n{line}"),
                loop,
            )

        await status.update(f"{header}\n⬇️ جاري التحميل ({q})...", force=True)
        media_path, info = await asyncio.to_thread(
            download_single_sync,
            url,
            q,
            work_dir,
            cookies_path,
            progress_hook,
        )

        last_size = media_path.stat().st_size
        if last_size <= MAX_TG_FILE_BYTES:
            return media_path, info, q

        if q != qualities_to_try[-1]:
            await status.update(
                f"{header}\n⚠️ حجم الملف ({format_bytes(last_size)}) يتجاوز حد 2 GB — جاري التجربة بجودة أقل...",
                force=True,
            )

    raise RuntimeError(
        f"حجم الملف ({format_bytes(last_size)}) يتجاوز الحد الأقصى لتليجرام (2 GB)."
    )


async def upload_media(
    client: TelegramClient,
    chat_id: int,
    media_path: Path,
    thumb_path: Optional[Path],
    info: dict[str, Any],
    quality: str,
    caption: str,
    status: StatusTracker,
    header: str,
) -> None:
    title = (info.get("title") or media_path.stem).strip()
    uploader = (info.get("uploader") or info.get("channel") or "YouTube").strip()
    duration, width, height = await asyncio.to_thread(probe_media, media_path, info)

    if quality == "mp3":
        file_name = f"{sanitize_filename(title)}.mp3"
        attributes = [
            DocumentAttributeAudio(
                duration=duration,
                title=title[:64],
                performer=uploader[:64],
            ),
            DocumentAttributeFilename(file_name=file_name),
        ]
        supports_streaming = False
    else:
        ext = media_path.suffix.lstrip(".") or "mp4"
        file_name = f"{sanitize_filename(title)}.{ext}"
        attributes = [
            DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True,
            ),
            DocumentAttributeFilename(file_name=file_name),
        ]
        supports_streaming = True

    start_time = time.monotonic()
    last_cb_ts = 0.0

    async def upload_progress(current: int, total: int) -> None:
        nonlocal last_cb_ts
        now = time.monotonic()
        if now - last_cb_ts < 4.5 and current < total:
            return
        last_cb_ts = now
        elapsed = max(0.1, now - start_time)
        speed = current / elapsed
        pct = min(100.0, (current / max(1, total)) * 100.0)
        await status.update(
            f"{header}\n⬆️ جاري الرفع: {pct:.0f}% ({format_bytes(current)} / {format_bytes(total)}) • {format_bytes(speed)}/s"
        )

    await status.update(
        f"{header}\n⬆️ جاري الرفع إلى تليجرام ({format_bytes(media_path.stat().st_size)})...",
        force=True,
    )

    for attempt in range(3):
        try:
            await client.send_file(
                chat_id,
                file=str(media_path),
                caption=truncate_text(caption, 1020),
                thumb=str(thumb_path) if thumb_path and thumb_path.exists() else None,
                attributes=attributes,
                supports_streaming=supports_streaming,
                progress_callback=upload_progress,
            )
            return
        except FloodWaitError as exc:
            wait_sec = exc.seconds + 2
            await status.update(
                f"{header}\n⏳ تليجرام طلب الانتظار {wait_sec} ثانية (FloodWait)...",
                force=True,
            )
            await asyncio.sleep(wait_sec)
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(3)


def short_error(exc: Exception) -> str:
    msg = str(exc).strip().splitlines()
    text = msg[-1] if msg else exc.__class__.__name__
    if "Sign in to confirm you're not a bot" in str(exc):
        return "يوتيوب طلب التحقق (حدّث YOUTUBE_COOKIES في GitHub Secrets)."
    return truncate_text(text, 180)


async def main() -> int:
    bot_token = env_str("TELEGRAM_BOT_TOKEN")
    api_id = env_int("TELEGRAM_API_ID")
    api_hash = env_str("TELEGRAM_API_HASH")
    chat_id = env_int("CHAT_ID")
    status_msg_id = env_int("STATUS_MSG_ID")
    video_url = env_str("VIDEO_URL")
    mode = env_str("MODE", "video").lower()
    quality = env_str("QUALITY", "720").lower()
    playlist_items = env_str("PLAYLIST_ITEMS")
    max_playlist_videos = env_int("MAX_PLAYLIST_VIDEOS", 100)
    raw_cookies = os.environ.get("YOUTUBE_COOKIES", "")

    if not all([bot_token, api_id, api_hash, chat_id, video_url]):
        print(
            "Missing required environment variables: "
            "TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH, CHAT_ID, VIDEO_URL",
            file=sys.stderr,
        )
        return 1

    if quality not in VALID_QUALITIES:
        quality = "720"

    cookies_path = prepare_cookies_file(raw_cookies)
    client = TelegramClient(MemorySession(), api_id, api_hash)

    try:
        await client.start(bot_token=bot_token)
        status = StatusTracker(client, chat_id, status_msg_id)

        if mode == "playlist":
            await status.update("🔍 جاري فحص البلايليست واستخراج قائمة الفيديوهات...", force=True)
            playlist_title, items, was_capped = await asyncio.to_thread(
                extract_playlist_entries,
                video_url,
                playlist_items,
                max_playlist_videos,
                cookies_path,
            )
            if not items:
                await status.update("⚠️ لم يتم العثور على أي فيديوهات متاحة في البلايليست.", force=True)
                return 0
        else:
            playlist_title = ""
            items = [{"url": video_url, "title": ""}]
            was_capped = False

        total = len(items)
        succeeded = 0
        failed: list[str] = []

        if mode == "playlist" and total > 1:
            intro = f"📋 البلايليست: {truncate_text(playlist_title, 120)}\n📦 عدد الفيديوهات: {total}"
            if was_capped:
                intro += f"\nℹ️ تم تحديد أول {max_playlist_videos} فيديو (أرسل نطاقاً مثل 101-200 لاستكمال الباقي)."
            await status.update(intro, force=True)

        for idx, item in enumerate(items, start=1):
            item_url = item["url"]
            item_hint = item.get("title") or ""
            header = (
                f"📋 [{idx}/{total}] {truncate_text(item_hint, 80)}"
                if total > 1
                else "🎬 جاري معالجة الفيديو..."
            )

            work_dir = Path(tempfile.mkdtemp(prefix="ytdl_"))
            try:
                media_path, info, actual_quality = await download_with_size_fallback(
                    url=item_url,
                    requested_quality=quality,
                    work_dir=work_dir,
                    cookies_path=cookies_path,
                    status=status,
                    header=header,
                )

                title = (info.get("title") or item_hint or "YouTube").strip()
                header = (
                    f"📋 [{idx}/{total}] {truncate_text(title, 80)}"
                    if total > 1
                    else f"🎬 {truncate_text(title, 80)}"
                )
                thumb_path = await asyncio.to_thread(prepare_thumbnail, work_dir)

                caption = f"[{idx}/{total}] {title}" if total > 1 else title
                if actual_quality != quality and quality != "mp3":
                    caption += f"\n({actual_quality}p)"

                await upload_media(
                    client=client,
                    chat_id=chat_id,
                    media_path=media_path,
                    thumb_path=thumb_path,
                    info=info,
                    quality=actual_quality,
                    caption=caption,
                    status=status,
                    header=header,
                )
                succeeded += 1
            except Exception as exc:
                err_msg = short_error(exc)
                print(f"[error] item {idx}/{total} ({item_url}): {exc}", file=sys.stderr)
                failed.append(f"• #{idx}: {err_msg}")
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)

            if idx < total:
                await asyncio.sleep(1.5)

        # Final summary message
        if total == 1:
            if succeeded == 1:
                await status.update("✅ تم التحميل والرفع بنجاح!", force=True)
            else:
                await status.update(f"❌ فشل التحميل:\n{failed[0]}", force=True)
        else:
            lines = [
                f"✅ انتهى تحميل البلايليست: {truncate_text(playlist_title, 100)}",
                f"✔️ نجح: {succeeded} / {total}",
            ]
            if failed:
                lines.append(f"❌ فشل: {len(failed)}")
                lines.extend(failed[:10])
                if len(failed) > 10:
                    lines.append(f"... و {len(failed) - 10} أخطاء أخرى.")
            await status.update("\n".join(lines), force=True)

        return 0
    except Exception as exc:
        print(f"[fatal] {exc}", file=sys.stderr)
        try:
            await client.send_message(
                chat_id,
                f"❌ حدث خطأ أثناء المعالجة:\n{short_error(exc)}",
            )
        except Exception:
            pass
        return 1
    finally:
        await client.disconnect()
        if cookies_path and cookies_path.exists():
            cookies_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
