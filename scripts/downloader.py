#!/usr/bin/env python3
"""
High-Speed YouTube Video & Playlist Downloader -> Telegram (up to 2 GB via Telethon MTProto).

Features:
- 3-stage async playlist pipeline: concurrent downloads + concurrent parallel MTProto uploads
  + strict-order Telegram delivery (#1, #2, #3, ...).
- FastUploader: 12 parallel MTProtoSender connections with 512 KB pipelined chunks.
- Automatic quality fallback when a video exceeds Telegram's 2 GB limit.
- Immediate temp-directory cleanup after each upload stage to keep disk usage minimal.
"""

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import json
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Optional, Union

from PIL import Image
from telethon import TelegramClient, helpers
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.network import MTProtoSender
from telethon.sessions import MemorySession
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InitConnectionRequest, InvokeWithLayerRequest
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputFile,
    InputFileBig,
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
        min_interval: float = 4.0,
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


class PlaylistDashboard:
    """Aggregates concurrent download/upload progress into one clean status message."""

    def __init__(
        self,
        status: StatusTracker,
        playlist_title: str,
        total: int,
        was_capped: bool = False,
        max_videos: int = 100,
    ):
        self.status = status
        self.playlist_title = truncate_text(playlist_title, 100)
        self.total = total
        self.was_capped = was_capped
        self.max_videos = max_videos
        self.done_count = 0
        self.fail_count = 0
        self.active: dict[int, tuple[str, str]] = {}  # idx -> (phase, detail)

    def render(self) -> str:
        uploading = sum(1 for phase, _ in self.active.values() if phase == "⬆️")
        downloading = sum(1 for phase, _ in self.active.values() if phase == "⬇️")
        lines = [
            f"📋 البلايليست: {self.playlist_title}",
            f"✅ تم: {self.done_count}/{self.total}  |  ⬆️ يرفع: {uploading}  |  ⬇️ يحمّل: {downloading}",
        ]
        if self.fail_count:
            lines[-1] += f"  |  ❌ فشل: {self.fail_count}"
        if self.was_capped:
            lines.append(
                f"ℹ️ تم تحديد أول {self.max_videos} فيديو (أرسل نطاقاً مثل 101-200 لاستكمال الباقي)."
            )
        for idx in sorted(self.active.keys())[:8]:
            phase, detail = self.active[idx]
            lines.append(f"• {phase} #{idx}: {detail}")
        return "\n".join(lines)

    async def set_active(self, idx: int, phase: str, detail: str, force: bool = False) -> None:
        self.active[idx] = (phase, detail)
        await self.status.update(self.render(), force=force)

    async def mark_done(self, idx: int, succeeded: bool) -> None:
        self.active.pop(idx, None)
        if succeeded:
            self.done_count += 1
        else:
            self.fail_count += 1
        await self.status.update(self.render(), force=False)


def build_base_ydl_opts(cookies_path: Optional[Path]) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "retries": 5,
        "fragment_retries": 5,
        "file_access_retries": 3,
        "concurrent_fragment_downloads": 8,
        "http_chunk_size": 10 * 1024 * 1024,
        "socket_timeout": 30,
        "source_address": "0.0.0.0",
        "js_runtimes": {"deno": {}},
        "remote_components": ["ejs:github"],
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
        opts["playlist_items"] = f"1-{max_videos + 1}"

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    if not info:
        raise RuntimeError("تعذر قراءة بيانات الرابط من يوتيوب.")

    entries = info.get("entries")
    if entries is None:
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
    on_status: Callable[[str, bool], Any],
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
        for child in work_dir.iterdir():
            if child.is_file():
                child.unlink(missing_ok=True)

        last_hook_ts = 0.0

        def progress_hook(d: dict[str, Any]) -> None:
            nonlocal last_hook_ts
            if d.get("status") != "downloading":
                return
            now = time.monotonic()
            if now - last_hook_ts < 3.5:
                return
            last_hook_ts = now
            downloaded = d.get("downloaded_bytes") or 0
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            speed = d.get("speed") or 0
            if total > 0:
                pct = min(100.0, (downloaded / total) * 100.0)
                line = f"{pct:.0f}% ({format_bytes(downloaded)} / {format_bytes(total)})"
            else:
                line = f"{format_bytes(downloaded)}"
            if speed:
                line += f" • {format_bytes(speed)}/s"
            asyncio.run_coroutine_threadsafe(on_status(line, False), loop)

        await on_status(f"جاري التحميل ({q})...", False)
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
            await on_status(
                f"⚠️ الحجم ({format_bytes(last_size)}) > 2 GB — تجربة جودة أقل...",
                True,
            )

    raise RuntimeError(
        f"حجم الملف ({format_bytes(last_size)}) يتجاوز الحد الأقصى لتليجرام (2 GB)."
    )


class FastUploader:
    """
    Parallel MTProto file uploader using multiple concurrent MTProtoSender
    connections to the bot's DC with 512 KB pipelined chunks.
    Safe to share across multiple simultaneous file uploads.
    """

    def __init__(self, client: TelegramClient, connections: int = 12):
        self.client = client
        self.connections = max(2, connections)
        self.senders: list[MTProtoSender] = []
        self._start_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._start_lock:
            if self.senders:
                return
            dc = await self.client._get_dc(self.client.session.dc_id)

            async def make_sender() -> MTProtoSender:
                sender = MTProtoSender(
                    self.client.session.auth_key, loggers=self.client._log
                )
                await sender.connect(
                    self.client._connection(
                        dc.ip_address,
                        dc.port,
                        dc.id,
                        loggers=self.client._log,
                        proxy=self.client._proxy,
                        local_addr=self.client._local_addr,
                    )
                )
                init_req = InitConnectionRequest(
                    api_id=self.client.api_id,
                    device_model="yt-tg-bot",
                    system_version="Linux",
                    app_version="2.0",
                    system_lang_code="en",
                    lang_pack="",
                    lang_code="en",
                    query=GetConfigRequest(),
                )
                await sender.send(InvokeWithLayerRequest(LAYER, init_req))
                return sender

            results = await asyncio.gather(
                *(make_sender() for _ in range(self.connections)),
                return_exceptions=True,
            )
            for item in results:
                if isinstance(item, MTProtoSender):
                    self.senders.append(item)
                else:
                    print(f"[warn] parallel sender init failed: {item}", file=sys.stderr)

    async def close(self) -> None:
        async with self._start_lock:
            if not self.senders:
                return
            await asyncio.gather(
                *(s.disconnect() for s in self.senders), return_exceptions=True
            )
            self.senders.clear()

    async def upload(
        self,
        file_path: Path,
        file_name: str,
        workers_per_sender: int = 2,
        progress_callback: Optional[Callable[[int, int], Any]] = None,
    ) -> Union[InputFileBig, InputFile]:
        file_size = file_path.stat().st_size
        if file_size < 1024 * 1024:
            return await self.client.upload_file(
                str(file_path),
                file_name=file_name,
                part_size_kb=512,
                progress_callback=progress_callback,
            )

        try:
            if not self.senders:
                await self.start()
            if not self.senders:
                raise RuntimeError("No parallel senders available")
            return await self._parallel_upload(
                file_path,
                file_size,
                file_name,
                workers_per_sender=workers_per_sender,
                progress_callback=progress_callback,
            )
        except Exception as exc:
            print(
                f"[warn] parallel upload failed ({exc}), falling back to standard upload",
                file=sys.stderr,
            )
            return await self.client.upload_file(
                str(file_path),
                file_name=file_name,
                part_size_kb=512,
                progress_callback=progress_callback,
            )

    async def _parallel_upload(
        self,
        file_path: Path,
        file_size: int,
        file_name: str,
        workers_per_sender: int = 2,
        progress_callback: Optional[Callable[[int, int], Any]] = None,
    ) -> Union[InputFileBig, InputFile]:
        part_size = 512 * 1024  # 512 KB max allowed MTProto chunk size
        part_count = (file_size + part_size - 1) // part_size
        is_big = file_size > 10 * 1024 * 1024
        file_id = helpers.generate_random_long()

        md5_hex = ""
        if not is_big:
            def calc_md5() -> str:
                h = hashlib.md5()
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        h.update(chunk)
                return h.hexdigest()

            md5_hex = await asyncio.to_thread(calc_md5)

        senders = list(self.senders)
        total_workers = len(senders) * max(1, workers_per_sender)
        queue: asyncio.Queue[Optional[tuple[int, bytes]]] = asyncio.Queue(
            maxsize=total_workers
        )
        uploaded_bytes = 0
        progress_lock = asyncio.Lock()
        first_error: list[BaseException] = []

        async def producer() -> None:
            try:
                with open(file_path, "rb") as f:
                    for part_index in range(part_count):
                        if first_error:
                            break
                        data = await asyncio.to_thread(f.read, part_size)
                        if not data:
                            break
                        await queue.put((part_index, data))
            except BaseException as exc:
                if not first_error:
                    first_error.append(exc)
            finally:
                for _ in range(total_workers):
                    await queue.put(None)

        async def worker(sender: MTProtoSender) -> None:
            nonlocal uploaded_bytes
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return
                part_index, data = item
                try:
                    if first_error:
                        continue
                    req = (
                        SaveBigFilePartRequest(file_id, part_index, part_count, data)
                        if is_big
                        else SaveFilePartRequest(file_id, part_index, data)
                    )
                    for attempt in range(4):
                        try:
                            ok = await sender.send(req)
                            if ok:
                                break
                            raise RuntimeError(
                                f"SavePart returned False for part {part_index}"
                            )
                        except FloodWaitError as exc:
                            await asyncio.sleep(exc.seconds + 1)
                        except Exception:
                            if attempt == 3:
                                raise
                            await asyncio.sleep(1 + attempt)

                    async with progress_lock:
                        uploaded_bytes += len(data)
                        cur = uploaded_bytes
                    if progress_callback:
                        res = progress_callback(cur, file_size)
                        if asyncio.iscoroutine(res):
                            await res
                except BaseException as exc:
                    if not first_error:
                        first_error.append(exc)
                finally:
                    queue.task_done()

        worker_tasks = [
            asyncio.create_task(worker(sender))
            for sender in senders
            for _ in range(max(1, workers_per_sender))
        ]
        prod_task = asyncio.create_task(producer())
        await asyncio.gather(prod_task, *worker_tasks)

        if first_error:
            raise first_error[0]

        if is_big:
            return InputFileBig(file_id, part_count, file_name)
        return InputFile(file_id, part_count, file_name, md5_checksum=md5_hex)


def build_media_metadata(
    media_path: Path,
    info: dict[str, Any],
    quality: str,
    duration: int,
    width: int,
    height: int,
) -> tuple[str, str, list[Any], bool]:
    title = (info.get("title") or media_path.stem).strip()
    uploader = (info.get("uploader") or info.get("channel") or "YouTube").strip()
    if quality == "mp3":
        file_name = f"{sanitize_filename(title)}.mp3"
        mime_type = "audio/mpeg"
        attributes = [
            DocumentAttributeAudio(
                duration=duration,
                title=title[:64],
                performer=uploader[:64],
            ),
            DocumentAttributeFilename(file_name=file_name),
        ]
        return file_name, mime_type, attributes, False

    ext = media_path.suffix.lstrip(".") or "mp4"
    file_name = f"{sanitize_filename(title)}.{ext}"
    mime_type = mimetypes.guess_type(file_name)[0] or "video/mp4"
    attributes = [
        DocumentAttributeVideo(
            duration=duration,
            w=width,
            h=height,
            supports_streaming=True,
        ),
        DocumentAttributeFilename(file_name=file_name),
    ]
    return file_name, mime_type, attributes, True


async def deliver_uploaded_media(
    client: TelegramClient,
    chat_id: int,
    uploaded_file: Any,
    uploaded_thumb: Any,
    mime_type: str,
    attributes: list[Any],
    caption: str,
    supports_streaming: bool,
) -> None:
    for attempt in range(4):
        try:
            await client.send_file(
                chat_id,
                file=uploaded_file,
                mime_type=mime_type,
                caption=truncate_text(caption, 1020),
                thumb=uploaded_thumb,
                attributes=attributes,
                supports_streaming=supports_streaming,
            )
            return
        except FloodWaitError as exc:
            await asyncio.sleep(exc.seconds + 2)
        except Exception:
            if attempt == 3:
                raise
            await asyncio.sleep(2)


def is_bot_block_error(exc: Exception) -> bool:
    low = str(exc).lower()
    return "not a bot" in low or "sign in to confirm" in low


def short_error(exc: Exception) -> str:
    if is_bot_block_error(exc):
        return "يوتيوب طلب التحقق (أضف أو حدّث YOUTUBE_COOKIES في GitHub Secrets)."
    msg = str(exc).strip().splitlines()
    text = msg[-1] if msg else exc.__class__.__name__
    return truncate_text(text, 180)


@dataclass
class DownloadedItem:
    idx: int
    title: str
    media_path: Path
    thumb_path: Optional[Path]
    info: dict[str, Any]
    actual_quality: str
    duration: int
    width: int
    height: int
    work_dir: Path


@dataclass
class ReadyToSend:
    idx: int
    uploaded_file: Any
    uploaded_thumb: Any
    mime_type: str
    attributes: list[Any]
    caption: str
    supports_streaming: bool


@dataclass
class PipelineError:
    idx: int
    message: str
    skipped: bool = False


async def run_single_video(
    client: TelegramClient,
    fast_uploader: FastUploader,
    chat_id: int,
    url: str,
    quality: str,
    cookies_path: Optional[Path],
    status: StatusTracker,
) -> None:
    header = "🎬 جاري معالجة الفيديو..."
    work_dir = Path(tempfile.mkdtemp(prefix="ytdl_"))
    try:
        async def on_dl_status(line: str, force: bool) -> None:
            await status.update(f"{header}\n⬇️ {line}", force=force)

        media_path, info, actual_quality = await download_with_size_fallback(
            url=url,
            requested_quality=quality,
            work_dir=work_dir,
            cookies_path=cookies_path,
            on_status=on_dl_status,
        )

        title = (info.get("title") or "YouTube").strip()
        header = f"🎬 {truncate_text(title, 80)}"
        thumb_path, (duration, width, height) = await asyncio.gather(
            asyncio.to_thread(prepare_thumbnail, work_dir),
            asyncio.to_thread(probe_media, media_path, info),
        )
        file_name, mime_type, attributes, supports_streaming = build_media_metadata(
            media_path, info, actual_quality, duration, width, height
        )

        start_time = time.monotonic()
        last_cb_ts = 0.0

        async def upload_progress(current: int, total: int) -> None:
            nonlocal last_cb_ts
            now = time.monotonic()
            if now - last_cb_ts < 3.5 and current < total:
                return
            last_cb_ts = now
            elapsed = max(0.1, now - start_time)
            speed = current / elapsed
            pct = min(100.0, (current / max(1, total)) * 100.0)
            await status.update(
                f"{header}\n⬆️ جاري الرفع السريع: {pct:.0f}% ({format_bytes(current)} / {format_bytes(total)}) • {format_bytes(speed)}/s"
            )

        await status.update(
            f"{header}\n⬆️ جاري الرفع السريع ({format_bytes(media_path.stat().st_size)})...",
            force=True,
        )

        # Single video gets 3 pipelined workers per sender (12 * 3 = 36 chunks in flight)
        uploaded_file = await fast_uploader.upload(
            media_path,
            file_name=file_name,
            workers_per_sender=3,
            progress_callback=upload_progress,
        )
        uploaded_thumb = (
            await client.upload_file(str(thumb_path))
            if thumb_path and thumb_path.exists()
            else None
        )

        caption = title
        if actual_quality != quality and quality != "mp3":
            caption += f"\n({actual_quality}p)"

        await deliver_uploaded_media(
            client=client,
            chat_id=chat_id,
            uploaded_file=uploaded_file,
            uploaded_thumb=uploaded_thumb,
            mime_type=mime_type,
            attributes=attributes,
            caption=caption,
            supports_streaming=supports_streaming,
        )
        await status.update("✅ تم التحميل والرفع بنجاح!", force=True)
    except Exception as exc:
        print(f"[error] single video ({url}): {exc}", file=sys.stderr)
        await status.update(f"❌ فشل التحميل:\n• {short_error(exc)}", force=True)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def run_playlist_pipeline(
    client: TelegramClient,
    fast_uploader: FastUploader,
    chat_id: int,
    playlist_title: str,
    items: list[dict[str, str]],
    quality: str,
    was_capped: bool,
    max_playlist_videos: int,
    cookies_path: Optional[Path],
    status: StatusTracker,
    download_concurrency: int = 3,
    upload_concurrency: int = 2,
) -> None:
    total = len(items)
    dl_workers_n = min(max(1, download_concurrency), total)
    up_workers_n = min(max(1, upload_concurrency), total)

    dashboard = PlaylistDashboard(
        status=status,
        playlist_title=playlist_title,
        total=total,
        was_capped=was_capped,
        max_videos=max_playlist_videos,
    )
    await status.update(dashboard.render(), force=True)

    # Pre-warm parallel MTProto senders while initial downloads run
    prewarm_task = asyncio.create_task(fast_uploader.start())

    dl_queue: asyncio.Queue[Optional[tuple[int, dict[str, str]]]] = asyncio.Queue()
    for idx, item in enumerate(items, start=1):
        await dl_queue.put((idx, item))
    for _ in range(dl_workers_n):
        await dl_queue.put(None)

    # Bound total live temp dirs on disk
    disk_sem = asyncio.Semaphore(dl_workers_n + up_workers_n)
    up_queue: asyncio.Queue[Optional[Union[DownloadedItem, PipelineError]]] = asyncio.Queue(
        maxsize=up_workers_n + 2
    )

    ready_map: dict[int, Union[ReadyToSend, PipelineError]] = {}
    ready_event = asyncio.Event()

    abort_event = asyncio.Event()
    bot_block_lock = asyncio.Lock()
    consecutive_bot_blocks = 0

    def publish_ready(idx: int, obj: Union[ReadyToSend, PipelineError]) -> None:
        ready_map[idx] = obj
        ready_event.set()

    async def download_worker() -> None:
        nonlocal consecutive_bot_blocks
        while True:
            entry = await dl_queue.get()
            if entry is None:
                dl_queue.task_done()
                return
            idx, item = entry
            try:
                if abort_event.is_set():
                    publish_ready(idx, PipelineError(idx=idx, message="", skipped=True))
                    continue

                await disk_sem.acquire()
                work_dir = Path(tempfile.mkdtemp(prefix=f"ytdl_{idx}_"))
                handed_off = False
                try:
                    async def on_dl_status(line: str, force: bool, _idx: int = idx) -> None:
                        await dashboard.set_active(_idx, "⬇️", line, force=force)

                    media_path, info, actual_quality = await download_with_size_fallback(
                        url=item["url"],
                        requested_quality=quality,
                        work_dir=work_dir,
                        cookies_path=cookies_path,
                        on_status=on_dl_status,
                    )
                    title = (info.get("title") or item.get("title") or "YouTube").strip()
                    thumb_path, (duration, width, height) = await asyncio.gather(
                        asyncio.to_thread(prepare_thumbnail, work_dir),
                        asyncio.to_thread(probe_media, media_path, info),
                    )
                    async with bot_block_lock:
                        consecutive_bot_blocks = 0

                    await dashboard.set_active(
                        idx, "⏳", f"جاهز للرفع ({format_bytes(media_path.stat().st_size)})"
                    )
                    await up_queue.put(
                        DownloadedItem(
                            idx=idx,
                            title=title,
                            media_path=media_path,
                            thumb_path=thumb_path,
                            info=info,
                            actual_quality=actual_quality,
                            duration=duration,
                            width=width,
                            height=height,
                            work_dir=work_dir,
                        )
                    )
                    handed_off = True
                except Exception as exc:
                    err_msg = short_error(exc)
                    print(f"[error] dl #{idx}/{total} ({item['url']}): {exc}", file=sys.stderr)
                    if is_bot_block_error(exc):
                        async with bot_block_lock:
                            consecutive_bot_blocks += 1
                            if consecutive_bot_blocks >= 2:
                                abort_event.set()
                    else:
                        async with bot_block_lock:
                            consecutive_bot_blocks = 0
                    await dashboard.mark_done(idx, succeeded=False)
                    publish_ready(idx, PipelineError(idx=idx, message=f"• #{idx}: {err_msg}"))
                finally:
                    if not handed_off:
                        shutil.rmtree(work_dir, ignore_errors=True)
                        disk_sem.release()
            finally:
                dl_queue.task_done()

    async def upload_worker() -> None:
        await prewarm_task
        while True:
            item = await up_queue.get()
            if item is None:
                up_queue.task_done()
                return
            try:
                if isinstance(item, PipelineError):
                    publish_ready(item.idx, item)
                    continue

                try:
                    file_name, mime_type, attributes, supports_streaming = build_media_metadata(
                        item.media_path,
                        item.info,
                        item.actual_quality,
                        item.duration,
                        item.width,
                        item.height,
                    )
                    file_size = item.media_path.stat().st_size
                    start_time = time.monotonic()
                    last_cb_ts = 0.0

                    async def on_up_progress(
                        current: int, total_b: int, _idx: int = item.idx
                    ) -> None:
                        nonlocal last_cb_ts
                        now = time.monotonic()
                        if now - last_cb_ts < 3.5 and current < total_b:
                            return
                        last_cb_ts = now
                        elapsed = max(0.1, now - start_time)
                        speed = current / elapsed
                        pct = min(100.0, (current / max(1, total_b)) * 100.0)
                        await dashboard.set_active(
                            _idx, "⬆️", f"{pct:.0f}% • {format_bytes(speed)}/s"
                        )

                    await dashboard.set_active(
                        item.idx, "⬆️", f"0% ({format_bytes(file_size)})"
                    )
                    uploaded_file = await fast_uploader.upload(
                        item.media_path,
                        file_name=file_name,
                        workers_per_sender=2,
                        progress_callback=on_up_progress,
                    )
                    uploaded_thumb = (
                        await client.upload_file(str(item.thumb_path))
                        if item.thumb_path and item.thumb_path.exists()
                        else None
                    )

                    caption = f"[{item.idx}/{total}] {item.title}"
                    if item.actual_quality != quality and quality != "mp3":
                        caption += f"\n({item.actual_quality}p)"

                    await dashboard.set_active(item.idx, "📨", "بانتظار دوره للإرسال")
                    publish_ready(
                        item.idx,
                        ReadyToSend(
                            idx=item.idx,
                            uploaded_file=uploaded_file,
                            uploaded_thumb=uploaded_thumb,
                            mime_type=mime_type,
                            attributes=attributes,
                            caption=caption,
                            supports_streaming=supports_streaming,
                        ),
                    )
                except Exception as exc:
                    err_msg = short_error(exc)
                    print(f"[error] upload #{item.idx}/{total}: {exc}", file=sys.stderr)
                    await dashboard.mark_done(item.idx, succeeded=False)
                    publish_ready(
                        item.idx,
                        PipelineError(idx=item.idx, message=f"• #{item.idx}: {err_msg}"),
                    )
                finally:
                    # Free local disk immediately once uploaded to Telegram staging
                    shutil.rmtree(item.work_dir, ignore_errors=True)
                    disk_sem.release()
            finally:
                up_queue.task_done()

    succeeded = 0
    failed: list[str] = []

    async def ordered_sender() -> None:
        nonlocal succeeded
        for expected_idx in range(1, total + 1):
            while expected_idx not in ready_map:
                ready_event.clear()
                if expected_idx in ready_map:
                    break
                await ready_event.wait()

            entry = ready_map.pop(expected_idx)
            if isinstance(entry, PipelineError):
                if not entry.skipped and entry.message:
                    failed.append(entry.message)
                continue

            try:
                await deliver_uploaded_media(
                    client=client,
                    chat_id=chat_id,
                    uploaded_file=entry.uploaded_file,
                    uploaded_thumb=entry.uploaded_thumb,
                    mime_type=entry.mime_type,
                    attributes=entry.attributes,
                    caption=entry.caption,
                    supports_streaming=entry.supports_streaming,
                )
                succeeded += 1
                await dashboard.mark_done(expected_idx, succeeded=True)
            except Exception as exc:
                err_msg = short_error(exc)
                print(f"[error] send #{expected_idx}/{total}: {exc}", file=sys.stderr)
                failed.append(f"• #{expected_idx}: {err_msg}")
                await dashboard.mark_done(expected_idx, succeeded=False)

            if expected_idx < total:
                await asyncio.sleep(0.5)

    dl_tasks = [asyncio.create_task(download_worker()) for _ in range(dl_workers_n)]
    up_tasks = [asyncio.create_task(upload_worker()) for _ in range(up_workers_n)]
    send_task = asyncio.create_task(ordered_sender())

    await asyncio.gather(*dl_tasks)
    for _ in range(up_workers_n):
        await up_queue.put(None)
    await asyncio.gather(*up_tasks)
    await send_task

    aborted = abort_event.is_set()
    lines = [
        f"{'⛔ توقف' if aborted else '✅ انتهى'} تحميل البلايليست: {truncate_text(playlist_title, 100)}",
        f"✔️ نجح: {succeeded} / {total}",
    ]
    if aborted:
        lines.append(
            "⚠️ تم إيقاف الباقي لتفادي استهلاك الرصيد — أضف أو حدّث YOUTUBE_COOKIES في GitHub Secrets."
        )
    if failed:
        lines.append(f"❌ فشل: {len(failed)}")
        lines.extend(failed[:10])
        if len(failed) > 10:
            lines.append(f"... و {len(failed) - 10} أخطاء أخرى.")
    await status.update("\n".join(lines), force=True)


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
    upload_connections = env_int("UPLOAD_CONNECTIONS", 12)
    download_concurrency = env_int("DOWNLOAD_CONCURRENCY", 3)
    upload_concurrency = env_int("UPLOAD_CONCURRENCY", 2)
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
    fast_uploader = FastUploader(client, connections=upload_connections)

    try:
        await client.start(bot_token=bot_token)
        status = StatusTracker(client, chat_id, status_msg_id)

        if mode == "playlist":
            await status.update(
                "🔍 جاري فحص البلايليست واستخراج قائمة الفيديوهات...", force=True
            )
            playlist_title, items, was_capped = await asyncio.to_thread(
                extract_playlist_entries,
                video_url,
                playlist_items,
                max_playlist_videos,
                cookies_path,
            )
            if not items:
                await status.update(
                    "⚠️ لم يتم العثور على أي فيديوهات متاحة في البلايليست.", force=True
                )
                return 0
        else:
            playlist_title = ""
            items = [{"url": video_url, "title": ""}]
            was_capped = False

        if len(items) == 1:
            await run_single_video(
                client=client,
                fast_uploader=fast_uploader,
                chat_id=chat_id,
                url=items[0]["url"],
                quality=quality,
                cookies_path=cookies_path,
                status=status,
            )
        else:
            await run_playlist_pipeline(
                client=client,
                fast_uploader=fast_uploader,
                chat_id=chat_id,
                playlist_title=playlist_title,
                items=items,
                quality=quality,
                was_capped=was_capped,
                max_playlist_videos=max_playlist_videos,
                cookies_path=cookies_path,
                status=status,
                download_concurrency=download_concurrency,
                upload_concurrency=upload_concurrency,
            )

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
        await fast_uploader.close()
        await client.disconnect()
        if cookies_path and cookies_path.exists():
            cookies_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
