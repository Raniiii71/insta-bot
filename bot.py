#!/usr/bin/env python3
"""
Instagram Telegram Bot - Growth + Earnings Edition

Features
- Force join channel
- .env config
- cookies.txt support
- Instagram link cleaning
- Reel/post/media download when yt-dlp can access it
- Optional audio extraction
- Public profile info + profile picture
- Referral system with points
- Daily limits
- Premium users with expiry
- Sponsor/ad message after successful download
- Leaderboard / stats / points
- Admin tools: broadcast, ban, unban, setad, upgrade, user
- SQLite database

Limitations
- Does not bypass private, removed, or inaccessible content
- Instagram may still block/rate-limit some hosts or expired cookies
"""

import asyncio
import logging
import os
import re
import shutil
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

from dotenv import load_dotenv
load_dotenv()

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import yt_dlp

try:
    import instaloader
except Exception:
    instaloader = None


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "instasaavingbot").strip("@")
OWNER_ID = os.getenv("OWNER_ID", "").strip()
ADMIN_IDS = {x.strip() for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "49"))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "180"))
WORKERS = max(1, int(os.getenv("WORKERS", "2")))
ENABLE_PROFILE_LOOKUP = os.getenv("ENABLE_PROFILE_LOOKUP", "true").lower() == "true"
DB_PATH = os.getenv("DB_PATH", "bot_data.db").strip()
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.txt").strip()
ENABLE_AUDIO_EXTRACTION = os.getenv("ENABLE_AUDIO_EXTRACTION", "true").lower() == "true"
KEEP_CAPTIONS_SHORT = os.getenv("KEEP_CAPTIONS_SHORT", "true").lower() == "true"
FREE_DAILY_LIMIT = int(os.getenv("FREE_DAILY_LIMIT", "8"))
PREMIUM_DAILY_LIMIT = int(os.getenv("PREMIUM_DAILY_LIMIT", "60"))
REFERRAL_BONUS_POINTS = int(os.getenv("REFERRAL_BONUS_POINTS", "10"))
DOWNLOAD_POINTS = int(os.getenv("DOWNLOAD_POINTS", "1"))
AD_MESSAGE_DEFAULT = os.getenv("AD_MESSAGE_DEFAULT", "📢 Join our channel for updates and premium perks.")

if OWNER_ID:
    ADMIN_IDS.add(OWNER_ID)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")
if not REQUIRED_CHANNEL:
    raise RuntimeError("REQUIRED_CHANNEL is missing. Example: @yourchannel")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("insta_growth_earnings_bot")

INSTAGRAM_URL_RE = re.compile(r"(https?://[^\s]+instagram\.com[^\s]*)", re.IGNORECASE)
PROFILE_RE = re.compile(r"^(?:@)?([A-Za-z0-9._]{1,30})$")
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(WORKERS)


@dataclass
class MediaItem:
    file_path: Path
    kind: str
    caption: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today_str() -> str:
    return now_utc().date().isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def db_connect():
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TEXT,
                referred_by INTEGER,
                downloads_count INTEGER DEFAULT 0,
                referrals_count INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                points INTEGER DEFAULT 0,
                premium_until TEXT,
                downloads_today INTEGER DEFAULT 0,
                downloads_day TEXT,
                last_active_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()

        cur.execute("INSERT OR IGNORE INTO settings(key, value) VALUES('ad_message', ?)", (AD_MESSAGE_DEFAULT,))
        conn.commit()


def upsert_user(user_id: int, username: str, first_name: str, referred_by: Optional[int] = None) -> None:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, referred_by FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        now = now_utc().isoformat()

        if row:
            if row[1] is None and referred_by and referred_by != user_id:
                cur.execute(
                    "UPDATE users SET username=?, first_name=?, referred_by=?, last_active_at=? WHERE user_id=?",
                    (username, first_name, referred_by, now, user_id),
                )
                cur.execute(
                    "UPDATE users SET referrals_count = referrals_count + 1, points = points + ? WHERE user_id=?",
                    (REFERRAL_BONUS_POINTS, referred_by),
                )
            else:
                cur.execute(
                    "UPDATE users SET username=?, first_name=?, last_active_at=? WHERE user_id=?",
                    (username, first_name, now, user_id),
                )
        else:
            cur.execute(
                """
                INSERT INTO users (
                    user_id, username, first_name, joined_at, referred_by,
                    downloads_day, last_active_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id, username, first_name, now,
                    referred_by if referred_by != user_id else None,
                    today_str(), now
                ),
            )
            if referred_by and referred_by != user_id:
                cur.execute(
                    "UPDATE users SET referrals_count = referrals_count + 1, points = points + ? WHERE user_id=?",
                    (REFERRAL_BONUS_POINTS, referred_by),
                )
        conn.commit()


def refresh_daily_counter_if_needed(user_id: int) -> None:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT downloads_day FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        td = today_str()
        if row and row[0] != td:
            cur.execute(
                "UPDATE users SET downloads_today=0, downloads_day=? WHERE user_id=?",
                (td, user_id),
            )
            conn.commit()


def increment_download(user_id: int) -> None:
    refresh_daily_counter_if_needed(user_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET downloads_count = downloads_count + 1,
                downloads_today = downloads_today + 1,
                points = points + ?,
                last_active_at = ?
            WHERE user_id=?
            """,
            (DOWNLOAD_POINTS, now_utc().isoformat(), user_id),
        )
        conn.commit()


def get_user_stats(user_id: int) -> Optional[Dict]:
    refresh_daily_counter_if_needed(user_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, username, first_name, joined_at, referred_by, downloads_count,
                   referrals_count, is_banned, points, premium_until, downloads_today,
                   downloads_day, last_active_at
            FROM users WHERE user_id=?
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "user_id": row[0],
            "username": row[1],
            "first_name": row[2],
            "joined_at": row[3],
            "referred_by": row[4],
            "downloads_count": row[5],
            "referrals_count": row[6],
            "is_banned": row[7],
            "points": row[8],
            "premium_until": row[9],
            "downloads_today": row[10],
            "downloads_day": row[11],
            "last_active_at": row[12],
        }


def get_global_stats() -> Dict:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        users = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(downloads_count), 0) FROM users")
        downloads = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(points), 0) FROM users")
        points = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM users
            WHERE premium_until IS NOT NULL
        """)
        premium_total = cur.fetchone()[0]
        return {"users": users, "downloads": downloads, "points": points, "premium_total": premium_total}


def top_referrers(limit: int = 10) -> List[Tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT first_name, username, user_id, referrals_count, points
            FROM users
            ORDER BY referrals_count DESC, points DESC, downloads_count DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()


def set_ban_status(user_id: int, is_banned: bool) -> None:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if is_banned else 0, user_id))
        conn.commit()


def is_user_banned(user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return bool(row[0]) if row else False


def get_all_user_ids(limit: int = 5000) -> List[int]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE is_banned=0 LIMIT ?", (limit,))
        return [x[0] for x in cur.fetchall()]


def get_setting(key: str, default: str = "") -> str:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def is_admin(user_id: int) -> bool:
    return str(user_id) in ADMIN_IDS


def is_premium(stats: Optional[Dict]) -> bool:
    if not stats:
        return False
    until = parse_dt(stats.get("premium_until"))
    return bool(until and until > now_utc())


def get_daily_limit(stats: Optional[Dict]) -> int:
    return PREMIUM_DAILY_LIMIT if is_premium(stats) else FREE_DAILY_LIMIT


def can_download_today(stats: Optional[Dict]) -> Tuple[bool, int]:
    if not stats:
        return False, FREE_DAILY_LIMIT
    limit = get_daily_limit(stats)
    return stats["downloads_today"] < limit, limit


def set_premium_for_days(user_id: int, days: int) -> None:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT premium_until FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        base = parse_dt(row[0]) if row and row[0] else None
        current = now_utc()
        start = base if base and base > current else current
        new_until = start + timedelta(days=days)
        cur.execute(
            "UPDATE users SET premium_until=? WHERE user_id=?",
            (new_until.isoformat(), user_id),
        )
        conn.commit()


def premium_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Download Link", callback_data="help")],
        [InlineKeyboardButton("👤 Profile / DP", callback_data="profile_help"),
         InlineKeyboardButton("🎁 Refer", callback_data="invite")],
        [InlineKeyboardButton("📊 My Stats", callback_data="my_stats"),
         InlineKeyboardButton("🏆 Leaderboard", callback_data="leaderboard")],
        [InlineKeyboardButton("⭐ Premium", callback_data="premium_info")],
        [InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
    ])


def join_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
        [InlineKeyboardButton("✅ I Joined", callback_data="check_join")],
    ])


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Panel Stats", callback_data="admin_stats"),
         InlineKeyboardButton("🏆 Top Inviters", callback_data="leaderboard")],
    ])


def human_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 ** 2:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 ** 2):.1f} MB"


def is_instagram_url(text: str) -> Optional[str]:
    if not text:
        return None
    m = INSTAGRAM_URL_RE.search(text.strip())
    return m.group(1) if m else None


def make_temp_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="insta_growth_bot_"))


def cleanup_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def too_large(path: Path) -> bool:
    return path.stat().st_size > MAX_FILE_SIZE_MB * 1024 * 1024


def shorten_caption(text: str, max_len: int = 900) -> str:
    text = (text or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def clean_instagram_url(url: str) -> str:
    url = unquote(url.strip())
    parsed = urlsplit(url)
    qs = parse_qs(parsed.query)
    for key in ("u", "url"):
        if key in qs and qs[key]:
            inner = qs[key][0]
            if "instagram.com" in inner:
                url = inner
                parsed = urlsplit(url)
                break

    parsed = urlsplit(url)
    cleaned = urlunsplit((parsed.scheme or "https", parsed.netloc, parsed.path, "", ""))
    cleaned = cleaned.rstrip("/")
    return cleaned + "/"


def ydl_base_opts(workdir: Path, use_cookies: bool = True) -> Dict:
    opts = {
        "outtmpl": str(workdir / "%(title).80s_%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
        "windowsfilenames": True,
        "socket_timeout": 30,
        "retries": 2,
        "fragment_retries": 2,
        "concurrent_fragment_downloads": 1,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
    }
    if use_cookies and COOKIE_FILE and Path(COOKIE_FILE).exists():
        opts["cookiefile"] = COOKIE_FILE
    return opts


def collect_downloaded_files(workdir: Path) -> List[Path]:
    if not workdir.exists():
        return []
    return sorted([p for p in workdir.iterdir() if p.is_file()])


def get_public_profile(username: str) -> Optional[Dict]:
    if not ENABLE_PROFILE_LOOKUP or instaloader is None:
        return None
    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_comments=False,
        save_metadata=False,
        quiet=True,
    )
    profile = instaloader.Profile.from_username(loader.context, username)
    return {
        "username": profile.username,
        "full_name": profile.full_name or "—",
        "biography": profile.biography or "—",
        "followers": profile.followers,
        "followees": profile.followees,
        "posts": profile.mediacount,
        "is_private": profile.is_private,
        "is_verified": profile.is_verified,
    }


def download_profile_pic(username: str, workdir: Path) -> Optional[Path]:
    if not ENABLE_PROFILE_LOOKUP or instaloader is None:
        return None
    loader = instaloader.Instaloader(
        dirname_pattern=str(workdir),
        filename_pattern="{profile}",
        download_pictures=True,
        download_videos=False,
        download_video_thumbnails=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )
    profile = instaloader.Profile.from_username(loader.context, username)
    loader.download_profilepic(profile)
    files = collect_downloaded_files(workdir)
    images = [p for p in files if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}]
    return images[0] if images else None


def _download_with_opts(url: str, workdir: Path, use_cookies: bool = True) -> Dict:
    info_opts = ydl_base_opts(workdir, use_cookies=use_cookies)
    with yt_dlp.YoutubeDL(info_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    dl_opts = ydl_base_opts(workdir, use_cookies=use_cookies)
    dl_opts.update({
        "format": f"best[filesize<{MAX_FILE_SIZE_MB}M]/best",
        "merge_output_format": "mp4",
    })
    with yt_dlp.YoutubeDL(dl_opts) as ydl:
        ydl.download([url])
    return info


def _extract_audio(url: str, audio_dir: Path, use_cookies: bool = True) -> None:
    audio_opts = ydl_base_opts(audio_dir, use_cookies=use_cookies)
    audio_opts.update({
        "format": "bestaudio/best",
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
    })
    with yt_dlp.YoutubeDL(audio_opts) as ydl:
        ydl.download([url])


def download_media_bundle(url: str, workdir: Path) -> Tuple[List[MediaItem], str]:
    items: List[MediaItem] = []
    cleaned_url = clean_instagram_url(url)

    last_err = None
    info = None
    used_cookies = False

    try:
        if COOKIE_FILE and Path(COOKIE_FILE).exists():
            info = _download_with_opts(cleaned_url, workdir, use_cookies=True)
            used_cookies = True
        else:
            raise FileNotFoundError("cookies.txt not found")
    except Exception as e:
        last_err = e
        try:
            info = _download_with_opts(cleaned_url, workdir, use_cookies=False)
            used_cookies = False
        except Exception as e2:
            last_err = e2
            raise RuntimeError(str(last_err))

    title = (info or {}).get("title") or "Instagram media"
    uploader = (info or {}).get("uploader") or ""
    extractor = (info or {}).get("extractor_key") or "Instagram"

    files = collect_downloaded_files(workdir)
    media_files = [p for p in files if p.suffix.lower() in {".mp4", ".jpg", ".jpeg", ".png", ".webp"}]

    first_video = next((p for p in media_files if p.suffix.lower() == ".mp4"), None)
    if first_video and ENABLE_AUDIO_EXTRACTION:
        audio_dir = workdir / "audio"
        audio_dir.mkdir(exist_ok=True)
        try:
            _extract_audio(cleaned_url, audio_dir, use_cookies=used_cookies)
        except Exception:
            pass

    all_files = collect_downloaded_files(workdir)
    if (workdir / "audio").exists():
        all_files += collect_downloaded_files(workdir / "audio")

    seen = set()
    base_caption = f"@{uploader}" if uploader else title
    if KEEP_CAPTIONS_SHORT:
        base_caption = shorten_caption(base_caption, 200)

    for path in all_files:
        if path in seen:
            continue
        seen.add(path)
        suffix = path.suffix.lower()
        if suffix == ".mp4":
            items.append(MediaItem(path, "video", f"🎬 {base_caption}"))
        elif suffix in {".jpg", ".jpeg", ".png", ".webp"}:
            items.append(MediaItem(path, "photo", f"🖼️ {base_caption}"))
        elif suffix == ".mp3":
            items.append(MediaItem(path, "audio", f"🎵 Audio from: {base_caption}"))

    if not items:
        raise RuntimeError("No downloadable media files were produced.")
    summary = f"{title}\nSource: {extractor}\nURL: {cleaned_url}"
    return items, summary


async def is_user_joined(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in {"member", "administrator", "creator"}
    except (BadRequest, Forbidden, TelegramError):
        logger.exception("Join check failed")
        return False


async def require_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False

    if is_user_banned(user.id):
        text = "🚫 You are blocked from using this bot."
        if update.message:
            await update.message.reply_text(text)
        elif update.callback_query:
            await update.callback_query.answer(text, show_alert=True)
        return False

    joined = await is_user_joined(context, user.id)
    if joined:
        return True

    text = (
        "🔒 Join the channel first to use this bot.\n\n"
        f"Required channel: {REQUIRED_CHANNEL}\n"
        "Then tap *I Joined*."
    )
    if update.callback_query:
        await update.callback_query.answer("Please join first.", show_alert=True)
        await update.callback_query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=join_keyboard())
    elif update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=join_keyboard())
    return False


async def send_media_items(update: Update, context: ContextTypes.DEFAULT_TYPE, items: List[MediaItem]) -> None:
    chat_id = update.effective_chat.id
    sent_any = False
    for item in items:
        if too_large(item.file_path):
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ Skipped {item.file_path.name} because file is too large ({human_size(item.file_path.stat().st_size)})."
            )
            continue

        with item.file_path.open("rb") as f:
            if item.kind == "video":
                await context.bot.send_video(chat_id=chat_id, video=f, caption=item.caption)
                sent_any = True
            elif item.kind == "photo":
                await context.bot.send_photo(chat_id=chat_id, photo=f, caption=item.caption)
                sent_any = True
            elif item.kind == "audio":
                await context.bot.send_audio(chat_id=chat_id, audio=f, caption=item.caption)
                sent_any = True

    if not sent_any:
        raise RuntimeError("All produced files were too large to send.")


async def send_success_footer(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    bot_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    ad_message = get_setting("ad_message", AD_MESSAGE_DEFAULT)
    stats = get_user_stats(user_id) or {}
    premium_text = "⭐ Premium active" if is_premium(stats) else "⭐ Upgrade with /premium"

    text = (
        "✅ Download completed.\n\n"
        f"🎁 Invite friends: {bot_link}\n"
        f"💎 {premium_text}\n"
        f"📢 {ad_message}\n"
        f"📢 Join updates: https://t.me/{REQUIRED_CHANNEL.lstrip('@')}"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=text)


def format_failure_message(reason: str, url: str) -> str:
    clean = clean_instagram_url(url)
    return (
        "❌ Could not download that link.\n"
        "Possible reasons:\n"
        "• The content is private, removed, or login-protected\n"
        "• Instagram rate-limited or blocked access temporarily\n"
        "• cookies.txt is missing or expired\n"
        "• The link is invalid or unsupported\n\n"
        f"Cleaned URL: {clean}\n\n"
        f"Reason: {reason}"
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    referred_by = None
    if context.args:
        arg = context.args[0].strip()
        if arg.isdigit():
            referred_by = int(arg)

    upsert_user(
        user_id=user.id,
        username=user.username or "",
        first_name=user.first_name or "User",
        referred_by=referred_by,
    )

    text = (
        "👑 *Welcome to the Instagram Growth Bot*\n\n"
        "Features:\n"
        "• Reel / post / media download\n"
        "• Referral points + leaderboard\n"
        "• Daily free limit + premium\n"
        "• Public profile info / DP\n"
        "• Admin panel + sponsor/ad message\n\n"
        "Join the channel first, then send an Instagram link."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=join_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    text = (
        "📌 *How to use*\n\n"
        "1. Send any Instagram reel/post/media URL\n"
        "2. I clean the link automatically\n"
        "3. I try with cookies first, then fallback\n\n"
        "*Limits*\n"
        f"• Free daily limit: {FREE_DAILY_LIMIT}\n"
        f"• Premium daily limit: {PREMIUM_DAILY_LIMIT}\n\n"
        "*Profile mode*\n"
        "Use `/profile username` or send `@username`\n\n"
        "⚠️ This bot only works for content yt-dlp can access."
    )
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=premium_menu())
    else:
        await update.callback_query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=premium_menu())


async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    if not context.args:
        await update.message.reply_text("Usage: /profile username")
        return

    username = context.args[0].strip().lstrip("@")
    if not PROFILE_RE.match(username):
        await update.message.reply_text("Please send a valid Instagram username.")
        return

    await update.message.chat.send_action(action=ChatAction.TYPING)
    workdir = make_temp_dir()
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, get_public_profile, username)
        if not info:
            await update.message.reply_text("Profile lookup is unavailable right now.")
            return

        caption = (
            f"👤 @{info['username']}\n"
            f"📝 Name: {info['full_name']}\n"
            f"📚 Posts: {info['posts']}\n"
            f"👥 Followers: {info['followers']}\n"
            f"➡️ Following: {info['followees']}\n"
            f"🔒 Private: {'Yes' if info['is_private'] else 'No'}\n"
            f"✔️ Verified: {'Yes' if info['is_verified'] else 'No'}\n\n"
            f"Bio:\n{shorten_caption(info['biography'], 900)}"
        )

        pic_path = await loop.run_in_executor(None, download_profile_pic, username, workdir)
        if pic_path and pic_path.exists() and not too_large(pic_path):
            with pic_path.open("rb") as f:
                await update.message.reply_photo(photo=f, caption=caption)
        else:
            await update.message.reply_text(caption)
    except Exception as e:
        logger.exception("Profile failed")
        await update.message.reply_text(f"Could not fetch public profile.\nReason: {e}")
    finally:
        cleanup_dir(workdir)


async def my_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    stats = get_user_stats(update.effective_user.id)
    if not stats:
        await update.message.reply_text("No stats found yet.")
        return

    invite_link = f"https://t.me/{BOT_USERNAME}?start={update.effective_user.id}"
    premium_line = stats["premium_until"] if is_premium(stats) else "No"
    limit = get_daily_limit(stats)

    text = (
        f"📊 *Your Stats*\n\n"
        f"👤 Name: {stats['first_name']}\n"
        f"📥 Total downloads: {stats['downloads_count']}\n"
        f"📅 Downloads today: {stats['downloads_today']} / {limit}\n"
        f"🎁 Referrals: {stats['referrals_count']}\n"
        f"🏅 Points: {stats['points']}\n"
        f"⭐ Premium until: {premium_line}\n"
        f"🔗 Invite link:\n`{invite_link}`"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def invite_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    invite_link = f"https://t.me/{BOT_USERNAME}?start={update.effective_user.id}"
    stats = get_user_stats(update.effective_user.id) or {"referrals_count": 0, "points": 0}
    text = (
        "🎁 *Refer & Earn*\n\n"
        f"Your referrals: *{stats['referrals_count']}*\n"
        f"Your points: *{stats['points']}*\n\n"
        f"Each successful referral gives *{REFERRAL_BONUS_POINTS}* points.\n\n"
        "Share this link:\n"
        f"`{invite_link}`"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    rows = top_referrers(10)
    if not rows:
        await update.message.reply_text("No leaderboard data yet.")
        return
    lines = ["🏆 *Top Inviters*\n"]
    for i, (first_name, username, user_id, refs, points) in enumerate(rows, start=1):
        display = f"@{username}" if username else first_name or str(user_id)
        lines.append(f"{i}. {display} — {refs} invites • {points} pts")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    text = (
        "⭐ *Premium Plans*\n\n"
        "Suggested plans:\n"
        "• Weekly: ₹49\n"
        "• Monthly: ₹99\n\n"
        "Premium benefits:\n"
        f"• Up to {PREMIUM_DAILY_LIMIT} downloads per day\n"
        "• Priority support\n"
        "• Better usage limits\n\n"
        "Message the admin or channel for payment and activation."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_join(update, context):
        return
    stats = get_user_stats(update.effective_user.id)
    pts = stats["points"] if stats else 0
    await update.message.reply_text(f"🏅 You have {pts} points.")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    joined = await is_user_joined(context, update.effective_user.id)
    global_stats = get_global_stats()
    text = (
        "🤖 Bot status: online\n"
        f"📢 Required channel: {REQUIRED_CHANNEL}\n"
        f"✅ Joined: {'Yes' if joined else 'No'}\n"
        f"👥 Total users: {global_stats['users']}\n"
        f"📥 Total downloads: {global_stats['downloads']}\n"
        f"🏅 Total points: {global_stats['points']}\n"
        f"⭐ Premium users: {global_stats['premium_total']}\n"
        f"⚙️ Workers: {WORKERS}\n"
        f"🍪 Cookies file: {'Found' if Path(COOKIE_FILE).exists() else 'Not found'}"
    )
    await update.message.reply_text(text)


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    text = (
        "🧪 Debug info\n\n"
        f"COOKIE_FILE: {COOKIE_FILE}\n"
        f"Cookie exists: {Path(COOKIE_FILE).exists()}\n"
        f"DB_PATH: {DB_PATH}\n"
        f"WORKERS: {WORKERS}\n"
        f"FREE_DAILY_LIMIT: {FREE_DAILY_LIMIT}\n"
        f"PREMIUM_DAILY_LIMIT: {PREMIUM_DAILY_LIMIT}\n"
        f"ENABLE_PROFILE_LOOKUP: {ENABLE_PROFILE_LOOKUP}\n"
        f"ENABLE_AUDIO_EXTRACTION: {ENABLE_AUDIO_EXTRACTION}"
    )
    await update.message.reply_text(text)


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    stats = get_global_stats()
    text = (
        "🛠️ *Admin Panel*\n\n"
        f"👥 Users: {stats['users']}\n"
        f"📥 Downloads: {stats['downloads']}\n"
        f"🏅 Points: {stats['points']}\n"
        f"⭐ Premium users: {stats['premium_total']}\n\n"
        "Commands:\n"
        "/broadcast your message\n"
        "/ban user_id\n"
        "/unban user_id\n"
        "/user user_id\n"
        "/upgrade user_id days\n"
        "/setad your sponsor message\n"
        "/debug"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_keyboard())


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast your message")
        return

    msg = " ".join(context.args)
    user_ids = get_all_user_ids()
    sent = 0
    failed = 0
    progress = await update.message.reply_text(f"Broadcast started to {len(user_ids)} users...")

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=msg)
            sent += 1
        except Exception:
            failed += 1

    await progress.edit_text(f"✅ Broadcast finished.\nSent: {sent}\nFailed: {failed}")


async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /ban user_id")
        return
    uid = int(context.args[0])
    set_ban_status(uid, True)
    await update.message.reply_text(f"User {uid} banned.")


async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /unban user_id")
        return
    uid = int(context.args[0])
    set_ban_status(uid, False)
    await update.message.reply_text(f"User {uid} unbanned.")


async def user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /user user_id")
        return
    uid = int(context.args[0])
    stats = get_user_stats(uid)
    if not stats:
        await update.message.reply_text("User not found.")
        return
    username_line = f"@{stats['username']}" if stats['username'] else "No username"
    text = (
        f"👤 User ID: {stats['user_id']}\n"
        f"Name: {stats['first_name']}\n"
        f"Username: {username_line}\n"
        f"Downloads: {stats['downloads_count']}\n"
        f"Today: {stats['downloads_today']}\n"
        f"Referrals: {stats['referrals_count']}\n"
        f"Points: {stats['points']}\n"
        f"Premium until: {stats['premium_until']}\n"
        f"Banned: {'Yes' if stats['is_banned'] else 'No'}"
    )
    await update.message.reply_text(text)


async def upgrade_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    if len(context.args) < 2 or not context.args[0].isdigit() or not context.args[1].isdigit():
        await update.message.reply_text("Usage: /upgrade user_id days")
        return
    uid = int(context.args[0])
    days = int(context.args[1])
    set_premium_for_days(uid, days)
    await update.message.reply_text(f"Upgraded {uid} for {days} day(s).")


async def setad_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Unauthorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /setad your sponsor message")
        return
    message = " ".join(context.args)
    set_setting("ad_message", message)
    await update.message.reply_text("Sponsor message updated.")


async def process_media(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str) -> None:
    async with DOWNLOAD_SEMAPHORE:
        workdir = make_temp_dir()
        status_msg = None
        clean_url = clean_instagram_url(url)
        try:
            stats = get_user_stats(update.effective_user.id)
            allowed, limit = can_download_today(stats)
            if not allowed:
                await update.message.reply_text(
                    f"🚫 Daily limit reached: {stats['downloads_today']} / {limit}\nUse /premium or come back tomorrow."
                )
                return

            status_msg = await update.message.reply_text("⏳ Processing your link...")
            await update.message.chat.send_action(action=ChatAction.UPLOAD_VIDEO)

            loop = asyncio.get_running_loop()
            items, summary = await asyncio.wait_for(
                loop.run_in_executor(None, download_media_bundle, clean_url, workdir),
                timeout=DOWNLOAD_TIMEOUT,
            )

            await send_media_items(update, context, items)
            increment_download(update.effective_user.id)
            await send_success_footer(update, context, update.effective_user.id)

            if status_msg:
                await status_msg.edit_text(f"✅ Done.\n{summary}")

        except asyncio.TimeoutError:
            if status_msg:
                await status_msg.edit_text("⌛ Download timed out. Please try again with a clean reel/post link.")
        except Exception as e:
            logger.exception("Download failed")
            message = format_failure_message(str(e), clean_url)
            if status_msg:
                await status_msg.edit_text(message)
        finally:
            cleanup_dir(workdir)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "check_join":
        joined = await is_user_joined(context, query.from_user.id)
        if joined:
            await query.message.reply_text(
                "✅ Access granted.\nNow send any Instagram link or use /profile username",
                reply_markup=premium_menu(),
            )
        else:
            await query.message.reply_text(
                "❌ I still cannot verify your membership.\nJoin the channel and tap again.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=join_keyboard(),
            )
    elif query.data == "help":
        await help_command(update, context)
    elif query.data == "profile_help":
        await query.message.reply_text("Send `/profile username` or just `@username`.", parse_mode=ParseMode.MARKDOWN)
    elif query.data == "invite":
        invite_link = f"https://t.me/{BOT_USERNAME}?start={query.from_user.id}"
        stats = get_user_stats(query.from_user.id) or {"referrals_count": 0, "points": 0}
        await query.message.reply_text(
            f"🎁 Referrals: {stats['referrals_count']}\n🏅 Points: {stats['points']}\n\nInvite link:\n`{invite_link}`",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif query.data == "my_stats":
        stats = get_user_stats(query.from_user.id) or {
            "downloads_count": 0,
            "referrals_count": 0,
            "points": 0,
            "downloads_today": 0,
            "first_name": query.from_user.first_name,
        }
        await query.message.reply_text(
            f"📊 *Your Stats*\n\n👤 {stats['first_name']}\n📥 Downloads: {stats['downloads_count']}\n📅 Today: {stats['downloads_today']}\n🎁 Referrals: {stats['referrals_count']}\n🏅 Points: {stats['points']}",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif query.data == "leaderboard":
        rows = top_referrers(10)
        if not rows:
            await query.message.reply_text("No leaderboard data yet.")
        else:
            lines = ["🏆 *Top Inviters*\n"]
            for i, (first_name, username, user_id, refs, points) in enumerate(rows, start=1):
                display = f"@{username}" if username else first_name or str(user_id)
                lines.append(f"{i}. {display} — {refs} invites • {points} pts")
            await query.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    elif query.data == "premium_info":
        await query.message.reply_text(
            f"⭐ Premium increases your daily limit from {FREE_DAILY_LIMIT} to {PREMIUM_DAILY_LIMIT}. Use /premium.",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif query.data == "admin_stats":
        if not is_admin(query.from_user.id):
            await query.message.reply_text("Unauthorized.")
        else:
            stats = get_global_stats()
            await query.message.reply_text(
                f"📈 Users: {stats['users']}\n📥 Downloads: {stats['downloads']}\n🏅 Points: {stats['points']}\n⭐ Premium users: {stats['premium_total']}"
            )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    upsert_user(user.id, user.username or "", user.first_name or "User")

    if not await require_join(update, context):
        return

    text = (update.message.text or "").strip()

    if text.startswith("@") and PROFILE_RE.match(text):
        context.args = [text.lstrip("@")]
        await profile_command(update, context)
        return

    url = is_instagram_url(text)
    if url:
        await process_media(update, context, url)
        return

    await update.message.reply_text(
        "Send an Instagram link or use:\n"
        "/profile username\n"
        "/refer\n"
        "/points\n"
        "/mystats\n"
        "/premium",
        reply_markup=premium_menu(),
    )


def main() -> None:
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("profile", profile_command))
    app.add_handler(CommandHandler("mystats", my_stats_command))
    app.add_handler(CommandHandler("invite", invite_command))
    app.add_handler(CommandHandler("refer", invite_command))
    app.add_handler(CommandHandler("leaderboard", leaderboard_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("premium", premium_command))
    app.add_handler(CommandHandler("points", points_command))

    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("user", user_command))
    app.add_handler(CommandHandler("upgrade", upgrade_command))
    app.add_handler(CommandHandler("setad", setad_command))
    app.add_handler(CommandHandler("debug", debug_command))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Instagram Growth + Earnings bot is running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
