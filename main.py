import logging
import sqlite3
import os
import csv
import json
import asyncio
from datetime import datetime, timedelta
from flask import Flask
from threading import Thread
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.error import RetryAfter, Forbidden, BadRequest, TimedOut, NetworkError
from dotenv import load_dotenv

load_dotenv()

flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is alive ✅"

@flask_app.route("/ping")
def ping():
    return "pong"

def run_flask():
    try:
        flask_app.run(host="0.0.0.0", port=5000, use_reloader=False)
    except OSError as e:
        if "Address already in use" in str(e):
            logging.warning("Port 5000 already in use, trying port 8080...")
            flask_app.run(host="0.0.0.0", port=8080, use_reloader=False)
        else:
            raise

Thread(target=run_flask, daemon=True).start()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set! Please add your bot token to .env or Replit Secrets.")

CHANNEL_USERNAME = "@multklar_olami"
MAIN_ADMIN_ID = 5663190258

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("httpx").setLevel(logging.WARNING)

conn = sqlite3.connect("users.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
c = conn.cursor()

c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        join_date TEXT,
        last_active TEXT
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS films (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        file_id TEXT NOT NULL,
        file_type TEXT NOT NULL,
        caption TEXT,
        upload_date TEXT NOT NULL
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS admins (
        user_id INTEGER PRIMARY KEY,
        added_by INTEGER,
        added_date TEXT
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS blocked_users (
        user_id INTEGER PRIMARY KEY,
        blocked_by INTEGER,
        blocked_date TEXT,
        reason TEXT
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_username TEXT UNIQUE NOT NULL,
        channel_type TEXT,
        display_name TEXT,
        added_by INTEGER,
        added_date TEXT
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS admin_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_id INTEGER,
        action TEXT,
        details TEXT,
        timestamp TEXT
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS film_parts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        film_code TEXT NOT NULL,
        part_number INTEGER NOT NULL,
        file_id TEXT NOT NULL,
        file_type TEXT NOT NULL,
        caption TEXT,
        upload_date TEXT
    )
""")

c.execute("""
    CREATE TABLE IF NOT EXISTS bot_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
""")

def migrate_db():
    # Check for display_name in channels
    try:
        c.execute("SELECT display_name FROM channels LIMIT 1")
    except sqlite3.OperationalError:
        try:
            c.execute("ALTER TABLE channels ADD COLUMN display_name TEXT")
            logging.info("Added display_name column to channels")
        except Exception as e:
            logging.error(f"Migration error (channels): {e}")

    # Check for permissions in admins
    try:
        c.execute("SELECT permissions FROM admins LIMIT 1")
    except sqlite3.OperationalError:
        try:
            c.execute("ALTER TABLE admins ADD COLUMN permissions TEXT")
            logging.info("Added permissions column to admins")
        except Exception as e:
            logging.error(f"Migration error (admins): {e}")

    # Check for invite_link in channels
    try:
        c.execute("SELECT invite_link FROM channels LIMIT 1")
    except sqlite3.OperationalError:
        try:
            c.execute("ALTER TABLE channels ADD COLUMN invite_link TEXT")
            logging.info("Added invite_link column to channels")
        except Exception as e:
            logging.error(f"Migration error (channels invite_link): {e}")
    
    conn.commit()

migrate_db()

c.execute("INSERT OR IGNORE INTO admins (user_id, added_date, permissions) VALUES (?, ?, ?)", 
          (MAIN_ADMIN_ID, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "full"))
c.execute("INSERT OR IGNORE INTO channels (channel_username, channel_type, display_name, added_by, added_date) VALUES (?, ?, ?, ?, ?)",
          (CHANNEL_USERNAME, "Telegram", "Asosiy Kanal", MAIN_ADMIN_ID, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)",
          ("about_text", "ℹ️ <b>Bot haqida</b>\n\n<b>Name: Multifilm kodlari</b>\n<b>About: ✉️ Film kodini yuboring</b>\n\nVa sevimli filmlaringizni yuqori sifatda tomosha qiling‼️\n\n⚠️Botdan foydalanish tez va oson❗️\n\n🔎Instagram: https://www.instagram.com/premyera_multifilmlar?igsh=MTBqdTNpaHI1YWJ6bQ==\n\n‼️Bot ishlamasa adminga murojat qiling✅️\n🧑‍💻 @JavohirJalilovv"))
conn.commit()

def log_admin_action(admin_id, action, details=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO admin_logs (admin_id, action, details, timestamp) VALUES (?, ?, ?, ?)",
              (admin_id, action, details, now))
    conn.commit()

def save_user(user_id):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO users (user_id, join_date, last_active)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_active=?
    """, (user_id, now, now, now))
    conn.commit()

def is_admin(user_id):
    c.execute("SELECT user_id FROM admins WHERE user_id = ?", (user_id,))
    return c.fetchone() is not None

def is_blocked(user_id):
    c.execute("SELECT user_id FROM blocked_users WHERE user_id = ?", (user_id,))
    return c.fetchone() is not None

def get_all_users():
    c.execute("SELECT user_id FROM users")
    return [row[0] for row in c.fetchall()]

def get_all_admins():
    c.execute("SELECT user_id FROM admins")
    return [row[0] for row in c.fetchall()]

def get_all_channels():
    c.execute("SELECT channel_username, channel_type, display_name, invite_link FROM channels")
    return c.fetchall()

def _serialize_buttons(buttons):
    if not buttons:
        return []
    result = []
    for row in buttons:
        row_out = []
        for btn in row:
            # btn is InlineKeyboardButton
            row_out.append({"text": btn.text, "url": btn.url})
        result.append(row_out)
    return result

def _build_markup_from_serialized(serialized):
    if not serialized:
        return None
    kb = []
    for row in serialized:
        kb_row = []
        for item in row:
            kb_row.append(InlineKeyboardButton(item.get("text", ""), url=item.get("url")))
        kb.append(kb_row)
    return InlineKeyboardMarkup(kb)

def save_last_ad_state(admin_id, payload, failed_list, buttons_serialized):
    state = {
        "payload": payload,
        "failed": failed_list,
        "buttons": buttons_serialized,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    update_bot_setting(f"last_ad_state_{admin_id}", json.dumps(state))

def load_last_ad_state(admin_id):
    raw = get_bot_setting(f"last_ad_state_{admin_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

def clear_last_ad_state(admin_id):
    update_bot_setting(f"last_ad_state_{admin_id}", "")

async def broadcast_to_users(
    context: ContextTypes.DEFAULT_TYPE,
    users,
    payload,
    reply_markup,
    blocked_set,
    concurrency_limit=25,
    batch_size=200,
    batch_pause=0.25,
    max_attempts=3
):
    sem = asyncio.Semaphore(concurrency_limit)
    success_count = 0
    failed_ids = []
    skipped_blocked = 0
    skipped_unreachable_total = 0

    async def send_one(uid):
        attempts = 0
        while True:
            try:
                async with sem:
                    if payload["type"] == "photo":
                        await context.bot.send_photo(
                            chat_id=int(uid),
                            photo=payload["file_id"],
                            caption=payload.get("caption", ""),
                            parse_mode='HTML',
                            reply_markup=reply_markup
                        )
                    elif payload["type"] == "video":
                        await context.bot.send_video(
                            chat_id=int(uid),
                            video=payload["file_id"],
                            caption=payload.get("caption", ""),
                            parse_mode='HTML',
                            protect_content=True,
                            reply_markup=reply_markup
                        )
                    elif payload["type"] == "document":
                        await context.bot.send_document(
                            chat_id=int(uid),
                            document=payload["file_id"],
                            caption=payload.get("caption", ""),
                            parse_mode='HTML',
                            protect_content=True,
                            reply_markup=reply_markup
                        )
                    elif payload["type"] == "audio":
                        await context.bot.send_audio(
                            chat_id=int(uid),
                            audio=payload["file_id"],
                            caption=payload.get("caption", ""),
                            parse_mode='HTML',
                            reply_markup=reply_markup
                        )
                    elif payload["type"] == "voice":
                        await context.bot.send_voice(
                            chat_id=int(uid),
                            voice=payload["file_id"],
                            caption=payload.get("caption", ""),
                            parse_mode='HTML',
                            reply_markup=reply_markup
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=int(uid),
                            text=payload.get("text", ""),
                            parse_mode='HTML',
                            reply_markup=reply_markup
                        )
                return ("success", uid)
            except RetryAfter as e:
                attempts += 1
                await asyncio.sleep(int(getattr(e, "retry_after", 1)) + 1)
                if attempts >= max_attempts:
                    return ("failed", uid)
            except (TimedOut, NetworkError) as e:
                attempts += 1
                await asyncio.sleep(1 + attempts)
                if attempts >= max_attempts:
                    logging.error(f"Network error for {uid}: {e}")
                    return ("failed", uid)
            except (Forbidden, BadRequest):
                return ("skipped", uid)
            except Exception as e:
                attempts += 1
                if attempts >= max_attempts:
                    logging.error(f"Failed to send to {uid}: {e}")
                    return ("failed", uid)
                await asyncio.sleep(0.5 + attempts)

    users_list = list(users)
    total = len(users_list)
    idx = 0
    while idx < total:
        chunk = users_list[idx: idx + batch_size]
        idx += batch_size

        chunk_skipped_unreachable = []
        targets = []
        for uid in chunk:
            if uid in blocked_set:
                skipped_blocked += 1
            else:
                targets.append(uid)

        if not targets:
            continue

        results = await asyncio.gather(*(send_one(uid) for uid in targets))
        for status, uid in results:
            if status == "success":
                success_count += 1
            elif status == "skipped":
                chunk_skipped_unreachable.append(uid)
            else:
                failed_ids.append(uid)

        if chunk_skipped_unreachable:
            skipped_unreachable_total += len(chunk_skipped_unreachable)

        if batch_pause:
            await asyncio.sleep(batch_pause)

    return success_count, failed_ids, skipped_blocked, skipped_unreachable_total

def get_statistics():
    c.execute("SELECT COUNT(*) FROM users")
    total = c.fetchone()[0]

    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*) FROM users WHERE join_date LIKE ?", (f"{today}%",))
    today_joins = c.fetchone()[0]

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*) FROM users WHERE join_date LIKE ?", (f"{yesterday}%",))
    yesterday_joins = c.fetchone()[0]

    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("SELECT COUNT(*) FROM users WHERE join_date >= ?", (week_ago,))
    week_joins = c.fetchone()[0]

    hours_24_ago = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("SELECT COUNT(*) FROM users WHERE last_active >= ?", (hours_24_ago,))
    active_users = c.fetchone()[0]

    return {
        "total": total,
        "today_joins": today_joins,
        "yesterday_joins": yesterday_joins,
        "week_joins": week_joins,
        "active_users": active_users
    }

def save_film(code, file_id, file_type, caption):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO films (code, file_id, file_type, caption, upload_date)
        VALUES (?, ?, ?, ?, ?)
    """, (code, file_id, file_type, caption, now))
    conn.commit()

def save_film_part(film_code, part_number, file_id, file_type, caption):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO film_parts (film_code, part_number, file_id, file_type, caption, upload_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (film_code, part_number, file_id, file_type, caption, now))
    conn.commit()

def get_film_parts(film_code):
    c.execute("SELECT part_number, file_id, file_type, caption FROM film_parts WHERE film_code = ? ORDER BY part_number ASC", (film_code,))
    return c.fetchall()

def get_bot_setting(key):
    c.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
    result = c.fetchone()
    return result[0] if result else None

def update_bot_setting(key, value):
    c.execute("""
        INSERT INTO bot_settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = ?
    """, (key, value, value))
    conn.commit()

def to_bold(text):
    result = []
    for ch in text:
        if "A" <= ch <= "Z":
            result.append(chr(ord(ch) - ord("A") + 0x1D400))
        elif "a" <= ch <= "z":
            result.append(chr(ord(ch) - ord("a") + 0x1D41A))
        elif "0" <= ch <= "9":
            result.append(chr(ord(ch) - ord("0") + 0x1D7CE))
        else:
            result.append(ch)
    return "".join(result)

PERMISSIONS = [
    ("ADMIN_ADD", "Admin qo'shish"),
    ("ADMIN_REMOVE", "Admin o'chirish"),
    ("AD_SEND", "Reklama yuborish"),
    ("POST_CREATE", "Post yaratish"),
    ("DB_DOWNLOAD", "Users.db yuklash"),
    ("CHANNEL_ADD", "Kanal qo'shish"),
    ("CHANNEL_REMOVE", "Kanal o'chirish"),
    ("USER_BLOCK", "User bloklash"),
    ("USER_UNBLOCK", "User blokdan chiqarish"),
    ("FILM_UPLOAD", "Film yuklash"),
    ("MAIN_CHANNEL_CHANGE", "Asosiy kanalni o'zgartirish"),
    ("FILM_EDIT", "Film tahrirlash"),
    ("FILM_DELETE", "Film o'chirish"),
    ("PART_UPLOAD", "Qism qo'shish"),
    ("LOGS_DOWNLOAD", "Admin loglarini yuklab olish")
]

def parse_permissions(value):
    if not value:
        return set()
    if value.strip() == "full":
        return {"full"}
    return set(p.strip() for p in value.split(",") if p.strip())

def get_admin_permissions(user_id):
    c.execute("SELECT permissions FROM admins WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    if not row or row[0] is None:
        return set()
    return parse_permissions(row[0])

def has_permission(user_id, key):
    if user_id == MAIN_ADMIN_ID:
        return True
    perms = get_admin_permissions(user_id)
    if "full" in perms:
        return True
    return key in perms

def update_admin_permissions(user_id, permissions_set):
    if not permissions_set:
        value = ""
    elif "full" in permissions_set:
        value = "full"
    else:
        value = ",".join(sorted(list(permissions_set)))
    c.execute("UPDATE admins SET permissions = ? WHERE user_id = ?", (value, user_id))
    conn.commit()

def update_film_caption(code, new_caption):
    c.execute("UPDATE films SET caption = ? WHERE code = ?", (new_caption, code))
    conn.commit()

def delete_film(code):
    c.execute("DELETE FROM films WHERE code = ?", (code,))
    c.execute("DELETE FROM film_parts WHERE film_code = ?", (code,))
    conn.commit()

def update_film_file(code, file_id, file_type):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE films SET file_id = ?, file_type = ?, upload_date = ? WHERE code = ?", (file_id, file_type, now, code))
    conn.commit()

def update_film_part_caption(film_code, part_number, new_caption):
    c.execute("UPDATE film_parts SET caption = ? WHERE film_code = ? AND part_number = ?", (new_caption, film_code, part_number))
    conn.commit()

def update_film_part_file(film_code, part_number, file_id, file_type):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE film_parts SET file_id = ?, file_type = ?, upload_date = ? WHERE film_code = ? AND part_number = ?", (file_id, file_type, now, film_code, part_number))
    conn.commit()

def delete_film_part(film_code, part_number):
    c.execute("DELETE FROM film_parts WHERE film_code = ? AND part_number = ?", (film_code, part_number))
    conn.commit()
def get_film_by_code(code):
    c.execute("SELECT file_id, file_type, caption FROM films WHERE code = ?", (code,))
    result = c.fetchone()
    if result:
        return {"file_id": result[0], "file_type": result[1], "caption": result[2]}
    return None

def search_films(query):
    c.execute("SELECT code, caption, file_type FROM films WHERE code LIKE ? OR caption LIKE ?", 
              (f"%{query}%", f"%{query}%"))
    return c.fetchall()

def get_all_films(offset=0, limit=10):
    c.execute("SELECT code, caption, file_type, upload_date FROM films ORDER BY id DESC LIMIT ? OFFSET ?", 
              (limit, offset))
    return c.fetchall()

def get_films_count():
    c.execute("SELECT COUNT(*) FROM films")
    return c.fetchone()[0]

async def is_member(user_id):
    channels = get_all_channels()
    not_joined = []
    for channel_username, channel_type, display_name, invite_link in channels:
        if channel_type == "Telegram":
            try:
                # Skip invite-link style entries entirely (t.me/+ or joinchat), and any http t.me links
                lower = (channel_username or "").lower()
                if channel_username.startswith("http") or "t.me/+" in lower or "t.me/joinchat" in lower:
                    continue
                # Handle channel ID (starts with -100) or username (@...)
                chat_id = channel_username if channel_username.startswith("-100") else channel_username
                # Self-healing: Try to get invite link if missing
                if not invite_link:
                    try:
                        invite_link = await app.bot.export_chat_invite_link(chat_id)
                        c.execute("UPDATE channels SET invite_link = ? WHERE channel_username = ?", (invite_link, channel_username))
                        conn.commit()
                    except Exception as e:
                        logging.error(f"Invite link olishda xatolik ({channel_username}): {e}")
                member = await app.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if member.status not in ["member", "creator", "administrator"]:
                    not_joined.append((channel_username, display_name, invite_link))
            except Exception as e:
                logging.error(f"Kanalga a'zolikni tekshirishda xatolik ({channel_username}): {e}")
                pass
    return not_joined

def get_subscription_keyboard(not_joined_channels):
    keyboard = []
    for channel_username, display_name, invite_link in not_joined_channels:
        # Determine URL
        url = None
        if invite_link:
             url = invite_link
        elif channel_username.startswith("@"):
             url = f"https://t.me/{channel_username[1:]}"
        elif channel_username.startswith("http"):
             url = channel_username
        
        name = display_name if display_name else channel_username
        
        if url:
            keyboard.append([InlineKeyboardButton(f"➕ {name}", url=url)])
        else:
            # Fallback for ID-based channels without link
            keyboard.append([InlineKeyboardButton(f"➕ {name} (Havola yo'q)", callback_data=f"no_link_{channel_username}")])
    
    keyboard.append([InlineKeyboardButton("✅ Tekshirish", callback_data="check_membership")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_main_keyboard():
    keyboard = [
        [KeyboardButton("📢 Reklama"), KeyboardButton("⚙ Admin sozlamalari")],
        [KeyboardButton("📡 Kanal sozlamalari"), KeyboardButton("🎬 Film sozlamalari")],
        [KeyboardButton("📢 Kanalga Post"), KeyboardButton("📊 Statistika")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_user_keyboard():
    keyboard = [
        [KeyboardButton("ℹ️ Bot haqida")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_admin_settings_keyboard():
    keyboard = [
        [InlineKeyboardButton("➕ Admin qo'shish", callback_data="admin_add")],
        [InlineKeyboardButton("➖ Admin o'chirish", callback_data="admin_remove")],
        [InlineKeyboardButton("🚫 User bloklash", callback_data="user_block")],
        [InlineKeyboardButton("✅ User blokdan chiqarish", callback_data="user_unblock")],
        [InlineKeyboardButton("⛔ Bloklanganlar ro'yxati", callback_data="list_blocked")],
        [InlineKeyboardButton("📥 Bloklanganlar fayli", callback_data="download_blocked")],
        [InlineKeyboardButton("🤖 Bot haqida tahrirlash", callback_data="edit_about_text")],
        [InlineKeyboardButton("📥 Users.db yuklab olish", callback_data="download_db")],
        [InlineKeyboardButton("📋 Admin loglarini yuklab olish", callback_data="download_logs")],
        [InlineKeyboardButton("👤 Admin huquqlarini o'zgartirish", callback_data="admin_perms")],
        [InlineKeyboardButton("⬅ Orqaga", callback_data="back_main")],
        [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_channel_settings_keyboard():
    keyboard = [
        [InlineKeyboardButton("➕ Kanal qo'shish", callback_data="channel_add")],
        [InlineKeyboardButton("➖ Kanalni o'chirish", callback_data="channel_remove")],
        [InlineKeyboardButton("✏️ Kanal nomini o'zgartirish", callback_data="channel_rename")],
        [InlineKeyboardButton("⭐️ Asosiy kanalni o'zgartirish", callback_data="change_main_channel")],
        [InlineKeyboardButton("📋 Kanallar ro'yxati", callback_data="channel_list")],
        [InlineKeyboardButton("⬅ Orqaga", callback_data="back_main")],
        [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_film_settings_keyboard():
    keyboard = [
        [InlineKeyboardButton("📤 Film yuklash", callback_data="film_upload"),
         InlineKeyboardButton("➕ Qism qo'shish", callback_data="part_upload")],
        [InlineKeyboardButton("✏️ Filmni tahrirlash", callback_data="film_edit"),
         InlineKeyboardButton("🗑 Film o'chirish", callback_data="film_delete")],
        [InlineKeyboardButton("🔍 Film qidirish", callback_data="film_search"),
         InlineKeyboardButton("📋 Barcha filmlar", callback_data="film_list")],
        [InlineKeyboardButton("⬅ Orqaga", callback_data="back_main"),
         InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_channel_post_keyboard():
    keyboard = [
        [InlineKeyboardButton("📝 Post yaratish", callback_data="create_post")],
        [InlineKeyboardButton("⬅ Orqaga", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_blocked(user.id):
        await update.message.reply_text("❌ Siz bloklangansiz. Bot adminiga murojaat qiling.\n\n🧑‍💻 @JavohirJalilovv")
        return
    if is_admin(user.id):
        save_user(user.id)
        if context.args:
            code = context.args[0]
            await send_film_logic(update, context, code)
            return
        await update.message.reply_text(
            "🎛 <b>ADMIN PANEL</b>\n\n"
            "Xush kelibsiz! Admin panelidan foydalaning.\n\n"
            "📌 Mavjud bo'limlar:\n"
            "📢 Reklama - Barcha foydalanuvchilarga xabar yuborish\n"
            "⚙ Admin sozlamalari - Adminlar va foydalanuvchilarni boshqarish\n"
            "📡 Kanal sozlamalari - Majburiy kanallarni boshqarish\n"
            "🎬 Film sozlamalari - Filmlarni boshqarish\n"
            "📊 Statistika - Bot statistikasini ko'rish\n"
            "🔗 Kanalga post - Kanal uchun tugmali post yaratish\n\n"
            "Yoki to'g'ridan-to'g'ri film kodini yuboring.",
            parse_mode='HTML',
            reply_markup=get_admin_main_keyboard()
        )
        return
    not_joined = await is_member(user.id)
    if not_joined:
        await update.message.reply_text(
            "⚠️ <b>DIQQAT!</b>\n\n"
            "Botdan foydalanish uchun quyidagi kanallarga a'zo bo'ling va <b>'✅ Tekshirish'</b> tugmasini bosing:",
            parse_mode='HTML',
            reply_markup=get_subscription_keyboard(not_joined)
        )
        return
    save_user(user.id)
    if context.args:
        code = context.args[0]
        await send_film_logic(update, context, code)
        return
    await update.message.reply_text(
        "✍️ Film kodini yuboring",
        reply_markup=get_user_keyboard()
    )

async def send_film_logic(update: Update, context: ContextTypes.DEFAULT_TYPE, code: str):
    film = get_film_by_code(code)
    if film:
        parts = get_film_parts(code)
        if parts:
            # Multi-part film
            keyboard = []
            row = []
            for part in parts:
                part_num = part[0]
                row.append(InlineKeyboardButton(f"{part_num}-qism", callback_data=f"get_part_{code}_{part_num}"))
                if len(row) == 5: # 5 ta qism bir qatorda
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            # Send the main film file if exists, otherwise the first part
            # User request: "bot film vediosi+captionsi... va ostida qismlar soni"
            # We prioritize the main film file (if it's a trailer or the full movie container)
            # But usually for multi-part, the main entry might just be a placeholder.
            # Let's check if main film has a file_id (it is NOT NULL in DB).
            
            # Actually, we should send the specific part if the user asked for a part?
            # But here the user sent a CODE. So we send the main entry or the first part.
            
            # Let's try to send the main film file.
            target_file_id = film['file_id']
            target_file_type = film['file_type']
            target_caption = film['caption']
            
            # If main film is just a placeholder (e.g. text/image), maybe we should send the first part?
            # But the user said "film vediosi".
            # If the admin uploaded a video for the main code, we use it.
            
            try:
                if target_file_type == "video":
                    await update.message.reply_video(
                        video=target_file_id,
                        caption=target_caption,
                        parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        protect_content=True
                    )
                elif target_file_type == "document":
                    await update.message.reply_document(
                        document=target_file_id,
                        caption=target_caption,
                        parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        protect_content=True
                    )
                else:
                    # Fallback if main file is not video/doc
                    await update.message.reply_text(
                        f"🎬 <b>{target_caption}</b>\n\nQismlarni tanlang:",
                        parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
            except Exception as e:
                logging.error(f"Error sending multi-part main film: {e}")
                await update.message.reply_text("❌ Film yuborishda xatolik!")
                
        else:
            # Single film
            try:
                if film['file_type'] == "video":
                    await update.message.reply_video(
                        video=film['file_id'],
                        caption=film['caption'] if film['caption'] else None,
                        parse_mode='HTML',
                        protect_content=True
                    )
                elif film['file_type'] == "document":
                    await update.message.reply_document(
                        document=film['file_id'],
                        caption=film['caption'] if film['caption'] else None,
                        parse_mode='HTML',
                        protect_content=True
                    )
            except Exception as e:
                logging.error(f"Film yuborishda xatolik: {e}")
                await update.message.reply_text("❌ Film yuborishda xatolik yuz berdi.")
    else:
        # Film not found
        if code.isdigit():
            main_channel = get_bot_setting("main_channel")
            if not main_channel:
                main_channel = CHANNEL_USERNAME
            
            if main_channel.startswith("@"):
                 link = f"https://t.me/{main_channel[1:]}/{code}"
            else:
                 # If it's ID, we can't generate a direct link easily without invite link format or if it's public link
                 # But usually main channel for file storage is public or has a username.
                 # If it is private ID, we can't link to message like this unless we have private link format t.me/c/ID/MSG_ID
                 # For now, let's assume if it's not starting with @, we might not be able to link properly or it is a link itself?
                 # If main_channel is just ID, we can try to use t.me/c/ID_WITHOUT_100/CODE
                 if main_channel.startswith("-100"):
                     # Remove -100 prefix for t.me/c/ link
                     clean_id = main_channel[4:]
                     link = f"https://t.me/c/{clean_id}/{code}"
                 else:
                     link = "#"

            await update.message.reply_text(f"🎬 Film:\n\n{link}")
        else:
            await update.message.reply_text("❌ Iltimos, faqat raqamli kod yuboring!")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not user:
        return

    if is_blocked(user.id):
        await update.message.reply_text("❌ Siz bloklangansiz. Bot adminiga murojaat qiling.\n\n🧑‍💻 @JavohirJalilovv")
        return

    if not is_admin(user.id):
        not_joined = await is_member(user.id)
        if not_joined:
            await update.message.reply_text(
                "⚠️ <b>DIQQAT!</b>\n\n"
                "Siz botning majburiy kanallaridan chiqib ketgansiz yoki hali a'zo emassiz.\n"
                "Botdan foydalanish uchun quyidagi kanallarga a'zo bo'ling va <b>'✅ Tekshirish'</b> tugmasini bosing:", 
                parse_mode='HTML',
                reply_markup=get_subscription_keyboard(not_joined)
            )
            return

    save_user(user.id)
    text = update.message.text.strip() if update.message.text else ""

    if text == "📢 Kanalga Post":
        if is_admin(user.id):
            await update.message.reply_text(
                "📢 <b>KANALGA POST YARATISH</b>\n\n"
                "1-qadam: Post uchun media yuboring (Rasm, Video yoki shunchaki Matn yozing):",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Bekor qilish")]], resize_keyboard=True)
            )
            context.user_data["waiting_post_media"] = True
        return

    if text == "❌ Bekor qilish":
        context.user_data.clear()
        if is_admin(user.id):
             await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_admin_main_keyboard())
        else:
             await update.message.reply_text("❌ Bekor qilindi.", reply_markup=get_user_keyboard())
        return

    if context.user_data.get("waiting_post_media"):
        if update.message.photo:
            context.user_data["post_file_id"] = update.message.photo[-1].file_id
            context.user_data["post_file_type"] = "photo"
        elif update.message.video:
            context.user_data["post_file_id"] = update.message.video.file_id
            context.user_data["post_file_type"] = "video"
        else:
            context.user_data["post_file_type"] = "text"
            # If text, we use the text as caption/content later, but here we just mark it.
            # Wait, if it's text, we should probably ask for content now or treat this text as content?
            # User said: "Admin media yuboradi (photo/video/text) -> bot captions so'raydi".
            # So if it's text, this IS the content.
            # But then "bot captions so'raydi" might be redundant for text-only posts, 
            # or maybe it adds more text? Let's assume for text post, this IS the text.
            # But for consistency, let's treat this input as the "media" part. 
            # If it's text, we store it as 'post_text_content'.
            context.user_data["post_text_content"] = update.message.text # Use the text sent here

        await update.message.reply_text(
            "2-qadam: Caption (matn) yozing:\n"
            "(HTML, shriftlar, emojilar ishlaydi)\n"
            "Agar rasm/video bo'lsa, bu tagiga yoziladi.\n"
            "Agar faqat matn bo'lsa, bu davomiga qo'shiladi yoki o'rniga o'tadi.",
            parse_mode='HTML'
        )
        context.user_data["waiting_post_media"] = False
        context.user_data["waiting_post_caption"] = True
        return

    if context.user_data.get("waiting_post_caption"):
        context.user_data["post_caption"] = update.message.text_html # Use text_html to preserve formatting
        
        await update.message.reply_text(
            "3-qadam: Tugma nomini yozing (Masalan: 🎥 Filmni ko'rish):",
            parse_mode='HTML'
        )
        context.user_data["waiting_post_caption"] = False
        context.user_data["waiting_post_btn_text"] = True
        return
    
    if context.user_data.get("waiting_post_btn_text"):
        context.user_data["post_btn_text"] = to_bold(update.message.text)
        
        await update.message.reply_text(
            "4-qadam: Film kodini yozing (Masalan: 123):",
            parse_mode='HTML'
        )
        context.user_data["waiting_post_btn_text"] = False
        context.user_data["waiting_post_code"] = True
        return

    if context.user_data.get("waiting_post_code"):
        code = update.message.text
        context.user_data["post_code"] = code
        context.user_data["waiting_post_code"] = False
        
        # Generate Preview
        file_type = context.user_data.get("post_file_type")
        caption = context.user_data.get("post_caption")
        btn_text = context.user_data.get("post_btn_text")
        
        # Bot username needed for deep link
        bot_username = context.bot.username
        url = f"https://t.me/{bot_username}?start={code}"
        
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(btn_text, url=url)]])
        
        await update.message.reply_text("👁 <b>POST KO'RINISHI (PREVIEW):</b>", parse_mode='HTML')
        
        try:
            if file_type == "photo":
                await update.message.reply_photo(
                    photo=context.user_data["post_file_id"],
                    caption=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            elif file_type == "video":
                await update.message.reply_video(
                    video=context.user_data["post_file_id"],
                    caption=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            else:
                # Text only
                # Combine initial text content (if any) with caption? 
                # Or just use caption? User flow: Media (Text) -> Caption.
                # If Media was Text, and Caption is provided, maybe join them?
                # Or Caption overrides? Let's use Caption as the main text.
                # If Media was text, maybe that was the title?
                # Let's just use the caption provided in step 2 as the message text.
                await update.message.reply_text(
                    text=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
        except Exception as e:
            await update.message.reply_text(f"❌ Xatolik: {e}")
            return
        
        # Ask for target channel
        channels = get_all_channels()
        telegram_channels = [(u, t, n, l) for (u, t, n, l) in channels if t == "Telegram"]
        if not telegram_channels:
            await update.message.reply_text(
                "❌ Hech qanday Telegram kanal topilmadi. Avval kanal qo'shing.",
                parse_mode='HTML'
            )
            return
        
        context.user_data["post_available_channels"] = telegram_channels
        
        kb = []
        for idx, (username, ch_type, display_name, invite_link) in enumerate(telegram_channels):
            name = display_name if display_name else username
            kb.append([InlineKeyboardButton(name, callback_data=f"post_target_idx_{idx}")])
        kb.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="post_cancel")])
        
        await update.message.reply_text(
            "📡 <b>QAYSI KANALGA YUBORILSIN?</b>\n\nKanalni tanlang:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    if text == "📊 Statistika":
        stats = get_statistics()
        c.execute("SELECT COUNT(*) FROM films")
        films_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM admins")
        admins_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM blocked_users")
        blocked_count = c.fetchone()[0]

        growth = ""
        if stats['yesterday_joins'] > 0:
            percent = ((stats['today_joins'] - stats['yesterday_joins']) / stats['yesterday_joins']) * 100
            if percent > 0:
                growth = f"📈 +{percent:.1f}%"
            elif percent < 0:
                growth = f"📉 {percent:.1f}%"
            else:
                growth = "➖ 0%"

        stats_text = f"""
📊 <b>BOT STATISTIKASI</b>

━━━━━━━━━━━━━━━━━━━━
👥 <b>Foydalanuvchilar:</b>
├ Jami: <b>{stats['total']}</b>
├ Faol (24 soat): <b>{stats['active_users']}</b>
└ Bloklangan: <b>{blocked_count}</b>

📈 <b>Qo'shilish:</b>
├ Bugun: <b>{stats['today_joins']}</b> ta {growth}
├ Kecha: <b>{stats['yesterday_joins']}</b> ta
└ 7 kunlik: <b>{stats['week_joins']}</b> ta

🎬 <b>Kontent:</b>
├ Filmlar: <b>{films_count}</b> ta
└ Adminlar: <b>{admins_count}</b> ta

━━━━━━━━━━━━━━━━━━━━
📅 {datetime.now().strftime("%d.%m.%Y %H:%M")}
"""
        keyboard = [[InlineKeyboardButton("⬅ Asosiy menyu", callback_data="back_main")]]
        await update.message.reply_text(stats_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if text == "⚙ Admin sozlamalari":
        if is_admin(user.id):
            await update.message.reply_text(
                "⚙ <b>ADMIN SOZLAMALARI</b>\n\n"
                "Quyidagi amallardan birini tanlang:",
                parse_mode='HTML',
                reply_markup=get_admin_settings_keyboard()
            )
        return

    if text == "📡 Kanal sozlamalari":
        if is_admin(user.id):
            await update.message.reply_text(
                "📡 <b>KANAL SOZLAMALARI</b>\n\n"
                "Majburiy kanallarni boshqaring:",
                parse_mode='HTML',
                reply_markup=get_channel_settings_keyboard()
            )
        return

    if text == "🎬 Film sozlamalari":
        if is_admin(user.id):
            await update.message.reply_text(
                "🎬 <b>FILM SOZLAMALARI</b>\n\n"
                "Filmlarni boshqaring:",
                parse_mode='HTML',
                reply_markup=get_film_settings_keyboard()
            )
        return

    if text == "📢 Reklama":
        if is_admin(user.id):
            keyboard = [
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_reklama")],
                [InlineKeyboardButton("⬅ Asosiy menyu", callback_data="back_main")]
            ]
            await update.message.reply_text(
                "📢 <b>REKLAMA YUBORISH</b>\n\n"
                "Reklama sifatida quyidagilarni yuborishingiz mumkin:\n\n"
                "📸 Rasm, 🎥 Video, 📄 Hujjat, 🎵 Audio, 🎤 Ovozli xabar, 💬 Matn\n\n"
                "📝 Media yuborgan holda, caption qo'shishingiz mumkin.\n\n"
                "⚠️ Keyingi xabaringiz reklama sifatida qabul qilinadi!",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data["reklama_mode"] = True
        return

    if text == "🔗 Kanalga post":
        if is_admin(user.id):
            await update.message.reply_text(
                "🔗 <b>KANALGA POST YARATISH</b>\n\n"
                "Post yaratish uchun quyidagi tugmani bosing:",
                parse_mode='HTML',
                reply_markup=get_channel_post_keyboard()
            )
        return

    if text == "ℹ️ Bot haqida":
        about_text = get_bot_setting("about_text")
        if not about_text:
            about_text = (
                "ℹ️ <b>Bot haqida</b>\n\n"
                "<b>Name: Multifilm kodlari</b>\n"
                "<b>About: ✉️ Film kodini yuboring</b>\n\n"
                "Va sevimli filmlaringizni yuqori sifatda tomosha qiling‼️\n\n"
                "⚠️Botdan foydalanish tez va oson❗️\n\n"
                "🔎Instagram: https://www.instagram.com/premyera_multifilmlar?igsh=MTBqdTNpaHI1YWJ6bQ==\n\n"
                "‼️Bot ishlamasa adminga murojat qiling✅️\n"
                "🧑‍💻 @JavohirJalilovv"
            )
        
        await update.message.reply_text(
            about_text,
            parse_mode='HTML'
        )
        return

    if context.user_data.get("reklama_mode"):
        context.user_data["reklama_content"] = update.message
        context.user_data["reklama_mode"] = False
        
        await update.message.reply_text(
            "✅ Media qabul qilindi.\n\n"
            "Tugma qo'shishni xohlaysizmi?\n"
            "Format: <code>Tugma nomi - https://link.com</code>\n"
            "Agar tugma kerak bo'lmasa '0' yuboring.",
            parse_mode='HTML'
        )
        context.user_data["waiting_reklama_buttons"] = True
        return

    if context.user_data.get("waiting_reklama_buttons"):
        buttons = []
        if text != "0":
            lines = text.split("\n")
            for line in lines:
                parts = line.split("-")
                if len(parts) >= 2:
                    name = parts[0].strip()
                    link = "-".join(parts[1:]).strip()
                    buttons.append([InlineKeyboardButton(name, url=link)])
        
        context.user_data["reklama_buttons"] = buttons
        context.user_data["waiting_reklama_buttons"] = False

        preview_text = "👁 <b>REKLAMA OLDINDAN KO'RISH</b>\n\n"
        reklama_msg = context.user_data.get("reklama_content")
        
        caption_preview = ""
        if hasattr(reklama_msg, 'caption') and reklama_msg.caption:
            caption_preview = reklama_msg.caption[:100] + "..."

        if reklama_msg.photo:
            preview_text += "� Media turi: <b>Rasm</b>\n"
        elif reklama_msg.video:
            preview_text += "🎥 Media turi: <b>Video</b>\n"
        elif reklama_msg.document:
            preview_text += f"📄 Media turi: <b>Fayl ({reklama_msg.document.file_name})</b>\n"
        elif reklama_msg.audio:
            preview_text += "🎵 Media turi: <b>Audio</b>\n"
        elif reklama_msg.voice:
            preview_text += "� Media turi: <b>Ovozli xabar</b>\n"
        elif reklama_msg.text:
            preview_text += f"� Matn xabari:\n\n<i>{reklama_msg.text[:200]}...</i>\n"
        
        if caption_preview:
            preview_text += f"📝 Matn: <i>{caption_preview}</i>\n"
            
        if buttons:
            preview_text += f"\n� Tugmalar: <b>{len(buttons)}</b> ta qator"

        preview_text += f"\n\n👥 Yuboriladi: <b>{len(get_all_users())}</b> ta foydalanuvchiga"

        keyboard = [
            [InlineKeyboardButton("✅ Yuborish", callback_data="approve_ad")],
            [InlineKeyboardButton("❌ Bekor qilish", callback_data="reject_ad")]
        ]
        
        # Show preview message with buttons if possible, otherwise just text preview
        # Usually we reply with the media AND the buttons to show exactly how it looks.
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send text preview first
        await update.message.reply_text(
            preview_text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
        # Also send the actual preview with buttons
        try:
            preview_markup = InlineKeyboardMarkup(buttons) if buttons else None
            if reklama_msg.photo:
                await update.message.reply_photo(reklama_msg.photo[-1].file_id, caption=reklama_msg.caption, parse_mode='HTML', reply_markup=preview_markup)
            elif reklama_msg.video:
                await update.message.reply_video(reklama_msg.video.file_id, caption=reklama_msg.caption, parse_mode='HTML', reply_markup=preview_markup)
            elif reklama_msg.document:
                await update.message.reply_document(reklama_msg.document.file_id, caption=reklama_msg.caption, parse_mode='HTML', reply_markup=preview_markup)
            elif reklama_msg.audio:
                await update.message.reply_audio(reklama_msg.audio.file_id, caption=reklama_msg.caption, parse_mode='HTML', reply_markup=preview_markup)
            elif reklama_msg.voice:
                await update.message.reply_voice(reklama_msg.voice.file_id, caption=reklama_msg.caption, parse_mode='HTML', reply_markup=preview_markup)
            elif reklama_msg.text:
                await update.message.reply_text(reklama_msg.text, parse_mode='HTML', reply_markup=preview_markup)
        except Exception as e:
            await update.message.reply_text(f"⚠️ Preview ko'rsatishda xatolik: {e}")
            
        return

    if context.user_data.get("waiting_film_upload"):
        if update.message.video or update.message.document:
            context.user_data["film_content"] = update.message
            context.user_data["waiting_film_upload"] = False

            await update.message.reply_text(
                "✅ Film qabul qilindi!\n\n"
                "📝 Endi bu film uchun <b>kod</b> yuboring.\n\n"
                "💡 Masalan: <code>123</code> yoki <code>avengers</code>",
                parse_mode='HTML'
            )
            context.user_data["waiting_for_code"] = True
            return
        else:
            await update.message.reply_text("❌ Iltimos, video yoki hujjat formatida film yuboring!")
            return

    if context.user_data.get("waiting_for_code") and text:
        film_msg = context.user_data.get("film_content")
        if film_msg:
            try:
                if film_msg.video:
                    file_id = film_msg.video.file_id
                    file_type = "video"
                elif film_msg.document:
                    file_id = film_msg.document.file_id
                    file_type = "document"
                else:
                    await update.message.reply_text("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring.")
                    context.user_data.clear()
                    return

                caption = film_msg.caption or ""
                save_film(text, file_id, file_type, caption)
                log_admin_action(user.id, "Film qo'shildi", f"Kod: {text}")

                await update.message.reply_text(
                    f"✅ <b>Film muvaffaqiyatli saqlandi!</b>\n\n"
                    f"🎬 Film kodi: <code>{text}</code>\n"
                    f"📅 Sana: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                    parse_mode='HTML'
                )
                context.user_data.clear()
                return
            except sqlite3.IntegrityError:
                await update.message.reply_text(
                    f"❌ Bu kod (<code>{text}</code>) allaqachon ishlatilgan.\n\n"
                    f"Iltimos, boshqa kod yuboring.",
                    parse_mode='HTML'
                )
                return
            except Exception as e:
                logging.error(f"Film saqlashda xatolik: {e}")
                await update.message.reply_text("❌ Film saqlashda xatolik yuz berdi.")
                context.user_data.clear()
                return

    if context.user_data.get("waiting_admin_id"):
        if text.isdigit():
            new_admin_id = int(text)
            keyboard = [
                [InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"confirm_add_admin_{new_admin_id}")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")]
            ]
            await update.message.reply_text(
                f"Admin qo'shish:\n\nUser ID: <code>{new_admin_id}</code>\n\nTasdiqlaysizmi?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, to'g'ri User ID yuboring (faqat raqam)!")
        return

    if context.user_data.get("waiting_block_user_id"):
        if text.isdigit():
            block_user_id = int(text)
            keyboard = [
                [InlineKeyboardButton("✅ Bloklash", callback_data=f"confirm_block_user_{block_user_id}")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")]
            ]
            await update.message.reply_text(
                f"User bloklash:\n\nUser ID: <code>{block_user_id}</code>\n\nTasdiqlaysizmi?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, to'g'ri User ID yuboring!")
        return

    if context.user_data.get("waiting_unblock_user_id"):
        if text.isdigit():
            unblock_user_id = int(text)
            keyboard = [
                [InlineKeyboardButton("✅ Blokdan chiqarish", callback_data=f"confirm_unblock_user_{unblock_user_id}")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")]
            ]
            await update.message.reply_text(
                f"User blokdan chiqarish:\n\nUser ID: <code>{unblock_user_id}</code>\n\nTasdiqlaysizmi?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, to'g'ri User ID yuboring!")
        return

    if context.user_data.get("waiting_about_text"):
        update_bot_setting("about_text", text)
        await update.message.reply_text("✅ 'Bot haqida' ma'lumoti muvaffaqiyatli yangilandi!")
        log_admin_action(user.id, "Bot haqida o'zgartirildi", "")
        context.user_data.clear()
        return

    if context.user_data.get("waiting_main_channel"):
        new_channel = text.strip()
        if new_channel.startswith("@") or new_channel.startswith("-100"):
            update_bot_setting("main_channel", new_channel)
            await update.message.reply_text(f"✅ Asosiy kanal o'zgartirildi: {new_channel}")
            log_admin_action(user.id, "Asosiy kanal o'zgartirildi", f"{new_channel}")
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, to'g'ri formatda kanal Username (@...) yoki ID (-100...) yuboring!")
        return

    if context.user_data.get("waiting_channel_username"):
        channel_username = text.strip()
        if channel_username.startswith("@") or channel_username.startswith("-100") or channel_username.startswith("http"):
            if channel_username.startswith("http"):
                lower = channel_username.lower()
                if "t.me" in lower:
                    await update.message.reply_text(
                        "❌ Telegram kanalni <b>taklif havolasi</b> orqali qo'shish taqiqlangan.\n"
                        "Iltimos, kanalni <b>@username</b> yoki <b>-100… ID</b> bilan yuboring.",
                        parse_mode='HTML'
                    )
                    return
                channel_type = "Web"
            else:
                channel_type = "Telegram"
            keyboard = [
                [InlineKeyboardButton("✅ Qo'shish", callback_data=f"confirm_add_channel_{channel_username}_{channel_type}")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")]
            ]
            await update.message.reply_text(
                f"Kanal qo'shish:\n\n{channel_username}\n\nTasdiqlaysizmi?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, @ bilan boshlanadigan kanal, ID (-100...) yoki http link yuboring!")
        return

    if context.user_data.get("waiting_channel_new_name"):
        new_name = text.strip()
        username = context.user_data.get("rename_channel_username")
        if username and new_name:
            c.execute("UPDATE channels SET display_name = ? WHERE channel_username = ?", (new_name, username))
            conn.commit()
            log_admin_action(user.id, "Kanal nomi o'zgartirildi", f"{username} -> {new_name}")
            await update.message.reply_text(
                "✅ Kanal nomi muvaffaqiyatli yangilandi!",
                parse_mode='HTML',
                reply_markup=get_channel_settings_keyboard()
            )
        else:
            await update.message.reply_text("❌ Yangi nom noto'g'ri!")
        context.user_data.clear()
        return

    if context.user_data.get("waiting_part_code"):
        # Check if code exists
        film = get_film_by_code(text)
        if film:
            context.user_data["part_film_code"] = text
            context.user_data["waiting_part_code"] = False
            
            await update.message.reply_text(
                f"✅ Kod qabul qilindi: <code>{text}</code>\n"
                f"Film: <b>{film['caption']}</b>\n\n"
                f"Nechinchi qismni qo'shmoqchisiz? (Raqam yuboring):",
                parse_mode='HTML'
            )
            context.user_data["waiting_part_number"] = True
        else:
            await update.message.reply_text("❌ Bu kodda film topilmadi! Iltimos, mavjud kodni yuboring.")
        return

    if context.user_data.get("waiting_part_number"):
        if text.isdigit():
            part_num = int(text)
            context.user_data["part_number"] = part_num
            context.user_data["waiting_part_number"] = False
            
            await update.message.reply_text(
                f"✅ {part_num}-qism tanlandi.\n\n"
                f"Endi shu qismning faylini (video/hujjat) Caption (izoh) bilan birga yuboring:\n"
                f"Eslatma: Captionda HTML format (havola, yashirin link) ishlatishingiz mumkin.",
                parse_mode='HTML'
            )
            context.user_data["waiting_part_file"] = True
        else:
             await update.message.reply_text("❌ Iltimos, faqat raqam yuboring!")
        return

    if context.user_data.get("waiting_part_file"):
        if update.message.video or update.message.document:
            film_code = context.user_data.get("part_film_code")
            part_number = context.user_data.get("part_number")
            
            if update.message.video:
                file_id = update.message.video.file_id
                file_type = "video"
            else:
                file_id = update.message.document.file_id
                file_type = "document"
            
            # Use caption_html if available (requires proper configuration)
            # or use caption_entities to reconstruct HTML.
            # For simplicity, if update.message.caption_html is available, use it.
            # If not, use update.message.caption.
            
            # Note: PTB objects usually have caption (str).
            # To get HTML, we might need to parse entities.
            # But let's assume the user sends text that might contain HTML tags IF the bot parses it?
            # No, user sends formatted text (bold, link) in Telegram client.
            # Bot receives text + entities.
            # We need to convert (text + entities) -> HTML string.
            
            # Helper function to convert entities to HTML?
            # Or just save the caption as is, and rely on send_video(..., caption=..., parse_mode=None)
            # BUT we want to support hidden links.
            # If we save plain text, hidden links are lost.
            
            # Let's try to get HTML caption.
            # If we can't easily, we just take the caption text.
            # However, the user specifically asked for "HTML, shrift, emoji, yashirin havola".
            
            caption = update.message.caption_html if hasattr(update.message, 'caption_html') else (update.message.caption or f"{part_number}-qism")
            
            save_film_part(film_code, part_number, file_id, file_type, caption)
            log_admin_action(user.id, "Film qismi qo'shildi", f"Kod: {film_code}, Part: {part_number}")
            
            await update.message.reply_text(
                f"✅ <b>{part_number}-qism muvaffaqiyatli saqlandi!</b>\n\n"
                f"Film kodi: <code>{film_code}</code>",
                parse_mode='HTML'
            )
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, video yoki hujjat formatida fayl yuboring!")
        return

    if context.user_data.get("waiting_post_content"):
        if update.message.photo or update.message.video or update.message.text:
            context.user_data["post_content"] = update.message
            context.user_data["waiting_post_content"] = False
            
            await update.message.reply_text(
                "✅ Post kontenti qabul qilindi.\n\n"
                "Endi tugmalarni quyidagi formatda yuboring:\n"
                "<code>Tugma nomi - https://link.com</code>\n"
                "<code>Ikkinchi tugma - https://link2.com</code>\n\n"
                "Agar tugma kerak bo'lmasa '0' yuboring.",
                parse_mode='HTML'
            )
            context.user_data["waiting_post_buttons"] = True
        else:
            await update.message.reply_text("❌ Iltimos, Rasm, Video yoki Matn yuboring!")
        return

    if context.user_data.get("waiting_post_buttons"):
        buttons = []
        if text != "0":
            lines = text.split("\n")
            for line in lines:
                parts = line.split("-")
                if len(parts) >= 2:
                    name = to_bold(parts[0].strip())
                    link = "-".join(parts[1:]).strip()
                    buttons.append([InlineKeyboardButton(name, url=link)])
        
        context.user_data["post_buttons"] = buttons
        context.user_data["waiting_post_buttons"] = False
        
        # Show preview
        post_msg = context.user_data.get("post_content")
        reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
        
        await update.message.reply_text("👁 <b>POST OLDINDAN KO'RISH</b>", parse_mode='HTML')
        
        try:
            if post_msg.photo:
                await update.message.reply_photo(post_msg.photo[-1].file_id, caption=post_msg.caption, reply_markup=reply_markup)
            elif post_msg.video:
                await update.message.reply_video(post_msg.video.file_id, caption=post_msg.caption, reply_markup=reply_markup)
            else:
                await update.message.reply_text(post_msg.text, reply_markup=reply_markup)
        except Exception as e:
            await update.message.reply_text(f"❌ Xatolik: {e}")
            return

        keyboard = [
            [InlineKeyboardButton("✅ Kanalga yuborish", callback_data="confirm_post_send")],
            [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")]
        ]
        await update.message.reply_text(
            "Postni kanalga yuborasizmi?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if context.user_data.get("waiting_film_code_delete"):
        film = get_film_by_code(text)
        if film:
            keyboard = [
                [InlineKeyboardButton("✅ O'chirish", callback_data=f"confirm_delete_film_{text}")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")]
            ]
            await update.message.reply_text(
                f"Film o'chirish:\n\nKod: <code>{text}</code>\n\nTasdiqlaysizmi?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text("❌ Bu kodda film topilmadi!")
        context.user_data.clear()
        return

    if context.user_data.get("waiting_film_code_edit"):
        film = get_film_by_code(text)
        if film:
            context.user_data["edit_film_code"] = text
            context.user_data["waiting_film_code_edit"] = False
            current_caption = film['caption'] or "Yo'q"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Film captionni tahrirlash", callback_data=f"film_edit_caption_{text}")],
                [InlineKeyboardButton("🎞 Film faylini yangilash", callback_data=f"film_edit_file_{text}")],
                [InlineKeyboardButton("🧩 Qismlar ro'yxati", callback_data=f"film_parts_list_{text}")],
                [InlineKeyboardButton("⬅ Orqaga", callback_data="show_film_settings")]
            ])
            await update.message.reply_text(
                f"Film: <code>{text}</code>\n\n"
                f"Joriy caption: <i>{current_caption}</i>\n\n"
                f"Tanlang:",
                parse_mode='HTML',
                reply_markup=keyboard
            )
        else:
            await update.message.reply_text("❌ Bu kodda film topilmadi!")
            context.user_data.clear()
        return

    if context.user_data.get("waiting_new_caption"):
        film_code = context.user_data.get("edit_film_code")
        if film_code:
            update_film_caption(film_code, text)
            log_admin_action(user.id, "Film tahrirlandi", f"Kod: {film_code}")
            await update.message.reply_text("✅ Film caption yangilandi!")
            context.user_data.clear()
        return
    
    if context.user_data.get("waiting_main_film_file_update"):
        if update.message.video or update.message.document:
            film_code = context.user_data.get("edit_film_code")
            if update.message.video:
                file_id = update.message.video.file_id
                file_type = "video"
            else:
                file_id = update.message.document.file_id
                file_type = "document"
            update_film_file(film_code, file_id, file_type)
            log_admin_action(user.id, "Film fayli yangilandi", f"Kod: {film_code}")
            await update.message.reply_text("✅ Film fayli yangilandi!", reply_markup=get_film_settings_keyboard())
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, video yoki hujjat yuboring!")
        return

    if context.user_data.get("waiting_part_file_update"):
        if update.message.video or update.message.document:
            film_code = context.user_data.get("edit_part_film_code")
            part_number = context.user_data.get("edit_part_number")
            if update.message.video:
                file_id = update.message.video.file_id
                file_type = "video"
            else:
                file_id = update.message.document.file_id
                file_type = "document"
            update_film_part_file(film_code, part_number, file_id, file_type)
            log_admin_action(user.id, "Film qismi tahrirlandi (fayl)", f"Kod: {film_code}, Part: {part_number}")
            await update.message.reply_text("✅ Qism fayli yangilandi!", reply_markup=get_film_settings_keyboard())
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Iltimos, video yoki hujjat yuboring!")
        return

    if context.user_data.get("waiting_part_caption_update"):
        film_code = context.user_data.get("edit_part_film_code")
        part_number = context.user_data.get("edit_part_number")
        update_film_part_caption(film_code, part_number, text)
        log_admin_action(user.id, "Film qismi tahrirlandi (caption)", f"Kod: {film_code}, Part: {part_number}")
        await update.message.reply_text("✅ Qism caption yangilandi!", reply_markup=get_film_settings_keyboard())
        context.user_data.clear()
        return

    if context.user_data.get("waiting_film_search_query"):
        results = search_films(text)
        if results:
            result_text = f"🔍 <b>Qidiruv natijalari: \"{text}\"</b>\n\n"
            for code, caption, file_type in results[:10]:
                result_text += f"📌 Kod: <code>{code}</code>\n"
                result_text += f"   Tur: {file_type}\n"
                if caption:
                    result_text += f"   Caption: {caption[:50]}...\n"
                result_text += "\n"
            await update.message.reply_text(result_text, parse_mode='HTML')
        else:
            await update.message.reply_text("❌ Hech narsa topilmadi!")
        context.user_data.clear()
        return

    if text and not text.startswith(("ℹ️", "📢", "⚙", "📡", "🎬", "📊", "/")):
        await send_film_logic(update, context, text)

async def send_channel_post(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    
    target_channel = data.get("target_channel")
    chat_id = None
    if target_channel:
        chat_id = target_channel if target_channel.startswith("-100") else target_channel
    else:
        main_channel = get_bot_setting("main_channel")
        if not main_channel:
            main_channel = CHANNEL_USERNAME
        chat_id = main_channel if main_channel.startswith("-100") else (main_channel if main_channel.startswith("@") else None)
    
    if not chat_id:
        logging.error("Channel not configured correctly for post")
        return

    try:
        # Bot username for deep link
        bot_username = context.bot.username
        url = f"https://t.me/{bot_username}?start={data['code']}"
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(data['btn_text'], url=url)]])
        
        if data['file_type'] == "photo":
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=data['file_id'],
                caption=data['caption'],
                parse_mode='HTML',
                reply_markup=keyboard
            )
        elif data['file_type'] == "video":
            await context.bot.send_video(
                chat_id=chat_id,
                video=data['file_id'],
                caption=data['caption'],
                parse_mode='HTML',
                reply_markup=keyboard
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=data['caption'],
                parse_mode='HTML',
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        
        # Notify admin
        if data.get('admin_id'):
            await context.bot.send_message(data['admin_id'], "✅ Post kanalga yuborildi!")
            
    except Exception as e:
        logging.error(f"Error sending channel post: {e}")
        if data.get('admin_id'):
            await context.bot.send_message(data['admin_id'], f"❌ Post yuborishda xatolik: {e}")

async def send_post_to_channel_immediate(context: ContextTypes.DEFAULT_TYPE, data: dict):
    target_channel = data.get("target_channel")
    chat_id = None
    if target_channel:
        chat_id = target_channel if target_channel.startswith("-100") else target_channel
    else:
        main_channel = get_bot_setting("main_channel")
        if not main_channel:
            main_channel = CHANNEL_USERNAME
        chat_id = main_channel if main_channel.startswith("-100") else (main_channel if main_channel.startswith("@") else None)
    if not chat_id:
        return False, "Channel not configured"
    try:
        bot_username = context.bot.username
        url = f"https://t.me/{bot_username}?start={data['code']}"
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(data['btn_text'], url=url)]])
        if data['file_type'] == "photo":
            await context.bot.send_photo(chat_id=chat_id, photo=data['file_id'], caption=data['caption'], parse_mode='HTML', reply_markup=keyboard)
        elif data['file_type'] == "video":
            await context.bot.send_video(chat_id=chat_id, video=data['file_id'], caption=data['caption'], parse_mode='HTML', reply_markup=keyboard)
        else:
            await context.bot.send_message(chat_id=chat_id, text=data['caption'], parse_mode='HTML', reply_markup=keyboard, disable_web_page_preview=True)
        if data.get('admin_id'):
            await context.bot.send_message(data['admin_id'], "✅ Post kanalga yuborildi!")
        return True, ""
    except Exception as e:
        if data.get('admin_id'):
            await context.bot.send_message(data['admin_id'], f"❌ Post yuborishda xatolik: {e}")
        return False, str(e)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if query.data == "post_cancel":
        context.user_data.clear()
        await query.message.edit_text("❌ Post yaratish bekor qilindi.")
        return

    if query.data.startswith("post_target_idx_"):
        if not has_permission(user_id, "POST_CREATE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
            return
        
        try:
            idx = int(query.data.replace("post_target_idx_", ""))
        except ValueError:
            await query.answer("Noto'g'ri kanal tanlovi!", show_alert=True)
            return
        
        channels = context.user_data.get("post_available_channels")
        if not channels or idx < 0 or idx >= len(channels):
            await query.answer("Kanal topilmadi. Qaytadan urinib ko'ring.", show_alert=True)
            return
        
        username, ch_type, display_name, invite_link = channels[idx]
        context.user_data["post_target_channel"] = username
        name = display_name if display_name else username
        
        schedule_keyboard = [
            [InlineKeyboardButton("Hozir yuborish", callback_data="post_schedule_now")],
            [InlineKeyboardButton("1 soatdan keyin", callback_data="post_schedule_1h"),
             InlineKeyboardButton("2 soatdan keyin", callback_data="post_schedule_2h")],
            [InlineKeyboardButton("3 soatdan keyin", callback_data="post_schedule_3h"),
             InlineKeyboardButton("4 soatdan keyin", callback_data="post_schedule_4h")],
            [InlineKeyboardButton("5 soatdan keyin", callback_data="post_schedule_5h"),
             InlineKeyboardButton("6 soatdan keyin", callback_data="post_schedule_6h")],
            [InlineKeyboardButton("7 soatdan keyin", callback_data="post_schedule_7h"),
             InlineKeyboardButton("8 soatdan keyin", callback_data="post_schedule_8h")],
            [InlineKeyboardButton("9 soatdan keyin", callback_data="post_schedule_9h"),
             InlineKeyboardButton("10 soatdan keyin", callback_data="post_schedule_10h")],
             [InlineKeyboardButton("❌ Bekor qilish", callback_data="post_cancel")]
        ]
        
        await query.message.edit_text(
            f"📡 <b>KANAL TANLANDI:</b> {name}\n\n"
            "⏰ Qachon yuborilsin?",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(schedule_keyboard)
        )
        return

    if query.data.startswith("post_schedule_"):
        schedule_type = query.data.replace("post_schedule_", "")
        
        if not has_permission(user_id, "POST_CREATE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
            return
        
        # Prepare data
        post_data = {
            "file_type": context.user_data.get("post_file_type"),
            "file_id": context.user_data.get("post_file_id"),
            "caption": context.user_data.get("post_caption"),
            "btn_text": context.user_data.get("post_btn_text"),
            "code": context.user_data.get("post_code"),
            "admin_id": user_id,
            "target_channel": context.user_data.get("post_target_channel")
        }
        
        if not post_data["code"] or not post_data["target_channel"]:
             await query.message.edit_text("❌ Ma'lumotlar yo'qolgan. Kanal va kodni qaytadan belgilang.")
             return

        delay = 0
        msg_text = ""
        
        if schedule_type == "now":
            delay = 0
            msg_text = "✅ Post hozir yuborilmoqda..."
        elif schedule_type.endswith("h"):
            hours = int(schedule_type[:-1])
            delay = hours * 3600
            msg_text = f"✅ Post {hours} soatdan keyin yuboriladi."
        
        if delay == 0:
            ok, err = await send_post_to_channel_immediate(context, post_data)
            if ok:
                await query.message.edit_text("✅ Post yuborildi!")
            else:
                await query.message.edit_text(f"❌ Xatolik: {err}")
            context.user_data.clear()
        else:
            context.job_queue.run_once(send_channel_post, delay, data=post_data)
            await query.message.edit_text(msg_text)
            context.user_data.clear()
        return

    # 1. Membership check (admins are exempt)
    if query.data != "check_membership" and not is_admin(user_id):
        not_joined = await is_member(user_id)
        if not_joined:
            await query.answer("⚠️ Avval kanallarga a'zo bo'ling!", show_alert=True)
            try:
                await query.message.edit_text(
                    "⚠️ <b>DIQQAT!</b>\n\n"
                    "Siz botning majburiy kanallaridan chiqib ketgansiz.\n"
                    "Botdan foydalanish uchun quyidagi kanallarga a'zo bo'ling:",
                    parse_mode='HTML',
                    reply_markup=get_subscription_keyboard(not_joined)
                )
            except:
                await query.message.reply_text(
                    "⚠️ <b>DIQQAT!</b>\n\n"
                    "Siz botning majburiy kanallaridan chiqib ketgansiz.\n"
                    "Botdan foydalanish uchun quyidagi kanallarga a'zo bo'ling:",
                    parse_mode='HTML',
                    reply_markup=get_subscription_keyboard(not_joined)
                )
            return

    # 2. Access control
    # Admins have full access.
    # Users only have access to "check_membership" (handled above) and "get_part_..." buttons.
    if not is_admin(user_id):
        if query.data != "check_membership" and not query.data.startswith("get_part_"):
             await query.answer("Sizda ruxsat yo'q!", show_alert=True)
             return

    if query.data == "check_membership":
        if is_blocked(user_id):
            await query.answer("Siz bloklangansiz!", show_alert=True)
            return
            
        not_joined = await is_member(user_id)
        if not not_joined:
            save_user(user_id)
            if is_admin(user_id):
                await query.message.reply_text(
                    "✅ Botdan foydalanishingiz mumkin.\n\n🎛 <b>ADMIN PANEL</b>",
                    parse_mode='HTML',
                    reply_markup=get_admin_main_keyboard()
                )
            else:
                await query.message.reply_text(
                    "✅ Botdan foydalanishingiz mumkin.\n\n✍️ Film kodini yuboring.",
                    reply_markup=get_user_keyboard()
                )
        else:
            await query.answer("Hali kanallarga a'zo bo'lmadingiz!", show_alert=True)
            # Optionally update the message with new list
            await query.message.edit_reply_markup(reply_markup=get_subscription_keyboard(not_joined))

    elif query.data == "back_main":
        await query.message.edit_text(
            "🎛 <b>ADMIN PANEL</b>\n\nBo'limlardan birini tanlang:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Reklama", callback_data="show_ad")],
                [InlineKeyboardButton("⚙ Admin sozlamalari", callback_data="show_admin_settings")],
                [InlineKeyboardButton("📡 Kanal sozlamalari", callback_data="show_channel_settings")],
                [InlineKeyboardButton("🎬 Film sozlamalari", callback_data="show_film_settings")],
                [InlineKeyboardButton("📊 Statistika", callback_data="show_stats")]
            ])
        )

    elif query.data == "return_main_menu":
        context.user_data.clear()
        await query.message.delete()
        await context.bot.send_message(
            chat_id=user_id,
            text="🎛 <b>ADMIN PANEL</b>\n\nBo'limlardan birini tanlang:",
            parse_mode='HTML',
            reply_markup=get_admin_main_keyboard()
        )

    elif query.data == "show_admin_settings":
        await query.message.edit_text(
            "⚙ <b>ADMIN SOZLAMALARI</b>\n\nQuyidagi amallardan birini tanlang:",
            parse_mode='HTML',
            reply_markup=get_admin_settings_keyboard()
        )

    elif query.data == "show_channel_settings":
        await query.message.edit_text(
            "📡 <b>KANAL SOZLAMALARI</b>\n\nMajburiy kanallarni boshqaring:",
            parse_mode='HTML',
            reply_markup=get_channel_settings_keyboard()
        )

    elif query.data == "show_film_settings":
        await query.message.edit_text(
            "🎬 <b>FILM SOZLAMALARI</b>\n\nFilmlarni boshqaring:",
            parse_mode='HTML',
            reply_markup=get_film_settings_keyboard()
        )

    elif query.data == "admin_add":
        if not has_permission(user_id, "ADMIN_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "➕ <b>ADMIN QO'SHISH</b>\n\n"
                "Qo'shmoqchi bo'lgan foydalanuvchining User ID sini yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_admin_id"] = True

    elif query.data.startswith("confirm_add_admin_"):
        new_admin_id = int(query.data.split("_")[-1])
        context.user_data["perm_mode"] = "new"
        context.user_data["perm_target_admin_id"] = new_admin_id
        context.user_data["perm_selected"] = set()
        kb = []
        row = []
        for key, label in PERMISSIONS:
            mark = "✅" if key in context.user_data["perm_selected"] else "➖"
            row.append(InlineKeyboardButton(f"{mark} {label}", callback_data=f"perm_toggle_{key}"))
            if len(row) == 2:
                kb.append(row)
                row = []
        if row:
            kb.append(row)
        kb.append([InlineKeyboardButton("💾 Saqlash", callback_data="perm_save_new")])
        kb.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")])
        await query.message.edit_text(
            f"Yangi admin: <code>{new_admin_id}</code>\nHuquqlarni tanlang:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif query.data == "admin_remove":
        c.execute("SELECT user_id FROM admins WHERE user_id != ?", (MAIN_ADMIN_ID,))
        admins = c.fetchall()
        if admins:
            keyboard = []
            for (admin_id,) in admins:
                keyboard.append([InlineKeyboardButton(f"🗑 {admin_id}", callback_data=f"del_admin_{admin_id}")])
            keyboard.append([InlineKeyboardButton("⬅ Orqaga", callback_data="show_admin_settings")])
            keyboard.append([InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")])
            await query.message.edit_text(
                "➖ <b>ADMIN O'CHIRISH</b>\n\n"
                "O'chirmoqchi bo'lgan adminni tanlang:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await query.message.edit_text(
                "❌ Hech qanday admin yo'q (asosiy admindan tashqari)!",
                reply_markup=get_admin_settings_keyboard()
            )

    elif query.data.startswith("del_admin_"):
        admin_to_del = int(query.data.split("_")[-1])
        keyboard = [
            [InlineKeyboardButton("✅ Ha, o'chirish", callback_data=f"confirm_del_admin_{admin_to_del}")],
            [InlineKeyboardButton("❌ Bekor qilish", callback_data="admin_remove")],
            [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")]
        ]
        await query.message.edit_text(
            f"⚠️ <b>TASDIQLASH</b>\n\n"
            f"Admin ID: <code>{admin_to_del}</code>\n\n"
            f"Haqiqatan ham bu adminni o'chirmoqchimisiz?",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data.startswith("confirm_del_admin_"):
        admin_to_del = int(query.data.split("_")[-1])
        if not has_permission(user_id, "ADMIN_REMOVE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            c.execute("DELETE FROM admins WHERE user_id = ?", (admin_to_del,))
            conn.commit()
            log_admin_action(user_id, "Admin o'chirildi", f"ID: {admin_to_del}")
            await query.message.edit_text(
                f"✅ Admin muvaffaqiyatli o'chirildi!\n\nID: <code>{admin_to_del}</code>",
                parse_mode='HTML',
                reply_markup=get_admin_settings_keyboard()
            )

    elif query.data == "user_block":
        if not has_permission(user_id, "USER_BLOCK"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "🚫 <b>USER BLOKLASH</b>\n\n"
                "Bloklamoqchi bo'lgan foydalanuvchining User ID sini yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_block_user_id"] = True

    elif query.data.startswith("confirm_block_user_"):
        block_user_id = int(query.data.split("_")[-1])
        if not has_permission(user_id, "USER_BLOCK"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            try:
                c.execute("INSERT INTO blocked_users (user_id, blocked_by, blocked_date) VALUES (?, ?, ?)",
                          (block_user_id, user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
                log_admin_action(user_id, "User bloklandi", f"ID: {block_user_id}")
                await query.message.edit_text(f"✅ User muvaffaqiyatli bloklandi!\n\nID: <code>{block_user_id}</code>", parse_mode='HTML')
            except:
                await query.message.edit_text("❌ Bu user allaqachon bloklangan!")

    elif query.data == "user_unblock":
        if not has_permission(user_id, "USER_UNBLOCK"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "✅ <b>USER BLOKDAN CHIQARISH</b>\n\n"
                "Blokdan chiqarmoqchi bo'lgan foydalanuvchining User ID sini yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_unblock_user_id"] = True

    elif query.data.startswith("confirm_unblock_user_"):
        unblock_user_id = int(query.data.split("_")[-1])
        if not has_permission(user_id, "USER_UNBLOCK"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            c.execute("DELETE FROM blocked_users WHERE user_id = ?", (unblock_user_id,))
            conn.commit()
            log_admin_action(user_id, "User blokdan chiqarildi", f"ID: {unblock_user_id}")
            await query.message.edit_text(f"✅ User muvaffaqiyatli blokdan chiqarildi!\n\nID: <code>{unblock_user_id}</code>", parse_mode='HTML')

    elif query.data == "edit_about_text":
        current_text = get_bot_setting("about_text")
        if not current_text:
            current_text = "Hozircha ma'lumot yo'q."
            
        await query.message.edit_text(
            f"📝 <b>BOT HAQIDA TAHRIRLASH</b>\n\n"
            f"Joriy matn:\n<i>{current_text}</i>\n\n"
            f"Yangi matnni yuboring:",
            parse_mode='HTML'
        )
        context.user_data["waiting_about_text"] = True

    elif query.data == "download_db":
        if not has_permission(user_id, "DB_DOWNLOAD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text("📥 Users.db tayyorlanmoqda...")
            await context.bot.send_document(
                chat_id=user_id,
                document=open("users.db", "rb"),
                filename="users.db",
                caption="📥 Users database"
            )
            log_admin_action(user_id, "Users.db yuklab olindi", "")

    elif query.data == "download_logs":
        if not has_permission(user_id, "LOGS_DOWNLOAD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text("📥 Admin loglari tayyorlanmoqda...")
            c.execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 1000")
            logs = c.fetchall()
            
            csv_file = "admin_logs.csv"
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["ID", "Admin ID", "Action", "Details", "Timestamp"])
                writer.writerows(logs)
            
            await context.bot.send_document(
                chat_id=user_id,
                document=open(csv_file, "rb"),
                filename="admin_logs.csv",
                caption="📋 Admin logs (oxirgi 1000 ta)"
            )
            os.remove(csv_file)
            log_admin_action(user_id, "Admin loglari yuklab olindi", "")

    elif query.data == "download_blocked":
        if not has_permission(user_id, "LOGS_DOWNLOAD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text("📥 Bloklanganlar fayli tayyorlanmoqda...")
            c.execute("SELECT user_id, blocked_by, blocked_date, reason FROM blocked_users ORDER BY blocked_date DESC")
            rows = c.fetchall()
            csv_file = "blocked_users.csv"
            admin_count = 0
            auto_count = 0
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["category", "user_id", "blocked_by", "blocked_date", "reason"])
                for uid, by, date, reason in rows:
                    category = "bot_blocked" if (reason == "bot_blocked" or by == 0) else "admin_blocked"
                    if category == "bot_blocked":
                        auto_count += 1
                    else:
                        admin_count += 1
                    writer.writerow([category, uid, by, date, reason or ""])
                writer.writerow([])
                writer.writerow(["summary", f"admin_blocked={admin_count}", f"bot_blocked={auto_count}", f"total={len(rows)}"])
            await context.bot.send_document(
                chat_id=user_id,
                document=open(csv_file, "rb"),
                filename="blocked_users.csv",
                caption="⛔ Bloklanganlar ro'yxati (kategoriya bilan)"
            )
            os.remove(csv_file)
            log_admin_action(user_id, "Bloklanganlar fayli yuklandi", f"admin={admin_count}, bot={auto_count}, total={len(rows)}")

    elif query.data == "reset_db":
        await query.answer("Bu funksiya o'chirilgan.", show_alert=True)
    elif query.data == "confirm_reset_db":
        await query.answer("Bu funksiya o'chirilgan.", show_alert=True)

    elif query.data == "admin_perms":
        if not has_permission(user_id, "ADMIN_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            c.execute("SELECT user_id, permissions FROM admins ORDER BY user_id")
            admins = c.fetchall()
            if admins:
                keyboard = []
                for admin_id_value, perms in admins:
                    keyboard.append([InlineKeyboardButton(f"{admin_id_value}", callback_data=f"perm_edit_{admin_id_value}")])
                keyboard.append([InlineKeyboardButton("⬅ Orqaga", callback_data="show_admin_settings")])
                await query.message.edit_text(
                    "Adminni tanlang:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await query.message.edit_text("❌ Adminlar yo'q!", reply_markup=get_admin_settings_keyboard())

    elif query.data == "channel_add":
        if not has_permission(user_id, "CHANNEL_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
            "➕ <b>KANAL QO'SHISH</b>\n\n"
            "Kanal username (@channel) yoki havola yuboring:\n\n"
            "Misol: @multklar_olami\n"
            "Misol: https://instagram.com/...",
            parse_mode='HTML'
            )
            context.user_data["waiting_channel_username"] = True

    elif query.data.startswith("confirm_add_channel_"):
        if not has_permission(user_id, "CHANNEL_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
            return
        parts = query.data.replace("confirm_add_channel_", "").split("_")
        channel_type = parts[-1]
        channel_username = "_".join(parts[:-1])
        
        display_name = None
        invite_link = None
        
        if channel_type == "Telegram":
            try:
                chat_id = channel_username if channel_username.startswith("-100") else channel_username
                chat = await context.bot.get_chat(chat_id)
                display_name = chat.title
                invite_link = chat.invite_link
                if not invite_link:
                    try:
                        invite_link = await context.bot.export_chat_invite_link(chat_id)
                    except:
                        pass
            except Exception as e:
                logging.error(f"Error getting chat info for {channel_username}: {e}")
                # If we can't get info, we might not be admin or it's invalid.
                # But let's try to add it anyway if the user insists? 
                # Or fail gracefully? Let's add it, maybe they make bot admin later.
        
        try:
            c.execute("INSERT INTO channels (channel_username, channel_type, added_by, added_date, display_name, invite_link) VALUES (?, ?, ?, ?, ?, ?)",
                      (channel_username, channel_type, user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), display_name, invite_link))
            conn.commit()
            log_admin_action(user_id, "Kanal qo'shildi", f"{channel_username}")
            await query.message.edit_text(f"✅ Kanal muvaffaqiyatli qo'shildi!\n\n{channel_username}\nNomi: {display_name}", parse_mode='HTML')
        except sqlite3.IntegrityError:
            await query.message.edit_text("❌ Bu kanal allaqachon qo'shilgan!")
        except Exception as e:
            logging.error(f"Error adding channel DB: {e}")
            await query.message.edit_text("❌ Xatolik yuz berdi!")

    elif query.data == "channel_remove":
        if not has_permission(user_id, "CHANNEL_REMOVE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            channels = get_all_channels()
            if channels:
                keyboard = []
                for username, ch_type, display_name, invite_link in channels:
                    name = display_name if display_name else username
                    keyboard.append([InlineKeyboardButton(f"🗑 {name} ({ch_type})", callback_data=f"del_channel_{username}")])
                keyboard.append([InlineKeyboardButton("⬅ Orqaga", callback_data="show_channel_settings")])
                keyboard.append([InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")])
                await query.message.edit_text(
                    "➖ <b>KANALNI O'CHIRISH</b>\n\n"
                    "O'chirmoqchi bo'lgan kanalni tanlang:",
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await query.message.edit_text(
                    "❌ Hech qanday kanal yo'q!",
                    reply_markup=get_channel_settings_keyboard()
                )

    elif query.data == "channel_rename":
        channels = get_all_channels()
        if channels:
            keyboard = []
            for username, ch_type, display_name, invite_link in channels:
                name = display_name if display_name else username
                keyboard.append([InlineKeyboardButton(name, callback_data=f"rename_channel_{username}")])
            keyboard.append([InlineKeyboardButton("⬅ Orqaga", callback_data="show_channel_settings")])
            keyboard.append([InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")])
            await query.message.edit_text(
                "✏️ <b>KANAL NOMINI O'ZGARTIRISH</b>\n\nO'zgartirmoqchi bo'lgan kanalni tanlang:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await query.message.edit_text(
                "❌ Hech qanday kanal yo'q!",
                reply_markup=get_channel_settings_keyboard()
            )

    elif query.data == "change_main_channel":
        if not has_permission(user_id, "MAIN_CHANNEL_CHANGE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            current_main = get_bot_setting("main_channel")
            if not current_main:
                current_main = CHANNEL_USERNAME
                
            await query.message.edit_text(
                f"⭐️ <b>ASOSIY KANALNI O'ZGARTIRISH</b>\n\n"
                f"Joriy kanal: {current_main}\n\n"
                f"Yangi asosiy kanal Username yoki ID sini yuboring:\n"
                f"Misol: @kanal_username yoki -1001234567890",
                parse_mode='HTML'
            )
            context.user_data["waiting_main_channel"] = True

    elif query.data.startswith("del_channel_"):
        channel_to_del = query.data.replace("del_channel_", "")
        keyboard = [
            [InlineKeyboardButton("✅ Ha, o'chirish", callback_data=f"confirm_del_channel_{channel_to_del}")],
            [InlineKeyboardButton("❌ Bekor qilish", callback_data="channel_remove")],
            [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="return_main_menu")]
        ]
        await query.message.edit_text(
            f"⚠️ <b>TASDIQLASH</b>\n\n"
            f"Kanal: <code>{channel_to_del}</code>\n\n"
            f"Haqiqatan ham bu kanalni o'chirmoqchimisiz?",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data.startswith("confirm_del_channel_"):
        if not has_permission(user_id, "CHANNEL_REMOVE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            channel_to_del = query.data.replace("confirm_del_channel_", "")
            c.execute("DELETE FROM channels WHERE channel_username = ?", (channel_to_del,))
            conn.commit()
            log_admin_action(user_id, "Kanal o'chirildi", f"{channel_to_del}")
            await query.message.edit_text(
                f"✅ Kanal muvaffaqiyatli o'chirildi!\n\n{channel_to_del}",
                parse_mode='HTML',
                reply_markup=get_channel_settings_keyboard()
            )

    elif query.data.startswith("rename_channel_"):
        channel_to_rename = query.data.replace("rename_channel_", "")
        context.user_data["rename_channel_username"] = channel_to_rename
        context.user_data["waiting_channel_new_name"] = True
        await query.message.edit_text(
            f"Kanal: <code>{channel_to_rename}</code>\n\nYangi nomni yuboring:",
            parse_mode='HTML'
        )
    
    elif query.data.startswith("perm_edit_"):
        if not has_permission(user_id, "ADMIN_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            target_admin = int(query.data.split("_")[-1])
            selected = get_admin_permissions(target_admin)
            context.user_data["perm_mode"] = "edit"
            context.user_data["perm_target_admin_id"] = target_admin
            context.user_data["perm_selected"] = set(selected)
            kb = []
            row = []
            for key, label in PERMISSIONS:
                mark = "✅" if key in context.user_data["perm_selected"] else "➖"
                row.append(InlineKeyboardButton(f"{mark} {label}", callback_data=f"perm_toggle_{key}"))
                if len(row) == 2:
                    kb.append(row)
                    row = []
            if row:
                kb.append(row)
            kb.append([InlineKeyboardButton("💾 Saqlash", callback_data="perm_save_edit")])
            kb.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")])
            await query.message.edit_text(
                f"Admin: <code>{target_admin}</code>\nHuquqlarni tanlang:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(kb)
            )
    
    elif query.data.startswith("perm_toggle_"):
        if not has_permission(user_id, "ADMIN_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            key = query.data.replace("perm_toggle_", "")
            if "perm_selected" not in context.user_data:
                context.user_data["perm_selected"] = set()
            selected = context.user_data["perm_selected"]
            if key in selected:
                selected.remove(key)
            else:
                selected.add(key)
            kb = []
            row = []
            for k, label in PERMISSIONS:
                mark = "✅" if k in selected else "➖"
                row.append(InlineKeyboardButton(f"{mark} {label}", callback_data=f"perm_toggle_{k}"))
                if len(row) == 2:
                    kb.append(row)
                    row = []
            if row:
                kb.append(row)
            if context.user_data.get("perm_mode") == "new":
                kb.append([InlineKeyboardButton("💾 Saqlash", callback_data="perm_save_new")])
            else:
                kb.append([InlineKeyboardButton("💾 Saqlash", callback_data="perm_save_edit")])
            kb.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_action")])
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(kb))
    
    elif query.data == "perm_save_new":
        if not has_permission(user_id, "ADMIN_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            target_admin = context.user_data.get("perm_target_admin_id")
            perms = context.user_data.get("perm_selected", set())
            if target_admin:
                try:
                    c.execute("INSERT OR IGNORE INTO admins (user_id, added_by, added_date) VALUES (?, ?, ?)",
                              (target_admin, user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    conn.commit()
                    update_admin_permissions(target_admin, perms)
                    log_admin_action(user_id, "Admin qo'shildi", f"ID: {target_admin}")
                    await query.message.edit_text(
                        f"✅ Admin qo'shildi va huquqlar saqlandi!\nID: <code>{target_admin}</code>",
                        parse_mode='HTML',
                        reply_markup=get_admin_settings_keyboard()
                    )
                except Exception as e:
                    await query.message.edit_text(f"❌ Xatolik: {e}")
            else:
                await query.answer("Ma'lumot yetarli emas!", show_alert=True)
            context.user_data.clear()
    
    elif query.data == "perm_save_edit":
        if not has_permission(user_id, "ADMIN_ADD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            target_admin = context.user_data.get("perm_target_admin_id")
            perms = context.user_data.get("perm_selected", set())
            if target_admin:
                update_admin_permissions(target_admin, perms)
                log_admin_action(user_id, "Admin huquqlari o'zgartirildi", f"ID: {target_admin}")
                await query.message.edit_text(
                    f"✅ Admin huquqlari yangilandi!\nID: <code>{target_admin}</code>",
                    parse_mode='HTML',
                    reply_markup=get_admin_settings_keyboard()
                )
            else:
                await query.answer("Ma'lumot yetarli emas!", show_alert=True)
            context.user_data.clear()

    elif query.data.startswith("no_link_"):
         await query.answer("⚠️ Bu kanal uchun havola topilmadi. Admin hali qo'shmagan bo'lishi mumkin.", show_alert=True)

    elif query.data == "channel_list":
        channels = get_all_channels()
        if channels:
            text = "📋 <b>MAJBURIY KANALLAR</b>\n\n"
            for idx, (username, ch_type, display_name, invite_link) in enumerate(channels, 1):
                name = display_name if display_name else username
                link_status = "✅" if invite_link else "⚠️ Havola yo'q"
                text += f"{idx}. {name} ({ch_type}) - {link_status}\n"
            await query.message.edit_text(text, parse_mode='HTML', reply_markup=get_channel_settings_keyboard())
        else:
            await query.message.edit_text("❌ Hech qanday kanal yo'q!", reply_markup=get_channel_settings_keyboard())

    elif query.data == "film_upload":
        if not has_permission(user_id, "FILM_UPLOAD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "📤 <b>FILM YUKLASH</b>\n\n"
                "Yuklamoqchi bo'lgan filmni yuboring (video yoki document):",
                parse_mode='HTML'
            )
            context.user_data["waiting_film_upload"] = True

    elif query.data == "film_delete":
        if not has_permission(user_id, "FILM_DELETE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "🗑 <b>FILM O'CHIRISH</b>\n\n"
                "O'chirmoqchi bo'lgan film kodini yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_film_code_delete"] = True

    elif query.data.startswith("confirm_delete_film_"):
        if not has_permission(user_id, "FILM_DELETE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            film_code = query.data.replace("confirm_delete_film_", "")
            delete_film(film_code)
            log_admin_action(user_id, "Film o'chirildi", f"Kod: {film_code}")
            await query.message.edit_text(f"✅ Film o'chirildi!\n\nKod: <code>{film_code}</code>", parse_mode='HTML')

    elif query.data == "film_edit":
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "✏️ <b>FILMNI TAHRIRLASH</b>\n\n"
                "Tahrir qilmoqchi bo'lgan film kodini yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_film_code_edit"] = True
    
    elif query.data.startswith("film_edit_caption_"):
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            film_code = query.data.replace("film_edit_caption_", "")
            context.user_data["edit_film_code"] = film_code
            context.user_data["waiting_new_caption"] = True
            await query.message.edit_text("Yangi captionni yuboring:", parse_mode='HTML')

    elif query.data.startswith("film_edit_file_"):
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            film_code = query.data.replace("film_edit_file_", "")
            context.user_data["edit_film_code"] = film_code
            context.user_data["waiting_main_film_file_update"] = True
            await query.message.edit_text("Yangi film faylini (video/hujjat) yuboring:", parse_mode='HTML')

    elif query.data.startswith("film_parts_list_"):
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            film_code = query.data.replace("film_parts_list_", "")
            parts = get_film_parts(film_code)
            if not parts:
                await query.message.edit_text("❌ Bu filmda qismlar yo'q!", reply_markup=get_film_settings_keyboard())
            else:
                keyboard = []
                for part_number, file_id, file_type, caption in parts:
                    keyboard.append([
                        InlineKeyboardButton(f"✏️ {part_number}-qismni tahrirlash", callback_data=f"part_edit_{film_code}_{part_number}")
                    ])
                    keyboard.append([
                        InlineKeyboardButton(f"🗑 {part_number}-qismni o'chirish", callback_data=f"part_delete_{film_code}_{part_number}")
                    ])
                keyboard.append([InlineKeyboardButton("⬅ Orqaga", callback_data="show_film_settings")])
                await query.message.edit_text("🧩 Qismlar:", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith("part_edit_file_"):
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            suffix = query.data.replace("part_edit_file_", "", 1)
            film_code, part_num = suffix.rsplit("_", 1)
            part_number = int(part_num)
            context.user_data["edit_part_film_code"] = film_code
            context.user_data["edit_part_number"] = part_number
            context.user_data["waiting_part_file_update"] = True
            await query.message.edit_text("Yangi faylni (video/hujjat) yuboring:", parse_mode='HTML')

    elif query.data.startswith("part_edit_caption_"):
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            suffix = query.data.replace("part_edit_caption_", "", 1)
            film_code, part_num = suffix.rsplit("_", 1)
            part_number = int(part_num)
            context.user_data["edit_part_film_code"] = film_code
            context.user_data["edit_part_number"] = part_number
            context.user_data["waiting_part_caption_update"] = True
            await query.message.edit_text("Yangi captionni yuboring:", parse_mode='HTML')

    elif query.data.startswith("part_edit_"):
        if not has_permission(user_id, "FILM_EDIT"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            suffix = query.data.replace("part_edit_", "", 1)
            film_code, part_num = suffix.rsplit("_", 1)
            part_number = int(part_num)
            context.user_data["edit_part_film_code"] = film_code
            context.user_data["edit_part_number"] = part_number
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎞 Faylni yangilash", callback_data=f"part_edit_file_{film_code}_{part_number}")],
                [InlineKeyboardButton("✏️ Captionni tahrirlash", callback_data=f"part_edit_caption_{film_code}_{part_number}")],
                [InlineKeyboardButton("⬅ Orqaga", callback_data=f"film_parts_list_{film_code}")]
            ])
            await query.message.edit_text(f"{part_number}-qism uchun amal tanlang:", parse_mode='HTML', reply_markup=keyboard)

    elif query.data.startswith("part_delete_"):
        if not has_permission(user_id, "FILM_DELETE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            suffix = query.data.replace("part_delete_", "", 1)
            film_code, part_num = suffix.rsplit("_", 1)
            part_number = int(part_num)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Ha, o'chirish", callback_data=f"confirm_del_part_{film_code}_{part_number}")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data=f"film_parts_list_{film_code}")]
            ])
            await query.message.edit_text(f"{part_number}-qismni o'chirishni tasdiqlaysizmi?", parse_mode='HTML', reply_markup=keyboard)

    elif query.data.startswith("confirm_del_part_"):
        if not has_permission(user_id, "FILM_DELETE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            suffix = query.data.replace("confirm_del_part_", "", 1)
            film_code, part_num = suffix.rsplit("_", 1)
            part_number = int(part_num)
            delete_film_part(film_code, part_number)
            log_admin_action(user_id, "Film qismi o'chirildi", f"Kod: {film_code}, Part: {part_number}")
            await query.message.edit_text("✅ Qism o'chirildi!", parse_mode='HTML', reply_markup=get_film_settings_keyboard())

    elif query.data == "film_search":
        await query.message.edit_text(
            "🔍 <b>FILM QIDIRISH</b>\n\n"
            "Qidiruv so'zini yuboring (kod yoki caption):",
            parse_mode='HTML'
        )
        context.user_data["waiting_film_search_query"] = True

    elif query.data == "film_list" or query.data.startswith("film_list_page_"):
        page = 0
        if query.data.startswith("film_list_page_"):
            page = int(query.data.split("_")[-1])
        
        limit = 10
        offset = page * limit
        films = get_all_films(offset, limit)
        total_films = get_films_count()
        total_pages = (total_films + limit - 1) // limit

        if films:
            text = f"📋 <b>BARCHA FILMLAR</b> ({page + 1}/{total_pages})\n\n"
            for code, caption, file_type, upload_date in films:
                text += f"📌 <code>{code}</code> ({file_type})\n"
                if caption:
                    text += f"   {caption[:40]}...\n"
                text += f"   📅 {upload_date}\n\n"
            
            keyboard = []
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅ Oldingi", callback_data=f"film_list_page_{page-1}"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Keyingi ➡", callback_data=f"film_list_page_{page+1}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("⬅ Orqaga", callback_data="show_film_settings")])
            
            await query.message.edit_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.message.edit_text("❌ Hech qanday film yo'q!", reply_markup=get_film_settings_keyboard())

    elif query.data == "part_upload":
        if not has_permission(user_id, "PART_UPLOAD"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "➕ <b>QISM QO'SHISH</b>\n\n"
                "Qaysi filmga qism qo'shmoqchisiz? Film kodini yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_part_code"] = True

    elif query.data == "create_post":
        if not has_permission(user_id, "POST_CREATE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            await query.message.edit_text(
                "📝 <b>POST YARATISH</b>\n\n"
                "Post uchun Rasm, Video yoki Matn yuboring:",
                parse_mode='HTML'
            )
            context.user_data["waiting_post_content"] = True

    elif query.data == "confirm_post_send":
        if not has_permission(user_id, "POST_CREATE"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            post_msg = context.user_data.get("post_content")
            buttons = context.user_data.get("post_buttons", [])
            reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
            
            channels = get_all_channels()
            count = 0
            for channel_username, channel_type, display_name, invite_link in channels:
                if channel_type == "Telegram":
                    try:
                        chat_id = channel_username if channel_username.startswith("-100") else channel_username
                        if post_msg.photo:
                            await context.bot.send_photo(chat_id, post_msg.photo[-1].file_id, caption=post_msg.caption, reply_markup=reply_markup)
                        elif post_msg.video:
                            await context.bot.send_video(chat_id, post_msg.video.file_id, caption=post_msg.caption, reply_markup=reply_markup)
                        else:
                            await context.bot.send_message(chat_id, post_msg.text, reply_markup=reply_markup)
                        count += 1
                    except Exception as e:
                        logging.error(f"Post sending error to {channel_username}: {e}")
            
            await query.message.edit_text(f"✅ Post {count} ta kanalga yuborildi!")
            context.user_data.clear()

    elif query.data.startswith("get_part_"):
        # Format: get_part_CODE_PARTNUM
        parts = query.data.split("_")
        if len(parts) >= 4:
            code = parts[2]
            part_num = int(parts[3])
            
            # Find specific part
            film_parts = get_film_parts(code)
            target_part = None

            for p in film_parts:
                if p[0] == part_num:
                    target_part = p
                    break
            
            if target_part:
                # part: (part_number, file_id, file_type, caption)
                file_id = target_part[1]
                file_type = target_part[2]
                caption = target_part[3]
                
                try:
                    if file_type == "video":
                        await context.bot.send_video(
                            user_id, 
                            video=file_id, 
                            caption=caption, 
                            parse_mode='HTML', 
                            protect_content=True
                        )
                    elif file_type == "document":
                        await context.bot.send_document(
                            user_id, 
                            document=file_id, 
                            caption=caption, 
                            parse_mode='HTML', 
                            protect_content=True
                        )
                except Exception as e:
                    await query.answer("Fayl yuborishda xatolik!", show_alert=True)
            else:
                await query.answer("Qism topilmadi!", show_alert=True)

    elif query.data == "cancel_action":
        context.user_data.clear()
        await query.message.edit_text(
            "🚫 Amal bekor qilindi.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅ Orqaga", callback_data="back_main")]
            ])
        )

    elif query.data == "approve_ad":
        if not has_permission(user_id, "AD_SEND"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            reklama_msg = context.user_data.get("reklama_content")
            if not reklama_msg:
                await query.answer("Reklama topilmadi!", show_alert=True)
                return
            
            buttons = context.user_data.get("reklama_buttons", [])
            reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
            buttons_serialized = _serialize_buttons(buttons) if buttons else []
            
            if reklama_msg.photo:
                payload = {
                    "type": "photo",
                    "file_id": reklama_msg.photo[-1].file_id,
                    "caption": (reklama_msg.caption_html if hasattr(reklama_msg, "caption_html") else (reklama_msg.caption or ""))
                }
            elif reklama_msg.video:
                payload = {
                    "type": "video",
                    "file_id": reklama_msg.video.file_id,
                    "caption": (reklama_msg.caption_html if hasattr(reklama_msg, "caption_html") else (reklama_msg.caption or ""))
                }
            elif reklama_msg.document:
                payload = {
                    "type": "document",
                    "file_id": reklama_msg.document.file_id,
                    "caption": (reklama_msg.caption_html if hasattr(reklama_msg, "caption_html") else (reklama_msg.caption or ""))
                }
            elif reklama_msg.audio:
                payload = {
                    "type": "audio",
                    "file_id": reklama_msg.audio.file_id,
                    "caption": (reklama_msg.caption_html if hasattr(reklama_msg, "caption_html") else (reklama_msg.caption or ""))
                }
            elif reklama_msg.voice:
                payload = {
                    "type": "voice",
                    "file_id": reklama_msg.voice.file_id,
                    "caption": (reklama_msg.caption_html if hasattr(reklama_msg, "caption_html") else (reklama_msg.caption or ""))
                }
            else:
                payload = {
                    "type": "text",
                    "text": (reklama_msg.text_html if hasattr(reklama_msg, "text_html") else (reklama_msg.text or ""))
                }
            
            await query.message.edit_text(
                "⏳ <b>Reklama yuborilmoqda...</b>\n\nIltimos kuting...",
                parse_mode='HTML'
            )
            
            users = get_all_users()
            c.execute("SELECT user_id FROM blocked_users")
            blocked_set = set(row[0] for row in c.fetchall())
            
            success_count, failed_ids, skipped_blocked, skipped_unreachable = await broadcast_to_users(
                context=context,
                users=users,
                payload=payload,
                reply_markup=reply_markup,
                blocked_set=blocked_set
            )
            
            failed_count = len(failed_ids)
            not_sent = skipped_blocked + skipped_unreachable + failed_count
            
            if failed_ids:
                save_last_ad_state(user_id, payload, failed_ids, buttons_serialized)
                report_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("♻️ Qayta yuborish", callback_data="ad_retry_failed"),
                     InlineKeyboardButton("❌ Bekor qilish", callback_data="ad_cancel_retry")]
                ])
            else:
                clear_last_ad_state(user_id)
                report_kb = None
            
            result_text = (
                "✅ <b>REKLAMA YUBORISH HISOBOTI</b>\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Jami user: <b>{len(users)}</b>\n"
                f"✅ Yuborildi: <b>{success_count}</b>\n"
                f"🚫 Bloklangan (DB): <b>{skipped_blocked}</b>\n"
                f"⏭ Yetib bormagan (o'tkazib yuborildi): <b>{skipped_unreachable}</b>\n"
                f"❌ Yuborilmadi (xato): <b>{failed_count}</b>\n"
                f"📌 Umumiy yuborilmadi: <b>{not_sent}</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await query.message.edit_text(result_text, parse_mode='HTML', reply_markup=report_kb)
            log_admin_action(user_id, "Reklama yuborildi", f"Yuborildi: {success_count}, Yuborilmadi: {not_sent}")
            context.user_data.clear()
    
    elif query.data == "ad_retry_failed":
        if not has_permission(user_id, "AD_SEND"):
            await query.answer("Sizda ruxsat yo'q!", show_alert=True)
        else:
            state = load_last_ad_state(user_id)
            if not state or not state.get("failed"):
                await query.answer("Qayta yuboriladigan user qolmadi.", show_alert=True)
                return
            
            payload = state.get("payload", {})
            failed_ids = state.get("failed", [])
            reply_markup = _build_markup_from_serialized(state.get("buttons", []))
            
            await query.message.edit_text(
                "⏳ <b>Qayta yuborish boshlandi...</b>\n\nFaqat yuborilmay qolganlarga yuboriladi.",
                parse_mode='HTML'
            )
            
            c.execute("SELECT user_id FROM blocked_users")
            blocked_set = set(row[0] for row in c.fetchall())
            
            success_count, new_failed_ids, skipped_blocked, skipped_unreachable = await broadcast_to_users(
                context=context,
                users=failed_ids,
                payload=payload,
                reply_markup=reply_markup,
                blocked_set=blocked_set
            )
            
            failed_count = len(new_failed_ids)
            not_sent = skipped_blocked + skipped_unreachable + failed_count
            
            if new_failed_ids:
                save_last_ad_state(user_id, payload, new_failed_ids, state.get("buttons", []))
                report_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("♻️ Qayta yuborish", callback_data="ad_retry_failed"),
                     InlineKeyboardButton("❌ Bekor qilish", callback_data="ad_cancel_retry")]
                ])
            else:
                clear_last_ad_state(user_id)
                report_kb = None
            
            result_text = (
                "✅ <b>QAYTA YUBORISH HISOBOTI</b>\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Qayta yuborish ro'yxati: <b>{len(failed_ids)}</b>\n"
                f"✅ Yuborildi: <b>{success_count}</b>\n"
                f"🚫 Bloklangan (DB): <b>{skipped_blocked}</b>\n"
                f"⏭ Yetib bormagan (o'tkazib yuborildi): <b>{skipped_unreachable}</b>\n"
                f"❌ Yuborilmadi (xato): <b>{failed_count}</b>\n"
                f"📌 Umumiy yuborilmadi: <b>{not_sent}</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await query.message.edit_text(result_text, parse_mode='HTML', reply_markup=report_kb)
            log_admin_action(user_id, "Reklama qayta yuborildi", f"Yuborildi: {success_count}, Yuborilmadi: {not_sent}")
    
    elif query.data == "ad_cancel_retry":
        clear_last_ad_state(user_id)
        await query.message.edit_text("🚫 Qayta yuborish bekor qilindi.", parse_mode='HTML')

    elif query.data == "reject_ad":
        await query.message.edit_text("❌ Reklama bekor qilindi.", parse_mode='HTML')
        context.user_data.clear()

    elif query.data == "cancel_action":
        await query.message.edit_text("❌ Amal bekor qilindi.")
        context.user_data.clear()

    elif query.data == "cancel_reklama":
        context.user_data.clear()
        await query.message.edit_text("❌ Reklama bekor qilindi.")

    elif query.data == "show_ad":
        keyboard = [
            [InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_reklama")],
            [InlineKeyboardButton("⬅ Asosiy menyu", callback_data="back_main")]
        ]
        await query.message.edit_text(
            "📢 <b>REKLAMA YUBORISH</b>\n\n"
            "Reklama sifatida quyidagilarni yuborishingiz mumkin:\n\n"
            "📸 Rasm, 🎥 Video, 📄 Hujjat, 🎵 Audio, 🎤 Ovozli xabar, 💬 Matn\n\n"
            "📝 Media yuborgan holda, caption qo'shishingiz mumkin.\n\n"
            "⚠️ Keyingi xabaringiz reklama sifatida qabul qilinadi!",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data["reklama_mode"] = True

    elif query.data == "show_stats":
        stats = get_statistics()
        c.execute("SELECT COUNT(*) FROM films")
        films_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM admins")
        admins_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM blocked_users")
        blocked_count = c.fetchone()[0]

        growth = ""
        if stats['yesterday_joins'] > 0:
            percent = ((stats['today_joins'] - stats['yesterday_joins']) / stats['yesterday_joins']) * 100
            if percent > 0:
                growth = f"📈 +{percent:.1f}%"
            elif percent < 0:
                growth = f"📉 {percent:.1f}%"
            else:
                growth = "➖ 0%"

        stats_text = f"""
📊 <b>BOT STATISTIKASI</b>

━━━━━━━━━━━━━━━━━━━━
👥 <b>Foydalanuvchilar:</b>
├ Jami: <b>{stats['total']}</b>
├ Faol (24 soat): <b>{stats['active_users']}</b>
└ Bloklangan: <b>{blocked_count}</b>

📈 <b>Qo'shilish:</b>
├ Bugun: <b>{stats['today_joins']}</b> ta {growth}
├ Kecha: <b>{stats['yesterday_joins']}</b> ta
└ 7 kunlik: <b>{stats['week_joins']}</b> ta

🎬 <b>Kontent:</b>
├ Filmlar: <b>{films_count}</b> ta
└ Adminlar: <b>{admins_count}</b> ta

━━━━━━━━━━━━━━━━━━━━
📅 {datetime.now().strftime("%d.%m.%Y %H:%M")}
"""
        keyboard = [[InlineKeyboardButton("⬅ Asosiy menyu", callback_data="back_main")]]
        await query.message.edit_text(stats_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL |
         filters.AUDIO | filters.VOICE) & ~filters.COMMAND,
        handle_message
    ))
    app.add_handler(CallbackQueryHandler(button_callback))

    logging.info("Bot ishga tushdi ✅")
    app.run_polling()
