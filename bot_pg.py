#!/usr/bin/env python3
"""
Protein & Calories Tracker Bot — PostgreSQL (Railway)
----------------------------------------------------
Зависимости: python-telegram-bot==21.4, asyncpg==0.29.0, python-dateutil, certifi

ENV (Railway → Variables):
  TELEGRAM_TOKEN=...                        # токен бота из BotFather
  DATABASE_URL=...                          # возьми PUBLIC URL из Postgres (обычно DATABASE_PUBLIC_URL)
                                            # и добавь '?sslmode=require' если его нет
  # или вместо DATABASE_URL можно задать DATABASE_PUBLIC_URL (код сам подхватит)
  USER_TZ=Europe/Amsterdam                  # (опционально)
  ALLOW_SELF_SIGNED=1                       # (опционально, временно) отключить проверку TLS-цепочки
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import re
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import asyncpg
import certifi
from dateutil import tz
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ---- базовые логи в stdout ----
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("bot")

TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PUBLIC_URL")

POOL: asyncpg.Pool | None = None

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS entries (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  date_local DATE NOT NULL,
  tz_offset TEXT NOT NULL,
  item TEXT NOT NULL,
  protein_g DOUBLE PRECISION NOT NULL,
  calories_kcal DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entries_user_date ON entries(user_id, date_local);
"""

@dataclass
class Entry:
    id: int
    user_id: int
    ts_utc: datetime
    date_local: datetime
    tz_offset: str
    item: str
    protein_g: float
    calories_kcal: float


# ===================== DB POOL =====================

def _mask_dsn(dsn: str) -> str:
    """Скрыть пароль в DSN для логов."""
    try:
        u = urlparse(dsn)
        netloc = u.netloc
        if "@" in netloc:
            creds, host = netloc.split("@", 1)
            if ":" in creds:
                user = creds.split(":", 1)[0]
                netloc = f"{user}:***@{host}"
        return urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment))
    except Exception:
        return "<hidden>"

def _ensure_sslmode_require(dsn: str) -> str:
    u = urlparse(dsn)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.setdefault("sslmode", "require")
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

async def get_pool() -> asyncpg.Pool:
    """
    Создаёт (один раз) пул соединений с PostgreSQL.
    - Берём DSN из DATABASE_URL (или DATABASE_PUBLIC_URL).
    - Если не *.railway.internal → публичный хост: включаем строгий SSL (certifi) и sslmode=require.
    - Если *.railway.internal → внутренний хост: без SSL (нужна Private Networking).
    - Если строгий SSL падает и ALLOW_SELF_SIGNED=1 → пробуем RELAXED SSL (временно, небезопасно).
    """
    global POOL
    if POOL is not None:
        return POOL

    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL (или DATABASE_PUBLIC_URL) must be set")

    dsn = DATABASE_URL.strip()
    is_internal = ".railway.internal" in dsn
    use_ssl = not is_internal

    # Публичный хост — добавим sslmode=require, если забыли
    if use_ssl:
        dsn = _ensure_sslmode_require(dsn)

    # Строгий SSL для публичного хоста
    strict_ssl = None
    if use_ssl:
        strict_ssl = ssl.create_default_context(cafile=certifi.where())
        strict_ssl.check_hostname = True
        strict_ssl.verify_mode = ssl.CERT_REQUIRED

    log.info("[DB] DSN: %s", _mask_dsn(dsn))
    log.info("[DB] Host type: %s", "external (SSL strict)" if use_ssl else "internal (no SSL)")

    try:
        POOL = await asyncpg.create_pool(dsn, min_size=1, max_size=4, ssl=strict_ssl)
    except Exception as e:
        msg = repr(e)
        log.error("[DB] Strict connection failed: %s", msg)

        allow_relax = os.environ.get("ALLOW_SELF_SIGNED") == "1"
        if use_ssl and allow_relax and "CERTIFICATE_VERIFY_FAILED" in msg:
            log.warning("[DB] Retrying with RELAXED SSL (NOT secure, temporary)…")
            relaxed_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            relaxed_ssl.check_hostname = False
            relaxed_ssl.verify_mode = ssl.CERT_NONE
            POOL = await asyncpg.create_pool(dsn, min_size=1, max_size=4, ssl=relaxed_ssl)
        else:
            if is_internal:
                log.error("[DB] Hint: internal URL требует один проект/окружение/регион и Private Networking.")
            if use_ssl:
                log.error("[DB] Hint: используй PUBLIC URL (*.railway.app) с '?sslmode=require'.")
            raise

    async with POOL.acquire() as conn:
        await conn.execute(CREATE_SQL)

    log.info("[DB] Pool ready.")
    return POOL


# ===================== DB QUERIES =====================

async def add_entry(user_id: int, date_local: str, tz_offset: str,
                    item: str, protein_g: float, calories_kcal: float) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO entries (user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal)
            VALUES ($1, NOW() AT TIME ZONE 'UTC', $2::date, $3, $4, $5, $6)
            RETURNING id
            """,
            user_id, date_local, tz_offset, item, protein_g, calories_kcal,
        )
        return int(row["id"])

async def get_entries(user_id: int, date_local: str) -> List[Entry]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal
            FROM entries WHERE user_id=$1 AND date_local=$2::date ORDER BY id ASC
            """,
            user_id, date_local,
        )
        return [
            Entry(
                id=r["id"], user_id=r["user_id"], ts_utc=r["ts_utc"], date_local=r["date_local"],
                tz_offset=r["tz_offset"], item=r["item"],
                protein_g=float(r["protein_g"]), calories_kcal=float(r["calories_kcal"])
            ) for r in rows
        ]

async def delete_last_today(user_id: int, date_local: str) -> Optional[Entry]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM entries WHERE id IN (
              SELECT id FROM entries WHERE user_id=$1 AND date_local=$2::date ORDER BY id DESC LIMIT 1
            )
            RETURNING id, user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal
            """,
            user_id, date_local,
        )
        if not row:
            return None
        return Entry(
            id=row["id"], user_id=row["user_id"], ts_utc=row["ts_utc"], date_local=row["date_local"],
            tz_offset=row["tz_offset"], item=row["item"],
            protein_g=float(row["protein_g"]), calories_kcal=float(row["calories_kcal"])
        )

async def totals_for_date(user_id: int, date_local: str) -> Tuple[float, float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              COALESCE(SUM(protein_g), 0) AS sum_protein,
              COALESCE(SUM(calories_kcal), 0) AS sum_cal
            FROM entries
            WHERE user_id=$1 AND date_local=$2::date
            """,
            user_id, date_local,
        )
        return float(row["sum_protein"]), float(row["sum_cal"])


# ===================== HELPERS =====================

PROTEIN_RE = r"(?:(?:белка?|протеин|protein|prot)\s*[:=]?\s*|\b)(-?\d+[\.,]?\d*)\s*(?:г|g|гр|grams?)?\b"
CALORIES_RE = r"(?:(?:ккал|кило?кал|calories?|k?kcals?|k?cal)\s*[:=]?\s*|\b)(-?\d+[\.,]?\d*)\b"

def clean_number(s: str) -> float:
    return float(str(s).replace(",", "."))

def parse_freeform(text: str) -> Optional[Tuple[str, float, float]]:
    # Формат A: "название; белок; ккал"
    if ";" in text:
        parts = [p.strip() for p in text.split(";")]
        if len(parts) >= 3:
            item = parts[0]
            try:
                protein = clean_number(parts[1])
                calories = clean_number(parts[2])
                return item, protein, calories
            except ValueError:
                pass
    # Формат B: свободный текст "йогурт 20г белка 120 ккал"
    prot_match = re.search(PROTEIN_RE, text, flags=re.I)
    cal_match = re.search(CALORIES_RE, text, flags=re.I)
    if prot_match and cal_match:
        protein = clean_number(prot_match.group(1))
        calories = clean_number(cal_match.group(1))
        tmp = re.sub(PROTEIN_RE, "", text, flags=re.I)
        tmp = re.sub(CALORIES_RE, "", tmp, flags=re.I)
        item = re.sub(r"\s+", " ", tmp).strip(" -:.,\n") or "без названия"
        return item, protein, calories
    # Формат C: "стейк 60 450"
    m = re.search(r"^/?(?:add\s+)?(.+?)\s+(-?\d+[\.,]?\d*)\s+(-?\d+[\.,]?\d*)$", text.strip(), flags=re.I)
    if m:
        item = m.group(1).strip()
        protein = clean_number(m.group(2))
        calories = clean_number(m.group(3))
        return item, protein, calories
    return None

def fmt_amount(x: float, unit: str) -> str:
    if abs(x - round(x)) < 1e-9:
        return f"{int(round(x))} {unit}"
    return f"{x:.1f} {unit}"

def user_tz(update: Update):
    tz_name = os.environ.get("USER_TZ")
    if tz_name:
        return tz.gettz(tz_name) or timezone.utc
    return tz.tzlocal() or timezone.utc

def today_local_str(tzinfo) -> str:
    now = datetime.now(tzinfo)
    return now.strftime("%Y-%m-%d")

def tz_offset_str(tzinfo) -> str:
    now = datetime.now(tzinfo)
    off = now.utcoffset() or timedelta(0)
    total = int(off.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    h, r = divmod(total, 3600)
    m, _ = divmod(r, 60)
    return f"{sign}{h:02d}:{m:02d}"


# ===================== HANDLERS =====================

WELCOME = (
    "Привет! Я считаю белок и калории (PostgreSQL).\n\n"
    "Добавляй записи так:\n"
    "1) /add куриная грудка; 45; 220\n"
    "2) Просто сообщением: \"йогурт 20г белка 120 ккал\"\n"
    "3) \"стейк 60 450\" (белок г, ккал)\n\n"
    "Команды: /today, /sum [YYYY-MM-DD], /undo, /export [YYYY-MM-DD], /help"
)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME)

async def handle_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    payload = text[len("/add"):].strip() if text.lower().startswith("/add") else text
    parsed = parse_freeform(payload)
    if not parsed:
        await update.message.reply_text(
            "Не понял формат. Примеры:\n"
            "/add омлет; 24; 300\n"
            "или: \"творог 28 белка 160 ккал\""
        )
        return
    item, protein, calories = parsed
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo)
    off = tz_offset_str(tzinfo)
    rid = await add_entry(update.effective_user.id, date_str, off, item, protein, calories)
    await update.message.reply_text(
        f"Добавлено: {item} — {fmt_amount(protein, 'г белка')}, {fmt_amount(calories, 'ккал')} (#{rid})"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = parse_freeform(update.message.text)
    if not parsed:
        await update.message.reply_text("Сообщение не распознано. Используй /help для примеров.")
        return
    await handle_add(update, context)

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo)
    await send_summary(update, context, date_str)

async def cmd_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo) if not context.args else context.args[0]
    await send_summary(update, context, date_str)

async def send_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, date_str: str):
    uid = update.effective_user.id
    p, c = await totals_for_date(uid, date_str)
    entries = await get_entries(uid, date_str)
    header = f"Итого за {date_str}: {fmt_amount(p, 'г белка')}, {fmt_amount(c, 'ккал')}\n"
    if not entries:
        await update.message.reply_text(header + "\nЗаписей нет. Добавь что-нибудь через /add.")
        return
    lines = [header, "\nЗаписи:"]
    for e in entries:
        lines.append(
            f"• {e.item} — {fmt_amount(e.protein_g, 'г')}, {fmt_amount(e.calories_kcal, 'ккал')} (#{e.id})"
        )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="Экспорт CSV", callback_data=f"export:{date_str}")]])
    await update.message.reply_text("\n".join(lines), reply_markup=kb)

async def cmd_undo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo)
    entry = await delete_last_today(update.effective_user.id, date_str)
    if not entry:
        await update.message.reply_text("За сегодня записей нет — удалять нечего.")
        return
    await update.message.reply_text(
        f"Удалено: {entry.item} — {fmt_amount(entry.protein_g, 'г')}, {fmt_amount(entry.calories_kcal, 'ккал')} (#{entry.id})"
    )

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo) if not context.args else context.args[0]
    await do_export(update, context, date_str)

async def on_export_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not query.data or not query.data.startswith("export:"):
        return
    date_str = query.data.split(":", 1)[1]
    await do_export(update, context, date_str)

async def do_export(update: Update, context: ContextTypes.DEFAULT_TYPE, date_str: str):
    uid = update.effective_user.id
    entries = await get_entries(uid, date_str)
    if not entries:
        if update.message:
            await update.message.reply_text("Нет записей для экспорта.")
        else:
            await update.callback_query.edit_message_text("Нет записей для экспорта.")
        return
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "date", "time_utc", "item", "protein_g", "calories_kcal"])
    for e in entries:
        writer.writerow([e.id, e.date_local, e.ts_utc.isoformat(), e.item, e.protein_g, e.calories_kcal])
    data = buf.getvalue().encode("utf-8")
    filename = f"nutrition_{date_str}.csv"
    await update.effective_message.reply_document(
        document=InputFile(io.BytesIO(data), filename=filename),
        caption=f"Экспорт за {date_str}"
    )


# ===================== MAIN =====================

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Handler error: %r", context.error)

async def init_db_once():
    await get_pool()

def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_TOKEN env var is required")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL (или DATABASE_PUBLIC_URL) env var is required")

    # Разовая инициализация БД (асинхронно)
    asyncio.run(init_db_once())

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("add", handle_add))
    app.add_handler(CommandHandler(["today"], cmd_today))
    app.add_handler(CommandHandler(["sum"], cmd_sum))
    app.add_handler(CommandHandler(["undo"], cmd_undo))
    app.add_handler(CommandHandler(["export"], cmd_export))
    app.add_handler(CallbackQueryHandler(on_export_cb, pattern=r"^export:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    app.add_error_handler(on_error)

    log.info("Bot starting (PostgreSQL)…")
    # ВАЖНО: не await — метод блокирующий, сам крутит цикл
    app.run_polling()

if __name__ == "__main__":
    main()