#!/usr/bin/env python3
"""
Protein & Calories Tracker Bot — PostgreSQL (Railway)
----------------------------------------------------
Зависимости: python-telegram-bot==21.4, asyncpg==0.29.0, python-dateutil, certifi

ENV (Railway → Variables):
  TELEGRAM_TOKEN=...                        # токен бота из BotFather
  DATABASE_URL=...                          # PUBLIC URL из Postgres (или DATABASE_PUBLIC_URL) + ?sslmode=require
  USER_TZ=Europe/Amsterdam                  # (опционально)
  ALLOW_SELF_SIGNED=1                       # (опционально, временно) ослабленный SSL, если strict падает
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date as Date, time as Time
from typing import List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import asyncpg
import certifi
from dateutil import tz
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ---- логирование ----
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

-- История веса; берём последнее значение на конец дня
CREATE TABLE IF NOT EXISTS user_weights (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  weight_kg DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_weights_user_ts ON user_weights(user_id, ts_utc DESC);
"""

DEFAULT_WEIGHT_KG = 80.0   # если пользователь ни разу не задавал вес
PROTEIN_PER_KG = 2.0       # 2 г белка на кг

# Одна большая кнопка под полем ввода
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["/add"]],
    resize_keyboard=True
)

@dataclass
class Entry:
    id: int
    user_id: int
    ts_utc: datetime
    date_local: Date
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
    - Если не *.railway.internal → публичный хост: строгий SSL (certifi) и sslmode=require.
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

    if use_ssl:
        dsn = _ensure_sslmode_require(dsn)

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
                log.error("[DB] Hint: PUBLIC URL (*.railway.app) + '?sslmode=require'.")
            raise

    async with POOL.acquire() as conn:
        await conn.execute("SET TIME ZONE 'UTC'")
        await conn.execute(CREATE_SQL)

    log.info("[DB] Pool ready.")
    return POOL


# ===================== DB QUERIES =====================

async def add_entry(user_id: int, date_local: Date, tz_offset: str,
                    item: str, protein_g: float, calories_kcal: float) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO entries (user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal)
            VALUES ($1, NOW(), $2, $3, $4, $5, $6)
            RETURNING id
            """,
            user_id, date_local, tz_offset, item, protein_g, calories_kcal,
        )
        return int(row["id"])

async def get_entries(user_id: int, date_local: Date) -> List[Entry]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal
            FROM entries WHERE user_id=$1 AND date_local=$2 ORDER BY id ASC
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

async def delete_last_today(user_id: int, date_local: Date) -> Optional[Entry]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM entries WHERE id IN (
              SELECT id FROM entries WHERE user_id=$1 AND date_local=$2 ORDER BY id DESC LIMIT 1
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

async def delete_by_id(user_id: int, entry_id: int) -> Optional[Entry]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM entries
            WHERE id = $1 AND user_id = $2
            RETURNING id, user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal
            """,
            entry_id, user_id
        )
        if not row:
            return None
        return Entry(
            id=row["id"], user_id=row["user_id"], ts_utc=row["ts_utc"], date_local=row["date_local"],
            tz_offset=row["tz_offset"], item=row["item"],
            protein_g=float(row["protein_g"]), calories_kcal=float(row["calories_kcal"])
        )

async def totals_for_date(user_id: int, date_local: Date) -> Tuple[float, float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              COALESCE(SUM(protein_g), 0) AS sum_protein,
              COALESCE(SUM(calories_kcal), 0) AS sum_cal
            FROM entries
            WHERE user_id=$1 AND date_local=$2
            """,
            user_id, date_local,
        )
        return float(row["sum_protein"]), float(row["sum_cal"])

# ---- вес пользователя ----
async def set_weight(user_id: int, weight_kg: float) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_weights (user_id, weight_kg) VALUES ($1, $2)",
            user_id, weight_kg
        )

async def get_latest_weight(user_id: int) -> Optional[float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT weight_kg FROM user_weights WHERE user_id=$1 ORDER BY ts_utc DESC LIMIT 1",
            user_id
        )
        if not row:
            return None
        return float(row["weight_kg"])

async def get_weight_for_date(user_id: int, date_obj: Date, tzinfo) -> float:
    """
    Вес на конец указанной даты (локальное время пользователя).
    Берём запись из user_weights с ts_utc <= конец_дня_UTC, последнюю по времени.
    Если записей нет — DEFAULT_WEIGHT_KG.
    """
    local_eod = datetime.combine(date_obj, Time(23, 59, 59), tzinfo=tzinfo)
    cutoff_utc = local_eod.astimezone(timezone.utc)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT weight_kg
            FROM user_weights
            WHERE user_id = $1 AND ts_utc <= $2
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            user_id, cutoff_utc
        )
    return float(row["weight_kg"]) if row else DEFAULT_WEIGHT_KG


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

    # Формат C (поднят выше): "/add омлет 45 400" или "стейк 60 450"
    m = re.search(r"^\s*/?(?:add\s+)?(.+?)\s+(-?\d+[\.,]?\d*)\s+(-?\d+[\.,]?\d*)\s*$",
                  text.strip(), flags=re.I)
    if m:
        item = m.group(1).strip()
        protein = clean_number(m.group(2))
        calories = clean_number(m.group(3))
        return item, protein, calories

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

    return None

def fmt_amount(x: float, unit: str) -> str:
    return f"{int(round(x))} {unit}" if abs(x - round(x)) < 1e-9 else f"{x:.1f} {unit}"

def user_tz(update: Update):
    tz_name = os.environ.get("USER_TZ")
    return (tz.gettz(tz_name) if tz_name else tz.tzlocal()) or timezone.utc

def today_local_date(tzinfo) -> Date:
    return datetime.now(tzinfo).date()

def parse_date_arg(args, tzinfo) -> Date:
    if not args:
        return today_local_date(tzinfo)
    try:
        return datetime.strptime(args[0], "%Y-%m-%d").date()
    except Exception:
        return today_local_date(tzinfo)

def tz_offset_str(tzinfo) -> str:
    off = datetime.now(tzinfo).utcoffset() or timedelta(0)
    total = int(off.total_seconds()); sign = "+" if total >= 0 else "-"
    total = abs(total); h, r = divmod(total, 3600); m, _ = divmod(r, 60)
    return f"{sign}{h:02d}:{m:02d}"

def get_goal_protein_for_user(weight_kg: Optional[float]) -> float:
    w = weight_kg if weight_kg is not None else DEFAULT_WEIGHT_KG
    return w * PROTEIN_PER_KG


# ===================== HANDЛERS =====================

WELCOME = (
    "Привет! Я считаю белок и калории (PostgreSQL).\n\n"
    "Добавляй записи так:\n"
    "• /add омлет 45 400\n"
    "• или: омлет; 45; 400\n"
    "• или: йогурт 20г белка 120 ккал\n\n"
    "Вес и цель по белку:\n"
    "• /weight 82 — установить вес (цель = 2 г/кг)\n"
    "• Цель в прошлых днях считается по весу на конец того дня (если не было веса — 80 кг)\n\n"
    "Команды: /today, /sum [YYYY-MM-DD], /undo, /delete <id>, /export [YYYY-MM-DD], /help"
)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME, reply_markup=MAIN_KEYBOARD)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME, reply_markup=MAIN_KEYBOARD)

# --- Пошаговый /add ---
async def process_add_payload(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    parsed = parse_freeform(text)
    if not parsed:
        await update.message.reply_text(
            "Не понял формат. Примеры:\n"
            "• омлет 24 300\n"
            "• омлет; 24; 300\n"
            "• йогурт 20г белка 120 ккал",
            reply_markup=MAIN_KEYBOARD
        )
        return

    item, protein, calories = parsed
    tzinfo = user_tz(update)
    date_obj = today_local_date(tzinfo)
    off = tz_offset_str(tzinfo)

    rid = await add_entry(update.effective_user.id, date_obj, off, item, protein, calories)
    total_p, total_c = await totals_for_date(update.effective_user.id, date_obj)
    weight = await get_weight_for_date(update.effective_user.id, date_obj, tzinfo)
    goal = get_goal_protein_for_user(weight)
    remain = max(goal - total_p, 0.0)

    context.user_data.pop("awaiting_add", None)

    await update.message.reply_text(
        f"Добавлено: {item} — {fmt_amount(protein, 'г белка')}, {fmt_amount(calories, 'ккал')} (#{rid})\n"
        f"Итого за {date_obj.isoformat()}: {fmt_amount(total_p, 'г белка')}, {fmt_amount(total_c, 'ккал')}\n"
        f"Цель (2 г/кг): {fmt_amount(goal, 'г')}, осталось: {fmt_amount(remain, 'г')}",
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    args = text.split(maxsplit=1)
    if len(args) > 1:
        # /add с аргументами → сразу добавляем
        payload = args[1]
        await process_add_payload(update, context, payload)
        return

    # /add без аргументов → включаем режим ожидания
    context.user_data["awaiting_add"] = True
    await update.message.reply_text(
        "Окей, что добавить? Напиши в одном сообщении:\n"
        "• омлет 24 300\n"
        "• или: омлет; 24; 300\n"
        "• или: йогурт 20г белка 120 ккал",
        reply_markup=MAIN_KEYBOARD
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""

    if context.user_data.get("awaiting_add"):
        await process_add_payload(update, context, text)
        return

    parsed = parse_freeform(text)
    if parsed:
        await process_add_payload(update, context, text)
    else:
        await update.message.reply_text(
            "Сообщение не распознано. Нажми Add или /help.",
            reply_markup=MAIN_KEYBOARD
        )

# --- Остальные команды ---
async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_obj = today_local_date(tzinfo)
    await send_summary(update, context, date_obj)

async def cmd_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_obj = parse_date_arg(context.args, tzinfo)
    await send_summary(update, context, date_obj)

async def send_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, date_obj: Date):
    uid = update.effective_user.id
    tzinfo = user_tz(update)
    p, c = await totals_for_date(uid, date_obj)
    entries = await get_entries(uid, date_obj)

    weight = await get_weight_for_date(uid, date_obj, tzinfo)
    goal = get_goal_protein_for_user(weight)
    remain = max(goal - p, 0.0)

    date_str = date_obj.isoformat()
    header = (
        f"Итого за {date_str}: {fmt_amount(p, 'г белка')}, {fmt_amount(c, 'ккал')}\n"
        f"Цель по белку (2 г/кг): {fmt_amount(goal, 'г')}\n"
        f"Осталось: {fmt_amount(remain, 'г')}\n"
    )
    if not entries:
        await update.message.reply_text(header + "\nЗаписей нет. Нажми Add, чтобы добавить.", reply_markup=MAIN_KEYBOARD)
        return
    lines = [header, "Записи:"]
    for e in entries:
        lines.append(f"• {e.item} — {fmt_amount(e.protein_g, 'г')}, {fmt_amount(e.calories_kcal, 'ккал')} (#{e.id})")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="Экспорт CSV", callback_data=f"export:{date_str}")]])
    await update.message.reply_text("\n".join(lines), reply_markup=kb)

async def cmd_undo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_obj = today_local_date(tzinfo)
    entry = await delete_last_today(update.effective_user.id, date_obj)
    if not entry:
        await update.message.reply_text("За сегодня записей нет — удалять нечего.", reply_markup=MAIN_KEYBOARD)
        return
    p, c = await totals_for_date(update.effective_user.id, date_obj)
    weight = await get_weight_for_date(update.effective_user.id, date_obj, tzinfo)
    goal = get_goal_protein_for_user(weight)
    remain = max(goal - p, 0.0)

    await update.message.reply_text(
        "Удалено: {item} — {p1}, {c1} (#{id})\n"
        "Итого за {date}: {p2}, {c2}\n"
        "Цель: {goal}, осталось: {remain}".format(
            item=entry.item,
            p1=fmt_amount(entry.protein_g, "г"),
            c1=fmt_amount(entry.calories_kcal, "ккал"),
            id=entry.id,
            date=date_obj.isoformat(),
            p2=fmt_amount(p, "г белка"),
            c2=fmt_amount(c, "ккал"),
            goal=fmt_amount(goal, "г"),
            remain=fmt_amount(remain, "г"),
        ),
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи id записи: /delete 12", reply_markup=MAIN_KEYBOARD)
        return
    try:
        entry_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом: /delete 12", reply_markup=MAIN_KEYBOARD)
        return

    uid = update.effective_user.id
    deleted = await delete_by_id(uid, entry_id)
    if not deleted:
        await update.message.reply_text("Запись не найдена или не твоя.", reply_markup=MAIN_KEYBOARD)
        return

    tzinfo = user_tz(update)
    p, c = await totals_for_date(uid, deleted.date_local)
    weight = await get_weight_for_date(uid, deleted.date_local, tzinfo)
    goal = get_goal_protein_for_user(weight)
    remain = max(goal - p, 0.0)

    await update.message.reply_text(
        "Удалено: {item} — {p1}, {c1} (#{id})\n"
        "Итого за {date}: {p2}, {c2}\n"
        "Цель: {goal}, осталось: {remain}".format(
            item=deleted.item,
            p1=fmt_amount(deleted.protein_g, "г"),
            c1=fmt_amount(deleted.calories_kcal, "ккал"),
            id=deleted.id,
            date=deleted.date_local.isoformat(),
            p2=fmt_amount(p, "г белка"),
            c2=fmt_amount(c, "ккал"),
            goal=fmt_amount(goal, "г"),
            remain=fmt_amount(remain, "г"),
        ),
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_obj = parse_date_arg(context.args, tzinfo)
    await do_export(update, context, date_obj)

async def on_export_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not q.data or not q.data.startswith("export:"):
        return
    date_str = q.data.split(":", 1)[1]
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        date_obj = today_local_date(tz.tzlocal() or timezone.utc)
    await do_export(update, context, date_obj)

async def do_export(update: Update, context: ContextTypes.DEFAULT_TYPE, date_obj: Date):
    uid = update.effective_user.id
    entries = await get_entries(uid, date_obj)
    if not entries:
        if update.message:
            await update.message.reply_text("Нет записей для экспорта.", reply_markup=MAIN_KEYBOARD)
        else:
            await update.callback_query.edit_message_text("Нет записей для экспорта.")
        return
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "date", "time_utc", "item", "protein_g", "calories_kcal"])
    for e in entries:
        w.writerow([e.id, e.date_local.isoformat(), e.ts_utc.isoformat(), e.item, e.protein_g, e.calories_kcal])
    data = buf.getvalue().encode("utf-8")
    fname = f"nutrition_{date_obj.isoformat()}.csv"
    await update.effective_message.reply_document(
        document=InputFile(io.BytesIO(data), filename=fname),
        caption=f"Экспорт за {date_obj.isoformat()}"
    )

# ---- вес: команды ----
async def cmd_weight(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        current = await get_latest_weight(update.effective_user.id)
        current = current if current is not None else DEFAULT_WEIGHT_KG
        await update.message.reply_text(
            f"Текущий вес: {fmt_amount(current, 'кг')}\n"
            f"Цель по белку: {fmt_amount(get_goal_protein_for_user(current), 'г')} (2 г/кг)\n"
            f"Чтобы задать новый: /weight 82",
            reply_markup=MAIN_KEYBOARD
        )
        return
    try:
        w = float(str(context.args[0]).replace(",", "."))
        if w <= 0 or w > 500:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Укажи корректный вес в килограммах, например: /weight 82", reply_markup=MAIN_KEYBOARD)
        return
    await set_weight(update.effective_user.id, w)
    await update.message.reply_text(
        f"Вес обновлён: {fmt_amount(w, 'кг')}. "
        f"Новая цель по белку: {fmt_amount(get_goal_protein_for_user(w), 'г')} в день.",
        reply_markup=MAIN_KEYBOARD
    )


# ===================== PTB hooks & MAIN =====================

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Handler error: %r", context.error)

async def post_init(app: Application):
    # Инициализация пула/схемы внутри event loop PTB
    await get_pool()

def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_TOKEN env var is required")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL (или DATABASE_PUBLIC_URL) env var is required")

    app = Application.builder().token(TOKEN).build()
    app.post_init = post_init
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("weight", cmd_weight))
    app.add_handler(CommandHandler("add", cmd_add))  # один хендлер /add
    app.add_handler(CommandHandler(["today"], cmd_today))
    app.add_handler(CommandHandler(["sum"], cmd_sum))
    app.add_handler(CommandHandler(["undo"], cmd_undo))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler(["export"], cmd_export))
    app.add_handler(CallbackQueryHandler(on_export_cb, pattern=r"^export:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    log.info("Bot starting (PostgreSQL)…")
    app.run_polling()  # синхронный запуск; PTB сам управляет event loop

if __name__ == "__main__":
    main()
