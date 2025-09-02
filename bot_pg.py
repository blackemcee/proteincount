#!/usr/bin/env python3
"""
Protein & Calories Tracker Bot — PostgreSQL (Railway)
----------------------------------------------------
Зависимости: python-telegram-bot==21.4, asyncpg==0.29.0, python-dateutil, certifi

ENV (Railway → Variables):
  TELEGRAM_TOKEN=...                        # токен бота
  DATABASE_URL=...                          # PUBLIC URL Postgres (+ ?sslmode=require)
  USER_TZ=Europe/Amsterdam                  # (опционально)
  ALLOW_SELF_SIGNED=1                       # (временно, если строгий SSL не проходит)
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

-- История веса
CREATE TABLE IF NOT EXISTS user_weights (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  weight_kg DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_weights_user_ts ON user_weights(user_id, ts_utc DESC);

-- История лимита по калориям
CREATE TABLE IF NOT EXISTS user_cal_limits (
  id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  limit_kcal DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_callimits_user_ts ON user_cal_limits(user_id, ts_utc DESC);
"""

DEFAULT_WEIGHT_KG = 80.0        # если пользователь ещё не задавал вес
PROTEIN_PER_KG = 2.0            # 2 г/кг
DEFAULT_CAL_LIMIT_KCAL = 2000.0 # базовый дневной лимит калорий
QUICK_ITEMS = {
    "Капучино": ("капучино", 4.0, 60.0),
    "Протеин":  ("протеин", 24.0, 113.0),
    "Казеин":   ("казеин", 24.0, 116.0),
}

# Одна большая кнопка
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["/add", "Капучино"], ["Протеин", "Казеин"]],
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
    global POOL
    if POOL is not None:
        return POOL
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL must be set")

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
    log.info("[DB] Host: %s", "external SSL" if use_ssl else "internal no-SSL")

    try:
        POOL = await asyncpg.create_pool(dsn, min_size=1, max_size=4, ssl=strict_ssl)
    except Exception as e:
        msg = repr(e)
        log.error("[DB] Strict connect failed: %s", msg)
        if use_ssl and os.environ.get("ALLOW_SELF_SIGNED") == "1" and "CERTIFICATE_VERIFY_FAILED" in msg:
            log.warning("[DB] Retrying with RELAXED SSL (NOT secure)…")
            relaxed = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            relaxed.check_hostname = False
            relaxed.verify_mode = ssl.CERT_NONE
            POOL = await asyncpg.create_pool(dsn, min_size=1, max_size=4, ssl=relaxed)
        else:
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
            FROM entries
            WHERE user_id=$1 AND date_local=$2
            ORDER BY id ASC
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
            SELECT COALESCE(SUM(protein_g), 0) AS sum_protein,
                   COALESCE(SUM(calories_kcal), 0) AS sum_cal
            FROM entries
            WHERE user_id=$1 AND date_local=$2
            """,
            user_id, date_local,
        )
        return float(row["sum_protein"]), float(row["sum_cal"])

# ---- вес ----
async def set_weight(user_id: int, weight_kg: float) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO user_weights (user_id, weight_kg) VALUES ($1, $2)", user_id, weight_kg)

async def get_latest_weight(user_id: int) -> Optional[float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT weight_kg FROM user_weights WHERE user_id=$1 ORDER BY ts_utc DESC LIMIT 1", user_id
        )
        return float(row["weight_kg"]) if row else None

async def get_weight_for_date(user_id: int, date_obj: Date, tzinfo) -> float:
    local_eod = datetime.combine(date_obj, Time(23, 59, 59), tzinfo=tzinfo)
    cutoff_utc = local_eod.astimezone(timezone.utc)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT weight_kg
            FROM user_weights
            WHERE user_id=$1 AND ts_utc <= $2
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            user_id, cutoff_utc
        )
    return float(row["weight_kg"]) if row else DEFAULT_WEIGHT_KG

# ---- лимит калорий ----
async def set_cal_limit(user_id: int, limit_kcal: float) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO user_cal_limits (user_id, limit_kcal) VALUES ($1, $2)", user_id, limit_kcal)

async def get_latest_cal_limit(user_id: int) -> Optional[float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT limit_kcal FROM user_cal_limits WHERE user_id=$1 ORDER BY ts_utc DESC LIMIT 1", user_id
        )
        return float(row["limit_kcal"]) if row else None

async def get_cal_limit_for_date(user_id: int, date_obj: Date, tzinfo) -> float:
    local_eod = datetime.combine(date_obj, Time(23, 59, 59), tzinfo=tzinfo)
    cutoff_utc = local_eod.astimezone(timezone.utc)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT limit_kcal
            FROM user_cal_limits
            WHERE user_id=$1 AND ts_utc <= $2
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            user_id, cutoff_utc
        )
    return float(row["limit_kcal"]) if row else DEFAULT_CAL_LIMIT_KCAL

# ===================== HELPERS =====================
PROTEIN_RE = r"(?:(?:белка?|протеин|protein|prot)\s*[:=]?\s*|\b)(-?\d+[\.,]?\d*)\s*(?:г|g|гр|grams?)?\b"
CALORIES_RE = r"(?:(?:ккал|кило?кал|calories?|k?kcals?|k?cal)\s*[:=]?\s*|\b)(-?\d+[\.,]?\d*)\b"

def clean_number(s: str) -> float:
    return float(str(s).replace(",", "."))

def parse_freeform(text: str) -> Optional[Tuple[str, float, float]]:
    if ";" in text:
        parts = [p.strip() for p in text.split(";")]
        if len(parts) >= 3:
            item = parts[0]
            try:
                return item, clean_number(parts[1]), clean_number(parts[2])
            except ValueError:
                pass
    m = re.search(r"^\s*/?(?:add\s+)?(.+?)\s+(-?\d+[\.,]?\d*)\s+(-?\d+[\.,]?\d*)\s*$", text.strip(), flags=re.I)
    if m:
        return m.group(1).strip(), clean_number(m.group(2)), clean_number(m.group(3))
    prot = re.search(PROTEIN_RE, text, flags=re.I)
    cal = re.search(CALORIES_RE, text, flags=re.I)
    if prot and cal:
        protein = clean_number(prot.group(1)); calories = clean_number(cal.group(1))
        tmp = re.sub(PROTEIN_RE, "", text, flags=re.I); tmp = re.sub(CALORIES_RE, "", tmp, flags=re.I)
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
    return (weight_kg if weight_kg is not None else DEFAULT_WEIGHT_KG) * PROTEIN_PER_KG

# ===================== HANDLERS =====================
WELCOME = (
    "Привет! Я считаю белок и калории.\n\n"
    "Добавляй записи:\n"
    "• /add омлет 45 400  |  омлет; 45; 400  |  йогурт 20г белка 120 ккал\n\n"
    "Вес и цели:\n"
    "• /weight 82 — установить вес (цель по белку = 2 г/кг)\n"
    "• /calories 2200 — установить дневной лимит калорий (по умолчанию 2000)\n"
    "• и для прошлых дат цель/лимит берутся по значениям на конец того дня.\n\n"
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
            "Не понял формат. Примеры: «омлет 24 300» или «омлет; 24; 300»",
            reply_markup=MAIN_KEYBOARD
        )
        return
    item, protein, calories = parsed
    tzinfo = user_tz(update)
    date_obj = today_local_date(tzinfo)
    rid = await add_entry(update.effective_user.id, date_obj, tz_offset_str(tzinfo), item, protein, calories)

    total_p, total_c = await totals_for_date(update.effective_user.id, date_obj)
    weight = await get_weight_for_date(update.effective_user.id, date_obj, tzinfo)
    goal_p = get_goal_protein_for_user(weight)
    cal_lim = await get_cal_limit_for_date(update.effective_user.id, date_obj, tzinfo)
    remain_p = max(goal_p - total_p, 0.0)
    remain_c = max(cal_lim - total_c, 0.0)

    context.user_data.pop("awaiting_add", None)
    await update.message.reply_text(
        f"Добавлено: {item} — {fmt_amount(protein, 'г белка')}, {fmt_amount(calories, 'ккал')} (#{rid})\n"
        f"Итого за {date_obj.isoformat()}: {fmt_amount(total_p, 'г белка')}, {fmt_amount(total_c, 'ккал')}\n"
        f"Цель по белку: {fmt_amount(goal_p, 'г')}, осталось: {fmt_amount(remain_p, 'г')}\n"
        f"Лимит по калориям: {fmt_amount(cal_lim, 'ккал')}, осталось: {fmt_amount(remain_c, 'ккал')}",
        reply_markup=MAIN_KEYBOARD
    )

async def process_quick_item(update: Update, context: ContextTypes.DEFAULT_TYPE, item: str, protein: float, calories: float):
    tzinfo = user_tz(update)
    date_obj = today_local_date(tzinfo)
    off = tz_offset_str(tzinfo)

    rid = await add_entry(update.effective_user.id, date_obj, off, item, protein, calories)

    total_p, total_c = await totals_for_date(update.effective_user.id, date_obj)
    weight = await get_weight_for_date(update.effective_user.id, date_obj, tzinfo)
    goal_p = get_goal_protein_for_user(weight)
    cal_lim = await get_cal_limit_for_date(update.effective_user.id, date_obj, tzinfo)
    remain_p = max(goal_p - total_p, 0.0)
    remain_c = max(cal_lim - total_c, 0.0)

    # если мы были в режиме ожидания /add — сбросим
    context.user_data.pop("awaiting_add", None)

    await update.message.reply_text(
        f"Добавлено: {item} — {fmt_amount(protein, 'г белка')}, {fmt_amount(calories, 'ккал')} (#{rid})\n"
        f"Итого за {date_obj.isoformat()}: {fmt_amount(total_p, 'г белка')}, {fmt_amount(total_c, 'ккал')}\n"
        f"Цель по белку: {fmt_amount(goal_p, 'г')}, осталось: {fmt_amount(remain_p, 'г')}\n"
        f"Лимит по калориям: {fmt_amount(cal_lim, 'ккал')}, осталось: {fmt_amount(remain_c, 'ккал')}",
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    args = text.split(maxsplit=1)
    if len(args) > 1:
        await process_add_payload(update, context, args[1])
        return
    context.user_data["awaiting_add"] = True
    await update.message.reply_text(
        "Окей, что добавить? Напиши «омлет 24 300» или «омлет; 24; 300»",
        reply_markup=MAIN_KEYBOARD
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    # ➊ Быстрые кнопки
    if text in QUICK_ITEMS:
        item, p, c = QUICK_ITEMS[text]
        await process_quick_item(update, context, item, p, c)
        return

    # ➋ Если ждём продолжение после /add — обрабатываем
    if context.user_data.get("awaiting_add"):
        await process_add_payload(update, context, text)
        return

    # ➌ Свободный текст: попробуем распарсить и добавить
    parsed = parse_freeform(text)
    if parsed:
        await process_add_payload(update, context, text)
    else:
        await update.message.reply_text(
            "Сообщение не распознано. Нажми Add или одну из быстрых кнопок.",
            reply_markup=MAIN_KEYBOARD
        )

# --- Итоги/резюме ---
async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    await send_summary(update, context, today_local_date(tzinfo))

async def cmd_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    await send_summary(update, context, parse_date_arg(context.args, tzinfo))

async def send_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, date_obj: Date):
    uid = update.effective_user.id
    tzinfo = user_tz(update)
    p, c = await totals_for_date(uid, date_obj)
    entries = await get_entries(uid, date_obj)

    goal_p = get_goal_protein_for_user(await get_weight_for_date(uid, date_obj, tzinfo))
    cal_lim = await get_cal_limit_for_date(uid, date_obj, tzinfo)
    remain_p = max(goal_p - p, 0.0)
    remain_c = max(cal_lim - c, 0.0)

    date_str = date_obj.isoformat()
    header = (
        f"Итого за {date_str}: {fmt_amount(p, 'г белка')}, {fmt_amount(c, 'ккал')}\n"
        f"Цель по белку: {fmt_amount(goal_p, 'г')}  |  осталось: {fmt_amount(remain_p, 'г')}\n"
        f"Лимит калорий: {fmt_amount(cal_lim, 'ккал')}  |  осталось: {fmt_amount(remain_c, 'ккал')}\n"
    )
    if not entries:
        await update.message.reply_text(header + "\nЗаписей нет. Нажми Add, чтобы добавить.", reply_markup=MAIN_KEYBOARD)
        return
    lines = [header, "Записи:"]
    for e in entries:
        lines.append(f"• {e.item} — {fmt_amount(e.protein_g, 'г')}, {fmt_amount(e.calories_kcal, 'ккал')} (#{e.id})")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="Экспорт CSV", callback_data=f"export:{date_str}")]])
    await update.message.reply_text("\n".join(lines), reply_markup=kb)

# --- Удаление/экспорт ---
async def cmd_undo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_obj = today_local_date(tzinfo)
    entry = await delete_last_today(update.effective_user.id, date_obj)
    if not entry:
        await update.message.reply_text("За сегодня записей нет — удалять нечего.", reply_markup=MAIN_KEYBOARD)
        return
    p, c = await totals_for_date(update.effective_user.id, date_obj)
    goal_p = get_goal_protein_for_user(await get_weight_for_date(update.effective_user.id, date_obj, tzinfo))
    cal_lim = await get_cal_limit_for_date(update.effective_user.id, date_obj, tzinfo)
    remain_p = max(goal_p - p, 0.0); remain_c = max(cal_lim - c, 0.0)
    await update.message.reply_text(
        "Удалено: {item} — {p1}, {c1} (#{id})\n"
        "Итого за {date}: {p2}, {c2}\n"
        "Цель белка: {gp}, осталось: {rp}\n"
        "Лимит калорий: {gl}, осталось: {rl}".format(
            item=entry.item,
            p1=fmt_amount(entry.protein_g, "г"),
            c1=fmt_amount(entry.calories_kcal, "ккал"),
            id=entry.id, date=date_obj.isoformat(),
            p2=fmt_amount(p, "г белка"), c2=fmt_amount(c, "ккал"),
            gp=fmt_amount(goal_p, "г"), rp=fmt_amount(remain_p, "г"),
            gl=fmt_amount(cal_lim, "ккал"), rl=fmt_amount(remain_c, "ккал"),
        ),
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи id записи: /delete 12", reply_markup=MAIN_KEYBOARD); return
    try:
        entry_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом: /delete 12", reply_markup=MAIN_KEYBOARD); return
    uid = update.effective_user.id
    deleted = await delete_by_id(uid, entry_id)
    if not deleted:
        await update.message.reply_text("Запись не найдена или не твоя.", reply_markup=MAIN_KEYBOARD); return
    tzinfo = user_tz(update)
    p, c = await totals_for_date(uid, deleted.date_local)
    goal_p = get_goal_protein_for_user(await get_weight_for_date(uid, deleted.date_local, tzinfo))
    cal_lim = await get_cal_limit_for_date(uid, deleted.date_local, tzinfo)
    remain_p = max(goal_p - p, 0.0); remain_c = max(cal_lim - c, 0.0)
    await update.message.reply_text(
        "Удалено: {item} — {p1}, {c1} (#{id})\n"
        "Итого за {date}: {p2}, {c2}\n"
        "Цель белка: {gp}, осталось: {rp}\n"
        "Лимит калорий: {gl}, осталось: {rl}".format(
            item=deleted.item,
            p1=fmt_amount(deleted.protein_g, "г"), c1=fmt_amount(deleted.calories_kcal, "ккал"),
            id=deleted.id, date=deleted.date_local.isoformat(),
            p2=fmt_amount(p, "г белка"), c2=fmt_amount(c, "ккал"),
            gp=fmt_amount(goal_p, "г"), rp=fmt_amount(remain_p, "г"),
            gl=fmt_amount(cal_lim, "ккал"), rl=fmt_amount(remain_c, "ккал"),
        ),
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_obj = parse_date_arg(context.args, tzinfo)
    await do_export(update, context, date_obj)

async def on_export_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if not q.data or not q.data.startswith("export:"): return
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
        if update.message: await update.message.reply_text("Нет записей для экспорта.", reply_markup=MAIN_KEYBOARD)
        else: await update.callback_query.edit_message_text("Нет записей для экспорта.")
        return
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["id", "date", "time_utc", "item", "protein_g", "calories_kcal"])
    for e in entries:
        w.writerow([e.id, e.date_local.isoformat(), e.ts_utc.isoformat(), e.item, e.protein_g, e.calories_kcal])
    data = buf.getvalue().encode("utf-8")
    await update.effective_message.reply_document(
        document=InputFile(io.BytesIO(data), filename=f"nutrition_{date_obj.isoformat()}.csv"),
        caption=f"Экспорт за {date_obj.isoformat()}"
    )

# --- вес/лимит: команды ---
async def cmd_weight(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        current = await get_latest_weight(update.effective_user.id) or DEFAULT_WEIGHT_KG
        await update.message.reply_text(
            f"Текущий вес: {fmt_amount(current, 'кг')}\n"
            f"Цель по белку: {fmt_amount(get_goal_protein_for_user(current), 'г')} (2 г/кг)\n"
            f"Чтобы задать новый: /weight 82",
            reply_markup=MAIN_KEYBOARD
        ); return
    try:
        w = float(str(context.args[0]).replace(",", "."))
        if w <= 0 or w > 500: raise ValueError
    except ValueError:
        await update.message.reply_text("Укажи корректный вес, например: /weight 82", reply_markup=MAIN_KEYBOARD); return
    await set_weight(update.effective_user.id, w)
    await update.message.reply_text(
        f"Вес обновлён: {fmt_amount(w, 'кг')}. Цель по белку: {fmt_amount(get_goal_protein_for_user(w), 'г')}/день.",
        reply_markup=MAIN_KEYBOARD
    )

async def cmd_calories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        current = await get_latest_cal_limit(update.effective_user.id) or DEFAULT_CAL_LIMIT_KCAL
        await update.message.reply_text(
            f"Текущий дневной лимит калорий: {fmt_amount(current, 'ккал')}\n"
            f"Чтобы задать новый: /calories 2200",
            reply_markup=MAIN_KEYBOARD
        ); return
    try:
        k = float(str(context.args[0]).replace(",", "."))
        if k < 800 or k > 10000: raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Укажи корректный лимит в ккал, например: /calories 2200",
            reply_markup=MAIN_KEYBOARD
        ); return
    await set_cal_limit(update.effective_user.id, k)
    await update.message.reply_text(
        f"Лимит калорий обновлён: {fmt_amount(k, 'ккал')} в день.",
        reply_markup=MAIN_KEYBOARD
    )

# ===================== PTB hooks & MAIN =====================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Handler error: %r", context.error)

async def post_init(app: Application):
    await get_pool()

def main():
    if not TOKEN: raise SystemExit("TELEGRAM_TOKEN is required")
    if not DATABASE_URL: raise SystemExit("DATABASE_URL is required")

    app = Application.builder().token(TOKEN).build()
    app.post_init = post_init
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("weight", cmd_weight))
    app.add_handler(CommandHandler("calories", cmd_calories))   # <— НОВОЕ
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler(["today"], cmd_today))
    app.add_handler(CommandHandler(["sum"], cmd_sum))
    app.add_handler(CommandHandler(["undo"], cmd_undo))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler(["export"], cmd_export))
    app.add_handler(CallbackQueryHandler(on_export_cb, pattern=r"^export:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    log.info("Bot starting (PostgreSQL)…")
    app.run_polling()

if __name__ == "__main__":
    main()