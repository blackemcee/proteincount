#!/usr/bin/env python3
"""
Protein & Calories Tracker Bot
--------------------------------
Телеграм‑бот для учёта белка и калорий с дневными суммами.

Библиотека: python-telegram-bot >= 20
Установка: pip install python-telegram-bot==21.4 aiosqlite python-dateutil
Запуск:    TELEGRAM_TOKEN=123:ABC python bot.py

Функции:
  /start – краткое приветствие
  /help – справка
  /add <название>; <белок г>; <ккал> – добавить запись (пример: "/add куриная грудка; 45; 220")
  (или просто сообщение: "куриная грудка 45г белка 220 ккал")
  /today – показать суммы за сегодня и список записей
  /sum [YYYY-MM-DD] – суммы за дату (по умолчанию сегодня)
  /undo – удалить последнюю запись за сегодня
  /export [YYYY-MM-DD] – экспорт записей за дату (CSV)

Хранение: SQLite (async, файл tracker.db)
Локаль времени: по часовому поясу пользователя Telegram (если есть), иначе UTC
"""
from __future__ import annotations

import asyncio
import csv
import io
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import aiosqlite
from dateutil import tz
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

DB_PATH = os.environ.get("TRACKER_DB", "tracker.db")
TOKEN = os.environ.get("TELEGRAM_TOKEN")

# -------------------------- DB LAYER --------------------------

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  ts_utc TEXT NOT NULL,
  date_local TEXT NOT NULL,
  tz_offset TEXT NOT NULL,
  item TEXT NOT NULL,
  protein_g REAL NOT NULL,
  calories_kcal REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entries_user_date ON entries(user_id, date_local);
"""

@dataclass
class Entry:
    id: int
    user_id: int
    ts_utc: str
    date_local: str
    tz_offset: str
    item: str
    protein_g: float
    calories_kcal: float


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        for stmt in CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                await db.execute(s)
        await db.commit()


async def add_entry(user_id: int, date_local: str, tz_offset: str, item: str, protein_g: float, calories_kcal: float) -> int:
    ts_utc = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO entries (user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal),
        )
        await db.commit()
        return cur.lastrowid


async def get_entries(user_id: int, date_local: str) -> List[Entry]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal FROM entries WHERE user_id=? AND date_local=? ORDER BY id ASC",
            (user_id, date_local),
        )
        rows = await cur.fetchall()
        return [Entry(*row) for row in rows]


async def delete_last_today(user_id: int, date_local: str) -> Optional[Entry]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, user_id, ts_utc, date_local, tz_offset, item, protein_g, calories_kcal FROM entries WHERE user_id=? AND date_local=? ORDER BY id DESC LIMIT 1",
            (user_id, date_local),
        )
        row = await cur.fetchone()
        if not row:
            return None
        entry = Entry(*row)
        await db.execute("DELETE FROM entries WHERE id=?", (entry.id,))
        await db.commit()
        return entry


async def totals_for_date(user_id: int, date_local: str) -> Tuple[float, float]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COALESCE(SUM(protein_g),0), COALESCE(SUM(calories_kcal),0) FROM entries WHERE user_id=? AND date_local=?",
            (user_id, date_local),
        )
        p, c = await cur.fetchone()
        return float(p or 0), float(c or 0)


# ----------------------- UTILITIES ---------------------------

# Patterns: поддерживаем рус/англ, сокращения и порядок любой
PROTEIN_RE = r"(?:(?:белка?|протеин|protein|prot)\s*[:=]?\s*|\b)(-?\d+[\.,]?\d*)\s*(?:г|g|гр|grams?)?\b"
CALORIES_RE = r"(?:(?:ккал|кило?кал|calories?|k?kcals?|k?cal)\s*[:=]?\s*|\b)(-?\d+[\.,]?\d*)\b"

# Примеры: "яйцо; 12; 155", "йогурт 20г белка 120 ккал", "add стейк 60 450"

ADD_CMD_RE = re.compile(r"^/add\s+(.+)$", re.I)


def clean_number(s: str) -> float:
    return float(str(s).replace(",", "."))


def parse_freeform(text: str) -> Optional[Tuple[str, float, float]]:
    """Возвращает (item, protein_g, calories_kcal) или None."""
    # Разделение по ; на три части как простой путь
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

    # Гибкий парсер через регэкспы
    # Ищем число белка и калорий независимо от порядка, всё остальное – название
    prot_match = re.search(PROTEIN_RE, text, flags=re.I)
    cal_match = re.search(CALORIES_RE, text, flags=re.I)

    if prot_match and cal_match:
        protein = clean_number(prot_match.group(1))
        calories = clean_number(cal_match.group(1))
        # Название – текст, где убираем найденные куски чисел/единиц
        tmp = re.sub(PROTEIN_RE, "", text, flags=re.I)
        tmp = re.sub(CALORIES_RE, "", tmp, flags=re.I)
        item = re.sub(r"\s+", " ", tmp).strip(" -:,.\n")
        if not item:
            item = "без названия"
        return item, protein, calories

    # Поддержка формы: "/add стейк 60 450" или "стейк 60 450"
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


def user_tz(update: Update) -> tz.tzoffset | tz.tzlocal | timezone:
    # Telegram может прислать смещение только в WebApp; здесь используем локальный/UTC фоллбэк
    # Предоставим опцию через переменную окружения USER_TZ="Europe/Amsterdam" при деплое
    tz_name = os.environ.get("USER_TZ")
    if tz_name:
        try:
            return tz.gettz(tz_name) or timezone.utc
        except Exception:
            return timezone.utc
    return tz.tzlocal() or timezone.utc


def today_local_str(tzinfo) -> str:
    now = datetime.now(tzinfo)
    return now.strftime("%Y-%m-%d")


def tz_offset_str(tzinfo) -> str:
    now = datetime.now(tzinfo)
    off = now.utcoffset() or datetime.timedelta(0)
    total = int(off.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    h, r = divmod(total, 3600)
    m, _ = divmod(r, 60)
    return f"{sign}{h:02d}:{m:02d}"

# -------------------------- HANDLERS -------------------------

WELCOME = (
    "Привет! Я считаю белок и калории.\n\n"
    "Добавляй продукты тремя способами:\n"
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
            "Не понял формат. Примеры: \n"
            "/add омлет; 24; 300\n"
            "или: \"творог 28 белка 160 ккал\""
        )
        return

    item, protein, calories = parsed
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo)
    off = tz_offset_str(tzinfo)
    rid = await add_entry(update.effective_user.id, date_str, off, item, protein, calories)

    p_str = fmt_amount(protein, "г белка")
    c_str = fmt_amount(calories, "ккал")
    await update.message.reply_text(f"Добавлено: {item} — {p_str}, {c_str} (#{rid})")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Любое свободное сообщение пробуем распарсить
    parsed = parse_freeform(update.message.text)
    if not parsed:
        await update.message.reply_text("Сообщение не распознано. Используй /help для примеров.")
        return
    # Проксируем в add
    await handle_add(update, context)


def parse_date_arg(args: List[str], tzinfo) -> str:
    if not args:
        return today_local_str(tzinfo)
    try:
        # Поддержка YYYY-MM-DD
        dt = datetime.strptime(args[0], "%Y-%m-%d").replace(tzinfo=tzinfo)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return today_local_str(tzinfo)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_str = today_local_str(tzinfo)
    await send_summary(update, context, date_str)


async def cmd_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tzinfo = user_tz(update)
    date_str = parse_date_arg(context.args, tzinfo)
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
        lines.append(f"• {e.item} — {fmt_amount(e.protein_g, 'г')}, {fmt_amount(e.calories_kcal, 'ккал')} (#{e.id})")

    # Кнопка для экспорта
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
    date_str = parse_date_arg(context.args, tzinfo)
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
        writer.writerow([e.id, e.date_local, e.ts_utc, e.item, e.protein_g, e.calories_kcal])
    data = buf.getvalue().encode("utf-8")
    filename = f"nutrition_{date_str}.csv"
    await update.effective_message.reply_document(document=InputFile(io.BytesIO(data), filename=filename),
                                                 caption=f"Экспорт за {date_str}")


# -------------------------- APP SETUP ------------------------

async def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_TOKEN env var is required")

    await init_db()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("add", handle_add))
    app.add_handler(CommandHandler(["today"], cmd_today))
    app.add_handler(CommandHandler(["sum"], cmd_sum))
    app.add_handler(CommandHandler(["undo"], cmd_undo))
    app.add_handler(CommandHandler(["export"], cmd_export))
    app.add_handler(CallbackQueryHandler(on_export_cb, pattern=r"^export:"))

    # Любой текст – попытка парсинга как записи
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot started… Press Ctrl+C to stop.")
    await app.run_polling(close_loop=False)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
