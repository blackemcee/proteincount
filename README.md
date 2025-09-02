# Protein & Calories Tracker Bot
Телеграм‑бот для учёта белка и калорий. Хранение — SQLite. Готов к деплою на Railway (polling, том для БД).

## Быстрый старт локально
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
export TELEGRAM_TOKEN=123:ABC  # Windows: set TELEGRAM_TOKEN=...
python bot_pg.py