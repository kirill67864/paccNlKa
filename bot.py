"""Бот-счётчик постов админов каналов. aiogram 3 + PostgreSQL (asyncpg)."""
import asyncio
import csv
import html
import io
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Vladivostok"))
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()
}
NO_SIGNATURE = "Без подписи"
ADMIN_STATUSES = (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
ADMIN_CACHE_TTL = 300  # секунд: как долго помним, что человек админ канала

router = Router()
pool: asyncpg.Pool | None = None
background_tasks: set[asyncio.Task] = set()

# ключ: (кнопка, фраза в заголовке, сколько дней назад; None = всё время)
PERIODS = {
    "day": ("Сегодня", "за сегодня", 0),
    "week": ("7 дней", "за 7 дней", 6),
    "month": ("30 дней", "за 30 дней", 29),
    "all": ("Всё время", "за всё время", None),
}

PRIVATE = F.chat.type == ChatType.PRIVATE


class Feedback(StatesGroup):
    support = State()
    idea = State()


class Broadcast(StatesGroup):
    waiting = State()
    confirm = State()


# ---------- База данных ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    chat_id BIGINT PRIMARY KEY,
    title   TEXT
);
CREATE TABLE IF NOT EXISTS stats (
    chat_id BIGINT  NOT NULL,
    author  TEXT    NOT NULL,
    day     DATE    NOT NULL,
    count   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_id, author, day)
);
CREATE TABLE IF NOT EXISTS seen_albums (
    chat_id        BIGINT      NOT NULL,
    media_group_id TEXT        NOT NULL,
    seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, media_group_id)
);
CREATE TABLE IF NOT EXISTS users (
    user_id    BIGINT      PRIMARY KEY,
    first_name TEXT,
    username   TEXT,
    blocked    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS feedback (
    id         BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL,
    kind       TEXT        NOT NULL,
    text       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_db() -> None:
    global pool
    # statement_cache_size=0 — чтобы работало и через pgbouncer (Supabase, Neon и т.п.)
    pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0
    )
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)


async def cleanup_albums_loop() -> None:
    """Раз в час удаляем старые записи об альбомах, чтобы таблица не росла."""
    while True:
        try:
            await pool.execute("DELETE FROM seen_albums WHERE seen_at < now() - interval '3 days'")
        except Exception:
            logging.exception("Не удалось почистить seen_albums")
        await asyncio.sleep(3600)


def today() -> date:
    return datetime.now(TZ).date()


def period_start(key: str) -> date:
    days_back = PERIODS[key][2]
    return date(1970, 1, 1) if days_back is None else today() - timedelta(days=days_back)


async def upsert_user(user) -> None:
    await pool.execute(
        "INSERT INTO users (user_id, first_name, username) VALUES ($1, $2, $3) "
        "ON CONFLICT (user_id) DO UPDATE SET first_name = EXCLUDED.first_name, "
        "username = EXCLUDED.username, blocked = FALSE",
        user.id, user.full_name, user.username,
    )


# ---------- Проверка админов (с кэшем и параллельно) ----------

_admin_cache: dict[tuple[int, int], tuple[float, bool]] = {}


async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    key = (chat_id, user_id)
    cached = _admin_cache.get(key)
    if cached and time.monotonic() - cached[0] < ADMIN_CACHE_TTL:
        return cached[1]
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False  # бота убрали из канала или нет доступа; не кэшируем
    ok = member.status in ADMIN_STATUSES
    _admin_cache[key] = (time.monotonic(), ok)
    return ok


async def user_channels(bot: Bot, user_id: int) -> list[tuple[int, str]]:
    rows = await pool.fetch("SELECT chat_id, title FROM channels ORDER BY lower(title)")
    flags = await asyncio.gather(*(is_admin(bot, r["chat_id"], user_id) for r in rows))
    return [(r["chat_id"], r["title"] or str(r["chat_id"])) for r, ok in zip(rows, flags) if ok]


# ---------- Клавиатуры и тексты ----------

def main_menu(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="menu:stats")
    kb.button(text="💬 Поддержка", callback_data="menu:support")
    kb.button(text="💡 Предложение", callback_data="menu:idea")
    if user_id in ADMIN_IDS:
        kb.button(text="🛠 Панель владельца", callback_data="menu:owner")
    kb.adjust(1, 2, 1)
    return kb.as_markup()


def back_button(text: str = "⬅️ Меню") -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data="menu:home")


WELCOME = (
    "Привет! Я считаю, сколько постов опубликовал каждый админ твоих каналов.\n\n"
    "<b>Как подключить канал:</b>\n"
    "1. Добавь меня в канал админом.\n"
    "2. В настройках канала включи «Подписывать сообщения» — иначе я не вижу автора.\n\n"
    "Считаются только посты, опубликованные после добавления бота."
)


async def channel_picker(bot: Bot, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    channels = await user_channels(bot, user_id)
    kb = InlineKeyboardBuilder()
    if not channels:
        kb.row(back_button())
        return (
            "Не нашёл каналов, где ты админ и я тоже.\n"
            "Добавь меня в канал админом и опубликуй там пост.",
            kb.as_markup(),
        )
    for chat_id, title in channels:
        kb.button(text=title[:40], callback_data=f"st:{chat_id}:week")
    kb.adjust(2)
    kb.row(back_button())
    return "Выбери канал:", kb.as_markup()


async def stats_view(chat_id: int, key: str) -> tuple[str, InlineKeyboardMarkup]:
    title = await pool.fetchval("SELECT title FROM channels WHERE chat_id = $1", chat_id)
    rows = await pool.fetch(
        "SELECT author, SUM(count)::int AS cnt FROM stats "
        "WHERE chat_id = $1 AND day >= $2 GROUP BY author ORDER BY cnt DESC, author",
        chat_id, period_start(key),
    )
    lines = [f"<b>{html.escape(title or 'Канал')}</b>", f"Посты {PERIODS[key][1]}", ""]
    if not rows:
        lines.append("Постов нет.")
    else:
        medals = ["🥇", "🥈", "🥉"]
        for i, r in enumerate(rows):
            prefix = medals[i] if i < 3 else f"{i + 1}."
            lines.append(f"{prefix} {html.escape(r['author'])} — {r['cnt']}")
        lines += ["", f"Всего: <b>{sum(r['cnt'] for r in rows)}</b>"]

    kb = InlineKeyboardBuilder()
    for k, (label, _, _) in PERIODS.items():
        kb.button(text=("• " if k == key else "") + label, callback_data=f"st:{chat_id}:{k}")
    kb.adjust(4)
    kb.row(InlineKeyboardButton(text="⬅️ К каналам", callback_data="menu:stats"))
    return "\n".join(lines), kb.as_markup()


async def safe_edit(cq: CallbackQuery, text: str, kb: InlineKeyboardMarkup | None = None) -> None:
    try:
        await cq.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


# ---------- Каналы: бота добавили/убрали, новые посты ----------

@router.my_chat_member(F.chat.type == ChatType.CHANNEL)
async def on_bot_channel_status(event: ChatMemberUpdated) -> None:
    status = event.new_chat_member.status
    if status == ChatMemberStatus.ADMINISTRATOR:
        await pool.execute(
            "INSERT INTO channels (chat_id, title) VALUES ($1, $2) "
            "ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title",
            event.chat.id, event.chat.title,
        )
    elif status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
        # Статистика остаётся в базе, но канал пропадает из списков
        await pool.execute("DELETE FROM channels WHERE chat_id = $1", event.chat.id)


@router.my_chat_member(PRIVATE)
async def on_user_blocks_bot(event: ChatMemberUpdated) -> None:
    blocked = event.new_chat_member.status == ChatMemberStatus.KICKED
    await pool.execute("UPDATE users SET blocked = $2 WHERE user_id = $1", event.chat.id, blocked)


@router.channel_post()
async def count_post(message: Message) -> None:
    chat_id = message.chat.id

    # Альбом (несколько фото/видео) = один пост
    if message.media_group_id:
        first = await pool.fetchval(
            "INSERT INTO seen_albums (chat_id, media_group_id) VALUES ($1, $2) "
            "ON CONFLICT DO NOTHING RETURNING 1",
            chat_id, message.media_group_id,
        )
        if first is None:
            return

    await pool.execute(
        "INSERT INTO channels (chat_id, title) VALUES ($1, $2) "
        "ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title",
        chat_id, message.chat.title,
    )
    await pool.execute(
        "INSERT INTO stats (chat_id, author, day, count) VALUES ($1, $2, $3, 1) "
        "ON CONFLICT (chat_id, author, day) DO UPDATE SET count = stats.count + 1",
        chat_id, message.author_signature or NO_SIGNATURE, today(),
    )


# ---------- Команды ----------

@router.message(CommandStart(), PRIVATE)
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await upsert_user(message.from_user)
    await message.answer(WELCOME, reply_markup=main_menu(message.from_user.id))


@router.message(Command("cancel"), PRIVATE)
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Отменено.", reply_markup=main_menu(message.from_user.id))


@router.message(Command("top", "stats", "channels"), PRIVATE)
async def cmd_stats(message: Message, bot: Bot, state: FSMContext) -> None:
    await state.clear()
    text, kb = await channel_picker(bot, message.from_user.id)
    await message.answer(text, reply_markup=kb)


# ---------- Кнопки меню ----------

@router.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await safe_edit(cq, WELCOME, main_menu(cq.from_user.id))
    await cq.answer()


@router.callback_query(F.data == "menu:stats")
async def cb_stats_menu(cq: CallbackQuery, bot: Bot, state: FSMContext) -> None:
    await state.clear()
    text, kb = await channel_picker(bot, cq.from_user.id)
    await safe_edit(cq, text, kb)
    await cq.answer()


@router.callback_query(F.data.startswith("st:"))
async def cb_stats(cq: CallbackQuery, bot: Bot) -> None:
    try:
        _, raw_id, key = cq.data.split(":")
        chat_id = int(raw_id)
    except ValueError:
        await cq.answer()
        return
    if key not in PERIODS:
        await cq.answer()
        return
    if not await is_admin(bot, chat_id, cq.from_user.id):
        await cq.answer("Нет доступа к этому каналу", show_alert=True)
        return
    text, kb = await stats_view(chat_id, key)
    await safe_edit(cq, text, kb)
    await cq.answer()


# ---------- Поддержка и предложения ----------

FEEDBACK_TEXT = {
    "support": ("💬 Поддержка", "Опиши проблему или вопрос одним сообщением. Отмена: /cancel"),
    "idea": ("💡 Предложение", "Напиши, что стоит добавить или улучшить, одним сообщением. Отмена: /cancel"),
}


@router.callback_query(F.data.in_({"menu:support", "menu:idea"}))
async def cb_feedback_start(cq: CallbackQuery, state: FSMContext) -> None:
    if not ADMIN_IDS:
        await cq.answer("Эта функция пока не настроена", show_alert=True)
        return
    kind = cq.data.split(":")[1]
    await state.set_state(Feedback.support if kind == "support" else Feedback.idea)
    kb = InlineKeyboardBuilder()
    kb.row(back_button("❌ Отмена"))
    await safe_edit(cq, FEEDBACK_TEXT[kind][1], kb.as_markup())
    await cq.answer()


@router.message(Feedback.support, F.text, PRIVATE)
@router.message(Feedback.idea, F.text, PRIVATE)
async def feedback_received(message: Message, state: FSMContext, bot: Bot) -> None:
    kind = "support" if await state.get_state() == Feedback.support.state else "idea"
    await state.clear()
    text = message.text[:3500]
    user = message.from_user

    await pool.execute(
        "INSERT INTO feedback (user_id, kind, text) VALUES ($1, $2, $3)", user.id, kind, text
    )
    who = html.escape(user.full_name) + (f" (@{user.username})" if user.username else "")
    to_admin = (
        f"<b>{FEEDBACK_TEXT[kind][0]}</b>\n"
        f"👤 {who}\n🆔 {user.id}\n\n{html.escape(text)}\n\n"
        "<i>Ответьте на это сообщение, чтобы ответить пользователю.</i>"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, to_admin)
        except Exception:
            logging.warning("Не смог отправить обращение админу %s", admin_id)

    await message.answer("Спасибо! Сообщение отправлено ✅", reply_markup=main_menu(user.id))


@router.message(StateFilter(Feedback.support, Feedback.idea), PRIVATE)
async def feedback_not_text(message: Message) -> None:
    await message.answer("Напиши сообщение текстом или нажми /cancel.")


@router.message(
    StateFilter(None), PRIVATE, F.from_user.id.in_(ADMIN_IDS), F.reply_to_message, F.text
)
async def admin_reply(message: Message, bot: Bot) -> None:
    """Админ отвечает реплаем на обращение — ответ уходит пользователю."""
    match = re.search(r"🆔 (\d+)", message.reply_to_message.text or "")
    if not match:
        return
    try:
        await bot.send_message(int(match.group(1)), f"<b>Ответ поддержки:</b>\n{html.escape(message.text)}")
        await message.reply("Ответ отправлен ✅")
    except TelegramForbiddenError:
        await message.reply("Пользователь заблокировал бота.")
    except Exception as e:
        await message.reply(f"Не удалось отправить: {e}")


# ---------- Панель владельца и экспорт (только для ADMIN_IDS) ----------

OWNER = F.from_user.id.in_(ADMIN_IDS)


def owner_panel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📣 Рассылка", callback_data="menu:broadcast")
    kb.button(text="📤 Посты (CSV)", callback_data="exp:posts")
    kb.button(text="📤 Пользователи (CSV)", callback_data="exp:users")
    kb.adjust(1, 2)
    kb.row(back_button())
    return kb.as_markup()


async def owner_stats_text() -> str:
    row = await pool.fetchrow(
        "SELECT (SELECT count(*) FROM users) AS users,"
        " (SELECT count(*) FROM users WHERE blocked) AS blocked,"
        " (SELECT count(*) FROM channels) AS channels,"
        " (SELECT COALESCE(SUM(count), 0) FROM stats WHERE day = $1) AS today,"
        " (SELECT COALESCE(SUM(count), 0) FROM stats WHERE day >= $2) AS week,"
        " (SELECT count(*) FROM feedback) AS feedback",
        today(), today() - timedelta(days=6),
    )
    return (
        "<b>🛠 Панель владельца</b>\n\n"
        f"Пользователей: <b>{row['users']}</b> (заблокировали бота: {row['blocked']})\n"
        f"Каналов подключено: <b>{row['channels']}</b>\n"
        f"Постов сегодня: <b>{row['today']}</b>\n"
        f"Постов за 7 дней: <b>{row['week']}</b>\n"
        f"Обращений в поддержку/предложений: <b>{row['feedback']}</b>"
    )


def safe_cell(value):
    """Защита от формул в Excel: текст, начинающийся с = + - @, получает префикс."""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return value


def make_csv(header: list[str], rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows([[safe_cell(c) for c in row] for row in rows])
    return ("﻿" + buf.getvalue()).encode("utf-8")  # BOM, чтобы Excel понял кириллицу


@router.message(Command("owner"), PRIVATE, OWNER)
async def cmd_owner(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(await owner_stats_text(), reply_markup=owner_panel_kb())


@router.callback_query(F.data == "menu:owner", OWNER)
async def cb_owner(cq: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await safe_edit(cq, await owner_stats_text(), owner_panel_kb())
    await cq.answer()


@router.callback_query(F.data.in_({"exp:posts", "exp:users"}), OWNER)
async def cb_export(cq: CallbackQuery) -> None:
    await cq.answer("Готовлю файл…")
    if cq.data == "exp:posts":
        rows = await pool.fetch(
            "SELECT COALESCE(c.title, s.chat_id::text), s.chat_id, s.author, s.day, s.count "
            "FROM stats s LEFT JOIN channels c ON c.chat_id = s.chat_id "
            "ORDER BY s.day, 1, s.author"
        )
        data = make_csv(["channel", "chat_id", "author", "day", "posts"], [tuple(r) for r in rows])
        name, caption = f"posts_{today()}.csv", f"Посты по дням и авторам, строк: {len(rows)}"
    else:
        rows = await pool.fetch(
            "SELECT user_id, first_name, username, blocked, created_at FROM users ORDER BY created_at"
        )
        data = make_csv(
            ["user_id", "name", "username", "blocked", "created_at"], [tuple(r) for r in rows]
        )
        name, caption = f"users_{today()}.csv", f"Пользователи, строк: {len(rows)}"
    await cq.message.answer_document(BufferedInputFile(data, filename=name), caption=caption)


# ---------- Рассылка (только для ADMIN_IDS) ----------

async def start_broadcast(target: Message, state: FSMContext) -> None:
    n = await pool.fetchval("SELECT count(*) FROM users WHERE NOT blocked")
    await state.set_state(Broadcast.waiting)
    await target.answer(
        f"Получателей: <b>{n}</b>.\n"
        "Пришли сообщение для рассылки (текст, фото, видео — отправлю как есть).\n"
        "Отмена: /cancel"
    )


@router.message(Command("broadcast"), PRIVATE, F.from_user.id.in_(ADMIN_IDS))
async def cmd_broadcast(message: Message, state: FSMContext) -> None:
    await start_broadcast(message, state)


@router.callback_query(F.data == "menu:broadcast", F.from_user.id.in_(ADMIN_IDS))
async def cb_broadcast(cq: CallbackQuery, state: FSMContext) -> None:
    await start_broadcast(cq.message, state)
    await cq.answer()


@router.message(Broadcast.waiting, PRIVATE, F.from_user.id.in_(ADMIN_IDS))
async def broadcast_got_message(message: Message, state: FSMContext) -> None:
    n = await pool.fetchval("SELECT count(*) FROM users WHERE NOT blocked")
    await state.update_data(src_chat=message.chat.id, src_msg=message.message_id)
    await state.set_state(Broadcast.confirm)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Отправить", callback_data="bc:yes")
    kb.button(text="❌ Отмена", callback_data="bc:no")
    kb.adjust(2)
    await message.reply(f"Отправить это сообщение {n} пользователям?", reply_markup=kb.as_markup())


@router.callback_query(Broadcast.confirm, F.data.in_({"bc:yes", "bc:no"}), F.from_user.id.in_(ADMIN_IDS))
async def broadcast_confirm(cq: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    await state.clear()
    if cq.data == "bc:no" or "src_msg" not in data:
        await safe_edit(cq, "Рассылка отменена.")
        await cq.answer()
        return
    await safe_edit(cq, "Рассылка запущена. Пришлю отчёт, когда закончу.")
    await cq.answer()
    task = asyncio.create_task(
        run_broadcast(bot, cq.message.chat.id, data["src_chat"], data["src_msg"])
    )
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def run_broadcast(bot: Bot, report_chat: int, src_chat: int, src_msg: int) -> None:
    rows = await pool.fetch("SELECT user_id FROM users WHERE NOT blocked")
    sent, failed, blocked = 0, 0, []
    for r in rows:
        uid = r["user_id"]
        for attempt in range(2):
            try:
                await bot.copy_message(uid, src_chat, src_msg)
                sent += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
                continue  # одна повторная попытка
            except TelegramForbiddenError:
                blocked.append(uid)
            except Exception:
                failed += 1
            break
        await asyncio.sleep(0.05)  # ~20 сообщений в секунду, в пределах лимитов Telegram

    if blocked:
        await pool.execute("UPDATE users SET blocked = TRUE WHERE user_id = ANY($1::bigint[])", blocked)
    await bot.send_message(
        report_chat,
        f"<b>Рассылка завершена</b>\n✅ Доставлено: {sent}\n"
        f"🚫 Заблокировали бота: {len(blocked)}\n⚠️ Ошибки: {failed}",
    )


# ---------- Запуск ----------

async def main() -> None:
    if not TOKEN:
        raise SystemExit("Укажи BOT_TOKEN")
    if not DATABASE_URL:
        raise SystemExit("Укажи DATABASE_URL (строка подключения PostgreSQL)")
    logging.basicConfig(level=logging.INFO)
    await init_db()

    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    cleanup = asyncio.create_task(cleanup_albums_loop())
    try:
        # Если у бота был включён вебхук, getUpdates не работает — снимаем его
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        cleanup.cancel()
        await pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
