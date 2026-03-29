"""
Сервер авторизации + Telegram-бот Neuro Uploader.
API для приложения + админ-панель в Telegram (только ADMIN_ID).
"""
import asyncio
import html
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import uvicorn
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import Client, create_client

# ==========================================
# НАСТРОЙКИ (ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ)
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([BOT_TOKEN, ADMIN_ID, SUPABASE_URL, SUPABASE_KEY]):
    print("ВНИМАНИЕ: Не все переменные окружения заданы (BOT_TOKEN, ADMIN_ID, SUPABASE_*)!")

# Канал для обычного /start (авторизация по deep-link /start SESSION не затрагивается)
REQUIRED_CHANNEL_ID = (os.environ.get("REQUIRED_CHANNEL_ID") or "").strip()
CHANNEL_INVITE_LINK = (os.environ.get("CHANNEL_INVITE_LINK") or "").strip()
# Внешние ссылки для кнопок меню (опционально)
LINK_REVIEWS = (os.environ.get("LINK_REVIEWS") or "").strip()
LINK_BUY = (os.environ.get("LINK_BUY") or "").strip()
LINK_SUPPORT = (os.environ.get("LINK_SUPPORT") or "").strip()
# Ссылки на документы перед доступом к боту (после подписки на канал)
LINK_TERMS = (os.environ.get("LINK_TERMS") or "").strip()
LINK_PRIVACY = (os.environ.get("LINK_PRIVACY") or "").strip()
LINK_PRICING = (os.environ.get("LINK_PRICING") or "").strip()
# Чат для логов входа на сайт / в приложение (числовой id: группа или канал; бот должен быть участником)
AUTH_LOG_CHAT_ID = (os.environ.get("AUTH_LOG_CHAT_ID") or "").strip()

# Короткий префикс callback — лимит Telegram 64 байта (a: админ, u: пользователь)
CB = "a"
UCB = "u"

PAGE_SIZE = 7

app = FastAPI()
_raw_cors = (os.environ.get("CORS_ORIGINS") or "*").strip()
if not _raw_cors or _raw_cors == "*":
    _cors_list = ["*"]
else:
    _cors_list = [o.strip() for o in _raw_cors.split(",") if o.strip()]
    if not _cors_list:
        _cors_list = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_list,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


class AuthRequest(BaseModel):
    session_id: str
    hwid: str


class AdminStates(StatesGroup):
    """Ожидание ввода от админа"""
    sub_days = State()
    hwid_value = State()


def is_admin(user_id: int) -> bool:
    return bool(ADMIN_ID) and user_id == ADMIN_ID


# kind для сайта: app_success_first_pc | app_success_same_pc | app_fail_no_subscription | app_fail_other_pc | app_fail_invalid_link
def save_login_notification(telegram_id: int, kind: str, success: bool) -> None:
    """События входа в приложение для раздела «Уведомления» на сайте (таблица login_notifications)."""
    try:
        supabase.table("login_notifications").insert(
            {"telegram_id": telegram_id, "kind": kind, "success": success, "created_at": int(time.time())}
        ).execute()
    except Exception as e:
        print(f"login_notifications insert failed: {e}")


async def send_auth_log(title: str, lines: list[str]) -> None:
    """Отправка в отдельный чат (AUTH_LOG_CHAT_ID). Не ломает авторизацию при ошибке."""
    if not AUTH_LOG_CHAT_ID or not bot:
        return
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        body = "\n".join(f"• {x}" for x in lines)
        text = f"{title}\n{body}\n• Время: {ts}"
        if len(text) > 4000:
            text = text[:3997] + "..."
        await bot.send_message(chat_id=int(AUTH_LOG_CHAT_ID), text=text)
    except Exception as e:
        print(f"AUTH_LOG send failed: {e}")


def esc_html(s: str) -> str:
    """Экранирование для Telegram HTML (username с _, HWID и т.д.)."""
    return html.escape(str(s), quote=False)


def fmt_ts(ts: Optional[int]) -> str:
    if not ts:
        return "—"
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


def users_fetch_all():
    """Все пользователи из Supabase (разумный лимит)."""
    res = supabase.table("users").select("*").order("telegram_id", desc=False).limit(2000).execute()
    return res.data or []


def user_get(tid: int):
    r = supabase.table("users").select("*").eq("telegram_id", tid).execute()
    return r.data[0] if r.data else None


def policies_user_has_accepted(telegram_id: int) -> bool:
    """Пользователь нажал «Принимаю» в боте (таблица policy_acceptances)."""
    try:
        r = (
            supabase.table("policy_acceptances")
            .select("accepted_at")
            .eq("telegram_id", telegram_id)
            .execute()
        )
        return bool(r.data and r.data[0].get("accepted_at"))
    except Exception as e:
        print(f"policy_acceptances read failed: {e}")
        return False


def record_policies_acceptance(telegram_id: int) -> None:
    try:
        supabase.table("policy_acceptances").upsert(
            {"telegram_id": telegram_id, "accepted_at": int(time.time())}
        ).execute()
    except Exception as e:
        print(f"policy_acceptances upsert failed: {e}")


def build_user_card_text(tid: int, u: dict) -> str:
    now = int(time.time())
    sub = u.get("subscription_until") or 0
    status = "активна ✅" if sub >= now else "истекла ⛔"
    hw = u.get("hwid")
    hw_show = f"<code>{esc_html(hw)}</code>" if hw else "<i>не привязан</i>"
    un = esc_html(u.get("username") or "—")
    return (
        f"👤 <b>Пользователь</b>\n\n"
        f"• ID: <code>{tid}</code>\n"
        f"• Username: {un}\n"
        f"• Подписка: <b>{esc_html(status)}</b>\n"
        f"• До (UTC): {esc_html(fmt_ts(sub))}\n"
        f"• HWID:\n{hw_show}\n"
    )


async def edit_user_card(query: CallbackQuery, tid: int, answer_popup: Optional[str] = None):
    """Обновить сообщение карточки пользователя; один вызов query.answer."""
    u = user_get(tid)
    if not u:
        await query.answer("Пользователь не найден", show_alert=True)
        return
    text = build_user_card_text(tid, u)
    await query.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=kb_user_actions(tid),
    )
    await query.answer(answer_popup or "")


def kb_main_admin():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Все пользователи", callback_data=f"{CB}:list:0")],
            [InlineKeyboardButton(text="ℹ️ Справка", callback_data=f"{CB}:help")],
        ]
    )


def kb_user_actions(tid: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="+7 дн.", callback_data=f"{CB}:q:{tid}:7"),
                InlineKeyboardButton(text="+30 дн.", callback_data=f"{CB}:q:{tid}:30"),
                InlineKeyboardButton(text="+365 дн.", callback_data=f"{CB}:q:{tid}:365"),
            ],
            [
                InlineKeyboardButton(text="✏️ Свой срок (дней)", callback_data=f"{CB}:s:{tid}"),
            ],
            [
                InlineKeyboardButton(text="🗑 Сбросить HWID", callback_data=f"{CB}:h:{tid}"),
                InlineKeyboardButton(text="✏️ Задать HWID", callback_data=f"{CB}:w:{tid}"),
            ],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data=f"{CB}:list:0")],
            [InlineKeyboardButton(text="🏠 Админ-меню", callback_data=f"{CB}:menu")],
        ]
    )


async def user_passes_channel_gate(user_id: int) -> bool:
    """Подписка на канал не требуется, если REQUIRED_CHANNEL_ID пустой."""
    if not REQUIRED_CHANNEL_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        st = member.status
        if st in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
            return False
        return True
    except Exception:
        return False


def kb_channel_required():
    """Экран «сначала подпишитесь»."""
    rows = []
    if CHANNEL_INVITE_LINK:
        rows.append(
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_INVITE_LINK)]
        )
    rows.append(
        [InlineKeyboardButton(text="✅ Я подписался, проверить", callback_data=f"{UCB}:chk")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def text_policies_prompt_html(extra_html: str = "") -> str:
    base = (
        "📌 <b>Доступ к боту</b>\n\n"
        "Ознакомьтесь с документами по кнопкам ниже, затем нажмите "
        "<b>Принимаю все условия</b>."
    )
    return base + (f"\n\n{extra_html}" if extra_html else "")


def kb_policies_accept():
    rows: list[list[InlineKeyboardButton]] = []
    if LINK_TERMS:
        rows.append([InlineKeyboardButton(text="📋 Условия пользования", url=LINK_TERMS)])
    if LINK_PRIVACY:
        rows.append([InlineKeyboardButton(text="🔒 Политика конфиденциальности", url=LINK_PRIVACY)])
    if LINK_PRICING:
        rows.append([InlineKeyboardButton(text="💰 Ценовая политика", url=LINK_PRICING)])
    rows.append(
        [InlineKeyboardButton(text="✅ Принимаю все условия", callback_data=f"{UCB}:policies_ok")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_user_main_menu():
    """Главное меню пользователя."""
    row_rev = []
    if LINK_REVIEWS:
        row_rev = [InlineKeyboardButton(text="⭐ Отзывы", url=LINK_REVIEWS)]
    else:
        row_rev = [InlineKeyboardButton(text="⭐ Отзывы", callback_data=f"{UCB}:reviews")]

    row_buy = []
    if LINK_BUY:
        row_buy = [InlineKeyboardButton(text="💳 Купить подписку", url=LINK_BUY)]
    else:
        row_buy = [InlineKeyboardButton(text="💳 Купить подписку", callback_data=f"{UCB}:buy")]

    row_sup = []
    if LINK_SUPPORT:
        row_sup = [InlineKeyboardButton(text="💬 Поддержка", url=LINK_SUPPORT)]
    else:
        row_sup = [InlineKeyboardButton(text="💬 Поддержка", callback_data=f"{UCB}:support")]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Профиль", callback_data=f"{UCB}:profile")],
            row_rev,
            row_buy,
            row_sup,
        ]
    )


def kb_user_back_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def text_user_main_menu() -> str:
    return (
        "🏠 **Neuro Uploader**\n\n"
        "Выберите раздел ниже.\n\n"
        "Авторизация в приложении — через кнопку входа в программе (это отдельный сценарий)."
    )


def build_user_profile_public_text(tid: int, u: Optional[dict]) -> str:
    """Профиль для обычного пользователя (без HWID)."""
    if not u:
        return (
            "👤 <b>Профиль</b>\n\n"
            "Запись в базе не найдена. После покупки подписки данные появятся здесь.\n\n"
            "Используйте «Купить подписку», если ещё не оформляли доступ."
        )
    now = int(time.time())
    sub = u.get("subscription_until") or 0
    active = sub >= now
    status = "активна ✅" if active else "истекла ⛔"
    return (
        "👤 <b>Профиль</b>\n\n"
        f"• Telegram ID: <code>{tid}</code>\n"
        f"• Подписка: <b>{esc_html(status)}</b>\n"
        f"• До (UTC): {esc_html(fmt_ts(sub))}\n"
    )


def kb_list_nav(page: int, total_pages: int):
    row = []
    if page > 0:
        row.append(InlineKeyboardButton(text="◀️", callback_data=f"{CB}:list:{page - 1}"))
    row.append(InlineKeyboardButton(text=f"{page + 1}/{max(total_pages, 1)}", callback_data=f"{CB}:noop"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton(text="▶️", callback_data=f"{CB}:list:{page + 1}"))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            row,
            [InlineKeyboardButton(text="🏠 Админ-меню", callback_data=f"{CB}:menu")],
        ]
    )


def kb_web_login_confirm(session_id: str):
    """Кнопка подтверждения входа на сайт (callback ≤ 64 байт: u:auth:<uuid>)."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Авторизоваться на сайте",
                    callback_data=f"{UCB}:auth:{session_id}",
                )
            ]
        ]
    )


def complete_web_site_login_sync(session_id: str, telegram_id: int, username: str) -> tuple[Optional[str], bool]:
    """Вход на сайт: в БД только telegram_id и @username, HWID не трогаем (привязка ПК — только из приложения).

    Возвращает (ошибка или None, is_new_user).
    """
    session_res = (
        supabase.table("auth_sessions")
        .select("*")
        .eq("session_id", session_id)
        .eq("status", "pending")
        .execute()
    )
    if not session_res.data:
        return "Сессия не найдена или уже использована.", False
    session = session_res.data[0]
    if not str(session.get("hwid") or "").startswith("WEB_BROWSER_"):
        return "Неверный тип сессии.", False
    user_res = supabase.table("users").select("*").eq("telegram_id", telegram_id).execute()
    is_new = False
    if not user_res.data:
        supabase.table("users").insert(
            {"telegram_id": telegram_id, "username": username, "hwid": None, "subscription_until": 0}
        ).execute()
        is_new = True
    else:
        # Только username; hwid не перезаписываем — он задаётся при первом входе в приложение
        supabase.table("users").update({"username": username}).eq("telegram_id", telegram_id).execute()
        is_new = False
    supabase.table("auth_sessions").update({"status": "success", "telegram_id": telegram_id}).eq(
        "session_id", session_id
    ).execute()
    return None, is_new


# --- FastAPI (без изменений по смыслу) ---


@app.get("/")
async def root():
    return {"service": "neuro-uploader-auth", "docs": "/docs", "health": "/health", "ping": "/api/auth/ping"}


@app.get("/health")
async def health():
    """Проверка, что сервис поднят (удобно для Railway / браузера)."""
    return {"ok": True}


@app.get("/api/auth/ping")
async def auth_ping():
    """Проверка из браузера: откройте URL в новой вкладке или fetch с localhost."""
    return {"ok": True}


@app.post("/api/auth/start")
async def start_auth(req: AuthRequest):
    if not bot:
        raise HTTPException(status_code=503, detail="BOT_TOKEN не задан — бот не сконфигурирован")
    try:
        bot_info = await bot.get_me()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Telegram API недоступен: {e!s}") from e
    try:
        supabase.table("auth_sessions").upsert(
            {
                "session_id": req.session_id,
                "hwid": req.hwid,
                "status": "pending",
                "created_at": int(time.time()),
            }
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase (auth_sessions): {e!s}") from e
    return {"status": "ok", "login_url": f"https://t.me/{bot_info.username}?start={req.session_id}"}


@app.get("/api/auth/check/{session_id}")
async def check_auth(session_id: str):
    response = supabase.table("auth_sessions").select("*").eq("session_id", session_id).execute()
    if not response.data:
        raise HTTPException(status_code=404, detail="Session not found")
    session = response.data[0]
    tid = session.get("telegram_id")
    username = None
    if session.get("status") == "success" and tid:
        ur = supabase.table("users").select("username").eq("telegram_id", tid).execute()
        if ur.data:
            username = ur.data[0].get("username")
    return {"status": session["status"], "telegram_id": tid, "username": username}


@app.get("/api/notifications/{telegram_id}")
async def list_login_notifications(telegram_id: int, limit: int = 50):
    """Уведомления о входе в приложение для личного кабинета на сайте."""
    lim = max(1, min(limit, 100))
    res = (
        supabase.table("login_notifications")
        .select("id,kind,success,created_at")
        .eq("telegram_id", telegram_id)
        .order("created_at", desc=True)
        .limit(lim)
        .execute()
    )
    return {"items": res.data or []}


@app.get("/api/auth/verify/{telegram_id}")
async def verify_subscription(telegram_id: int):
    user_res = supabase.table("users").select("*").eq("telegram_id", telegram_id).execute()
    current_time = int(time.time())
    if not user_res.data or user_res.data[0]["subscription_until"] < current_time:
        return {"status": "failed", "message": "Subscription expired or user not found"}
    return {"status": "success", "message": "Subscription active"}


# --- Telegram: пользователи ---


async def require_policies_or_block(query: CallbackQuery) -> bool:
    """False = показан экран принятия, дальше не идём."""
    uid = query.from_user.id
    if is_admin(uid) or policies_user_has_accepted(uid):
        return True
    await query.message.edit_text(
        text_policies_prompt_html(),
        parse_mode="HTML",
        reply_markup=kb_policies_accept(),
    )
    await query.answer("Сначала примите условия.", show_alert=True)
    return False


@dp.message(CommandStart())
async def cmd_start(message: Message):
    args = message.text.split() if message.text else []
    if len(args) == 1:
        uid = message.from_user.id
        # Обычный /start без параметра: канал + условия. /start SESSION (сайт/приложение) — без них.
        if not is_admin(uid) and not await user_passes_channel_gate(uid):
            await message.answer(
                "📢 Чтобы пользоваться ботом, подпишитесь на наш канал.\n\n"
                "После подписки нажмите **«Я подписался, проверить»**.",
                parse_mode="Markdown",
                reply_markup=kb_channel_required(),
            )
            return

        if not is_admin(uid) and not policies_user_has_accepted(uid):
            await message.answer(
                text_policies_prompt_html(),
                parse_mode="HTML",
                reply_markup=kb_policies_accept(),
            )
            return

        extra = ""
        if is_admin(uid):
            extra = "\n\n🔐 /admin — панель администратора."
        await message.answer(
            text_user_main_menu() + extra,
            parse_mode="Markdown",
            reply_markup=kb_user_main_menu(),
        )
        return

    session_id = args[1]
    telegram_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else str(telegram_id)

    # Вход с сайта / из приложения: без канала и без экрана принятия условий
    session_res = (
        supabase.table("auth_sessions")
        .select("*")
        .eq("session_id", session_id)
        .eq("status", "pending")
        .execute()
    )
    if not session_res.data:
        await message.answer("❌ Ссылка недействительна.")
        save_login_notification(telegram_id, "app_fail_invalid_link", False)
        await send_auth_log(
            "⚠️ Авторизация",
            ["Сбой: недействительная ссылка сессии", f"ID: `{telegram_id}`", f"Username: {username}"],
        )
        return

    session = session_res.data[0]
    hwid_from_app = session["hwid"]
    is_web_login = hwid_from_app.startswith("WEB_BROWSER_")

    # Вход на сайт — без проверки подписки; десктоп — только с активной подпиской
    if is_web_login:
        await message.answer(
            "🔐 **Вход на сайт Neuro Uploader**\n\n"
            "Нажмите кнопку ниже, чтобы подтвердить вход.",
            parse_mode="Markdown",
            reply_markup=kb_web_login_confirm(session_id),
        )
        return

    user_res = supabase.table("users").select("*").eq("telegram_id", telegram_id).execute()
    current_time = int(time.time())

    if not user_res.data or user_res.data[0]["subscription_until"] < current_time:
        supabase.table("auth_sessions").update({"status": "failed"}).eq("session_id", session_id).execute()
        await message.answer("❌ У вас нет активной подписки.")
        save_login_notification(telegram_id, "app_fail_no_subscription", False)
        await send_auth_log(
            "⚠️ Вход в приложение отклонён",
            [f"Причина: нет активной подписки", f"ID: `{telegram_id}`", f"Username: {username}"],
        )
        return

    user = user_res.data[0]

    saved_hwid = user.get("hwid")
    if not saved_hwid:
        supabase.table("users").update({"hwid": hwid_from_app, "username": username}).eq(
            "telegram_id", telegram_id
        ).execute()
        supabase.table("auth_sessions").update({"status": "success", "telegram_id": telegram_id}).eq(
            "session_id", session_id
        ).execute()
        await message.answer("✅ Успешный вход! Аккаунт привязан к этому ПК.")
        save_login_notification(telegram_id, "app_success_first_pc", True)
        await send_auth_log(
            "💻 Вход в приложение",
            [
                f"ID: `{telegram_id}`",
                f"Username: {username}",
                "HWID: записан впервые (привязка к ПК)",
            ],
        )
    elif saved_hwid == hwid_from_app:
        supabase.table("users").update({"username": username}).eq("telegram_id", telegram_id).execute()
        supabase.table("auth_sessions").update({"status": "success", "telegram_id": telegram_id}).eq(
            "session_id", session_id
        ).execute()
        await message.answer("✅ Успешный вход! Возвращайтесь в программу.")
        save_login_notification(telegram_id, "app_success_same_pc", True)
        await send_auth_log(
            "💻 Вход в приложение",
            [
                f"ID: `{telegram_id}`",
                f"Username: {username}",
                "HWID: совпадает с сохранённым",
            ],
        )
    else:
        supabase.table("auth_sessions").update({"status": "failed"}).eq("session_id", session_id).execute()
        await message.answer("❌ Ошибка! Подписка уже используется на другом ПК.")
        save_login_notification(telegram_id, "app_fail_other_pc", False)
        await send_auth_log(
            "⚠️ Вход в приложение отклонён",
            [
                f"Причина: другой ПК уже привязан",
                f"ID: `{telegram_id}`",
                f"Username: {username}",
            ],
        )


@dp.callback_query(F.data.startswith(f"{UCB}:auth:"))
async def cb_web_login_confirm(query: CallbackQuery):
    parts = query.data.split(":", 2)
    if len(parts) < 3:
        await query.answer("Ошибка данных", show_alert=True)
        return
    session_id = parts[2]
    tid = query.from_user.id
    username = f"@{query.from_user.username}" if query.from_user.username else str(tid)
    err, is_new = complete_web_site_login_sync(session_id, tid, username)
    if err:
        await query.answer(err, show_alert=True)
        return
    await send_auth_log(
        "🌐 Вход на сайт",
        [
            f"ID: `{tid}`",
            f"Username: {username}",
            "Регистрация в БД" if is_new else "Пользователь уже был в БД (обновлён username)",
            "HWID не менялся (только сайт)",
        ],
    )
    await query.message.edit_text("✅ Вход на сайт выполнен успешно!")
    await query.answer()


# --- Пользователь: главное меню (не затрагивает авторизацию в приложении по deep-link) ---


@dp.callback_query(F.data == f"{UCB}:policies_ok")
async def cb_policies_accept(query: CallbackQuery):
    uid = query.from_user.id
    record_policies_acceptance(uid)
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    await query.message.edit_text(
        text_user_main_menu() + extra,
        parse_mode="Markdown",
        reply_markup=kb_user_main_menu(),
    )
    await query.answer("Доступ к боту открыт.")


@dp.callback_query(F.data == f"{UCB}:chk")
async def cb_user_channel_recheck(query: CallbackQuery):
    """Повторная проверка подписки на канал."""
    uid = query.from_user.id
    if not await user_passes_channel_gate(uid):
        await query.answer("Сначала подпишитесь на канал.", show_alert=True)
        return
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await query.message.edit_text(
            text_policies_prompt_html(),
            parse_mode="HTML",
            reply_markup=kb_policies_accept(),
        )
        await query.answer("Канал подтверждён. Осталось принять условия.")
        return
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    await query.message.edit_text(
        text_user_main_menu() + extra,
        parse_mode="Markdown",
        reply_markup=kb_user_main_menu(),
    )
    await query.answer("✅ Подписка подтверждена!")


@dp.callback_query(F.data == f"{UCB}:main")
async def cb_user_back_main(query: CallbackQuery):
    uid = query.from_user.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await query.message.edit_text(
            text_policies_prompt_html(),
            parse_mode="HTML",
            reply_markup=kb_policies_accept(),
        )
        await query.answer()
        return
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    await query.message.edit_text(
        text_user_main_menu() + extra,
        parse_mode="Markdown",
        reply_markup=kb_user_main_menu(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:profile")
async def cb_user_profile_menu(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    tid = query.from_user.id
    u = user_get(tid)
    await query.message.edit_text(
        build_user_profile_public_text(tid, u),
        parse_mode="HTML",
        reply_markup=kb_user_back_main(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:reviews")
async def cb_user_reviews_placeholder(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    text = os.environ.get(
        "TEXT_REVIEWS",
        "⭐ **Отзывы**\n\nЗдесь будет ссылка на отзывы или канал. Укажите `LINK_REVIEWS` в настройках.",
    )
    await query.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=kb_user_back_main(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:buy")
async def cb_user_buy_placeholder(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    text = os.environ.get(
        "TEXT_BUY",
        "💳 **Купить подписку**\n\nНапишите в поддержку или перейдите по ссылке из канала. "
        "Задайте переменную `LINK_BUY` для кнопки с оплатой.",
    )
    await query.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=kb_user_back_main(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:support")
async def cb_user_support_placeholder(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    text = os.environ.get(
        "TEXT_SUPPORT",
        "💬 **Поддержка**\n\nОпишите проблему в ответе на это сообщение или задайте `LINK_SUPPORT` "
        "(например ссылка на чат с менеджером).",
    )
    await query.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=kb_user_back_main(),
    )
    await query.answer()


# --- Админ: команды ---


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return
    await state.clear()
    await message.answer(
        "🔐 **Админ-панель Neuro Uploader**\n\n"
        "• Просмотр пользователей и подписок\n"
        "• Изменение срока подписки\n"
        "• Сброс и ручная установка HWID\n\n"
        "Команды вручную: `/add ID дней`, `/reset ID`",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено.", reply_markup=kb_main_admin())


@dp.callback_query(F.data == f"{CB}:noop")
async def cb_noop(query: CallbackQuery):
    await query.answer()


@dp.callback_query(F.data == f"{CB}:menu")
async def cb_menu(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await query.message.edit_text(
        "🔐 **Админ-панель Neuro Uploader**",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{CB}:help")
async def cb_help(query: CallbackQuery):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await query.message.edit_text(
        "📖 **Справка**\n\n"
        "• **Пользователи** — список из базы, пагинация.\n"
        "• **+7 / +30 / +365** — продлить подписку от текущего момента.\n"
        "• **Свой срок** — введите число **дней** (можно `0` — сразу истекает).\n"
        "• **Сброс HWID** — пользователь сможет войти с нового ПК.\n"
        "• **Задать HWID** — вставьте **64-символьный** hex (как в приложении).\n\n"
        "/cancel — отменить ввод.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{CB}:menu")]]
        ),
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:list:"))
async def cb_list(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    try:
        page = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        page = 0

    users = users_fetch_all()
    total = len(users)
    total_pages = max((total + PAGE_SIZE - 1) // PAGE_SIZE, 1)
    page = max(0, min(page, total_pages - 1))

    start = page * PAGE_SIZE
    chunk = users[start : start + PAGE_SIZE]

    lines = [f"📋 <b>Пользователи</b> (всего: {total})\n"]
    rows = []
    now = int(time.time())
    for u in chunk:
        tid = u.get("telegram_id")
        un = u.get("username") or "—"
        sub = u.get("subscription_until") or 0
        active = "✅" if sub >= now else "⛔"
        short_hw = "есть" if u.get("hwid") else "нет"
        lines.append(
            f"{active} <code>{tid}</code> {esc_html(un)}\n"
            f"   до: {esc_html(fmt_ts(sub))} · HWID: {short_hw}\n"
        )
        rows.append(
            [InlineKeyboardButton(text=f"✏️ {tid}", callback_data=f"{CB}:u:{tid}")]
        )

    text = "\n".join(lines) if lines else "База пуста."
    markup = kb_list_nav(page, total_pages)
    if rows:
        markup.inline_keyboard = rows + markup.inline_keyboard

    await query.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:u:"))
async def cb_user(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return

    await edit_user_card(query, tid)


@dp.callback_query(F.data.startswith(f"{CB}:q:"))
async def cb_quick_sub(query: CallbackQuery, state: FSMContext):
    """Быстрое продление: a:q:telegram_id:days"""
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    parts = query.data.split(":")
    if len(parts) < 4:
        await query.answer("Ошибка")
        return
    try:
        tid = int(parts[2])
        days = int(parts[3])
    except ValueError:
        await query.answer("Ошибка данных")
        return

    until = int(time.time()) + days * 86400
    u = user_get(tid)
    if u:
        supabase.table("users").update({"subscription_until": until}).eq("telegram_id", tid).execute()
    else:
        supabase.table("users").insert(
            {"telegram_id": tid, "subscription_until": until, "hwid": None}
        ).execute()

    await edit_user_card(query, tid, f"✅ +{days} дн.")


@dp.callback_query(F.data.startswith(f"{CB}:s:"))
async def cb_sub_prompt(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return

    await state.set_state(AdminStates.sub_days)
    await state.update_data(telegram_id=tid)
    await query.message.answer(
        f"Введите **количество дней** подписки от _сейчас_ для `{tid}`.\n"
        f"`0` — подписка сразу истекает.\n/cancel — отмена.",
        parse_mode="Markdown",
    )
    await query.answer()


@dp.message(AdminStates.sub_days, F.text)
async def process_sub_days(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    tid = data.get("telegram_id")
    if not tid:
        await state.clear()
        await message.answer("Сессия сброшена. /admin")
        return
    try:
        days = int(message.text.strip())
        if days < 0:
            raise ValueError()
    except ValueError:
        await message.answer("Нужно целое число дней (≥ 0).")
        return

    until = int(time.time()) + days * 86400
    u = user_get(tid)
    if u:
        supabase.table("users").update({"subscription_until": until}).eq("telegram_id", tid).execute()
    else:
        supabase.table("users").insert(
            {"telegram_id": tid, "subscription_until": until, "hwid": None}
        ).execute()

    await state.clear()
    await message.answer(
        f"✅ Подписка для `{tid}` установлена: **{fmt_ts(until)}** ({days} дн.)",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


@dp.callback_query(F.data.startswith(f"{CB}:h:"))
async def cb_hwid_clear(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка")
        return
    supabase.table("users").update({"hwid": None}).eq("telegram_id", tid).execute()
    await edit_user_card(query, tid, "HWID сброшен")


@dp.callback_query(F.data.startswith(f"{CB}:w:"))
async def cb_hwid_prompt(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return

    await state.set_state(AdminStates.hwid_value)
    await state.update_data(telegram_id=tid)
    await query.message.answer(
        f"Отправьте **HWID** (64 hex-символа) для `{tid}`.\n"
        f"Или отправьте `clear` чтобы сбросить.\n/cancel — отмена.",
        parse_mode="Markdown",
    )
    await query.answer()


@dp.message(AdminStates.hwid_value, F.text)
async def process_hwid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    tid = data.get("telegram_id")
    if not tid:
        await state.clear()
        await message.answer("Сессия сброшена. /admin")
        return

    raw = message.text.strip().lower()
    if raw in ("clear", "сброс", "none"):
        supabase.table("users").update({"hwid": None}).eq("telegram_id", tid).execute()
        await state.clear()
        await message.answer(f"✅ HWID сброшен для `{tid}`.", parse_mode="Markdown", reply_markup=kb_main_admin())
        return

    hwid = re.sub(r"\s+", "", raw)
    if not re.fullmatch(r"[0-9a-f]{64}", hwid):
        await message.answer(
            "Нужна строка из **64** шестнадцатеричных символов (как выдаёт приложение), или `clear`."
        )
        return

    u = user_get(tid)
    if not u:
        await message.answer("Пользователь не найден в базе. Сначала выдайте подписку.")
        await state.clear()
        return

    supabase.table("users").update({"hwid": hwid}).eq("telegram_id", tid).execute()

    await state.clear()
    await message.answer(
        f"✅ HWID для `{tid}` обновлён.",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


# Старые текстовые команды (совместимость)


@dp.message(Command("add"))
async def cmd_add_sub(message: Message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) != 3:
        await message.answer("Использование: /add TELEGRAM_ID ДНЕЙ")
        return
    try:
        target_id, days = int(args[1]), int(args[2])
    except ValueError:
        await message.answer("Нужны числа: /add ID дней")
        return
    until_time = int(time.time()) + (days * 24 * 60 * 60)
    user_res = supabase.table("users").select("*").eq("telegram_id", target_id).execute()
    if user_res.data:
        supabase.table("users").update({"subscription_until": until_time}).eq("telegram_id", target_id).execute()
    else:
        supabase.table("users").insert(
            {"telegram_id": target_id, "subscription_until": until_time, "hwid": None}
        ).execute()
    await message.answer(f"✅ Подписка выдана на {days} дней.")


@dp.message(Command("reset"))
async def cmd_reset_hwid(message: Message):
    if not is_admin(message.from_user.id):
        return
    args = message.text.split()
    if len(args) != 2:
        await message.answer("Использование: /reset TELEGRAM_ID")
        return
    try:
        target_id = int(args[1])
    except ValueError:
        await message.answer("Нужен числовой ID")
        return
    supabase.table("users").update({"hwid": None}).eq("telegram_id", target_id).execute()
    await message.answer("✅ Привязка сброшена.")


async def main():
    port = int(os.environ.get("PORT", 5000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    await asyncio.gather(server.serve(), dp.start_polling(bot))


if __name__ == "__main__":
    asyncio.run(main())
