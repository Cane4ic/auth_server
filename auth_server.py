"""
Сервер авторизации + Telegram-бот Neuro Uploader.
API для приложения + админ-панель в Telegram (ADMIN_ID — один или несколько id через запятую).
"""
import asyncio
import html
import io
import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from hmac import HMAC, compare_digest
from typing import Any, Optional

from contextlib import asynccontextmanager

import httpx
import uvicorn
from aiogram import Bot, Dispatcher, F
from aiogram.dispatcher.event.bases import UNHANDLED
from aiogram.exceptions import TelegramBadRequest
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaDocument,
    InputMediaPhoto,
    Message,
)
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from supabase import Client, create_client

try:
    from PIL import Image, ImageEnhance, ImageOps

    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    import imageio_ffmpeg
except ImportError:
    imageio_ffmpeg = None  # type: ignore

try:
    from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:  # pragma: no cover

    class PostgrestAPIError(Exception):
        """Заглушка, если нет postgrest в окружении."""

# ==========================================
# НАСТРОЙКИ (ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ)
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")


def _parse_admin_ids_from_env() -> frozenset[int]:
    """Один или несколько числовых Telegram user id: через запятую или пробел."""
    raw = (os.environ.get("ADMIN_ID") or "").strip()
    if not raw:
        return frozenset()
    out: set[int] = set()
    for part in raw.replace(",", " ").split():
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            print(f"ВНИМАНИЕ: пропуск некорректного фрагмента ADMIN_ID: {part!r}")
    return frozenset(out)


ADMIN_IDS = _parse_admin_ids_from_env()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([BOT_TOKEN, ADMIN_IDS, SUPABASE_URL, SUPABASE_KEY]):
    print("ВНИМАНИЕ: Не все переменные окружения заданы (BOT_TOKEN, ADMIN_ID, SUPABASE_*)!")

# Канал для обычного /start (авторизация по deep-link /start SESSION не затрагивается)
REQUIRED_CHANNEL_ID = (os.environ.get("REQUIRED_CHANNEL_ID") or "").strip()
CHANNEL_INVITE_LINK = (os.environ.get("CHANNEL_INVITE_LINK") or "").strip()
# Внешние ссылки для кнопок меню (опционально)
LINK_REVIEWS = (os.environ.get("LINK_REVIEWS") or "").strip()
LINK_BUY = (os.environ.get("LINK_BUY") or "").strip()
# Картинка раздела «Тарифы»: локальный файл (приоритет) или URL по HTTPS
TARIFFS_IMAGE_PATH = (os.environ.get("TARIFFS_IMAGE_PATH") or "").strip()
TARIFFS_IMAGE_URL = (os.environ.get("TARIFFS_IMAGE_URL") or "").strip()
# Картинка главного меню: локальный файл (приоритет) или URL по HTTPS
MAIN_MENU_IMAGE_PATH = (os.environ.get("MAIN_MENU_IMAGE_PATH") or "").strip()
MAIN_MENU_IMAGE_URL = (os.environ.get("MAIN_MENU_IMAGE_URL") or "").strip()
# Картинка профиля (доп. сообщение под полем «Действительна»)
PROFILE_IMAGE_PATH = (os.environ.get("PROFILE_IMAGE_PATH") or "").strip()
PROFILE_IMAGE_URL = (os.environ.get("PROFILE_IMAGE_URL") or "").strip()
# Картинка раздела «Отзывы»
REVIEWS_IMAGE_PATH = (os.environ.get("REVIEWS_IMAGE_PATH") or "").strip()
REVIEWS_IMAGE_URL = (os.environ.get("REVIEWS_IMAGE_URL") or "").strip()
# Картинка раздела «Рефералы»
REFERRALS_IMAGE_PATH = (os.environ.get("REFERRALS_IMAGE_PATH") or "").strip()
REFERRALS_IMAGE_URL = (os.environ.get("REFERRALS_IMAGE_URL") or "").strip()
LINK_SUPPORT = (os.environ.get("LINK_SUPPORT") or "").strip()
# Ссылки на документы перед доступом к боту (после подписки на канал)
LINK_TERMS = (os.environ.get("LINK_TERMS") or "").strip()
LINK_PRIVACY = (os.environ.get("LINK_PRIVACY") or "").strip()
LINK_PRICING = (os.environ.get("LINK_PRICING") or "").strip()
# Чат для логов входа на сайт / в приложение (числовой id: группа или канал; бот должен быть участником)
AUTH_LOG_CHAT_ID = (os.environ.get("AUTH_LOG_CHAT_ID") or "").strip()
# Crypto Pay (@CryptoBot / тест: @CryptoTestnetBot): токен — только CRYPTO_PAY_API_TOKEN в .env, не в коде.
# true = https://testnet-pay.crypt.bot (приложение из @CryptoTestnetBot); false = боевой pay.crypt.bot
CRYPTO_PAY_TESTNET = (os.environ.get("CRYPTO_PAY_TESTNET") or "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
CRYPTO_PAY_ASSET = (os.environ.get("CRYPTO_PAY_ASSET") or "TON").strip().upper()
# Текст «$ за место» в боте; сумма в крипте — CRYPTO_PAY_AMOUNT_TEAM_SEAT × места (см. team_bundle_crypto_amount_str).
TEAM_SEAT_PRICE_USD = float((os.environ.get("TEAM_SEAT_PRICE_USD") or "10").strip() or "10")
REFERRAL_PERCENT_DEFAULT = float((os.environ.get("REFERRAL_PERCENT_DEFAULT") or "10").strip())
REFERRAL_MIN_WITHDRAW_USD = float((os.environ.get("REFERRAL_MIN_WITHDRAW_USD") or "10").strip())
REFERRAL_BURN_INACTIVE_DAYS = int((os.environ.get("REFERRAL_BURN_INACTIVE_DAYS") or "30").strip() or "30")
SUBSCRIPTION_REMINDER_DAYS = (7, 3, 1)
SUBSCRIPTION_REMINDER_INTERVAL_SEC = int(
    (os.environ.get("SUBSCRIPTION_REMINDER_INTERVAL_SEC") or "3600").strip() or "3600"
)
UNIQUEIZER_MAX_COPIES = max(1, min(50, int((os.environ.get("UNIQUEIZER_MAX_COPIES") or "25").strip() or "25")))
UNIQUEIZER_MAX_FILE_MB = max(1, min(50, int((os.environ.get("UNIQUEIZER_MAX_FILE_MB") or "20").strip() or "20")))
# Уникализатор: полный путь к ffmpeg; пусто — поиск в PATH (видео и усиленная обработка фото).
FFMPEG_PATH = (os.environ.get("FFMPEG_PATH") or "").strip()

# Файлы выдачи после успешной оплаты подписки
APP_ZIP_PATH = (os.environ.get("APP_ZIP_PATH") or "app.zip").strip()
APP_TXT_PATH = (os.environ.get("APP_TXT_PATH") or "app.txt").strip()
APP_ZIP_URL = (os.environ.get("APP_ZIP_URL") or "").strip()
APP_TXT_URL = (os.environ.get("APP_TXT_URL") or "").strip()
APP_ZIP_FILE_ID = (os.environ.get("APP_ZIP_FILE_ID") or "").strip()
APP_TXT_FILE_ID = (os.environ.get("APP_TXT_FILE_ID") or "").strip()

_APP_FILES_READY = False
_APP_FILES_LOCK = asyncio.Lock()

# Прокси для десктопа Neuro Uploader: ключи не в клиенте, только на сервере (.env)
SADCAPTCHA_LICENSE_KEY = (os.environ.get("SADCAPTCHA_LICENSE_KEY") or "").strip()
ANTHROPIC_API_KEY = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
NU_CLAUDE_MODEL = (os.environ.get("NU_CLAUDE_MODEL") or "claude-3-5-sonnet-20241022").strip()

# Короткий префикс callback — лимит Telegram 64 байта (a: админ, u: пользователь)
CB = "a"
UCB = "u"
CB_REF_MIN = f"{CB}:rwmin"

PAGE_SIZE = 7


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Стартовая загрузка (вместо deprecated on_event)."""
    reminder_task: Optional[asyncio.Task] = None
    try:
        await ensure_app_files_downloaded()
    except Exception as e:
        print(f"startup: app files download failed: {e}")
    if bot:
        reminder_task = asyncio.create_task(subscription_reminder_worker())
    try:
        yield
    finally:
        if reminder_task:
            reminder_task.cancel()
            try:
                await reminder_task
            except asyncio.CancelledError:
                pass


app = FastAPI(lifespan=app_lifespan)
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


class NuTelegramBody(BaseModel):
    telegram_id: int


class NuSadPuzzle(NuTelegramBody):
    puzzleImageB64: str
    pieceImageB64: str


class NuSadRotate(NuTelegramBody):
    outerImageB64: str
    innerImageB64: str


class NuSadShapes(NuTelegramBody):
    imageB64: str


class NuSadIcon(NuTelegramBody):
    challenge: str
    imageB64: str


class NuAiPreset(NuTelegramBody):
    prompt: str


class AdminStates(StatesGroup):
    """Ожидание ввода от админа"""
    sub_days = State()
    uniqueizer_sub_days = State()
    hwid_value = State()
    referral_percent = State()
    referral_balance_set = State()
    referral_min_withdraw = State()
    tariff_plan_text = State()
    app_zip_wait = State()
    app_txt_wait = State()
    broadcast_wait = State()
    broadcast_confirm = State()


class UserReferralWithdrawStates(StatesGroup):
    amount = State()


class UserTeamStates(StatesGroup):
    """Создание команды: название → число мест → оплата; докупка мест; ввод ID участников."""
    name = State()
    seats_count = State()
    add_seats_count = State()
    add_member_id = State()


class UniqueizerStates(StatesGroup):
    """Уникализатор: шаблон (имя → настройки) и сценарий медиа → копии."""
    tpl_name = State()
    tpl_build = State()
    uniq_media = State()
    uniq_wait_tpl = State()
    uniq_copies = State()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def safe_edit_text(message: Message, text: str, **kwargs) -> None:
    """Редактирует текст или подпись к фото/документу/видео. Игнорирует «message is not modified».
    Если сообщение удалено или недоступно для правки — удаляет (если можно) и шлёт новое текстом."""
    media = bool(
        message.photo
        or message.document
        or message.video
        or message.animation
    )
    try:
        if media:
            await message.edit_caption(caption=text, **kwargs)
        else:
            await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            return
        if "message to edit not found" in err or "message can't be edited" in err:
            chat_id = message.chat.id
            try:
                await message.delete()
            except Exception:
                pass
            await message.bot.send_message(
                chat_id=chat_id,
                text=text,
                **kwargs,
            )
            return
        raise


# Один якорный пост на пользователя: правим caption/media вместо ленты новых сообщений.
# file_id кэшируется после первой загрузки — повторные экраны не перезаливают файл на серверы Telegram.
_USER_SHELL_ANCHOR: dict[int, tuple[int, int]] = {}
_USER_SHELL_MEDIA_FILE_IDS: dict[str, str] = {}


def _user_shell_uid(telegram_user_id: Optional[int], chat_id: int) -> int:
    return int(telegram_user_id) if telegram_user_id is not None else int(chat_id)


def _user_shell_coords(user_id: int, base_msg: Optional[Message]) -> Optional[tuple[int, int]]:
    if base_msg:
        return (base_msg.chat.id, base_msg.message_id)
    return _USER_SHELL_ANCHOR.get(user_id)


async def user_shell_apply_photo(
    bot: Bot,
    user_id: int,
    chat_id: int,
    base_msg: Optional[Message],
    *,
    cache_key: str,
    photo_source: object,
    caption: str,
    parse_mode: Optional[str],
    reply_markup: InlineKeyboardMarkup,
) -> None:
    coords = _user_shell_coords(user_id, base_msg)
    cap = caption.strip() if caption.strip() else "."
    media_val: object = _USER_SHELL_MEDIA_FILE_IDS.get(cache_key) or photo_source
    if coords:
        cid, mid = coords
        try:
            m = await bot.edit_message_media(
                chat_id=cid,
                message_id=mid,
                media=InputMediaPhoto(media=media_val, caption=cap, parse_mode=parse_mode),
                reply_markup=reply_markup,
            )
            if m.photo:
                _USER_SHELL_MEDIA_FILE_IDS[cache_key] = m.photo[-1].file_id
            _USER_SHELL_ANCHOR[user_id] = (cid, mid)
            return
        except TelegramBadRequest as e:
            err = str(e).lower()
            if "message is not modified" in err:
                return
            try:
                await bot.delete_message(chat_id=cid, message_id=mid)
            except Exception:
                pass
            _USER_SHELL_ANCHOR.pop(user_id, None)
    m = await bot.send_photo(
        chat_id=chat_id,
        photo=photo_source,
        caption=caption if caption.strip() else None,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
    )
    _USER_SHELL_ANCHOR[user_id] = (chat_id, m.message_id)
    if m.photo:
        _USER_SHELL_MEDIA_FILE_IDS[cache_key] = m.photo[-1].file_id


async def user_shell_apply_document(
    bot: Bot,
    user_id: int,
    chat_id: int,
    base_msg: Optional[Message],
    *,
    cache_key: str,
    document_source: object,
    caption: str,
    parse_mode: Optional[str],
    reply_markup: InlineKeyboardMarkup,
) -> None:
    coords = _user_shell_coords(user_id, base_msg)
    cap = caption.strip() if caption.strip() else "."
    media_val: object = _USER_SHELL_MEDIA_FILE_IDS.get(cache_key) or document_source
    if coords:
        cid, mid = coords
        try:
            m = await bot.edit_message_media(
                chat_id=cid,
                message_id=mid,
                media=InputMediaDocument(media=media_val, caption=cap, parse_mode=parse_mode),
                reply_markup=reply_markup,
            )
            if m.document:
                _USER_SHELL_MEDIA_FILE_IDS[cache_key] = m.document.file_id
            _USER_SHELL_ANCHOR[user_id] = (cid, mid)
            return
        except TelegramBadRequest as e:
            err = str(e).lower()
            if "message is not modified" in err:
                return
            try:
                await bot.delete_message(chat_id=cid, message_id=mid)
            except Exception:
                pass
            _USER_SHELL_ANCHOR.pop(user_id, None)
    m = await bot.send_document(
        chat_id=chat_id,
        document=document_source,
        caption=caption if caption.strip() else None,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
    )
    _USER_SHELL_ANCHOR[user_id] = (chat_id, m.message_id)
    if m.document:
        _USER_SHELL_MEDIA_FILE_IDS[cache_key] = m.document.file_id


async def user_shell_apply_text(
    bot: Bot,
    user_id: int,
    chat_id: int,
    base_msg: Optional[Message],
    *,
    text: str,
    parse_mode: Optional[str],
    reply_markup: InlineKeyboardMarkup,
) -> None:
    """Текст без медиа. Если якорь был с фото/документом — удаляем и шлём текст."""
    out = text.strip() if text.strip() else "."
    coords = _user_shell_coords(user_id, base_msg)
    if coords:
        cid, mid = coords
        try:
            await bot.edit_message_text(
                chat_id=cid,
                message_id=mid,
                text=out,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
            _USER_SHELL_ANCHOR[user_id] = (cid, mid)
            return
        except TelegramBadRequest as e:
            err = str(e).lower()
            if "message is not modified" in err:
                return
            try:
                await bot.delete_message(chat_id=cid, message_id=mid)
            except Exception:
                pass
            _USER_SHELL_ANCHOR.pop(user_id, None)
    m = await bot.send_message(
        chat_id=chat_id,
        text=out,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
    )
    _USER_SHELL_ANCHOR[user_id] = (chat_id, m.message_id)


async def user_shell_try_edit_caption(
    bot: Bot,
    user_id: int,
    chat_id: int,
    caption: str,
    *,
    parse_mode: Optional[str],
    reply_markup: InlineKeyboardMarkup,
) -> bool:
    t = _USER_SHELL_ANCHOR.get(user_id)
    if not t or t[0] != chat_id:
        return False
    _, mid = t
    cap = caption.strip() if caption.strip() else "."
    try:
        await bot.edit_message_caption(
            chat_id=chat_id,
            message_id=mid,
            caption=cap,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return True
        return False


async def user_shell_answer_or_edit(
    message: Message,
    *,
    text: str,
    parse_mode: Optional[str],
    reply_markup: InlineKeyboardMarkup,
) -> None:
    """Ответ на ввод: правим якорное сообщение; иначе — новое."""
    uid = message.from_user.id
    if await user_shell_try_edit_caption(
        message.bot, uid, message.chat.id, text, parse_mode=parse_mode, reply_markup=reply_markup
    ):
        return
    await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)


@dp.errors()
async def _suppress_benign_telegram_errors(event: ErrorEvent):
    """Не логируем как сбой устаревшие callback (ответ после таймаута Telegram)."""
    exc = event.exception
    if isinstance(exc, TelegramBadRequest):
        msg = str(exc).lower()
        if "query is too old" in msg or "query id is invalid" in msg:
            return True
    return UNHANDLED


def nu_require_active_subscription(telegram_id: int) -> None:
    """Доступ к прокси SadCaptcha / Claude при личной подписке или активном месте в команде TEAM."""
    if not nu_subscription_allowed(telegram_id):
        raise HTTPException(status_code=403, detail="Нет активной подписки")


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


def _subscription_reminder_text(days_left: int, until_ts: int) -> str:
    day_word = "дней"
    if days_left % 10 == 1 and days_left % 100 != 11:
        day_word = "день"
    elif days_left % 10 in (2, 3, 4) and days_left % 100 not in (12, 13, 14):
        day_word = "дня"
    return (
        "⏰ **Напоминание о подписке**\n\n"
        f"До окончания подписки осталось: **{days_left} {day_word}**.\n"
        f"Срок действия до: `{fmt_ts(until_ts)}`\n\n"
        "Продлите подписку заранее, чтобы не потерять доступ."
    )


def reminder_already_sent(telegram_id: int, days_before: int, subscription_until: int) -> bool:
    try:
        r = (
            supabase.table("subscription_reminders")
            .select("telegram_id")
            .eq("telegram_id", telegram_id)
            .eq("days_before", days_before)
            .eq("subscription_until", subscription_until)
            .limit(1)
            .execute()
        )
        return bool(r.data)
    except Exception as e:
        print(f"subscription_reminders read failed for {telegram_id}: {e}")
        return False


def reminder_mark_sent(telegram_id: int, days_before: int, subscription_until: int) -> None:
    supabase.table("subscription_reminders").insert(
        {
            "telegram_id": telegram_id,
            "days_before": days_before,
            "subscription_until": subscription_until,
            "sent_at": int(time.time()),
        }
    ).execute()


async def process_subscription_reminders_once() -> None:
    if not bot:
        return
    now = int(time.time())
    max_window = max(SUBSCRIPTION_REMINDER_DAYS) * 86400
    try:
        res = (
            supabase.table("users")
            .select("telegram_id,subscription_until")
            .gt("subscription_until", now)
            .lte("subscription_until", now + max_window)
            .execute()
        )
    except Exception as e:
        print(f"subscription reminders users query failed: {e}")
        return
    for u in (res.data or []):
        try:
            tid = int(u.get("telegram_id") or 0)
            until = int(u.get("subscription_until") or 0)
        except (TypeError, ValueError):
            continue
        if tid <= 0 or until <= now:
            continue
        remaining = until - now
        for d in SUBSCRIPTION_REMINDER_DAYS:
            upper = d * 86400
            lower = (d - 1) * 86400
            if remaining <= upper and remaining > lower:
                if reminder_already_sent(tid, d, until):
                    break
                try:
                    await bot.send_message(
                        chat_id=tid,
                        text=_subscription_reminder_text(d, until),
                        parse_mode="Markdown",
                    )
                    reminder_mark_sent(tid, d, until)
                except Exception as e:
                    print(f"subscription reminder send failed for {tid} ({d}d): {e}")
                break


async def subscription_reminder_worker() -> None:
    while True:
        try:
            await process_subscription_reminders_once()
        except Exception as e:
            print(f"subscription reminder worker error: {e}")
        await asyncio.sleep(max(60, SUBSCRIPTION_REMINDER_INTERVAL_SEC))


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


def users_telegram_ids_for_broadcast() -> list[int]:
    """ID для массовой рассылки (до BROADCAST_MAX_USERS записей)."""
    lim = int((os.environ.get("BROADCAST_MAX_USERS") or "10000").strip() or "10000")
    lim = max(1, min(lim, 50_000))
    out: list[int] = []
    try:
        res = (
            supabase.table("users")
            .select("telegram_id")
            .order("telegram_id", desc=False)
            .limit(lim)
            .execute()
        )
        for row in res.data or []:
            tid = row.get("telegram_id")
            if tid is None:
                continue
            try:
                out.append(int(tid))
            except (TypeError, ValueError):
                pass
    except Exception as e:
        print(f"users broadcast ids: {e}")
    return out


def user_get(tid: int):
    """Чтение users; при 5xx/сетевых сбоях Supabase — None (не роняем хендлер бота)."""
    try:
        r = supabase.table("users").select("*").eq("telegram_id", tid).execute()
        return r.data[0] if r.data else None
    except PostgrestAPIError as e:
        print(f"user_get({tid}) PostgREST: {e}")
        return None
    except Exception as e:
        print(f"user_get({tid}) {type(e).__name__}: {e}")
        return None


def user_has_active_team_plan(telegram_id: int) -> bool:
    """Активная подписка с планом TEAM (код t в users.subscription_plan)."""
    u = user_get(telegram_id)
    if not u:
        return False
    if (u.get("subscription_plan") or "").strip().lower() != "t":
        return False
    return int(u.get("subscription_until") or 0) >= int(time.time())


def user_has_active_uniqueizer_plan(telegram_id: int) -> bool:
    """Доступ к Уникализатору: uniqueizer_until или legacy (plan u + subscription_until)."""
    u = user_get(telegram_id)
    if not u:
        return False
    now = int(time.time())
    if _uniqueizer_until_ts(u) >= now:
        return True
    if (u.get("subscription_plan") or "").strip().lower() == "u":
        return int(u.get("subscription_until") or 0) >= now
    return False


# --- Уникализатор: шаблоны и обработка медиа ---------------------------------

UNIQUEIZER_OPTION_DEFS: tuple[tuple[str, str], ...] = (
    ("flip_h", "Отразить горизонтально"),
    ("flip_v", "Отразить вертикально"),
    ("rot_small", "Лёгкий поворот"),
    ("brightness", "Яркость"),
    ("contrast", "Контраст"),
    ("saturation", "Насыщенность"),
    ("sharpen", "Резкость"),
    ("resize_pct", "Масштаб ±%"),
    ("noise", "Лёгкий шум"),
    ("jpeg_q", "JPEG-качество"),
)
UNIQUEIZER_OPTION_KEYS = frozenset(k for k, _ in UNIQUEIZER_OPTION_DEFS)
UNIQUEIZER_OPT_LABEL = dict(UNIQUEIZER_OPTION_DEFS)


def _user_uniqueizer_selected_template_id(u: Optional[dict]) -> Optional[str]:
    if not u:
        return None
    raw = u.get("uniqueizer_selected_template_id")
    if raw is None or raw == "":
        return None
    return str(raw).strip()


def user_set_uniqueizer_selected_template(telegram_id: int, template_id: Optional[str]) -> None:
    patch: dict = {"uniqueizer_selected_template_id": template_id}
    if template_id is None:
        patch["uniqueizer_selected_template_id"] = None
    try:
        u = user_get(telegram_id)
        if u:
            supabase.table("users").update(patch).eq("telegram_id", telegram_id).execute()
        else:
            supabase.table("users").insert(
                {"telegram_id": telegram_id, "subscription_until": 0, "hwid": None, **patch}
            ).execute()
    except Exception as e:
        print(f"user_set_uniqueizer_selected_template {telegram_id}: {e}")


def uniqueizer_templates_list(telegram_id: int) -> list[dict]:
    try:
        r = (
            supabase.table("uniqueizer_templates")
            .select("*")
            .eq("telegram_id", telegram_id)
            .order("created_at", desc=True)
            .execute()
        )
        return list(r.data or [])
    except Exception as e:
        print(f"uniqueizer_templates_list {telegram_id}: {e}")
        return []


def uniqueizer_template_get_row(template_id: str) -> Optional[dict]:
    try:
        r = (
            supabase.table("uniqueizer_templates")
            .select("*")
            .eq("id", str(template_id).strip())
            .limit(1)
            .execute()
        )
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"uniqueizer_template_get_row {template_id}: {e}")
        return None


def uniqueizer_template_insert_row(telegram_id: int, name: str, options: list[str]) -> Optional[str]:
    nm = (name or "").strip()[:120] or "Шаблон"
    opts = [x for x in options if x in UNIQUEIZER_OPTION_KEYS]
    now = int(time.time())
    try:
        r = (
            supabase.table("uniqueizer_templates")
            .insert(
                {
                    "telegram_id": telegram_id,
                    "name": nm,
                    "options": opts,
                    "created_at": now,
                }
            )
            .execute()
        )
        if r.data:
            return str(r.data[0].get("id") or "")
    except Exception as e:
        print(f"uniqueizer_template_insert_row: {e}")
    return None


def _template_options_from_row(row: dict) -> list[str]:
    raw = row.get("options")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if str(x) in UNIQUEIZER_OPTION_KEYS]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if str(x) in UNIQUEIZER_OPTION_KEYS]
        except json.JSONDecodeError:
            pass
    return []


def kb_uniqueizer_hub() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📦 Уникализация", callback_data=f"{UCB}:uzrun")],
            [InlineKeyboardButton(text="📋 Шаблоны", callback_data=f"{UCB}:uztpl")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def kb_uniqueizer_cancel_hub() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{UCB}:uzhm")],
        ]
    )


def kb_uniqueizer_tpl_empty() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать шаблон", callback_data=f"{UCB}:uznew")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{UCB}:uzhm")],
        ]
    )


def kb_uniqueizer_tpl_list(uid: int, rows: list[dict], selected_id: Optional[str]) -> InlineKeyboardMarkup:
    lines: list[list[InlineKeyboardButton]] = []
    for row in rows:
        tid = str(row.get("id") or "")
        if not tid:
            continue
        mark = "✓ " if selected_id == tid else ""
        nm = (row.get("name") or "Шаблон")[:28]
        lines.append(
            [
                InlineKeyboardButton(
                    text=f"{mark}{nm}",
                    callback_data=f"{UCB}:uzsel:{tid}",
                )
            ]
        )
    lines.append([InlineKeyboardButton(text="➕ Создать шаблон", callback_data=f"{UCB}:uznew")])
    lines.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{UCB}:uzhm")])
    return InlineKeyboardMarkup(inline_keyboard=lines)


def kb_uniqueizer_tpl_build(selected: set[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row_buf: list[InlineKeyboardButton] = []
    for key, label in UNIQUEIZER_OPTION_DEFS:
        check = "✓ " if key in selected else ""
        row_buf.append(
            InlineKeyboardButton(
                text=f"{check}{label[:18]}",
                callback_data=f"{UCB}:uztgl:{key}",
            )
        )
        if len(row_buf) >= 2:
            rows.append(row_buf)
            row_buf = []
    if row_buf:
        rows.append(row_buf)
    rows.append(
        [InlineKeyboardButton(text="✅ Выбрать", callback_data=f"{UCB}:uzfin")],
    )
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"{UCB}:uzhm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_uniqueizer_copies_pick() -> InlineKeyboardMarkup:
    presets = [1, 3, 5, 10, 15, 20]
    row = [
        InlineKeyboardButton(text=str(n), callback_data=f"{UCB}:uzcp:{n}")
        for n in presets
        if n <= UNIQUEIZER_MAX_COPIES
    ]
    rows = [row[i : i + 3] for i in range(0, len(row), 3)]
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"{UCB}:uzhm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _ffmpeg_bin() -> Optional[str]:
    """Порядок: FFMPEG_PATH → ffmpeg в PATH → бинарник из пакета imageio-ffmpeg (pip)."""
    if FFMPEG_PATH:
        p = FFMPEG_PATH
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
        if os.path.isfile(p):
            return p
    w = shutil.which("ffmpeg")
    if w:
        return w
    if imageio_ffmpeg is not None:
        try:
            bundled = imageio_ffmpeg.get_ffmpeg_exe()
            if bundled and os.path.isfile(bundled):
                return bundled
        except Exception as e:
            print(f"imageio_ffmpeg.get_ffmpeg_exe: {e}")
    return None


def _ffmpeg_vf_for_uniqueize_image(opts: set[str], variant: int, rng: random.Random) -> str:
    """Цепочка фильтров libavfilter для одного кадра (фото)."""
    use = opts if opts else {"jpeg_q", "brightness"}
    parts: list[str] = []
    if "flip_h" in use and (variant % 2) == 0:
        parts.append("hflip")
    if "flip_v" in use and (variant % 2) == 1:
        parts.append("vflip")
    br, ctr, sat = 0.0, 1.0, 1.0
    if "brightness" in use:
        br += rng.uniform(-0.14, 0.14) + (variant % 7 - 3) * 0.018
    if "contrast" in use:
        ctr += rng.uniform(-0.18, 0.22) + (variant % 5) * 0.012
    if "saturation" in use:
        sat += rng.uniform(-0.32, 0.35) + (variant % 4) * 0.025
    if abs(br) > 1e-6 or abs(ctr - 1.0) > 1e-6 or abs(sat - 1.0) > 1e-6:
        parts.append(f"eq=brightness={br:.5f}:contrast={ctr:.5f}:saturation={sat:.5f}")
    if "saturation" in use:
        hue = ((variant * 23) % 90 - 45) + rng.uniform(-8.0, 8.0)
        parts.append(f"hue=h={hue}*PI/180")
    if "sharpen" in use:
        parts.append(
            f"unsharp=5:5:{0.45 + variant * 0.04:.2f}:5:5:{0.15 + (variant % 4) * 0.12:.2f}"
        )
    if "noise" in use:
        n = 12 + (variant % 18) * 3
        parts.append(f"noise=alls={n}:allf=t+u")
    if "resize_pct" in use:
        sc = 100 + (variant % 11 - 5)
        parts.append(f"scale=iw*{sc}/100:-2:flags=lanczos")
    if "rot_small" in use:
        parts.append(f"gblur=sigma={0.12 + (variant % 6) * 0.07 + rng.random() * 0.12:.3f}")
        vang = 0.45 + (variant % 7) * 0.09
        parts.append(f"vignette={vang:.4f}")
    if not parts:
        parts.append(
            f"eq=brightness={(variant % 5 - 2) * 0.025:.5f}:contrast={1.02 + (variant % 5) * 0.015:.5f}"
        )
    return ",".join(parts)


def _ffmpeg_run_image(
    ffmpeg_exe: str,
    in_path: str,
    out_jpg: str,
    vf: str,
    qv: int,
) -> None:
    cmd = [
        ffmpeg_exe,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        in_path,
        "-vf",
        vf,
        "-frames:v",
        "1",
        "-q:v",
        str(max(2, min(31, int(qv)))),
        out_jpg,
    ]
    subprocess.run(cmd, check=True, capture_output=True, timeout=120)


_UNIQUEIZER_VIDEO_EXTS = frozenset(
    {".mp4", ".mov", ".webm", ".mkv", ".avi", ".m4v", ".3gp", ".3g2"}
)


def _uniqueizer_path_looks_like_video(media_path: str) -> bool:
    return os.path.splitext(media_path)[1].lower() in _UNIQUEIZER_VIDEO_EXTS


def _ffmpeg_transcode_video_copy(
    ffmpeg_exe: str,
    media_path: str,
    out_path: str,
    variant_index: int,
) -> tuple[bool, str]:
    """
    Перекодирование одной копии видео (фильтр hue). Сначала libx264, иначе mpeg4 —
    урезанные сборки ffmpeg часто без x264.
    Возвращает (успех, текст_ошибки).
    """
    hue = ((variant_index * 17) % 50) - 25
    sat = 0.9 + (variant_index % 5) * 0.04
    vf = f"hue=h={hue}*PI/180:s={sat}"
    attempts: list[list[str]] = [
        [
            ffmpeg_exe,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            media_path,
            "-vf",
            vf,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            str(20 + (variant_index % 6)),
            "-pix_fmt",
            "yuv420p",
            "-an",
            "-movflags",
            "+faststart",
            out_path,
        ],
        [
            ffmpeg_exe,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            media_path,
            "-vf",
            vf,
            "-c:v",
            "mpeg4",
            "-q:v",
            str(3 + (variant_index % 8)),
            "-an",
            out_path,
        ],
    ]
    last_err = ""
    for cmd in attempts:
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=300)
            err_txt = (r.stderr or b"").decode(errors="replace").strip()
            if (
                r.returncode == 0
                and os.path.isfile(out_path)
                and os.path.getsize(out_path) > 0
            ):
                return True, ""
            last_err = err_txt or f"код выхода {r.returncode}"
            if os.path.isfile(out_path):
                try:
                    os.remove(out_path)
                except OSError:
                    pass
        except subprocess.TimeoutExpired:
            last_err = "превышено время обработки (5 мин)"
        except Exception as e:
            last_msg = str(e)
            last_err = last_msg
    if last_err:
        print(f"uniqueizer ffmpeg video: {last_err[:2000]}")
    return False, (last_err or "неизвестная ошибка ffmpeg")[:900]


def _apply_uniqueizer_pil(im: Image.Image, options: list[str], variant: int) -> Image.Image:
    rng = random.Random(variant * 13001 + (hash(tuple(sorted(options))) % 999983))
    img = im.convert("RGB")
    opts = set(options) if options else {"jpeg_q", "brightness"}
    if "flip_h" in opts and (variant % 3) != 0:
        img = ImageOps.mirror(img)
    if "flip_v" in opts and (variant % 3) == 1:
        img = ImageOps.flip(img)
    if "rot_small" in opts:
        ang = rng.uniform(-2.1, 2.1) + (variant % 7) * 0.15
        img = img.rotate(ang, resample=Image.BICUBIC, expand=False, fillcolor=(255, 255, 255))
    if "brightness" in opts:
        f = 1.0 + rng.uniform(-0.07, 0.07) + (variant % 5) * 0.003
        img = ImageEnhance.Brightness(img).enhance(f)
    if "contrast" in opts:
        f = 1.0 + rng.uniform(-0.09, 0.09)
        img = ImageEnhance.Contrast(img).enhance(f)
    if "saturation" in opts:
        f = 1.0 + rng.uniform(-0.14, 0.14)
        img = ImageEnhance.Color(img).enhance(f)
    if "sharpen" in opts:
        f = 1.0 + rng.uniform(0.05, 0.35)
        img = ImageEnhance.Sharpness(img).enhance(f)
    if "resize_pct" in opts:
        w, h = img.size
        sc = 1.0 + (variant % 5 - 2) * 0.0045 + rng.uniform(-0.002, 0.002)
        nw, nh = max(2, int(w * sc)), max(2, int(h * sc))
        img = img.resize((nw, nh), Image.LANCZOS)
        img = ImageOps.fit(img, (w, h), Image.LANCZOS)
    if "noise" in opts:
        px = img.load()
        mw, mh = img.size
        n_pix = max(80, mw * mh // 6000)
        for _ in range(n_pix):
            x, y = rng.randint(0, mw - 1), rng.randint(0, mh - 1)
            r, g, b = px[x, y]
            d = rng.randint(-5, 5)
            px[x, y] = (
                max(0, min(255, r + d)),
                max(0, min(255, g + d)),
                max(0, min(255, b + d)),
            )
    if "jpeg_q" in opts:
        q = 68 + (variant * 5) % 24
        bio = io.BytesIO()
        img.save(bio, format="JPEG", quality=q, optimize=True)
        bio.seek(0)
        img = Image.open(bio).convert("RGB")
    return img


def _uniqueizer_process_to_zip(
    media_path: str,
    *,
    is_video: bool,
    options: list[str],
    copies: int,
) -> tuple[Optional[str], str]:
    """Возвращает (путь к zip или None, сообщение об ошибке пользователю)."""
    copies = max(1, min(UNIQUEIZER_MAX_COPIES, int(copies)))
    work = tempfile.mkdtemp(prefix="uz_")
    try:
        if _uniqueizer_path_looks_like_video(media_path):
            is_video = True
        ffmpeg_exe = _ffmpeg_bin()
        io_ffmpeg_ok = imageio_ffmpeg is not None
        print(
            f"uniqueizer: is_video={is_video} ffmpeg_bin={ffmpeg_exe!r} "
            f"imageio_ffmpeg_imported={io_ffmpeg_ok} file={os.path.basename(media_path)!r}"
        )
        if not is_video and not ffmpeg_exe and not HAS_PIL:
            shutil.rmtree(work, ignore_errors=True)
            return None, "Для фото нужен ffmpeg (рекомендуется) или Pillow на сервере."
        if is_video and not ffmpeg_exe:
            shutil.rmtree(work, ignore_errors=True)
            return None, (
                "Видео не обработано: не найден ffmpeg.\n\n"
                "На сервере с ботом выполните: pip install -r requirements.txt "
                "(нужен пакет imageio-ffmpeg) или задайте FFMPEG_PATH / ffmpeg в PATH."
            )

        zip_path = os.path.join(work, "unique_pack.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            ext = os.path.splitext(media_path)[1].lower() or (".mp4" if is_video else ".jpg")
            for i in range(copies):
                if is_video:
                    out_v = os.path.join(work, f"u_{i+1:03d}{ext}")
                    ok_v, err_v = _ffmpeg_transcode_video_copy(
                        ffmpeg_exe, media_path, out_v, i
                    )
                    if not ok_v:
                        return None, (
                            "Видео: ffmpeg не смог перекодировать. "
                            "Проверьте, что в окружении есть рабочий ffmpeg "
                            "(pip: imageio-ffmpeg) или см. лог сервера. "
                            f"Детали: {err_v}"
                        )
                    zf.write(out_v, arcname=f"unique_{i+1:03d}{ext}")
                else:
                    out_p = os.path.join(work, f"u_{i+1:03d}.jpg")
                    opts_set = set(options) if options else set()
                    ok_ffmpeg = False
                    if ffmpeg_exe:
                        rng = random.Random(
                            i * 13001 + (hash(tuple(sorted(options))) % 999983)
                        )
                        vf = _ffmpeg_vf_for_uniqueize_image(opts_set, i, rng)
                        if "jpeg_q" in opts_set:
                            qv = 3 + (i * 4) % 14
                        else:
                            qv = 3 + (i * 2) % 6
                        try:
                            _ffmpeg_run_image(ffmpeg_exe, media_path, out_p, vf, qv)
                            ok_ffmpeg = os.path.isfile(out_p) and os.path.getsize(out_p) > 0
                        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
                            ok_ffmpeg = False
                    if not ok_ffmpeg:
                        if not HAS_PIL:
                            return None, (
                                "ffmpeg не смог обработать фото, а Pillow не установлен."
                                if ffmpeg_exe
                                else "Нет ffmpeg и Pillow — обработка фото недоступна."
                            )
                        try:
                            im = Image.open(media_path)
                            out_img = _apply_uniqueizer_pil(im, options, i)
                            out_img.save(out_p, format="JPEG", quality=88, optimize=True)
                        except Exception as e:
                            return None, f"Ошибка обработки изображения: {e}"
                    zf.write(out_p, arcname=f"unique_{i+1:03d}.jpg")
        final_zip = work + ".zip"
        shutil.move(zip_path, final_zip)
        shutil.rmtree(work, ignore_errors=True)
        return final_zip, ""
    except Exception as e:
        shutil.rmtree(work, ignore_errors=True)
        return None, f"Сборка архива: {e}"


async def _download_tg_file(bot: Bot, file_id: str, suffix: str) -> tuple[Optional[str], str]:
    try:
        tg_file = await bot.get_file(file_id)
        if not tg_file.file_path:
            return None, "Файл недоступен."
        fd, path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        await bot.download_file(tg_file.file_path, path)
        return path, ""
    except Exception as e:
        return None, str(e)


def team_get_by_owner(owner_telegram_id: int) -> Optional[dict]:
    try:
        r = (
            supabase.table("teams")
            .select("*")
            .eq("owner_telegram_id", owner_telegram_id)
            .limit(1)
            .execute()
        )
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"teams select owner={owner_telegram_id}: {e}")
        return None


def team_create_for_owner(owner_telegram_id: int, name: str) -> Optional[str]:
    """Создаёт команду, возвращает id (uuid str) или None."""
    nm = (name or "").strip()
    if len(nm) < 2:
        return None
    nm = nm[:200]
    now = int(time.time())
    try:
        supabase.table("teams").insert(
            {
                "owner_telegram_id": owner_telegram_id,
                "name": nm,
                "seats_purchased": 0,
                "created_at": now,
            }
        ).execute()
    except Exception as e:
        err = str(e).lower()
        if "23505" in str(e) or "duplicate" in err or "unique" in err:
            t = team_get_by_owner(owner_telegram_id)
            return str(t["id"]) if t else None
        print(f"teams insert failed: {e}")
        return None
    t = team_get_by_owner(owner_telegram_id)
    return str(t["id"]) if t else None


def team_add_seats_paid(team_id: str, owner_telegram_id: int, seats: int) -> bool:
    if seats < 1 or seats > 100:
        return False
    t = team_get_by_owner(owner_telegram_id)
    if not t or str(t.get("id")) != str(team_id):
        return False
    new_total = int(t.get("seats_purchased") or 0) + seats
    try:
        supabase.table("teams").update({"seats_purchased": new_total}).eq("id", team_id).eq(
            "owner_telegram_id", owner_telegram_id
        ).execute()
        return True
    except Exception as e:
        print(f"teams update seats failed: {e}")
        return False


def team_members_list(team_id: str) -> list[dict]:
    try:
        r = (
            supabase.table("team_members")
            .select("member_telegram_id, created_at")
            .eq("team_id", team_id)
            .order("created_at", desc=False)
            .execute()
        )
        return list(r.data or [])
    except Exception as e:
        print(f"team_members list team_id={team_id}: {e}")
        return []


def team_members_count(team_id: str) -> int:
    return len(team_members_list(team_id))


def team_member_display_line(member_telegram_id: int) -> str:
    u = user_get(member_telegram_id)
    un = ""
    if u:
        un = (u.get("username") or "").strip()
    if un:
        if not un.startswith("@"):
            un = "@" + un
        return f"• <code>{member_telegram_id}</code> — {esc_html(un)}"
    return f"• <code>{member_telegram_id}</code>"


def format_team_dashboard_html(team: dict, owner_telegram_id: int) -> str:
    seats = int(team.get("seats_purchased") or 0)
    tid = str(team["id"])
    members = team_members_list(tid)
    n_mem = len(members)
    name = esc_html(str(team.get("name") or "—"))
    lines = [
        "👥 <b>Команда</b>",
        "",
        f"Название: <b>{name}</b>",
        f"Оплачено мест: <b>{seats}</b>",
        f"Участников в списке: <b>{n_mem}</b> из <b>{seats}</b>",
        "",
    ]
    if members:
        lines.append("<b>Участники:</b>")
        for row in members:
            try:
                mid = int(row.get("member_telegram_id") or 0)
            except (TypeError, ValueError):
                continue
            if mid > 0:
                lines.append(team_member_display_line(mid))
    else:
        lines.append("<i>Список пуст. Добавьте участников по их Telegram ID.</i>")
    return "\n".join(lines)


def team_try_add_member(
    team_id: str,
    owner_telegram_id: int,
    member_telegram_id: int,
    *,
    occupied_slots: int,
    seats_total: int,
) -> tuple[bool, str]:
    """occupied_slots — сколько участников уже в команде; seats_total из teams.seats_purchased."""
    if member_telegram_id <= 0:
        return False, "bad_id"
    if member_telegram_id == owner_telegram_id:
        return False, "owner"
    t = team_get_by_owner(owner_telegram_id)
    if not t or str(t.get("id")) != str(team_id):
        return False, "not_owner"
    if occupied_slots >= seats_total:
        return False, "full"
    now = int(time.time())
    try:
        supabase.table("team_members").insert(
            {
                "team_id": team_id,
                "member_telegram_id": member_telegram_id,
                "created_at": now,
            }
        ).execute()
        return True, ""
    except Exception as e:
        err = str(e).lower()
        if "23505" in str(e) or "duplicate" in err or "unique" in err:
            return False, "dup"
        print(f"team_members insert failed: {e}")
        return False, "db"


def _subscription_until_ts(user_row: dict) -> int:
    """Безопасно привести users.subscription_until к unix-времени (Supabase может отдать int/str)."""
    raw = user_row.get("subscription_until")
    if raw is None:
        return 0
    if isinstance(raw, bool):
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return 0
        try:
            return int(s)
        except ValueError:
            pass
        try:
            from datetime import datetime, timezone

            iso = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0
    return 0


def _uniqueizer_until_ts(user_row: dict) -> int:
    """Срок доступа к Уникализатору в боте (колонка users.uniqueizer_until)."""
    raw = user_row.get("uniqueizer_until")
    if raw is None:
        return 0
    if isinstance(raw, bool):
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return 0
        try:
            return int(s)
        except ValueError:
            return 0
    return 0


def user_row_has_active_app_subscription(u_row: dict) -> bool:
    """Подписка на приложение: срок основной подписки и план не UNIQUEIZER (legacy u не даёт вход)."""
    if _subscription_until_ts(u_row) < int(time.time()):
        return False
    if (u_row.get("subscription_plan") or "").strip().lower() == "u":
        return False
    return True


def user_has_active_app_subscription(telegram_id: int) -> bool:
    u = user_get(telegram_id)
    return bool(u and user_row_has_active_app_subscription(u))


def team_member_has_active_team_access(member_telegram_id: int) -> bool:
    """Участник в team_members и у владельца команды активна подписка TEAM (plan t, срок не истёк)."""
    try:
        r = (
            supabase.table("team_members")
            .select("team_id")
            .eq("member_telegram_id", member_telegram_id)
            .execute()
        )
        for row in r.data or []:
            tid = row.get("team_id")
            if not tid:
                continue
            tr = (
                supabase.table("teams")
                .select("owner_telegram_id")
                .eq("id", str(tid))
                .limit(1)
                .execute()
            )
            if not tr.data:
                continue
            try:
                owner = int(tr.data[0].get("owner_telegram_id") or 0)
            except (TypeError, ValueError):
                continue
            if owner > 0 and user_has_active_team_plan(owner):
                return True
    except Exception as e:
        print(f"team_member_has_active_team_access({member_telegram_id}): {e}")
    return False


def ensure_user_row_for_login(telegram_id: int, username: str) -> Optional[dict]:
    """Минимальная строка users для HWID; для участника команды без своей подписки."""
    u = user_get(telegram_id)
    if u:
        return u
    raw = (username or "").strip()
    if raw.startswith("@"):
        uname_for_fmt = raw[1:] or None
    elif raw.isdigit():
        uname_for_fmt = None
    else:
        uname_for_fmt = raw or None
    un = format_telegram_username_for_db(telegram_id, uname_for_fmt)
    try:
        supabase.table("users").insert(
            {"telegram_id": telegram_id, "username": un, "subscription_until": 0, "hwid": None}
        ).execute()
    except Exception as e:
        err = str(e).lower()
        if "23505" not in str(e) and "duplicate" not in err and "unique" not in err:
            print(f"ensure_user_row_for_login insert {telegram_id}: {e}")
    return user_get(telegram_id)


def nu_subscription_allowed(telegram_id: int) -> bool:
    """Приложение: активная подписка s/p/m/t (не UNIQUEIZER) или место в команде TEAM."""
    if user_has_active_app_subscription(telegram_id):
        return True
    return team_member_has_active_team_access(telegram_id)


def team_bundle_crypto_amount_str(seats: int) -> str:
    per = (os.environ.get("CRYPTO_PAY_AMOUNT_TEAM_SEAT") or "10").strip()
    try:
        p = float(per)
    except (TypeError, ValueError):
        p = 10.0
    total = max(0.0, p * float(seats))
    s = f"{total:.8f}".rstrip("0").rstrip(".")
    return s if s else "0"


def parse_team_bundle_payload(raw: Optional[str]) -> Optional[tuple[int, str, int]]:
    """payload: nu_kind=team_bundle;tg=...;team=<uuid>;seats=N → (tg, team_id, seats)."""
    if not raw or not isinstance(raw, str):
        return None
    kind: Optional[str] = None
    tg: Optional[int] = None
    team_id: Optional[str] = None
    seats: Optional[int] = None
    for part in raw.split(";"):
        part = part.strip()
        if part.startswith("nu_kind="):
            kind = part[8:].strip().lower()
        elif part.startswith("tg="):
            try:
                tg = int(part[3:].strip())
            except ValueError:
                pass
        elif part.startswith("team="):
            team_id = part[5:].strip()
        elif part.startswith("seats="):
            try:
                seats = int(part[6:].strip())
            except ValueError:
                pass
    if kind != "team_bundle" or tg is None or not team_id or seats is None:
        return None
    if seats < 1 or seats > 100:
        return None
    try:
        uuid.UUID(team_id)
    except (ValueError, TypeError):
        return None
    return tg, team_id, seats


def format_telegram_username_for_db(telegram_id: int, username: Optional[str]) -> str:
    """Как в complete_web_site_login_sync: @name или числовой id."""
    return f"@{username}" if username else str(telegram_id)


def ensure_user_row_from_bot(telegram_id: int, username: Optional[str] = None) -> None:
    """Строка в users при первом показе главного меню (после канала и принятия политик)."""
    un = format_telegram_username_for_db(telegram_id, username)
    u = user_get(telegram_id)
    if u:
        if (u.get("username") or "") != un:
            try:
                supabase.table("users").update({"username": un}).eq("telegram_id", telegram_id).execute()
            except Exception as e:
                print(f"ensure_user_row_from_bot username update: {e}")
        return
    try:
        supabase.table("users").insert(
            {"telegram_id": telegram_id, "subscription_until": 0, "hwid": None, "username": un}
        ).execute()
    except Exception as e:
        print(f"ensure_user_row_from_bot insert failed for {telegram_id}: {e!s}")


def get_user_referral_balance_adjustment_usd(tid: int) -> float:
    """Админская корректировка к формуле «доступно к выводу» (начисления − списания + корр.)."""
    u = user_get(tid)
    if not u:
        return 0.0
    raw = u.get("referral_balance_adjustment_usd")
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def set_user_referral_balance_adjustment_usd(tid: int, adjustment: float) -> None:
    adjustment = max(-1e9, min(1e9, float(adjustment)))
    u = user_get(tid)
    if not u:
        raise ValueError("Пользователь не найден в users")
    supabase.table("users").update({"referral_balance_adjustment_usd": adjustment}).eq(
        "telegram_id", tid
    ).execute()


def verify_crypto_pay_webhook_signature(body_text: str, signature_header: str, api_token: str) -> bool:
    """HMAC-SHA256(hex), секрет = SHA256(api_token) — как в официальных SDK Crypto Pay."""
    if not signature_header or not api_token:
        return False
    secret = sha256(api_token.encode("utf-8")).digest()
    expected = HMAC(secret, body_text.encode("utf-8"), sha256).hexdigest()
    return compare_digest(expected, signature_header.strip())


def parse_nu_crypto_invoice_payload(raw: Optional[str]) -> tuple[Optional[str], Optional[int], bool]:
    """Поле payload счёта: nu_plan=s;tg=...;renew=0|1. Третий элемент — тариф продления."""
    if not raw or not isinstance(raw, str):
        return None, None, False
    plan, tg = None, None
    renew = False
    for part in raw.split(";"):
        part = part.strip()
        if part.startswith("nu_plan="):
            plan = part[8:].strip().lower()
        elif part.startswith("tg="):
            try:
                tg = int(part[3:].strip())
            except ValueError:
                pass
        elif part.startswith("renew="):
            renew = part[6:].strip() == "1"
    return plan, tg, renew


def crypto_invoice_mark_processed(invoice_id: int, telegram_id: int, plan_code: str) -> str:
    """
    new — запись создана, начисляем подписку.
    duplicate — invoice_id уже был (ретрай вебхука).
    error — ошибка БД (ответьте 5xx, Crypto Pay повторит).
    """
    try:
        supabase.table("crypto_pay_processed_invoices").insert(
            {
                "invoice_id": invoice_id,
                "telegram_id": telegram_id,
                "plan_code": plan_code,
                "processed_at": int(time.time()),
            }
        ).execute()
        return "new"
    except Exception as e:
        err = str(e).lower()
        if (
            "23505" in str(e)
            or "duplicate" in err
            or "unique" in err
            or "violates unique constraint" in err
        ):
            return "duplicate"
        print(f"crypto_pay_processed_invoices insert failed: {e}")
        return "error"


def extend_user_subscription_days(
    telegram_id: int, days: int, plan_code: Optional[str] = None
) -> int:
    """Продление от max(now, текущий subscription_until). plan_code — записать в users.subscription_plan (оплата Crypto Pay)."""
    now = int(time.time())
    u = user_get(telegram_id)
    base = now
    if u:
        cur = int(u.get("subscription_until") or 0)
        if cur > base:
            base = cur
    until = base + max(1, int(days)) * 86400
    patch: dict = {"subscription_until": until}
    if plan_code:
        patch["subscription_plan"] = plan_code
    if u:
        supabase.table("users").update(patch).eq("telegram_id", telegram_id).execute()
    else:
        row = {"telegram_id": telegram_id, "subscription_until": until, "hwid": None}
        if plan_code:
            row["subscription_plan"] = plan_code
        supabase.table("users").insert(row).execute()
    return until


def extend_user_uniqueizer_days(telegram_id: int, days: int) -> int:
    """Продление только uniqueizer_until; не трогает подписку приложения. Снимает legacy subscription_plan=u."""
    now = int(time.time())
    u = user_get(telegram_id)
    base = now
    if u:
        cur = _uniqueizer_until_ts(u)
        if cur > base:
            base = cur
    until = base + max(1, int(days)) * 86400
    patch: dict = {"uniqueizer_until": until}
    if u and (u.get("subscription_plan") or "").strip().lower() == "u":
        patch["subscription_plan"] = None
    if u:
        supabase.table("users").update(patch).eq("telegram_id", telegram_id).execute()
    else:
        supabase.table("users").insert(
            {
                "telegram_id": telegram_id,
                "subscription_until": 0,
                "uniqueizer_until": until,
                "hwid": None,
            }
        ).execute()
    return until


def user_eligible_for_renewal_price(telegram_id: int, purchase_plan_code: str) -> bool:
    """Цена продления только если срок истёк и покупают тот же тариф, что был при последней Crypto Pay оплате."""
    u = user_get(telegram_id)
    if not u:
        return False
    sub = int(u.get("subscription_until") or 0)
    if sub <= 0:
        return False
    if sub >= int(time.time()):
        return False
    stored = (u.get("subscription_plan") or "").strip().lower()
    if not stored:
        return False
    return stored == (purchase_plan_code or "").strip().lower()


def user_eligible_for_uniqueizer_renewal_price(telegram_id: int) -> bool:
    """Цена продления UNIQUEIZER: раньше уже был оплаченный период (колонка или legacy plan u), срок истёк."""
    u = user_get(telegram_id)
    if not u:
        return False
    now = int(time.time())
    uz = _uniqueizer_until_ts(u)
    if uz > 0 and uz < now:
        return True
    plan = (u.get("subscription_plan") or "").strip().lower()
    legacy_sub = int(u.get("subscription_until") or 0)
    if plan == "u" and legacy_sub > 0 and legacy_sub < now:
        return True
    return False


def get_referral_percent() -> float:
    try:
        r = supabase.table("app_settings").select("value").eq("key", "referral_percent").execute()
        if r.data:
            v = float(r.data[0]["value"])
            return max(0.0, min(100.0, v))
    except Exception as e:
        print(f"app_settings referral_percent: {e}")
    return max(0.0, min(100.0, REFERRAL_PERCENT_DEFAULT))


def set_referral_percent(pct: float) -> None:
    pct = max(0.0, min(100.0, float(pct)))
    supabase.table("app_settings").upsert(
        {"key": "referral_percent", "value": str(pct)}
    ).execute()


def get_referral_min_withdraw_usd() -> float:
    """Минимальная сумма вывода реферального баланса (USD-эквивалент)."""
    try:
        r = (
            supabase.table("app_settings")
            .select("value")
            .eq("key", "referral_min_withdraw_usd")
            .execute()
        )
        if r.data:
            v = float(r.data[0]["value"])
            return max(0.01, min(1_000_000.0, v))
    except Exception as e:
        print(f"app_settings referral_min_withdraw_usd: {e}")
    return max(0.01, min(1_000_000.0, float(REFERRAL_MIN_WITHDRAW_USD)))


def set_referral_min_withdraw_usd(amt: float) -> None:
    amt = max(0.01, min(1_000_000.0, float(amt)))
    supabase.table("app_settings").upsert(
        {"key": "referral_min_withdraw_usd", "value": str(amt)}
    ).execute()


def referral_admin_settings_markdown() -> str:
    pct = get_referral_percent()
    m = get_referral_min_withdraw_usd()
    return (
        "🎁 **Реферальная программа**\n\n"
        f"• Процент: **{pct:g}%** (с каждой оплаты приглашённого)\n"
        f"• Минимум вывода реф. баланса: **{m:g}** USD\n\n"
        "Выберите пресеты или «Свой %» / «Свой мин.»."
    )


def get_user_referral_percent(telegram_id: int) -> tuple[float, bool]:
    """Персональный процент пользователя; если не задан — глобальный из app_settings."""
    default_pct = get_referral_percent()
    try:
        r = (
            supabase.table("users")
            .select("referral_percent")
            .eq("telegram_id", telegram_id)
            .limit(1)
            .execute()
        )
        if r.data:
            raw = r.data[0].get("referral_percent")
            if raw is not None:
                pct = max(0.0, min(100.0, float(raw)))
                return pct, True
    except Exception as e:
        print(f"users referral_percent read for {telegram_id}: {e}")
    return default_pct, False


def set_user_referral_percent(telegram_id: int, pct: Optional[float]) -> None:
    """Установить персональный % (None — использовать глобальный по умолчанию)."""
    patch: dict[str, Any] = {"referral_percent": None}
    if pct is not None:
        patch["referral_percent"] = max(0.0, min(100.0, float(pct)))
    u = user_get(telegram_id)
    if u:
        supabase.table("users").update(patch).eq("telegram_id", telegram_id).execute()
    else:
        row: dict[str, Any] = {
            "telegram_id": telegram_id,
            "subscription_until": 0,
            "hwid": None,
            **patch,
        }
        supabase.table("users").insert(row).execute()


def ensure_referred_by_set(user_id: int, referrer_id: int) -> None:
    if referrer_id == user_id or referrer_id <= 0:
        return
    try:
        u = user_get(user_id)
        if u and u.get("referred_by"):
            return
        if u:
            supabase.table("users").update({"referred_by": referrer_id}).eq("telegram_id", user_id).execute()
        else:
            supabase.table("users").insert(
                {
                    "telegram_id": user_id,
                    "referred_by": referrer_id,
                    "subscription_until": 0,
                    "hwid": None,
                }
            ).execute()
    except Exception as e:
        print(f"ensure_referred_by_set failed: {e}")


async def referral_process_paid_invoice(inv: dict, buyer_id: int, is_renewal_price: bool) -> None:
    # Начисляем с любой оплаты, но сохраняем идемпотентность по invoice_id.
    try:
        invoice_id = int(inv.get("invoice_id") or 0)
    except (TypeError, ValueError):
        invoice_id = 0
    if invoice_id <= 0:
        return
    try:
        chk = (
            supabase.table("referral_rewards")
            .select("invoice_id")
            .eq("invoice_id", invoice_id)
            .limit(1)
            .execute()
        )
        if chk.data:
            return
    except Exception as e:
        print(f"referral_rewards invoice check: {e}")
        return

    buyer = user_get(buyer_id)
    if not buyer:
        return
    ref_uid = buyer.get("referred_by")
    if not ref_uid or int(ref_uid) == int(buyer_id):
        return

    pct, _ = get_user_referral_percent(int(ref_uid))
    if pct <= 0:
        return

    amt_raw = inv.get("paid_amount")
    if amt_raw is None:
        amt_raw = inv.get("amount")
    try:
        base = float(amt_raw)
    except (TypeError, ValueError):
        return
    if base <= 0:
        return

    reward = base * pct / 100.0
    if reward <= 0:
        return

    asset = str(inv.get("paid_asset") or inv.get("asset") or "TON")
    now = int(time.time())

    try:
        supabase.table("referral_rewards").insert(
            {
                "referred_telegram_id": buyer_id,
                "referrer_telegram_id": int(ref_uid),
                "invoice_id": invoice_id,
                "asset": asset,
                "reward_amount": f"{reward:.8f}".rstrip("0").rstrip("."),
                "percent_used": pct,
                "created_at": now,
            }
        ).execute()
    except Exception as e:
        err = str(e).lower()
        if "23505" in str(e) or "duplicate" in err or "unique" in err:
            return
        print(f"referral_rewards insert failed: {e}")
        return

    if bot:
        try:
            await bot.send_message(
                int(ref_uid),
                "💰 **Реферальное вознаграждение**\n\n"
                f"Начислено: `{reward:.6f}` {asset}\n"
                f"({pct:g}% от суммы счёта).\n\n"
                "_Начисления действуют с каждой оплаты реферала._",
                parse_mode="Markdown",
            )
        except Exception as e:
            print(f"referral notify referrer {ref_uid}: {e}")


_EXCHANGE_RATES_CACHE: Optional[tuple[float, list]] = None


async def crypto_pay_app_get(method: str, params: Optional[dict] = None) -> Any:
    token = (os.environ.get("CRYPTO_PAY_API_TOKEN") or "").strip()
    if not token:
        raise ValueError("CRYPTO_PAY_API_TOKEN не задан")
    base = "https://testnet-pay.crypt.bot" if CRYPTO_PAY_TESTNET else "https://pay.crypt.bot"
    api_url = f"{base}/api/{method}"
    headers = {"Crypto-Pay-API-Token": token}
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    async with httpx.AsyncClient(timeout=45.0) as client:
        r = await client.get(api_url, params=clean, headers=headers)
    try:
        data = r.json()
    except Exception:
        raise ValueError(f"Crypto Pay: ответ не JSON (HTTP {r.status_code}).") from None
    if not data.get("ok"):
        err = data.get("error") or {}
        name = err.get("name") or err.get("code") or str(data)
        raise ValueError(f"Crypto Pay: {name}")
    return data.get("result")


async def crypto_pay_fetch_exchange_rates() -> list:
    try:
        res = await crypto_pay_app_get("getExchangeRates")
    except ValueError as e:
        print(f"getExchangeRates: {e}")
        return []
    if isinstance(res, list):
        return res
    if isinstance(res, dict) and "items" in res:
        return res["items"]
    return []


async def crypto_pay_get_exchange_rates_cached() -> list:
    global _EXCHANGE_RATES_CACHE
    now = time.time()
    if _EXCHANGE_RATES_CACHE and now - _EXCHANGE_RATES_CACHE[0] < 60:
        return _EXCHANGE_RATES_CACHE[1]
    rates = await crypto_pay_fetch_exchange_rates()
    _EXCHANGE_RATES_CACHE = (now, rates)
    return rates


def asset_amount_to_usd(asset: str, amount: float, rates: list) -> float:
    asset = str(asset or "").strip().upper()
    if asset in ("USDT", "USD", "BUSD", "USDC"):
        return amount
    if amount <= 0:
        return 0.0
    for r in rates:
        if not r:
            continue
        if r.get("is_valid") is False:
            continue
        src = str(r.get("source", "")).upper()
        tgt = str(r.get("target", "")).upper()
        try:
            rt = float(r.get("rate") or 0)
        except (TypeError, ValueError):
            continue
        if not rt:
            continue
        if src == asset and tgt in ("USD", "USDT"):
            return amount * rt
        if tgt == asset and src in ("USD", "USDT"):
            return amount / rt
    return 0.0


async def referral_rewards_total_usd(telegram_id: int) -> float:
    rates = await crypto_pay_get_exchange_rates_cached()
    try:
        rr = (
            supabase.table("referral_rewards")
            .select("asset, reward_amount")
            .eq("referrer_telegram_id", telegram_id)
            .execute()
        )
    except Exception as e:
        print(f"referral_rewards list: {e}")
        return 0.0
    total = 0.0
    for row in rr.data or []:
        a = str(row.get("asset") or "TON")
        try:
            amt = float(row.get("reward_amount") or 0)
        except (TypeError, ValueError):
            continue
        total += asset_amount_to_usd(a, amt, rates)
    return total


async def referral_withdrawals_total_usd(telegram_id: int) -> float:
    try:
        r = (
            supabase.table("referral_withdrawals")
            .select("amount_usd")
            .eq("telegram_id", telegram_id)
            .execute()
        )
    except Exception as e:
        print(f"referral_withdrawals sum: {e}")
        return 0.0
    s = 0.0
    for row in r.data or []:
        try:
            s += float(row.get("amount_usd") or 0)
        except (TypeError, ValueError):
            pass
    return s


def _parse_any_ts_to_unix(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = int(raw)
        return v if v > 0 else None
    s = str(raw).strip()
    if not s:
        return None
    if s.isdigit():
        v = int(s)
        return v if v > 0 else None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


async def referral_last_invite_ts(telegram_id: int) -> Optional[int]:
    """Момент последней «активности» по рефералке для правила сгорания.

    Используется время последнего начисления в referral_rewards (есть created_at).
    Запрос к users.created_at не делаем: в вашей схеме колонки может не быть (42703).
    Если позже добавите created_at у приглашённых — можно расширить логику.
    """
    try:
        rr = (
            supabase.table("referral_rewards")
            .select("created_at")
            .eq("referrer_telegram_id", telegram_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if rr.data:
            return _parse_any_ts_to_unix(rr.data[0].get("created_at"))
    except Exception as e:
        print(f"referral_rewards last ts read failed for {telegram_id}: {e}")
    return None


async def maybe_burn_inactive_referral_balance(telegram_id: int) -> float:
    """Сжигает баланс, если давно не было новых начислений рефералки (см. referral_last_invite_ts)."""
    inactive_days = max(1, int(REFERRAL_BURN_INACTIVE_DAYS))
    last_invite_ts = await referral_last_invite_ts(telegram_id)
    if not last_invite_ts:
        return 0.0
    now = int(time.time())
    if now - last_invite_ts < inactive_days * 86400:
        return 0.0

    earned = await referral_rewards_total_usd(telegram_id)
    withdrawn = await referral_withdrawals_total_usd(telegram_id)
    adj = get_user_referral_balance_adjustment_usd(telegram_id)
    available = max(0.0, earned - withdrawn + adj)
    if available <= 1e-9:
        return 0.0

    amt = float(f"{available:.8f}")
    try:
        supabase.table("referral_withdrawals").insert(
            {
                "telegram_id": telegram_id,
                "amount_usd": f"{amt:.8f}".rstrip("0").rstrip("."),
                "asset": "USDT",
                "check_id": None,
                "bot_check_url": "burn_inactive_referrals",
                "created_at": now,
            }
        ).execute()
    except Exception as e:
        print(f"referral burn insert failed for {telegram_id}: {e}")
        return 0.0

    if bot:
        try:
            await bot.send_message(
                telegram_id,
                "⚠️ Реферальный баланс сгорел: не было новых начислений рефералки более "
                f"{inactive_days} дней.\n"
                f"Списано: `{amt:.6f}` USDT (эквивалент).",
                parse_mode="Markdown",
            )
        except Exception as e:
            print(f"referral burn notify failed for {telegram_id}: {e}")
    return amt


async def referral_available_usd(telegram_id: int) -> float:
    await maybe_burn_inactive_referral_balance(telegram_id)
    earned = await referral_rewards_total_usd(telegram_id)
    withdrawn = await referral_withdrawals_total_usd(telegram_id)
    adj = get_user_referral_balance_adjustment_usd(telegram_id)
    return max(0.0, earned - withdrawn + adj)


async def crypto_pay_create_check_usdt(
    amount: float, pin_to_user_id: int
) -> tuple[Optional[str], Optional[str], Optional[int]]:
    amt_str = f"{amount:.8f}".rstrip("0").rstrip(".")
    try:
        res = await crypto_pay_app_get(
            "createCheck",
            {"asset": "USDT", "amount": amt_str, "pin_to_user_id": pin_to_user_id},
        )
    except ValueError as e:
        return None, str(e), None
    if not isinstance(res, dict):
        return None, "Crypto Pay: пустой ответ createCheck", None
    url = res.get("bot_check_url") or res.get("botCheckUrl")
    cid = res.get("check_id")
    if cid is not None:
        try:
            cid = int(cid)
        except (TypeError, ValueError):
            cid = None
    if not url:
        return None, "Crypto Pay не вернул ссылку на чек.", None
    return str(url), None, cid


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
    ref_pct, is_override = get_user_referral_percent(tid)
    ref_src = "персональный" if is_override else "по умолчанию"
    bal_adj = get_user_referral_balance_adjustment_usd(tid)
    bal_line = ""
    if abs(bal_adj) > 1e-9:
        bal_line = f"• Корр. реф. вывода: <b>{bal_adj:+.2f}</b> USD\n"
    uz = _uniqueizer_until_ts(u)
    uz_ok = uz >= now
    uz_status = "активен ✅" if uz_ok else "истёк ⛔"
    uz_disp = fmt_ts(uz) if uz > 0 else "—"
    return (
        f"👤 <b>Пользователь</b>\n\n"
        f"• ID: <code>{tid}</code>\n"
        f"• Username: {un}\n"
        f"• Подписка (приложение): <b>{esc_html(status)}</b>\n"
        f"• До (UTC): {esc_html(fmt_ts(sub))}\n"
        f"• Уникализатор (бот): <b>{esc_html(uz_status)}</b>\n"
        f"• Уник. до (UTC): {esc_html(uz_disp)}\n"
        f"• Реф. %: <b>{ref_pct:g}%</b> ({ref_src})\n"
        f"{bal_line}"
        f"• HWID:\n{hw_show}\n"
    )


async def edit_user_card(query: CallbackQuery, tid: int, answer_popup: Optional[str] = None):
    """Обновить сообщение карточки пользователя; один вызов query.answer."""
    u = user_get(tid)
    if not u:
        await query.answer("Пользователь не найден", show_alert=True)
        return
    text = build_user_card_text(tid, u)
    await safe_edit_text(query.message,
        text,
        parse_mode="HTML",
        reply_markup=kb_user_actions(tid),
    )
    await query.answer(answer_popup or "")


def kb_main_admin():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Все пользователи", callback_data=f"{CB}:list:0")],
            [InlineKeyboardButton(text="💳 Описания тарифов", callback_data=f"{CB}:tpl")],
            [InlineKeyboardButton(text="🎁 Реферальный %", callback_data=f"{CB}:refpct")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data=f"{CB}:broadcast")],
            [InlineKeyboardButton(text="ℹ️ Справка", callback_data=f"{CB}:help")],
        ]
    )


def kb_admin_tariff_plans():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="STANDART", callback_data=f"{CB}:tpe:s")],
            [InlineKeyboardButton(text="PRO", callback_data=f"{CB}:tpe:p")],
            [InlineKeyboardButton(text="MAX", callback_data=f"{CB}:tpe:m")],
            [InlineKeyboardButton(text="TEAM", callback_data=f"{CB}:tpe:t")],
            [InlineKeyboardButton(text="UNIQUEIZER", callback_data=f"{CB}:tpe:u")],
            [InlineKeyboardButton(text="⬅️ Админ-меню", callback_data=f"{CB}:menu")],
        ]
    )


def kb_referral_admin():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="5%", callback_data=f"{CB}:refset:5"),
                InlineKeyboardButton(text="10%", callback_data=f"{CB}:refset:10"),
                InlineKeyboardButton(text="15%", callback_data=f"{CB}:refset:15"),
            ],
            [
                InlineKeyboardButton(text="20%", callback_data=f"{CB}:refset:20"),
                InlineKeyboardButton(text="Свой %", callback_data=f"{CB}:refcust"),
            ],
            [
                InlineKeyboardButton(text="$5 вывод", callback_data=f"{CB_REF_MIN}:5"),
                InlineKeyboardButton(text="$10", callback_data=f"{CB_REF_MIN}:10"),
                InlineKeyboardButton(text="$25", callback_data=f"{CB_REF_MIN}:25"),
            ],
            [
                InlineKeyboardButton(text="$50", callback_data=f"{CB_REF_MIN}:50"),
                InlineKeyboardButton(text="💸 Свой мин.", callback_data=f"{CB}:rwmincust"),
            ],
            [InlineKeyboardButton(text="⬅️ Админ-меню", callback_data=f"{CB}:menu")],
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
                InlineKeyboardButton(text="🎯 +7 Уник.", callback_data=f"{CB}:uzq:{tid}:7"),
                InlineKeyboardButton(text="🎯 +30", callback_data=f"{CB}:uzq:{tid}:30"),
                InlineKeyboardButton(text="🎯 +365", callback_data=f"{CB}:uzq:{tid}:365"),
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Уник. свой срок (дней)",
                    callback_data=f"{CB}:uzs:{tid}",
                ),
            ],
            [
                InlineKeyboardButton(text="🗑 Сбросить HWID", callback_data=f"{CB}:h:{tid}"),
                InlineKeyboardButton(text="✏️ Задать HWID", callback_data=f"{CB}:w:{tid}"),
            ],
            [
                InlineKeyboardButton(text="🎁 Реф. %", callback_data=f"{CB}:urp:{tid}"),
                InlineKeyboardButton(text="♻️ % дефолт", callback_data=f"{CB}:urc:{tid}"),
            ],
            [
                InlineKeyboardButton(text="💰 Реф. баланс", callback_data=f"{CB}:urb:{tid}"),
                InlineKeyboardButton(text="↩️ Корр. 0", callback_data=f"{CB}:urbr:{tid}"),
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


def kb_user_main_menu(telegram_user_id: Optional[int] = None):
    """Главное меню: Профиль|Отзывы, Тарифы|Рефералы, Уникализатор (всегда), «Команда» — при TEAM."""
    profile_btn = InlineKeyboardButton(text="👤 Профиль", callback_data=f"{UCB}:profile")
    # По требованию: «Отзывы» — просто картинка, поэтому всегда открываем callback.
    reviews_btn = InlineKeyboardButton(text="⭐ Отзывы", callback_data=f"{UCB}:reviews")

    if LINK_BUY:
        tariffs_btn = InlineKeyboardButton(text="💳 Тарифы", url=LINK_BUY)
    else:
        tariffs_btn = InlineKeyboardButton(text="💳 Тарифы", callback_data=f"{UCB}:buy")

    referrals_btn = InlineKeyboardButton(text="🎁 Рефералы", callback_data=f"{UCB}:referrals")

    if LINK_SUPPORT:
        support_btn = InlineKeyboardButton(text="💬 Поддержка", url=LINK_SUPPORT)
    else:
        support_btn = InlineKeyboardButton(text="💬 Поддержка", callback_data=f"{UCB}:support")

    rows: list[list[InlineKeyboardButton]] = [
        [profile_btn, reviews_btn],
        [tariffs_btn, referrals_btn],
    ]
    if telegram_user_id is not None:
        rows.append(
            [InlineKeyboardButton(text="🎯 Уникализатор", callback_data=f"{UCB}:uniqueizer")]
        )
    if telegram_user_id is not None and user_has_active_team_plan(telegram_user_id):
        rows.append(
            [InlineKeyboardButton(text="👥 Команда", callback_data=f"{UCB}:team")]
        )
    rows.append([support_btn])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_user_back_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def kb_referrals_screen():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💸 Вывести", callback_data=f"{UCB}:refw")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def kb_referrals_withdraw_cancel():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{UCB}:refw_cancel")],
        ]
    )


def text_tariffs_caption_html() -> str:
    if LINK_TERMS:
        agree = f'<a href="{html.escape(LINK_TERMS, quote=True)}">пользовательским соглашением</a>'
    else:
        agree = "пользовательским соглашением"
    return (
        "🔔 Приобретая любую подписку из представленных, вы соглашаетесь с нашим "
        f"{agree}. 📄\n\n"
        "Выберите тарифный план:"
    )


def text_uniqueizer_no_access_html() -> str:
    raw = (os.environ.get("TEXT_UNIQUEIZER_NO_ACCESS") or "").strip()
    if raw:
        return raw
    return (
        "Для использования Уникализатора, пожалуйста приобретите подписку в разделе — <b>Тарифы</b>."
    )


def text_uniqueizer_screen_html() -> str:
    raw = (os.environ.get("TEXT_UNIQUEIZER_SCREEN") or "").strip()
    if raw:
        return raw
    return (
        "🎯 <b>Уникализатор</b>\n\n"
        "• <b>Уникализация</b> — загрузите фото или видео, выберите шаблон (если ещё не выбран), "
        "задайте число копий; бот вернёт ZIP с вариантами.\n"
        "• <b>Шаблоны</b> — набор опций обработки; один шаблон отмечен ✓ и используется при уникализации.\n\n"
        "<i><b>ffmpeg</b> подключается автоматически через пакет <code>imageio-ffmpeg</code> (уже в зависимостях). "
        "При желании можно указать свой бинарник: <code>FFMPEG_PATH</code> или системный ffmpeg в PATH. "
        "Если ffmpeg недоступен: видео в ZIP — дубликаты одного файла; фото — через Pillow.</i>"
    )


def kb_tariffs():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="STANDART", callback_data=f"{UCB}:tariff:s")],
            [InlineKeyboardButton(text="PRO", callback_data=f"{UCB}:tariff:p")],
            [InlineKeyboardButton(text="MAX", callback_data=f"{UCB}:tariff:m")],
            [InlineKeyboardButton(text="TEAM", callback_data=f"{UCB}:tariff:t")],
            [InlineKeyboardButton(text="UNIQUEIZER", callback_data=f"{UCB}:tariff:u")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def kb_tariff_subplan_detail(code: str) -> InlineKeyboardMarkup:
    """Экран описания тарифа: купить, назад к списку, главное меню."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Купить", callback_data=f"{UCB}:tbuy:{code}")],
            [InlineKeyboardButton(text="⬅️ К тарифам", callback_data=f"{UCB}:tariffs_menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def kb_tariff_after_invoice(code: str, pay_url: str) -> InlineKeyboardMarkup:
    """После createInvoice: открытие оплаты в Crypto Bot + навигация."""
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="💎 Оплатить в Crypto Bot", url=pay_url)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{UCB}:tariff:{code}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"{UCB}:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_team_after_invoice(pay_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Оплатить в Crypto Bot", url=pay_url)],
            [InlineKeyboardButton(text="⬅️ К разделу «Команда»", callback_data=f"{UCB}:team")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def kb_team_setup_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{UCB}:team_cancel")],
        ]
    )


def kb_team_dashboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить участников (оплата)",
                    callback_data=f"{UCB}:team_buy_seats",
                )
            ],
            [
                InlineKeyboardButton(
                    text="👤 Добавить по Telegram ID",
                    callback_data=f"{UCB}:team_add_member",
                )
            ],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
        ]
    )


def text_user_main_menu() -> str:
    # Главное меню теперь — это картинка, а не текст.
    return ""


def build_user_profile_public_text(tid: int, u: Optional[dict]) -> str:
    """Профиль для обычного пользователя (без HWID)."""
    if not u:
        return (
            "👤 <b>Профиль</b>\n\n"
            "Запись в базе не найдена. После покупки подписки данные появятся здесь.\n\n"
            "Используйте «Купить подписку», если ещё не оформляли доступ."
        )
    now = int(time.time())
    sub = int(u.get("subscription_until") or 0)
    plan_code = (u.get("subscription_plan") or "").strip().lower()
    app_codes = ("s", "p", "m", "t")
    app_plan_code = plan_code if plan_code in app_codes else ""
    if plan_code == "u":
        app_plan_code = ""
    app_label = {"s": "STANDART", "p": "PRO", "m": "MAX", "t": "TEAM"}.get(app_plan_code, "—")
    app_active = bool(app_plan_code) and sub >= now
    app_word = "активна" if app_active else "не активна"

    uz_col = _uniqueizer_until_ts(u)
    uz_until = uz_col
    if plan_code == "u" and sub > uz_until:
        uz_until = sub
    uniq_active = uz_until >= now
    uniq_word = "активен" if uniq_active else "не активен"
    app_until_disp = fmt_ts(sub) if app_plan_code and sub > 0 else "—"
    uniq_until_disp = fmt_ts(uz_until) if uz_until > 0 else "—"

    username = (u.get("username") or "").strip() or "—"
    return (
        "👤 <b>Профиль</b>\n\n"
        f"Username: <b>{esc_html(username)}</b>\n"
        f"ID: <code>{tid}</code>\n\n"
        "<b>Приложение</b>\n"
        f"Тариф: <b>{esc_html(app_label)}</b> — <b>{esc_html(app_word)}</b>\n"
        f"До (UTC): <b>{esc_html(app_until_disp)}</b>\n\n"
        "<b>Уникализатор</b> (бот)\n"
        f"Доступ: <b>{esc_html(uniq_word)}</b>\n"
        f"До (UTC): <b>{esc_html(uniq_until_disp)}</b>\n"
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
    return {
        "service": "neuro-uploader-auth",
        "docs": "/docs",
        "health": "/health",
        "ping": "/api/auth/ping",
        "crypto_pay_webhook": "/api/crypto-pay/webhook",
        "crypto_pay_webhook_root": "POST / (алиас, если в Crypto Pay указан только корень домена)",
    }


@app.get("/health")
async def health():
    """Проверка, что сервис поднят (удобно для Railway / браузера)."""
    return {"ok": True}


@app.get("/api/auth/ping")
async def auth_ping():
    """Проверка из браузера: откройте URL в новой вкладке или fetch с localhost."""
    return {"ok": True}


@app.post("/api/crypto-pay/webhook")
@app.post("/")
async def crypto_pay_webhook(request: Request):
    """Вебхук Crypto Pay: оплата счёта → crypto_pay_processed_invoices + продление users.subscription_until.

    Дублируется на POST / — Crypto Pay и Railway часто настроены на корень домена без пути /api/....
    """
    body_text = (await request.body()).decode("utf-8")
    sig = (
        request.headers.get("Crypto-Pay-Api-Signature")
        or request.headers.get("crypto-pay-api-signature")
        or ""
    ).strip()
    token = (os.environ.get("CRYPTO_PAY_API_TOKEN") or "").strip()
    if not token:
        raise HTTPException(status_code=503, detail="CRYPTO_PAY_API_TOKEN не задан")
    if not verify_crypto_pay_webhook_signature(body_text, sig, token):
        raise HTTPException(status_code=401, detail="Неверная подпись вебхука")
    try:
        data = json.loads(body_text)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Неверный JSON")

    ut = str(data.get("update_type") or data.get("updateType") or "").lower()
    if ut != "invoice_paid":
        return PlainTextResponse("OK", status_code=200)

    inv = data.get("payload")
    if not isinstance(inv, dict):
        return PlainTextResponse("OK", status_code=200)
    if str(inv.get("status") or "").lower() != "paid":
        return PlainTextResponse("OK", status_code=200)

    try:
        invoice_id = int(inv["invoice_id"])
    except (KeyError, TypeError, ValueError):
        return PlainTextResponse("OK", status_code=200)

    bundle = parse_team_bundle_payload(inv.get("payload"))
    if bundle is not None:
        telegram_id, team_uuid, n_seats = bundle
        mark_tb = crypto_invoice_mark_processed(invoice_id, telegram_id, "team_bundle")
        if mark_tb == "duplicate":
            return PlainTextResponse("OK", status_code=200)
        if mark_tb == "error":
            raise HTTPException(
                status_code=503,
                detail="Таблица crypto_pay_processed_invoices недоступна — выполните SQL из crypto_pay_processed_invoices.sql",
            )
        if not team_add_seats_paid(team_uuid, telegram_id, n_seats):
            print(f"crypto webhook team_bundle: не удалось начислить места, invoice={invoice_id}")
        await referral_process_paid_invoice(inv, telegram_id, False)
        if bot:
            t = team_get_by_owner(telegram_id)
            nm = (t or {}).get("name") or "—"
            total_seats = int((t or {}).get("seats_purchased") or 0)
            try:
                await bot.send_message(
                    chat_id=telegram_id,
                    text=(
                        "✅ <b>Оплата получена</b> — места для команды начислены.\n\n"
                        f"Команда: <b>{esc_html(str(nm))}</b>\n"
                        f"В этой оплате: <b>{n_seats}</b> мест(а)\n"
                        f"Всего оплаченных мест: <b>{total_seats}</b>\n\n"
                        "Добавьте участников в боте: <b>Команда</b> → "
                        "<b>Добавить по Telegram ID</b>."
                    ),
                    parse_mode="HTML",
                )
            except Exception as e:
                print(f"crypto pay team_bundle: уведомление пользователю {telegram_id}: {e}")
        return PlainTextResponse("OK", status_code=200)

    plan_code, telegram_id, is_renewal_price = parse_nu_crypto_invoice_payload(inv.get("payload"))
    if not plan_code or telegram_id is None:
        print(f"crypto webhook: нет nu_plan/tg в payload счёта {invoice_id}")
        return PlainTextResponse("OK", status_code=200)
    if plan_code not in _TARIFF_PLAN_ENV:
        print(f"crypto webhook: неизвестный план {plan_code!r}, invoice {invoice_id}")
        return PlainTextResponse("OK", status_code=200)

    mark = crypto_invoice_mark_processed(invoice_id, telegram_id, plan_code)
    if mark == "duplicate":
        return PlainTextResponse("OK", status_code=200)
    if mark == "error":
        raise HTTPException(
            status_code=503,
            detail="Таблица crypto_pay_processed_invoices недоступна — выполните SQL из crypto_pay_processed_invoices.sql",
        )

    days = subscription_days_for_plan(plan_code)
    if plan_code == "u":
        until = extend_user_uniqueizer_days(telegram_id, days)
    else:
        until = extend_user_subscription_days(telegram_id, days, plan_code=plan_code)

    await referral_process_paid_invoice(inv, telegram_id, is_renewal_price)

    if bot:
        try:
            if plan_code == "u":
                await bot.send_message(
                    chat_id=telegram_id,
                    text=(
                        "✅ **Оплата получена** — доступ к **Уникализатору** в боте активирован.\n\n"
                        f"До: `{fmt_ts(until)}`\n\n"
                        "Это не подписка на приложение: для входа в программу оформите тариф "
                        "STANDART / PRO / MAX или TEAM."
                    ),
                    parse_mode="Markdown",
                )
            else:
                await bot.send_message(
                    chat_id=telegram_id,
                    text=(
                        "✅ **Оплата получена** — подписка активирована.\n\n"
                        f"Доступ до: `{fmt_ts(until)}`\n\n"
                        "Можно входить в приложение с этого Telegram-аккаунта."
                    ),
                    parse_mode="Markdown",
                )
        except Exception as e:
            print(f"crypto pay: уведомление пользователю {telegram_id}: {e}")

        if plan_code != "u":
            # Выдача файлов после оплаты подписки на приложение
            try:
                zip_file_id = get_app_zip_file_id()
                txt_file_id = get_app_txt_file_id()

                sent_any = False
                if zip_file_id or txt_file_id:
                    if zip_file_id:
                        await bot.send_document(
                            chat_id=telegram_id,
                            document=zip_file_id,
                        )
                        sent_any = True
                    if txt_file_id:
                        await bot.send_document(
                            chat_id=telegram_id,
                            document=txt_file_id,
                        )
                        sent_any = True

                # Fallback: отправка с диска (если file_id не задан или отсутствует)
                if not sent_any:
                    await ensure_app_files_downloaded()
                    zip_p = _resolve_local_file_path(APP_ZIP_PATH)
                    txt_p = _resolve_local_file_path(APP_TXT_PATH)
                    if os.path.isfile(zip_p):
                        await bot.send_document(
                            chat_id=telegram_id,
                            document=FSInputFile(zip_p, filename=os.path.basename(zip_p)),
                        )
                        sent_any = True
                    if os.path.isfile(txt_p):
                        await bot.send_document(
                            chat_id=telegram_id,
                            document=FSInputFile(txt_p, filename=os.path.basename(txt_p)),
                        )
                        sent_any = True

                if not sent_any:
                    await bot.send_message(
                        chat_id=telegram_id,
                        text="Файлы для скачивания не найдены. Установите APP_ZIP_FILE_ID/APP_TXT_FILE_ID или разместите файлы на сервере.",
                    )
            except Exception as e:
                print(f"crypto pay: send app files failed for {telegram_id}: {e}")

    return PlainTextResponse("OK", status_code=200)


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
    current_time = int(time.time())
    user_res = supabase.table("users").select("*").eq("telegram_id", telegram_id).execute()
    if user_res.data:
        u = user_res.data[0]
        if _subscription_until_ts(u) >= current_time:
            raw_plan = u.get("subscription_plan")
            if isinstance(raw_plan, str):
                plan_norm = raw_plan.strip().lower()
            else:
                plan_norm = "m"
            if plan_norm == "u":
                pass
            elif plan_norm in ("s", "p", "m", "t"):
                return {"status": "success", "message": "Subscription active", "subscription_plan": plan_norm}
            else:
                return {"status": "success", "message": "Subscription active", "subscription_plan": "m"}
    if team_member_has_active_team_access(telegram_id):
        # Лимиты как у MAX: отдельную подписку покупать не нужно
        return {
            "status": "success",
            "message": "Team seat active",
            "subscription_plan": "m",
        }
    return {"status": "failed", "message": "Subscription expired or user not found"}


@app.post("/api/nu/sadcaptcha/puzzle")
async def nu_sadcaptcha_puzzle(req: NuSadPuzzle):
    nu_require_active_subscription(req.telegram_id)
    if not SADCAPTCHA_LICENSE_KEY:
        raise HTTPException(status_code=503, detail="SadCaptcha не настроен на сервере (SADCAPTCHA_LICENSE_KEY)")
    url = f"https://www.sadcaptcha.com/api/v1/puzzle?licenseKey={SADCAPTCHA_LICENSE_KEY}"
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            r = await client.post(
                url,
                json={"puzzleImageB64": req.puzzleImageB64, "pieceImageB64": req.pieceImageB64},
            )
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"SadCaptcha HTTP {r.status_code}: {r.text[:500]}")
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SadCaptcha: {e!s}") from e


@app.post("/api/nu/sadcaptcha/rotate")
async def nu_sadcaptcha_rotate(req: NuSadRotate):
    nu_require_active_subscription(req.telegram_id)
    if not SADCAPTCHA_LICENSE_KEY:
        raise HTTPException(status_code=503, detail="SadCaptcha не настроен на сервере (SADCAPTCHA_LICENSE_KEY)")
    url = f"https://www.sadcaptcha.com/api/v1/rotate?licenseKey={SADCAPTCHA_LICENSE_KEY}"
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            r = await client.post(
                url,
                json={"outerImageB64": req.outerImageB64, "innerImageB64": req.innerImageB64},
            )
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"SadCaptcha HTTP {r.status_code}: {r.text[:500]}")
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SadCaptcha: {e!s}") from e


@app.post("/api/nu/sadcaptcha/shapes")
async def nu_sadcaptcha_shapes(req: NuSadShapes):
    nu_require_active_subscription(req.telegram_id)
    if not SADCAPTCHA_LICENSE_KEY:
        raise HTTPException(status_code=503, detail="SadCaptcha не настроен на сервере (SADCAPTCHA_LICENSE_KEY)")
    url = f"https://www.sadcaptcha.com/api/v1/shapes?licenseKey={SADCAPTCHA_LICENSE_KEY}"
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            r = await client.post(url, json={"imageB64": req.imageB64})
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"SadCaptcha HTTP {r.status_code}: {r.text[:500]}")
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SadCaptcha: {e!s}") from e


@app.post("/api/nu/sadcaptcha/icon")
async def nu_sadcaptcha_icon(req: NuSadIcon):
    nu_require_active_subscription(req.telegram_id)
    if not SADCAPTCHA_LICENSE_KEY:
        raise HTTPException(status_code=503, detail="SadCaptcha не настроен на сервере (SADCAPTCHA_LICENSE_KEY)")
    url = f"https://www.sadcaptcha.com/api/v1/icon?licenseKey={SADCAPTCHA_LICENSE_KEY}"
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            r = await client.post(url, json={"challenge": req.challenge, "imageB64": req.imageB64})
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"SadCaptcha HTTP {r.status_code}: {r.text[:500]}")
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SadCaptcha: {e!s}") from e


@app.post("/api/nu/ai/preset")
async def nu_ai_preset(req: NuAiPreset):
    """Генерация описания/хештегов для пресетов через Claude (ключ только на сервере)."""
    nu_require_active_subscription(req.telegram_id)
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic не настроен на сервере (ANTHROPIC_API_KEY)")
    system_msg = (
        "Ты — SMM-специалист. По заданному промту создай:\n"
        "1. Описание видео (1-3 предложения, живой стиль, без эмодзи в начале).\n"
        "2. Хештеги (5-10 штук через пробел, начинаются с #).\n\n"
        'Ответь строго в JSON формате: {"description": "...", "hashtags": "#тег1 #тег2 ..."}'
    )
    payload = {
        "model": NU_CLAUDE_MODEL,
        "max_tokens": 400,
        "system": system_msg,
        "messages": [{"role": "user", "content": req.prompt}],
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
                json=payload,
            )
        if r.status_code != 200:
            try:
                err = r.json()
                msg = err.get("error", {}).get("message", r.text)
            except Exception:
                msg = r.text[:500]
            raise HTTPException(status_code=502, detail=f"Anthropic {r.status_code}: {msg}")
        data = r.json()
        content = data["content"][0]["text"]
        content = content.strip()
        if "```" in content:
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        parsed = json.loads(content.strip())
        return {
            "status": "success",
            "description": parsed.get("description", ""),
            "hashtags": parsed.get("hashtags", ""),
            "model": "Claude Sonnet",
        }
    except HTTPException:
        raise
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Некорректный JSON от модели: {e!s}") from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Claude: {e!s}") from e


# --- Telegram: пользователи ---


def tariffs_photo_for_new_message() -> Optional[object]:
    """Для send_photo: FSInputFile с диска или строка URL. None — без картинки."""
    if TARIFFS_IMAGE_PATH:
        p = (
            TARIFFS_IMAGE_PATH
            if os.path.isabs(TARIFFS_IMAGE_PATH)
            else os.path.join(os.path.dirname(os.path.abspath(__file__)), TARIFFS_IMAGE_PATH)
        )
        if os.path.isfile(p):
            return FSInputFile(p)
    if TARIFFS_IMAGE_URL:
        return TARIFFS_IMAGE_URL
    return None


# Картинка карточки конкретного тарифа (STANDART / PRO / MAX / TEAM / UNIQUEIZER).
_TARIFF_PLAN_IMAGE_PATH_KEYS: dict[str, tuple[str, ...]] = {
    "s": ("TARIFF_PLAN_IMAGE_PATH_S", "TARIFF_IMAGE_STANDART_PATH"),
    "p": ("TARIFF_PLAN_IMAGE_PATH_P", "TARIFF_IMAGE_PRO_PATH"),
    "m": ("TARIFF_PLAN_IMAGE_PATH_M", "TARIFF_IMAGE_MAX_PATH"),
    "t": ("TARIFF_PLAN_IMAGE_PATH_T", "TARIFF_IMAGE_TEAM_PATH"),
    "u": ("TARIFF_PLAN_IMAGE_PATH_U", "TARIFF_IMAGE_UNIQUEIZER_PATH"),
}
_TARIFF_PLAN_IMAGE_URL_KEYS: dict[str, tuple[str, ...]] = {
    "s": ("TARIFF_PLAN_IMAGE_URL_S", "TARIFF_IMAGE_STANDART_URL"),
    "p": ("TARIFF_PLAN_IMAGE_URL_P", "TARIFF_IMAGE_PRO_URL"),
    "m": ("TARIFF_PLAN_IMAGE_URL_M", "TARIFF_IMAGE_MAX_URL"),
    "t": ("TARIFF_PLAN_IMAGE_URL_T", "TARIFF_IMAGE_TEAM_URL"),
    "u": ("TARIFF_PLAN_IMAGE_URL_U", "TARIFF_IMAGE_UNIQUEIZER_URL"),
}


def tariff_plan_photo_for_plan(code: str) -> Optional[object]:
    """Для карточки тарифа s|p|m|t|u: FSInputFile или URL. None — только текст (как раньше)."""
    if code not in _TARIFF_PLAN_IMAGE_PATH_KEYS:
        return None
    for ek in _TARIFF_PLAN_IMAGE_PATH_KEYS[code]:
        raw = (os.environ.get(ek) or "").strip()
        if not raw:
            continue
        p = raw if os.path.isabs(raw) else os.path.join(os.path.dirname(os.path.abspath(__file__)), raw)
        if os.path.isfile(p):
            return FSInputFile(p)
    for ek in _TARIFF_PLAN_IMAGE_URL_KEYS[code]:
        raw = (os.environ.get(ek) or "").strip()
        if raw:
            return raw
    return None


def _resolve_local_file_path(rel_path: str) -> str:
    """Преобразовать относительный путь в абсолютный относительно auth_server.py."""
    if os.path.isabs(rel_path):
        return rel_path
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), rel_path)


async def _download_http_file(url: str, dst_path: str) -> None:
    """Скачать файл по HTTP(S) на диск Railway."""
    dst_dir = os.path.dirname(dst_path)
    if dst_dir:
        os.makedirs(dst_dir, exist_ok=True)
    async with httpx.AsyncClient(timeout=300.0) as client:
        async with client.stream("GET", url) as r:
            r.raise_for_status()
            with open(dst_path, "wb") as f:
                async for chunk in r.aiter_bytes(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)


async def ensure_app_files_downloaded() -> None:
    """Убедиться, что APP_ZIP_PATH/APP_TXT_PATH доступны на диске.

    Если файлов нет и заданы APP_*_URL — скачиваем.
    """
    global _APP_FILES_READY
    if _APP_FILES_READY:
        return
    async with _APP_FILES_LOCK:
        if _APP_FILES_READY:
            return

        zip_abs = _resolve_local_file_path(APP_ZIP_PATH)
        txt_abs = _resolve_local_file_path(APP_TXT_PATH)

        zip_exists = os.path.isfile(zip_abs)
        txt_exists = os.path.isfile(txt_abs)

        # Скачиваем только то, чего нет
        if not zip_exists and APP_ZIP_URL:
            await _download_http_file(APP_ZIP_URL, zip_abs)
            zip_exists = os.path.isfile(zip_abs)
        if not txt_exists and APP_TXT_URL:
            await _download_http_file(APP_TXT_URL, txt_abs)
            txt_exists = os.path.isfile(txt_abs)

        # Находимся в Railway: файловая система часто эфемерная.
        # Поэтому считаем готовым после одной попытки.
        _APP_FILES_READY = True


def app_setting_get_value(key: str) -> Optional[str]:
    """Чтение value из app_settings (sync запрос)."""
    try:
        r = supabase.table("app_settings").select("value").eq("key", key).execute()
        if r.data:
            v = r.data[0].get("value")
            return str(v) if v is not None else None
    except Exception as e:
        print(f"app_settings get {key} failed: {e}")
    return None


def app_setting_upsert_value(key: str, value: str) -> None:
    """Сохранение value в app_settings (sync запрос)."""
    try:
        supabase.table("app_settings").upsert({"key": key, "value": str(value)}).execute()
    except Exception as e:
        print(f"app_settings upsert {key} failed: {e}")
        raise


def app_setting_delete_key(key: str) -> None:
    """Удалить ключ из app_settings (сброс к .env / дефолтам)."""
    try:
        supabase.table("app_settings").delete().eq("key", key).execute()
    except Exception as e:
        print(f"app_settings delete {key} failed: {e}")
        raise


def get_app_zip_file_id() -> Optional[str]:
    return APP_ZIP_FILE_ID or app_setting_get_value("app_zip_file_id")


def get_app_txt_file_id() -> Optional[str]:
    return APP_TXT_FILE_ID or app_setting_get_value("app_txt_file_id")


def _document_asset_for_new_message(*, image_path: str, image_url: str, default_rel_path: str) -> Optional[object]:
    """Для send_document: FSInputFile (svg/zip/txt) или URL. None — без файла."""
    candidate = (image_path or "").strip() or default_rel_path
    p = _resolve_local_file_path(candidate)
    if p and os.path.isfile(p):
        return FSInputFile(p)
    if (image_url or "").strip():
        return image_url.strip()
    return None


def _photo_asset_for_new_message(*, image_path: str, image_url: str, default_rel_path: str) -> Optional[object]:
    """Для send_photo: FSInputFile с png/jpg/webp или строка URL. None — без фото."""
    allowed_ext = {".png", ".jpg", ".jpeg", ".webp"}
    candidate = (image_path or "").strip() or default_rel_path
    p = _resolve_local_file_path(candidate)
    if p and os.path.isfile(p):
        if os.path.splitext(p)[1].lower() in allowed_ext:
            return FSInputFile(p)
    if (image_url or "").strip() and (image_url or "").strip().lower().startswith("http"):
        return image_url.strip()
    return None


def main_menu_document_for_new_message() -> Optional[object]:
    return _document_asset_for_new_message(
        image_path=MAIN_MENU_IMAGE_PATH,
        image_url=MAIN_MENU_IMAGE_URL,
        default_rel_path=os.path.join("public", "logo.svg"),
    )


def main_menu_photo_for_new_message() -> Optional[object]:
    return _photo_asset_for_new_message(
        image_path=MAIN_MENU_IMAGE_PATH,
        image_url=MAIN_MENU_IMAGE_URL,
        default_rel_path=os.path.join("public", "logo.svg"),
    )


def profile_document_for_new_message() -> Optional[object]:
    return _document_asset_for_new_message(
        image_path=PROFILE_IMAGE_PATH,
        image_url=PROFILE_IMAGE_URL,
        default_rel_path=os.path.join("public", "Union.svg"),
    )


def profile_photo_for_new_message() -> Optional[object]:
    return _photo_asset_for_new_message(
        image_path=PROFILE_IMAGE_PATH,
        image_url=PROFILE_IMAGE_URL,
        default_rel_path=os.path.join("public", "Union.svg"),
    )


def reviews_document_for_new_message() -> Optional[object]:
    return _document_asset_for_new_message(
        image_path=REVIEWS_IMAGE_PATH,
        image_url=REVIEWS_IMAGE_URL,
        default_rel_path=os.path.join("public", "Union.svg"),
    )


def reviews_photo_for_new_message() -> Optional[object]:
    return _photo_asset_for_new_message(
        image_path=REVIEWS_IMAGE_PATH,
        image_url=REVIEWS_IMAGE_URL,
        default_rel_path=os.path.join("public", "Union.svg"),
    )


def referrals_photo_for_new_message() -> Optional[object]:
    """Для send_photo: FSInputFile с диска или строка URL. None — без картинки."""
    if REFERRALS_IMAGE_PATH:
        p = (
            REFERRALS_IMAGE_PATH
            if os.path.isabs(REFERRALS_IMAGE_PATH)
            else os.path.join(os.path.dirname(os.path.abspath(__file__)), REFERRALS_IMAGE_PATH)
        )
        if os.path.isfile(p):
            return FSInputFile(p)
    if REFERRALS_IMAGE_URL:
        return REFERRALS_IMAGE_URL
    return None


async def show_user_referrals_screen(query: CallbackQuery) -> None:
    """Экран рефералов: одно якорное сообщение (фото + подпись или правка текста)."""
    uid = query.from_user.id
    chat_id = query.message.chat.id
    text = await build_referrals_user_html(uid)
    markup = kb_referrals_screen()
    img = referrals_photo_for_new_message()
    if img:
        try:
            await user_shell_apply_photo(
                query.bot,
                uid,
                chat_id,
                query.message,
                cache_key="shell_referrals",
                photo_source=img,
                caption=text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"referrals photo shell failed: {e}")
            await safe_edit_text(query.message, text, parse_mode="HTML", reply_markup=markup)
    else:
        await safe_edit_text(query.message, text, parse_mode="HTML", reply_markup=markup)


async def show_user_profile_screen(query: CallbackQuery) -> None:
    """Профиль: одно якорное сообщение."""
    tid = query.from_user.id
    chat_id = query.message.chat.id
    u = user_get(tid)
    text = build_user_profile_public_text(tid, u)
    markup = kb_user_back_main()
    photo = profile_photo_for_new_message()
    if photo:
        try:
            await user_shell_apply_photo(
                query.bot,
                tid,
                chat_id,
                query.message,
                cache_key="shell_profile_photo",
                photo_source=photo,
                caption=text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"profile photo shell failed: {e}")
            await user_shell_apply_text(
                query.bot,
                tid,
                chat_id,
                query.message,
                text=text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        return
    doc = profile_document_for_new_message()
    if doc:
        try:
            await user_shell_apply_document(
                query.bot,
                tid,
                chat_id,
                query.message,
                cache_key="shell_profile_document",
                document_source=doc,
                caption=text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"profile document shell failed: {e}")
            await user_shell_apply_text(
                query.bot,
                tid,
                chat_id,
                query.message,
                text=text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        return
    await safe_edit_text(query.message, text, parse_mode="HTML", reply_markup=markup)


async def show_user_main_menu(
    bot_obj: Optional[Bot],
    chat_id: int,
    *,
    extra_caption: str = "",
    telegram_user_id: Optional[int] = None,
    telegram_username: Optional[str] = None,
    anchor_message: Optional[Message] = None,
) -> None:
    """Главное меню: одно якорное сообщение (фото/документ/текст) + кнопки."""
    if telegram_user_id is not None:
        ensure_user_row_from_bot(telegram_user_id, telegram_username)
    if not bot_obj:
        return
    uid = _user_shell_uid(telegram_user_id, chat_id)
    extra_caption = (extra_caption or "").strip()
    markup = kb_user_main_menu(telegram_user_id)
    pm: Optional[str] = "Markdown" if extra_caption else None
    cap = extra_caption if extra_caption else "."
    photo = main_menu_photo_for_new_message()
    if photo:
        try:
            await user_shell_apply_photo(
                bot_obj,
                uid,
                chat_id,
                anchor_message,
                cache_key="shell_main_photo",
                photo_source=photo,
                caption=cap,
                parse_mode=pm,
                reply_markup=markup,
            )
            return
        except Exception as e:
            print(f"main menu photo shell failed: {e}")
    doc = main_menu_document_for_new_message()
    if doc:
        try:
            await user_shell_apply_document(
                bot_obj,
                uid,
                chat_id,
                anchor_message,
                cache_key="shell_main_document",
                document_source=doc,
                caption=cap,
                parse_mode=pm,
                reply_markup=markup,
            )
            return
        except Exception as e:
            print(f"main menu document shell failed: {e}")
    text_out = extra_caption if extra_caption else "."
    if not text_out.strip():
        text_out = "."
    await user_shell_apply_text(
        bot_obj,
        uid,
        chat_id,
        anchor_message,
        text=text_out,
        parse_mode=pm,
        reply_markup=markup,
    )


async def _show_tariffs_text_fallback(
    query: CallbackQuery, caption: str, markup: InlineKeyboardMarkup
) -> None:
    await safe_edit_text(
        query.message,
        caption,
        parse_mode="HTML",
        reply_markup=markup,
    )


async def show_user_tariffs_screen(query: CallbackQuery) -> None:
    uid = query.from_user.id
    chat_id = query.message.chat.id
    caption = text_tariffs_caption_html()
    markup = kb_tariffs()
    img = tariffs_photo_for_new_message()

    if img:
        try:
            await user_shell_apply_photo(
                query.bot,
                uid,
                chat_id,
                query.message,
                cache_key="shell_tariffs",
                photo_source=img,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"tariffs photo shell failed: {e}")
            await _show_tariffs_text_fallback(query, caption, markup)
    else:
        await _show_tariffs_text_fallback(query, caption, markup)
    await query.answer()


async def require_policies_or_block(query: CallbackQuery) -> bool:
    """False = показан экран принятия, дальше не идём."""
    uid = query.from_user.id
    if is_admin(uid) or policies_user_has_accepted(uid):
        return True
    prompt = text_policies_prompt_html()
    markup = kb_policies_accept()
    await safe_edit_text(
        query.message,
        prompt,
        parse_mode="HTML",
        reply_markup=markup,
    )
    await query.answer("Сначала примите условия.", show_alert=True)
    return False


@dp.message(CommandStart())
async def cmd_start(message: Message):
    args = message.text.split() if message.text else []
    uid = message.from_user.id

    if len(args) >= 2:
        p0 = args[1].strip()
        if p0.startswith("ref_"):
            try:
                ref_uid = int(p0[4:])
                ensure_referred_by_set(uid, ref_uid)
            except ValueError:
                pass
            args = [args[0]]

    if len(args) == 1:
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
        await show_user_main_menu(
            bot,
            message.chat.id,
            extra_caption=extra or "",
            telegram_user_id=uid,
            telegram_username=message.from_user.username if message.from_user else None,
            anchor_message=message,
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
    u_row = user_res.data[0] if user_res.data else None
    direct_ok = u_row is not None and user_row_has_active_app_subscription(u_row)
    team_ok = team_member_has_active_team_access(telegram_id)

    if not direct_ok and not team_ok:
        supabase.table("auth_sessions").update({"status": "failed"}).eq("session_id", session_id).execute()
        await message.answer("❌ У вас нет активной подписки.")
        save_login_notification(telegram_id, "app_fail_no_subscription", False)
        await send_auth_log(
            "⚠️ Вход в приложение отклонён",
            [f"Причина: нет активной подписки", f"ID: `{telegram_id}`", f"Username: {username}"],
        )
        return

    if u_row is None and team_ok:
        u_row = ensure_user_row_for_login(telegram_id, username)
    if not u_row:
        supabase.table("auth_sessions").update({"status": "failed"}).eq("session_id", session_id).execute()
        await message.answer("❌ Не удалось подготовить профиль. Напишите в поддержку или откройте бота и нажмите Старт.")
        save_login_notification(telegram_id, "app_fail_no_subscription", False)
        return

    user = u_row

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
    await safe_edit_text(query.message,"✅ Вход на сайт выполнен успешно!")
    await query.answer()


# --- Пользователь: главное меню (не затрагивает авторизацию в приложении по deep-link) ---


@dp.callback_query(F.data == f"{UCB}:policies_ok")
async def cb_policies_accept(query: CallbackQuery):
    uid = query.from_user.id
    record_policies_acceptance(uid)
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    await show_user_main_menu(
        query.bot,
        query.message.chat.id,
        extra_caption=extra or "",
        telegram_user_id=uid,
        telegram_username=query.from_user.username if query.from_user else None,
        anchor_message=query.message,
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
        await safe_edit_text(query.message,
            text_policies_prompt_html(),
            parse_mode="HTML",
            reply_markup=kb_policies_accept(),
        )
        await query.answer("Канал подтверждён. Осталось принять условия.")
        return
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    await show_user_main_menu(
        query.bot,
        query.message.chat.id,
        extra_caption=extra or "",
        telegram_user_id=uid,
        telegram_username=query.from_user.username if query.from_user else None,
        anchor_message=query.message,
    )
    await query.answer("✅ Подписка подтверждена!")


@dp.callback_query(F.data == f"{UCB}:main")
async def cb_user_back_main(query: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = query.from_user.id
    chat_id = query.message.chat.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        prompt = text_policies_prompt_html()
        markup = kb_policies_accept()
        await safe_edit_text(
            query.message,
            prompt,
            parse_mode="HTML",
            reply_markup=markup,
        )
        await query.answer()
        return
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    await show_user_main_menu(
        query.bot,
        chat_id,
        extra_caption=extra or "",
        telegram_user_id=uid,
        telegram_username=query.from_user.username if query.from_user else None,
        anchor_message=query.message,
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:team")
async def cb_user_team_menu(query: CallbackQuery, state: FSMContext):
    if not await require_policies_or_block(query):
        return
    uid = query.from_user.id
    if not user_has_active_team_plan(uid):
        await query.answer("Доступно только при активной подписке TEAM.", show_alert=True)
        return
    await state.clear()
    team = team_get_by_owner(uid)
    if team and int(team.get("seats_purchased") or 0) > 0:
        text = format_team_dashboard_html(team, uid)
        await safe_edit_text(
            query.message,
            text,
            parse_mode="HTML",
            reply_markup=kb_team_dashboard(),
        )
        await query.answer()
        return
    if not team:
        await state.set_state(UserTeamStates.name)
        await safe_edit_text(
            query.message,
            "👥 <b>Создание команды</b>\n\n"
            "Введите <b>название команды</b> (от 2 символов).\n\n"
            "/cancel — прервать.",
            parse_mode="HTML",
            reply_markup=kb_team_setup_cancel(),
        )
        await query.answer()
        return
    await state.set_state(UserTeamStates.seats_count)
    await safe_edit_text(
        query.message,
        "👥 <b>Создание команды</b>\n\n"
        f"Команда: <b>{esc_html(str(team.get('name') or '—'))}</b>\n\n"
        "Введите <b>количество участников</b> целым числом от <b>1</b> до <b>100</b>.\n"
        f"Стоимость: <b>{TEAM_SEAT_PRICE_USD:g} USD</b> за каждого — оплата одним счётом в Crypto Bot.\n\n"
        "/cancel — прервать.",
        parse_mode="HTML",
        reply_markup=kb_team_setup_cancel(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:uniqueizer")
async def cb_user_uniqueizer_menu(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    uid = query.from_user.id
    if not user_has_active_uniqueizer_plan(uid):
        await safe_edit_text(
            query.message,
            text_uniqueizer_no_access_html(),
            parse_mode="HTML",
            reply_markup=kb_user_back_main(),
        )
        await query.answer()
        return
    await safe_edit_text(
        query.message,
        text_uniqueizer_screen_html(),
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_hub(),
    )
    await query.answer()


async def _uniqueizer_guard(query: CallbackQuery) -> bool:
    if not await require_policies_or_block(query):
        return False
    if not user_has_active_uniqueizer_plan(query.from_user.id):
        await query.answer("Нет доступа к Уникализатору.", show_alert=True)
        return False
    return True


@dp.callback_query(F.data == f"{UCB}:uzhm")
async def cb_uniqueizer_home(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    await state.clear()
    await safe_edit_text(
        query.message,
        text_uniqueizer_screen_html(),
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_hub(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:uzrun")
async def cb_uniqueizer_run_start(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    await state.set_state(UniqueizerStates.uniq_media)
    await state.set_data({})
    await safe_edit_text(
        query.message,
        "📦 <b>Уникализация</b>\n\n"
        "Отправьте <b>фото</b> или <b>видео</b> (можно как файл-документ).\n"
        f"Максимум ~<b>{UNIQUEIZER_MAX_FILE_MB} МБ</b>.\n\n"
        "Дальше при необходимости выберите шаблон и число копий.\n"
        "/cancel — отмена.",
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_cancel_hub(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:uztpl")
async def cb_uniqueizer_templates(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    uid = query.from_user.id
    u = user_get(uid)
    sel = _user_uniqueizer_selected_template_id(u)
    rows = uniqueizer_templates_list(uid)
    if not rows:
        await safe_edit_text(
            query.message,
            "📋 <b>Шаблоны</b>\n\n"
            "Пока нет ни одного шаблона — создайте первый.",
            parse_mode="HTML",
            reply_markup=kb_uniqueizer_tpl_empty(),
        )
        await query.answer()
        return
    lines = []
    for row in rows:
        tid = str(row.get("id") or "")
        mark = "✓ " if sel == tid else ""
        nm = esc_html((row.get("name") or "Шаблон")[:40])
        lines.append(f"• {mark}<b>{nm}</b>")
    body = "📋 <b>Шаблоны</b>\n\n" + "\n".join(lines)
    body += "\n\nНажмите шаблон, чтобы сделать его <b>активным</b> (✓)."
    await safe_edit_text(
        query.message,
        body,
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_tpl_list(uid, rows, sel),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:uznew")
async def cb_uniqueizer_tpl_new(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    await state.set_state(UniqueizerStates.tpl_name)
    await state.set_data({"uz_tpl_opts": []})
    await safe_edit_text(
        query.message,
        "➕ <b>Новый шаблон</b>\n\n"
        "Введите <b>название</b> шаблона одним сообщением.\n"
        "/cancel — отмена.",
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_cancel_hub(),
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{UCB}:uztgl:"))
async def cb_uniqueizer_tpl_toggle(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    if await state.get_state() != UniqueizerStates.tpl_build:
        await query.answer("Сначала создайте шаблон.", show_alert=True)
        return
    key = (query.data or "").split(":")[-1]
    if key not in UNIQUEIZER_OPTION_KEYS:
        await query.answer()
        return
    data = await state.get_data()
    opts: list[str] = list(data.get("uz_tpl_opts") or [])
    if key in opts:
        opts = [x for x in opts if x != key]
    else:
        opts = list(opts) + [key]
    await state.update_data(uz_tpl_opts=opts)
    try:
        await query.message.edit_reply_markup(reply_markup=kb_uniqueizer_tpl_build(set(opts)))
    except TelegramBadRequest:
        pass
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:uzfin")
async def cb_uniqueizer_tpl_finish(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    if await state.get_state() != UniqueizerStates.tpl_build:
        await query.answer()
        return
    data = await state.get_data()
    name = (data.get("uz_tpl_name") or "").strip() or "Шаблон"
    opts: list[str] = list(data.get("uz_tpl_opts") or [])
    uid = query.from_user.id
    tid_new = uniqueizer_template_insert_row(uid, name, opts)
    if not tid_new:
        await query.answer("Ошибка сохранения в базу (таблица uniqueizer_templates?).", show_alert=True)
        return
    user_set_uniqueizer_selected_template(uid, tid_new)
    await state.clear()
    await safe_edit_text(
        query.message,
        f"✅ Шаблон <b>{esc_html(name[:80])}</b> создан и выбран.\n"
        f"Опций: <b>{len(opts)}</b>.",
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_hub(),
    )
    await query.answer("Сохранено")


@dp.callback_query(F.data.startswith(f"{UCB}:uzsel:"))
async def cb_uniqueizer_tpl_select(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    parts = (query.data or "").split(":")
    if len(parts) < 3:
        await query.answer()
        return
    tpl_id = parts[2]
    uid = query.from_user.id
    row = uniqueizer_template_get_row(tpl_id)
    if not row or int(row.get("telegram_id") or 0) != uid:
        await query.answer("Чужой шаблон.", show_alert=True)
        return
    user_set_uniqueizer_selected_template(uid, tpl_id)
    cur = await state.get_state()
    if cur == UniqueizerStates.uniq_wait_tpl:
        data = await state.get_data()
        mpath = data.get("uz_media_path")
        is_vid = bool(data.get("uz_is_video"))
        if mpath and os.path.isfile(mpath):
            await state.set_state(UniqueizerStates.uniq_copies)
            await state.update_data(uz_template_id=tpl_id, uz_media_path=mpath, uz_is_video=is_vid)
            nm = esc_html((row.get("name") or "Шаблон")[:60])
            await safe_edit_text(
                query.message,
                f"✅ Шаблон <b>{nm}</b> выбран.\n\n"
                "Сколько <b>копий</b> сделать из загруженного файла?",
                parse_mode="HTML",
                reply_markup=kb_uniqueizer_copies_pick(),
            )
            await query.answer()
            return
        await state.set_state(UniqueizerStates.uniq_media)
        await query.answer("Файл устарел — загрузите медиа снова.", show_alert=True)
        return
    await cb_uniqueizer_templates(query, state)


@dp.callback_query(F.data.startswith(f"{UCB}:uzcp:"))
async def cb_uniqueizer_do_copies(query: CallbackQuery, state: FSMContext):
    if not await _uniqueizer_guard(query):
        return
    if await state.get_state() != UniqueizerStates.uniq_copies:
        await query.answer()
        return
    try:
        n = int((query.data or "").split(":")[-1])
    except ValueError:
        await query.answer()
        return
    n = max(1, min(UNIQUEIZER_MAX_COPIES, n))
    data = await state.get_data()
    mpath = data.get("uz_media_path")
    is_vid = bool(data.get("uz_is_video"))
    tpl_id = data.get("uz_template_id")
    if not mpath or not os.path.isfile(mpath):
        await query.answer("Файл не найден. Начните снова.", show_alert=True)
        await state.clear()
        return
    row = uniqueizer_template_get_row(str(tpl_id)) if tpl_id else None
    opts = _template_options_from_row(row) if row else []
    await query.answer("⏳ Обрабатываю…")
    try:
        zip_p, err = await asyncio.to_thread(
            _uniqueizer_process_to_zip,
            mpath,
            is_video=is_vid,
            options=opts,
            copies=n,
        )
        if err or not zip_p:
            await query.message.answer(
                f"⚠️ {esc_html(err or 'ошибка')}",
                parse_mode="HTML",
                reply_markup=kb_uniqueizer_hub(),
            )
        else:
            try:
                await query.message.answer_document(
                    FSInputFile(zip_p, filename="unique_pack.zip"),
                    caption=f"✅ Готово: <b>{n}</b> копий.",
                    parse_mode="HTML",
                )
            finally:
                try:
                    os.remove(zip_p)
                except OSError:
                    pass
    finally:
        try:
            os.remove(mpath)
        except OSError:
            pass
        await state.clear()
    await query.message.answer(
        "🎯 <b>Уникализатор</b>",
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_hub(),
    )


@dp.message(UniqueizerStates.tpl_name, F.text)
async def uniqueizer_tpl_name_message(message: Message, state: FSMContext):
    if not user_has_active_uniqueizer_plan(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    if raw.startswith("/"):
        return
    if len(raw) < 1 or len(raw) > 120:
        await message.answer("Название от 1 до 120 символов.")
        return
    await state.update_data(uz_tpl_name=raw, uz_tpl_opts=[])
    await state.set_state(UniqueizerStates.tpl_build)
    await message.answer(
        "Отметьте нужные <b>опции</b> (можно несколько), затем нажмите <b>Выбрать</b>.",
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_tpl_build(set()),
    )


@dp.message(UniqueizerStates.uniq_media, F.photo | F.video | F.video_note | F.document)
async def uniqueizer_receive_media(message: Message, state: FSMContext):
    if not user_has_active_uniqueizer_plan(message.from_user.id):
        await state.clear()
        return
    if not bot:
        return
    uid = message.from_user.id
    file_id: Optional[str] = None
    suffix = ".bin"
    is_video = False
    size_b: Optional[int] = None

    if message.photo:
        ph = message.photo[-1]
        file_id = ph.file_id
        suffix = ".jpg"
        size_b = getattr(ph, "file_size", None)
    elif message.video:
        file_id = message.video.file_id
        suffix = ".mp4"
        is_video = True
        size_b = getattr(message.video, "file_size", None)
    elif message.video_note:
        file_id = message.video_note.file_id
        suffix = ".mp4"
        is_video = True
        size_b = getattr(message.video_note, "file_size", None)
    elif message.document:
        doc = message.document
        mt = (doc.mime_type or "").lower()
        if not (mt.startswith("image/") or mt.startswith("video/")):
            await message.answer("Нужен файл с типом изображение или видео.")
            return
        file_id = doc.file_id
        is_video = mt.startswith("video/")
        suffix = ".mp4" if is_video else ".jpg"
        size_b = getattr(doc, "file_size", None)

    if not file_id:
        return
    if size_b is not None and size_b > UNIQUEIZER_MAX_FILE_MB * 1024 * 1024:
        await message.answer(f"Файл слишком большой (лимит {UNIQUEIZER_MAX_FILE_MB} МБ).")
        return

    path, err = await _download_tg_file(bot, file_id, suffix=suffix)
    if err or not path:
        await message.answer(f"Не удалось скачать файл: {esc_html(err or 'ошибка')}", parse_mode="HTML")
        return

    u = user_get(uid)
    sel = _user_uniqueizer_selected_template_id(u)
    if not sel:
        await state.set_state(UniqueizerStates.uniq_wait_tpl)
        await state.update_data(uz_media_path=path, uz_is_video=is_video)
        await message.answer(
            "Сначала выберите <b>шаблон</b> для уникализации.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Шаблоны", callback_data=f"{UCB}:uztpl")],
                    [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{UCB}:uzhm")],
                ]
            ),
        )
        return

    row = uniqueizer_template_get_row(sel)
    if not row or int(row.get("telegram_id") or 0) != uid:
        user_set_uniqueizer_selected_template(uid, None)
        await state.set_state(UniqueizerStates.uniq_wait_tpl)
        await state.update_data(uz_media_path=path, uz_is_video=is_video)
        await message.answer(
            "Активный шаблон недоступен. Выберите другой.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Шаблоны", callback_data=f"{UCB}:uztpl")],
                ]
            ),
        )
        return

    await state.set_state(UniqueizerStates.uniq_copies)
    await state.update_data(uz_media_path=path, uz_is_video=is_video, uz_template_id=sel)
    nm = esc_html((row.get("name") or "Шаблон")[:60])
    await message.answer(
        f"Шаблон: <b>{nm}</b>\n\n"
        "Сколько <b>копий</b> сделать?",
        parse_mode="HTML",
        reply_markup=kb_uniqueizer_copies_pick(),
    )


@dp.message(UniqueizerStates.uniq_media, F.text)
async def uniqueizer_media_hint_text(message: Message, state: FSMContext):
    if not user_has_active_uniqueizer_plan(message.from_user.id):
        await state.clear()
        return
    if (message.text or "").strip().startswith("/"):
        return
    await message.answer(
        "Пришлите <b>фото</b> или <b>видео</b> (или документ image/* / video/*).",
        parse_mode="HTML",
    )


@dp.callback_query(F.data == f"{UCB}:team_cancel")
async def cb_user_team_setup_cancel(query: CallbackQuery, state: FSMContext):
    if not await require_policies_or_block(query):
        return
    await state.clear()
    await safe_edit_text(
        query.message,
        "Действие прервано. Откройте «Команда» снова, чтобы продолжить.",
        parse_mode="HTML",
        reply_markup=kb_user_back_main(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:team_buy_seats")
async def cb_team_buy_seats(query: CallbackQuery, state: FSMContext):
    if not await require_policies_or_block(query):
        return
    uid = query.from_user.id
    if not user_has_active_team_plan(uid):
        await query.answer("Доступно только при активной подписке TEAM.", show_alert=True)
        return
    team = team_get_by_owner(uid)
    if not team or int(team.get("seats_purchased") or 0) < 1:
        await query.answer("Сначала завершите создание команды и первую оплату.", show_alert=True)
        return
    await state.set_state(UserTeamStates.add_seats_count)
    await safe_edit_text(
        query.message,
        "➕ <b>Дополнительные места</b>\n\n"
        f"Команда: <b>{esc_html(str(team.get('name') or '—'))}</b>\n\n"
        "Введите, <b>сколько мест</b> добавить — целое число от <b>1</b> до <b>100</b>.\n"
        f"Стоимость: <b>{TEAM_SEAT_PRICE_USD:g} USD</b> за каждое место — одним счётом в Crypto Bot.\n\n"
        "/cancel — прервать.",
        parse_mode="HTML",
        reply_markup=kb_team_setup_cancel(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:team_add_member")
async def cb_team_add_member(query: CallbackQuery, state: FSMContext):
    if not await require_policies_or_block(query):
        return
    uid = query.from_user.id
    if not user_has_active_team_plan(uid):
        await query.answer("Доступно только при активной подписке TEAM.", show_alert=True)
        return
    team = team_get_by_owner(uid)
    if not team or int(team.get("seats_purchased") or 0) < 1:
        await query.answer("Сначала завершите создание команды и оплату мест.", show_alert=True)
        return
    team_id = str(team["id"])
    seats = int(team.get("seats_purchased") or 0)
    if team_members_count(team_id) >= seats:
        await query.answer(
            "Нет свободных мест. Нажмите «Добавить участников (оплата)» и оплатите слоты.",
            show_alert=True,
        )
        return
    await state.set_state(UserTeamStates.add_member_id)
    free = seats - team_members_count(team_id)
    await safe_edit_text(
        query.message,
        "👤 <b>Добавление участника</b>\n\n"
        f"Свободных слотов: <b>{free}</b> из <b>{seats}</b>.\n\n"
        "Отправьте <b>числовой Telegram User ID</b> участника (узнать можно у @userinfobot и подобных).\n"
        "Можно несколько ID в одном сообщении через пробел или запятую.\n\n"
        "/cancel — прервать.",
        parse_mode="HTML",
        reply_markup=kb_team_setup_cancel(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:profile")
async def cb_user_profile_menu(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    await show_user_profile_screen(query)
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:reviews")
async def cb_user_reviews_placeholder(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    uid = query.from_user.id
    chat_id = query.message.chat.id
    markup = kb_user_back_main()
    photo = reviews_photo_for_new_message()
    doc = None if photo else reviews_document_for_new_message()
    cap_rev = os.environ.get(
        "TEXT_REVIEWS",
        "⭐ **Отзывы**\n\nЗдесь будет ссылка на отзывы или канал. Укажите `LINK_REVIEWS` в настройках.",
    )
    if photo:
        try:
            await user_shell_apply_photo(
                query.bot,
                uid,
                chat_id,
                query.message,
                cache_key="shell_reviews",
                photo_source=photo,
                caption=cap_rev,
                parse_mode="Markdown",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"reviews photo shell failed: {e}")
            await safe_edit_text(query.message, cap_rev, parse_mode="Markdown", reply_markup=markup)
    elif doc:
        try:
            await user_shell_apply_document(
                query.bot,
                uid,
                chat_id,
                query.message,
                cache_key="shell_reviews_doc",
                document_source=doc,
                caption=cap_rev,
                parse_mode="Markdown",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"reviews document shell failed: {e}")
            await safe_edit_text(query.message, cap_rev, parse_mode="Markdown", reply_markup=markup)
    else:
        await safe_edit_text(
            query.message,
            cap_rev,
            parse_mode="Markdown",
            reply_markup=markup,
        )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:buy")
async def cb_user_buy_placeholder(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    await show_user_tariffs_screen(query)


@dp.callback_query(F.data == f"{UCB}:tariffs_menu")
async def cb_user_tariffs_menu(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    await show_user_tariffs_screen(query)


_TARIFF_PLAN_ENV = {
    "s": "TEXT_PLAN_STANDART",
    "p": "TEXT_PLAN_PRO",
    "m": "TEXT_PLAN_MAX",
    "t": "TEXT_PLAN_TEAM",
    "u": "TEXT_PLAN_UNIQUEIZER",
}
# app_settings: полный HTML карточки тарифа (приоритет над TEXT_PLAN_* и _TARIFF_PLAN_DEFAULT).
_TARIFF_PLAN_SETTINGS_KEYS = {
    "s": "tariff_plan_html_s",
    "p": "tariff_plan_html_p",
    "m": "tariff_plan_html_m",
    "t": "tariff_plan_html_t",
    "u": "tariff_plan_html_u",
}
_TARIFF_PLAN_DEFAULT = {
    "s": (
        "<b>STANDART</b>\n\n"
        "• Покупка — <b>50 $</b>\n"
        "• Продление — <b>35 $</b>/мес\n\n"
        "• Интеграция с Dolphin{anty}\n"
        "• Статистика аккаунтов\n"
        "• Добавление до 20 аккаунтов\n"
        "• Базовая уникализация\n"
        "• Прогрев аккаунтов\n"
        "• Стандартная поддержка"
    ),
    "p": (
        "<b>PRO</b>\n\n"
        "• Покупка — <b>75 $</b>\n"
        "• Продление — <b>45 $</b>/мес\n\n"
        "• Интеграция с Dolphin{anty}\n"
        "• Статистика аккаунтов\n"
        "• Добавление до 30 аккаунтов\n"
        "• Базовая уникализация\n"
        "• Расширенная уникализация\n"
        "• Прогрев аккаунтов\n"
        "• Приоритетная поддержка"
    ),
    "m": (
        "<b>MAX</b>\n\n"
        "• Покупка — <b>100 $</b>\n"
        "• Продление — <b>65 $</b>/мес\n\n"
        "• Интеграция с Dolphin{anty}\n"
        "• Статистика аккаунтов\n"
        "• Добавление до 100 аккаунтов\n"
        "• Базовая уникализация\n"
        "• Расширенная уникализация\n"
        "• Прогрев аккаунтов\n"
        "• Приоритетная поддержка\n"
        "• Доступ к ИИ"
    ),
    "t": (
        "<b>TEAM</b>\n\n"
        "Командный тариф: доступ к софту для владельца и управление командой в боте "
        "(кнопка <b>«Команда»</b> в главном меню при активной подписке).\n\n"
        "Уточните цены и лимиты в поддержке или задайте <code>TEXT_PLAN_TEAM</code> в .env."
    ),
    "u": (
        "<b>UNIQUEIZER</b>\n\n"
        "Доступ только в <b>Telegram-боте</b> (раздел <b>«Уникализатор»</b>), "
        "срок хранится в <code>users.uniqueizer_until</code> — <b>не</b> даёт вход в приложение.\n\n"
        "Суммы: <code>CRYPTO_PAY_AMOUNT_UNIQUEIZER</code> / продление "
        "<code>CRYPTO_PAY_RENEW_AMOUNT_UNIQUEIZER</code> в .env."
    ),
}


def tariff_plan_body_html(code: str) -> str:
    if code not in _TARIFF_PLAN_ENV:
        return ""
    db_key = _TARIFF_PLAN_SETTINGS_KEYS.get(code)
    if db_key:
        raw = app_setting_get_value(db_key)
        if raw is not None and raw.strip():
            return raw.strip()
    env_key = _TARIFF_PLAN_ENV[code]
    return (os.environ.get(env_key) or "").strip() or _TARIFF_PLAN_DEFAULT[code]


_PLAN_INVOICE_LABEL = {"s": "STANDART", "p": "PRO", "m": "MAX", "t": "TEAM", "u": "UNIQUEIZER"}
# Имена без суффикса _P (в Railway «сырой» ввод переменных ломает строки вроде ..._P=...).
_PLAN_AMOUNT_ENV_KEYS = {
    "s": ("CRYPTO_PAY_AMOUNT_STANDART", "CRYPTO_PAY_AMOUNT_S"),
    "p": ("CRYPTO_PAY_AMOUNT_PRO", "CRYPTO_PAY_AMOUNT_P"),
    "m": ("CRYPTO_PAY_AMOUNT_MAX", "CRYPTO_PAY_AMOUNT_M"),
    "t": ("CRYPTO_PAY_AMOUNT_TEAM", "CRYPTO_PAY_AMOUNT_T"),
    "u": ("CRYPTO_PAY_AMOUNT_UNIQUEIZER", "CRYPTO_PAY_AMOUNT_U"),
}
_PLAN_AMOUNT_DEFAULT = {"s": "0.1", "p": "0.15", "m": "0.2", "t": "0.25", "u": "0.26"}
# Продление после истечения подписки (см. user_eligible_for_renewal_price).
_PLAN_RENEW_AMOUNT_ENV_KEYS = {
    "s": ("CRYPTO_PAY_RENEW_AMOUNT_STANDART", "CRYPTO_PAY_RENEW_AMOUNT_S"),
    "p": ("CRYPTO_PAY_RENEW_AMOUNT_PRO", "CRYPTO_PAY_RENEW_AMOUNT_P"),
    "m": ("CRYPTO_PAY_RENEW_AMOUNT_MAX", "CRYPTO_PAY_RENEW_AMOUNT_M"),
    "t": ("CRYPTO_PAY_RENEW_AMOUNT_TEAM", "CRYPTO_PAY_RENEW_AMOUNT_T"),
    "u": ("CRYPTO_PAY_RENEW_AMOUNT_UNIQUEIZER", "CRYPTO_PAY_RENEW_AMOUNT_U"),
}
_PLAN_RENEW_AMOUNT_DEFAULT = {"s": "0.08", "p": "0.12", "m": "0.18", "t": "0.2", "u": "0.21"}


def tariff_plan_invoice_label(code: str) -> str:
    return _PLAN_INVOICE_LABEL.get(code, code.upper())


def crypto_pay_amount_for_plan(code: str, *, renewal: bool = False) -> str:
    if renewal:
        for ek in _PLAN_RENEW_AMOUNT_ENV_KEYS.get(code, ()):
            v = (os.environ.get(ek) or "").strip()
            if v:
                return v
        return _PLAN_RENEW_AMOUNT_DEFAULT.get(code, _PLAN_AMOUNT_DEFAULT.get(code, "0.1"))
    for ek in _PLAN_AMOUNT_ENV_KEYS.get(code, ()):
        v = (os.environ.get(ek) or "").strip()
        if v:
            return v
    return _PLAN_AMOUNT_DEFAULT.get(code, "0.1")


_PLAN_SUB_DAYS_ENV_KEYS = {
    "s": ("SUBSCRIPTION_DAYS_STANDART", "SUBSCRIPTION_DAYS_S"),
    "p": ("SUBSCRIPTION_DAYS_PRO", "SUBSCRIPTION_DAYS_P"),
    "m": ("SUBSCRIPTION_DAYS_MAX", "SUBSCRIPTION_DAYS_M"),
    "t": ("SUBSCRIPTION_DAYS_TEAM", "SUBSCRIPTION_DAYS_T"),
    "u": ("SUBSCRIPTION_DAYS_UNIQUEIZER", "SUBSCRIPTION_DAYS_U"),
}
_PLAN_SUB_DAYS_DEFAULT = {"s": 30, "p": 30, "m": 30, "t": 30, "u": 30}


def subscription_days_for_plan(code: str) -> int:
    for ek in _PLAN_SUB_DAYS_ENV_KEYS.get(code, ()):
        v = (os.environ.get(ek) or "").strip()
        if v.isdigit():
            d = int(v)
            if d > 0:
                return d
    return max(1, int(_PLAN_SUB_DAYS_DEFAULT.get(code, 30)))


async def crypto_pay_create_invoice(
    *,
    asset: str,
    amount: str,
    description: str,
    payload: str,
) -> tuple[Optional[str], Optional[str]]:
    """Возвращает (bot_invoice_url, None) или (None, текст_ошибки)."""
    token = (os.environ.get("CRYPTO_PAY_API_TOKEN") or "").strip()
    if not token:
        return None, "Платежи не настроены: задайте CRYPTO_PAY_API_TOKEN в переменных окружения."
    base = "https://testnet-pay.crypt.bot" if CRYPTO_PAY_TESTNET else "https://pay.crypt.bot"
    api_url = f"{base}/api/createInvoice"
    params: dict[str, str] = {
        "asset": asset,
        "amount": amount,
        "description": description[:1024],
    }
    if payload:
        params["payload"] = payload[:4000]
    headers = {"Crypto-Pay-API-Token": token}
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            r = await client.get(api_url, params=params, headers=headers)
        try:
            data = r.json()
        except Exception:
            return None, f"Crypto Pay: ответ не JSON (HTTP {r.status_code})."
    except Exception as e:
        return None, f"Не удалось связаться с Crypto Pay: {e}"
    if not data.get("ok"):
        err = data.get("error") or {}
        name = err.get("name") or err.get("code") or str(data)
        return None, f"Crypto Pay: {name}"
    result = data.get("result") or {}
    pay_url = (
        result.get("bot_invoice_url")
        or result.get("mini_app_invoice_url")
        or result.get("web_app_invoice_url")
    )
    if not pay_url:
        return None, "Crypto Pay не вернул ссылку на оплату."
    return str(pay_url), None


@dp.callback_query(F.data.startswith(f"{UCB}:tariff:"))
async def cb_user_tariff_plan(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    code = (query.data or "").split(":")[-1]
    if code not in _TARIFF_PLAN_ENV:
        await query.answer()
        return
    sub_text = tariff_plan_body_html(code)
    uid = query.from_user.id
    if code == "u":
        if user_eligible_for_uniqueizer_renewal_price(uid):
            sub_text += "\n\n<i>По «Купить» — цена продления (Уникализатор).</i>"
    elif user_eligible_for_renewal_price(uid, code):
        sub_text += "\n\n<i>По «Купить» — цена продления (тот же тариф).</i>"
    else:
        u = user_get(uid)
        if u:
            sub_u = int(u.get("subscription_until") or 0)
            stored = (u.get("subscription_plan") or "").strip().lower()
            if (
                sub_u > 0
                and sub_u < int(time.time())
                and stored in _TARIFF_PLAN_ENV
                and stored != code
            ):
                sub_text += (
                    f"\n\n<i>Ранее: {tariff_plan_invoice_label(stored)}. "
                    f"Переход на {tariff_plan_invoice_label(code)} — полная стоимость.</i>"
                )
    markup = kb_tariff_subplan_detail(code)
    plan_img = tariff_plan_photo_for_plan(code)
    chat_id = query.message.chat.id
    if plan_img:
        try:
            await user_shell_apply_photo(
                query.bot,
                uid,
                chat_id,
                query.message,
                cache_key=f"shell_plan_{code}",
                photo_source=plan_img,
                caption=sub_text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        except Exception as e:
            print(f"tariff plan photo shell failed: {e}")
            await safe_edit_text(
                query.message,
                sub_text,
                parse_mode="HTML",
                reply_markup=markup,
            )
    else:
        await safe_edit_text(
            query.message,
            sub_text,
            parse_mode="HTML",
            reply_markup=markup,
        )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{UCB}:tbuy:"))
async def cb_tariff_buy_crypto_pay(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    code = (query.data or "").split(":")[-1]
    if code not in _TARIFF_PLAN_ENV:
        await query.answer()
        return
    tid = query.from_user.id
    label = tariff_plan_invoice_label(code)
    renewal = (
        user_eligible_for_uniqueizer_renewal_price(tid)
        if code == "u"
        else user_eligible_for_renewal_price(tid, code)
    )
    amount = crypto_pay_amount_for_plan(code, renewal=renewal)
    asset = CRYPTO_PAY_ASSET
    desc = (
        f"Neuro Uploader — продление {label}"
        if renewal
        else f"Neuro Uploader — подписка {label}"
    )
    payload = f"nu_plan={code};tg={tid};renew={'1' if renewal else '0'}"
    pay_url, err = await crypto_pay_create_invoice(
        asset=asset,
        amount=amount,
        description=desc,
        payload=payload,
    )
    body = tariff_plan_body_html(code)
    if err:
        fail_text = f"{body}\n\n⚠️ {html.escape(err)}"
        markup = kb_tariff_subplan_detail(code)
        if query.message.photo:
            await query.message.edit_caption(
                caption=fail_text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        else:
            await safe_edit_text(query.message,
                text=fail_text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        await query.answer("Не удалось создать счёт.", show_alert=True)
        return
    net_hint = (
        "Тестовая оплата через <b>@CryptoTestnetBot</b>."
        if CRYPTO_PAY_TESTNET
        else "Оплата через <b>@CryptoBot</b>."
    )
    price_note = (
        "<i>Тариф продления</i> (подписка ранее истекла)."
        if renewal
        else "<i>Основная цена</i> (новая подписка или активный период)."
    )
    # Экран после createInvoice: только выбор варианта оплаты.
    pay_text = "<b>Выберите вариант оплаты:</b>"
    markup = kb_tariff_after_invoice(code, pay_url)
    if query.message.photo:
        await query.message.edit_caption(
            caption=pay_text,
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        await safe_edit_text(query.message,
            text=pay_text,
            parse_mode="HTML",
            reply_markup=markup,
        )
    await query.answer()


async def build_referrals_user_html(uid: int) -> str:
    override = (os.environ.get("TEXT_REFERRALS") or "").strip()
    if override:
        return override
    if not bot:
        return "🎁 <b>Рефералы</b>\n\nБот недоступен."
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{uid}"
    pct, _ = get_user_referral_percent(uid)
    n_inv = 0
    try:
        res = supabase.table("users").select("telegram_id").eq("referred_by", uid).execute()
        n_inv = len(res.data or [])
    except Exception as e:
        print(f"referrals count: {e}")
    totals: defaultdict[str, float] = defaultdict(float)
    try:
        rr = (
            supabase.table("referral_rewards")
            .select("asset, reward_amount")
            .eq("referrer_telegram_id", uid)
            .execute()
        )
        for row in rr.data or []:
            a = str(row.get("asset") or "TON")
            try:
                totals[a] += float(row.get("reward_amount") or 0)
            except ValueError:
                pass
    except Exception as e:
        print(f"referral_rewards sum: {e}")
    lines = [
        "🎁 <b>Реферальная программа</b>",
        "",
        f"Вознаграждение: <b>{pct:g}%</b> с <b>каждой</b> оплаты каждого приглашённого по ссылке.",
        "",
        "Ваша ссылка:",
        f'<a href="{html.escape(link)}">{html.escape(link)}</a>',
        "",
        f"Перешло по ссылке: <b>{n_inv}</b>",
    ]
    if totals:
        parts = [f"{totals[k]:.6f} {k}" for k in sorted(totals.keys())]
        lines += ["", f"Начислено всего: <b>{html.escape(', '.join(parts))}</b>"]
    else:
        lines += ["", "Начислений пока нет — поделитесь ссылкой."]
    avail = await referral_available_usd(uid)
    lines += [
        "",
        f"Доступно к выводу (≈USD): <b>{avail:.2f}</b>",
        f"Минимум вывода: <b>{get_referral_min_withdraw_usd():g}</b> USD (чек в USDT).",
        f"Если новых начислений рефералки не было более <b>{max(1, REFERRAL_BURN_INACTIVE_DAYS)}</b> дней — баланс сгорает.",
    ]
    return "\n".join(lines)


async def user_shell_refresh_referrals(
    bot: Bot,
    uid: int,
    chat_id: int,
    *,
    prefix_html: str = "",
) -> None:
    """Обновить якорь экраном рефералов (после /cancel, вывода и т.п.)."""
    text = await build_referrals_user_html(uid)
    full = (prefix_html.rstrip() + "\n\n" + text) if prefix_html.strip() else text
    markup = kb_referrals_screen()
    img = referrals_photo_for_new_message()
    if img:
        await user_shell_apply_photo(
            bot,
            uid,
            chat_id,
            None,
            cache_key="shell_referrals",
            photo_source=img,
            caption=full,
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        if not await user_shell_try_edit_caption(
            bot, uid, chat_id, full, parse_mode="HTML", reply_markup=markup
        ):
            await bot.send_message(
                chat_id, full, parse_mode="HTML", reply_markup=markup
            )


@dp.callback_query(F.data == f"{UCB}:referrals")
async def cb_user_referrals(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    await show_user_referrals_screen(query)
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:refw")
async def cb_user_referral_withdraw_start(query: CallbackQuery, state: FSMContext):
    if not await require_policies_or_block(query):
        return
    tid = query.from_user.id
    avail = await referral_available_usd(tid)
    min_w = get_referral_min_withdraw_usd()
    if avail + 1e-9 < min_w:
        await query.answer(
            f"Недостаточно средств. Минимум {min_w:g} USD, доступно {avail:.2f} USD.",
            show_alert=True,
        )
        return
    await state.set_state(UserReferralWithdrawStates.amount)
    await safe_edit_text(query.message,
        "💸 <b>Вывод реферального баланса</b>\n\n"
        f"Доступно: <b>{avail:.2f}</b> USD (эквивалент).\n"
        f"Минимум: <b>{get_referral_min_withdraw_usd():g}</b> USD.\n\n"
        "Введите сумму вывода в долларах (например <code>10</code> или <code>25.5</code>).\n"
        "Чек будет в <b>USDT</b> и привязан к вашему Telegram.\n\n"
        "/cancel — отменить.",
        parse_mode="HTML",
        reply_markup=kb_referrals_withdraw_cancel(),
    )
    await query.answer()


@dp.callback_query(F.data == f"{UCB}:refw_cancel")
async def cb_user_referral_withdraw_cancel(query: CallbackQuery, state: FSMContext):
    if not await require_policies_or_block(query):
        return
    await state.clear()
    await show_user_referrals_screen(query)
    await query.answer("Отменено.")


@dp.message(UserReferralWithdrawStates.amount, F.text)
async def process_referral_withdraw_amount(message: Message, state: FSMContext):
    uid = message.from_user.id
    chat_id = message.chat.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await state.clear()
        return
    raw = (message.text or "").strip().replace(",", ".")
    try:
        amt = float(raw)
    except ValueError:
        await user_shell_answer_or_edit(
            message,
            text="Введите число, например <code>10</code> или <code>25.5</code>.",
            parse_mode="HTML",
            reply_markup=kb_referrals_withdraw_cancel(),
        )
        return
    min_w = get_referral_min_withdraw_usd()
    if amt < min_w - 1e-9:
        await user_shell_answer_or_edit(
            message,
            text=f"Минимальная сумма вывода — <b>{min_w:g}</b> USD.",
            parse_mode="HTML",
            reply_markup=kb_referrals_withdraw_cancel(),
        )
        return
    if amt <= 0:
        await user_shell_answer_or_edit(
            message,
            text="Сумма должна быть больше нуля.",
            parse_mode="HTML",
            reply_markup=kb_referrals_withdraw_cancel(),
        )
        return
    avail = await referral_available_usd(uid)
    if amt > avail + 1e-6:
        await user_shell_answer_or_edit(
            message,
            text=f"Недостаточно средств. Доступно: <b>{avail:.2f}</b> USD.",
            parse_mode="HTML",
            reply_markup=kb_referrals_withdraw_cancel(),
        )
        return
    url, err, check_id = await crypto_pay_create_check_usdt(amt, uid)
    if err or not url:
        await user_shell_answer_or_edit(
            message,
            text=esc_html(err or "Не удалось создать чек."),
            parse_mode="HTML",
            reply_markup=kb_referrals_withdraw_cancel(),
        )
        return
    now = int(time.time())
    try:
        supabase.table("referral_withdrawals").insert(
            {
                "telegram_id": uid,
                "amount_usd": amt,
                "asset": "USDT",
                "check_id": check_id,
                "bot_check_url": url,
                "created_at": now,
            }
        ).execute()
    except Exception as e:
        print(f"referral_withdrawals insert failed: {e}")
        await user_shell_answer_or_edit(
            message,
            text=(
                "Чек создан в Crypto Pay, но запись в базе не сохранилась. Обратитесь в поддержку.\n"
                f'<a href="{html.escape(url, quote=True)}">Открыть чек</a>'
            ),
            parse_mode="HTML",
            reply_markup=kb_referrals_withdraw_cancel(),
        )
        await state.clear()
        return
    await state.clear()
    done_caption = (
        "✅ <b>Чек готов</b>\n\n"
        f"Сумма: <b>{amt:.2f}</b> USD (USDT).\n"
        f'<a href="{html.escape(url, quote=True)}">Открыть чек в Crypto Bot</a>'
    )
    ref_img = referrals_photo_for_new_message()
    if ref_img:
        await user_shell_apply_photo(
            message.bot,
            uid,
            chat_id,
            None,
            cache_key="shell_referrals",
            photo_source=ref_img,
            caption=done_caption,
            parse_mode="HTML",
            reply_markup=kb_referrals_screen(),
        )
    else:
        await user_shell_answer_or_edit(
            message,
            text=done_caption,
            parse_mode="HTML",
            reply_markup=kb_referrals_screen(),
        )


@dp.message(UserTeamStates.name, F.text)
async def process_team_name(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await state.clear()
        return
    if not user_has_active_team_plan(uid):
        await state.clear()
        return
    raw = (message.text or "").strip()
    if len(raw) < 2:
        await user_shell_answer_or_edit(
            message,
            text="Название слишком короткое. Введите минимум 2 символа.",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    if team_get_by_owner(uid):
        await state.set_state(UserTeamStates.seats_count)
        await user_shell_answer_or_edit(
            message,
            text="Команда уже создана. Введите количество участников (целое число от 1 до 100).",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    if not team_create_for_owner(uid, raw):
        await user_shell_answer_or_edit(
            message,
            text="Не удалось сохранить команду. Проверьте, что в Supabase создана таблица teams (файл teams.sql).",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    await state.set_state(UserTeamStates.seats_count)
    await user_shell_answer_or_edit(
        message,
        text=(
            "👥 <b>Создание команды</b>\n\n"
            f"Команда: <b>{esc_html(raw)}</b>\n\n"
            "Введите <b>количество участников</b> целым числом от <b>1</b> до <b>100</b>.\n"
            f"Стоимость: <b>{TEAM_SEAT_PRICE_USD:g} USD</b> за каждого — одним счётом в Crypto Bot.\n\n"
            "/cancel — прервать."
        ),
        parse_mode="HTML",
        reply_markup=kb_team_setup_cancel(),
    )


@dp.message(UserTeamStates.seats_count, F.text)
async def process_team_seats(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await state.clear()
        return
    if not user_has_active_team_plan(uid):
        await state.clear()
        return
    raw = (message.text or "").strip()
    try:
        n = int(raw)
    except ValueError:
        await user_shell_answer_or_edit(
            message,
            text="Введите целое число от 1 до 100.",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    if n < 1 or n > 100:
        await user_shell_answer_or_edit(
            message,
            text="Число должно быть от 1 до 100.",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    team = team_get_by_owner(uid)
    if not team:
        await state.clear()
        await user_shell_answer_or_edit(
            message,
            text="Команда не найдена. Откройте раздел «Команда» в меню заново.",
            parse_mode=None,
            reply_markup=kb_user_back_main(),
        )
        return
    if int(team.get("seats_purchased") or 0) > 0:
        await state.clear()
        await user_shell_answer_or_edit(
            message,
            text="Места уже оплачены. Откройте «Команда» в меню.",
            parse_mode=None,
            reply_markup=kb_user_back_main(),
        )
        return
    team_id = str(team["id"])
    amount = team_bundle_crypto_amount_str(n)
    total_usd = float(n) * TEAM_SEAT_PRICE_USD
    payload = f"nu_kind=team_bundle;tg={uid};team={team_id};seats={n}"
    nm = str(team.get("name") or "")[:80]
    desc = f"Neuro Uploader — команда, {n} мест"
    if nm:
        desc = f"Neuro Uploader — «{nm}» — {n} мест(а)"
    pay_url, err = await crypto_pay_create_invoice(
        asset=CRYPTO_PAY_ASSET,
        amount=amount,
        description=desc,
        payload=payload,
    )
    await state.clear()
    if err:
        await user_shell_answer_or_edit(
            message,
            text=f"⚠️ {esc_html(err)}\n\nОткройте «Команда» и введите количество снова.",
            parse_mode="HTML",
            reply_markup=kb_user_back_main(),
        )
        return
    net_hint = (
        "Тестовая оплата через <b>@CryptoTestnetBot</b>."
        if CRYPTO_PAY_TESTNET
        else "Оплата через <b>@CryptoBot</b>."
    )
    await user_shell_answer_or_edit(
        message,
        text=(
            "<b>Счёт в Crypto Bot</b>\n\n"
            f"Мест: <b>{n}</b> × <b>{TEAM_SEAT_PRICE_USD:g} USD</b> ≈ <b>{total_usd:g} USD</b> "
            f"(сумма в счёте: <b>{esc_html(amount)} {esc_html(CRYPTO_PAY_ASSET)}</b>).\n"
            f"{net_hint}\n\n"
            "<b>Выберите вариант оплаты:</b>"
        ),
        parse_mode="HTML",
        reply_markup=kb_team_after_invoice(pay_url or ""),
    )


@dp.message(UserTeamStates.add_seats_count, F.text)
async def process_team_add_seats_count(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await state.clear()
        return
    if not user_has_active_team_plan(uid):
        await state.clear()
        return
    raw = (message.text or "").strip()
    try:
        n = int(raw)
    except ValueError:
        await user_shell_answer_or_edit(
            message,
            text="Введите целое число от 1 до 100.",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    if n < 1 or n > 100:
        await user_shell_answer_or_edit(
            message,
            text="Число должно быть от 1 до 100.",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    team = team_get_by_owner(uid)
    if not team or int(team.get("seats_purchased") or 0) < 1:
        await state.clear()
        await user_shell_answer_or_edit(
            message,
            text="Команда не найдена или нет оплаченных мест. Откройте «Команда» в меню.",
            parse_mode=None,
            reply_markup=kb_user_back_main(),
        )
        return
    team_id = str(team["id"])
    amount = team_bundle_crypto_amount_str(n)
    total_usd = float(n) * TEAM_SEAT_PRICE_USD
    payload = f"nu_kind=team_bundle;tg={uid};team={team_id};seats={n}"
    nm = str(team.get("name") or "")[:80]
    desc = f"Neuro Uploader — команда, +{n} мест"
    if nm:
        desc = f"Neuro Uploader — «{nm}» — +{n} мест(а)"
    pay_url, err = await crypto_pay_create_invoice(
        asset=CRYPTO_PAY_ASSET,
        amount=amount,
        description=desc,
        payload=payload,
    )
    await state.clear()
    if err:
        await user_shell_answer_or_edit(
            message,
            text=f"⚠️ {esc_html(err)}\n\nПовторите ввод или откройте «Команда».",
            parse_mode="HTML",
            reply_markup=kb_user_back_main(),
        )
        return
    net_hint = (
        "Тестовая оплата через <b>@CryptoTestnetBot</b>."
        if CRYPTO_PAY_TESTNET
        else "Оплата через <b>@CryptoBot</b>."
    )
    await user_shell_answer_or_edit(
        message,
        text=(
            "<b>Счёт в Crypto Bot</b> (доп. места)\n\n"
            f"Мест: <b>{n}</b> × <b>{TEAM_SEAT_PRICE_USD:g} USD</b> ≈ <b>{total_usd:g} USD</b> "
            f"(сумма в счёте: <b>{esc_html(amount)} {esc_html(CRYPTO_PAY_ASSET)}</b>).\n"
            f"{net_hint}\n\n"
            "<b>Выберите вариант оплаты:</b>"
        ),
        parse_mode="HTML",
        reply_markup=kb_team_after_invoice(pay_url or ""),
    )


@dp.message(UserTeamStates.add_member_id, F.text)
async def process_team_add_member_id(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        await state.clear()
        return
    if not user_has_active_team_plan(uid):
        await state.clear()
        return
    team = team_get_by_owner(uid)
    if not team or int(team.get("seats_purchased") or 0) < 1:
        await state.clear()
        await user_shell_answer_or_edit(
            message,
            text="Команда не найдена. Откройте «Команда» в меню.",
            parse_mode=None,
            reply_markup=kb_user_back_main(),
        )
        return
    team_id = str(team["id"])
    seats = int(team.get("seats_purchased") or 0)
    raw = (message.text or "").strip()
    tokens = [x for x in re.split(r"[\s,;]+", raw) if x.strip()]
    if not tokens:
        await user_shell_answer_or_edit(
            message,
            text="Введите один или несколько числовых Telegram ID.",
            parse_mode=None,
            reply_markup=kb_team_setup_cancel(),
        )
        return
    occupied = team_members_count(team_id)
    added: list[int] = []
    errs: list[str] = []
    for tok in tokens:
        try:
            mid = int(tok.strip())
        except ValueError:
            errs.append(f"Не число: <code>{esc_html(tok[:40])}</code>")
            continue
        ok, code = team_try_add_member(
            team_id, uid, mid, occupied_slots=occupied, seats_total=seats
        )
        if ok:
            added.append(mid)
            occupied += 1
        elif code == "dup":
            errs.append(f"Уже в списке: <code>{mid}</code>")
        elif code == "owner":
            errs.append("Нельзя добавить свой собственный ID.")
        elif code == "full":
            errs.append("Лимит мест исчерпан — оплатите дополнительные слоты.")
            break
        elif code == "not_owner":
            await state.clear()
            await user_shell_answer_or_edit(
                message,
                text="Ошибка доступа.",
                parse_mode=None,
                reply_markup=kb_user_back_main(),
            )
            return
        else:
            errs.append(
                f"Не добавлен <code>{mid}</code>: проверьте таблицу team_members (SQL) или повторите позже."
            )
    parts = []
    if added:
        parts.append("✅ Добавлено: " + ", ".join(f"<code>{x}</code>" for x in added))
    if errs:
        parts.append("\n".join(errs))
    free_now = max(0, seats - occupied)
    parts.append(
        f"\nСвободных слотов: <b>{free_now}</b> из <b>{seats}</b>.\n"
        "Отправьте ещё ID или откройте «Команда» для списка."
    )
    await user_shell_answer_or_edit(
        message,
        text="\n\n".join(parts),
        parse_mode="HTML",
        reply_markup=kb_team_setup_cancel() if free_now > 0 else kb_team_dashboard(),
    )
    if free_now <= 0:
        await state.clear()


@dp.callback_query(F.data == f"{UCB}:support")
async def cb_user_support_placeholder(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    text = os.environ.get(
        "TEXT_SUPPORT",
        "💬 **Поддержка**\n\nОпишите проблему в ответе на это сообщение или задайте `LINK_SUPPORT` "
        "(например ссылка на чат с менеджером).",
    )
    await safe_edit_text(query.message,
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
        "• Сброс и ручная установка HWID\n"
        "• Рассылка всем пользователям из базы\n\n"
        "Команды: `/add ID дней`, `/reset ID`\n"
        "Файлы после оплаты: `/set_app_zip` → затем отправьте zip; `/set_app_txt` → затем txt.",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if current == UserReferralWithdrawStates.amount:
        await state.clear()
        await user_shell_refresh_referrals(
            message.bot,
            message.from_user.id,
            message.chat.id,
            prefix_html="<i>Отменено.</i>",
        )
        return
    if current in (
        UserTeamStates.name,
        UserTeamStates.seats_count,
        UserTeamStates.add_seats_count,
        UserTeamStates.add_member_id,
    ):
        await state.clear()
        await show_user_main_menu(
            message.bot,
            message.chat.id,
            extra_caption="Действие с командой отменено.",
            telegram_user_id=message.from_user.id,
            telegram_username=message.from_user.username if message.from_user else None,
            anchor_message=None,
        )
        return
    cur_uz = await state.get_state()
    if cur_uz and str(cur_uz).startswith("UniqueizerStates"):
        await state.clear()
        await message.answer(
            "Уникализатор: отменено.",
            reply_markup=kb_uniqueizer_hub(),
        )
        return
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено.", reply_markup=kb_main_admin())


@dp.message(Command("set_app_zip"))
async def cmd_set_app_zip(message: Message, state: FSMContext):
    """Шаг 1: команда, затем одним сообщением пришлите файл (zip) как документ."""
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.app_zip_wait)
    await message.answer(
        "Отправьте **следующим сообщением** файл архива как **документ** (не фото).\n"
        "Можно переслать из канала. /cancel — отмена.",
        parse_mode="Markdown",
    )


@dp.message(Command("set_app_txt"))
async def cmd_set_app_txt(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.app_txt_wait)
    await message.answer(
        "Отправьте **следующим сообщением** файл `.txt` как **документ**.\n"
        "Можно переслать из канала. /cancel — отмена.",
        parse_mode="Markdown",
    )


@dp.message(AdminStates.app_zip_wait, F.document)
async def admin_receive_app_zip_document(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    doc = message.document
    if not doc:
        return
    try:
        app_setting_upsert_value("app_zip_file_id", doc.file_id)
    except Exception:
        await message.answer("Не удалось сохранить в Supabase. Проверьте таблицу app_settings.")
        return
    await state.clear()
    await message.answer("✅ APP_ZIP_FILE_ID сохранён. После оплаты пользователи получат этот архив.")


@dp.message(AdminStates.app_txt_wait, F.document)
async def admin_receive_app_txt_document(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    doc = message.document
    if not doc:
        return
    try:
        app_setting_upsert_value("app_txt_file_id", doc.file_id)
    except Exception:
        await message.answer("Не удалось сохранить в Supabase. Проверьте таблицу app_settings.")
        return
    await state.clear()
    await message.answer("✅ APP_TXT_FILE_ID сохранён.")


@dp.message(F.document)
async def admin_set_app_file_id_from_document(message: Message):
    """Админ: отправьте документ в боте с caption:
    /set_app_zip — app.zip
    /set_app_txt — app.txt
    Бот сохранит file_id в Supabase app_settings.
    """
    if not is_admin(message.from_user.id):
        return
    doc = message.document
    if not doc:
        return
    caption = (message.caption or "").strip()
    if not caption:
        return
    if caption.startswith("/set_app_zip"):
        app_setting_upsert_value("app_zip_file_id", doc.file_id)
        await message.answer("✅ APP_ZIP_FILE_ID обновлён (file_id сохранён).")
        return
    if caption.startswith("/set_app_txt"):
        app_setting_upsert_value("app_txt_file_id", doc.file_id)
        await message.answer("✅ APP_TXT_FILE_ID обновлён (file_id сохранён).")
        return


@dp.callback_query(F.data == f"{CB}:noop")
async def cb_noop(query: CallbackQuery):
    await query.answer()


@dp.callback_query(F.data == f"{CB}:broadcast")
async def cb_broadcast_start(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await state.set_state(AdminStates.broadcast_wait)
    lim = int((os.environ.get("BROADCAST_MAX_USERS") or "10000").strip() or "10000")
    lim = max(1, min(lim, 50_000))
    await query.message.answer(
        "📢 **Рассылка**\n\n"
        "Отправьте **следующим сообщением** то, что получат пользователи из базы "
        f"(до **{lim}** записей в `users`). Подойдёт текст, фото с подписью, документ — "
        "можно **переслать** сообщение.\n\n"
        "Отправка идёт через копирование сообщения, формат сохраняется.\n\n"
        "/cancel — отмена.",
        parse_mode="Markdown",
    )
    await query.answer()


@dp.message(AdminStates.broadcast_wait)
async def admin_broadcast_capture(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    if message.text and message.text.strip().startswith("/"):
        if message.text.strip().split()[0] in ("/cancel",):
            await state.clear()
            await message.answer("Отменено.", reply_markup=kb_main_admin())
        else:
            await message.answer(
                "Сейчас режим рассылки. Отправьте сообщение для рассылки или **/cancel**.",
                parse_mode="Markdown",
            )
        return
    await state.update_data(bc_from_chat=message.chat.id, bc_message_id=message.message_id)
    await state.set_state(AdminStates.broadcast_confirm)
    n = len(users_telegram_ids_for_broadcast())
    await message.answer(
        f"Сообщение принято. Получателей в выборке: **{n}**.\n\nОтправить всем?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Отправить всем", callback_data=f"{CB}:bc_ok")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{CB}:bc_no")],
            ]
        ),
    )


@dp.callback_query(F.data == f"{CB}:bc_no", StateFilter(AdminStates.broadcast_confirm))
async def cb_broadcast_cancel(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await query.answer("Отменено")
    await query.message.edit_text("Рассылка отменена.")


@dp.callback_query(F.data == f"{CB}:bc_ok", StateFilter(AdminStates.broadcast_confirm))
async def cb_broadcast_run(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    if not bot:
        await state.clear()
        await query.answer("Бот недоступен", show_alert=True)
        return
    data = await state.get_data()
    from_chat = data.get("bc_from_chat")
    msg_id = data.get("bc_message_id")
    if not from_chat or not msg_id:
        await state.clear()
        await query.answer("Нет данных — начните снова", show_alert=True)
        return
    await query.answer("Отправляем…")
    await state.clear()
    ids = users_telegram_ids_for_broadcast()
    ok = fail = 0
    delay = float((os.environ.get("BROADCAST_SEND_DELAY_SEC") or "0.04").strip() or "0.04")
    delay = max(0.02, min(delay, 2.0))
    for uid in ids:
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=from_chat, message_id=msg_id)
            ok += 1
        except Exception as e:
            fail += 1
            if fail <= 5:
                print(f"broadcast to {uid}: {e}")
        await asyncio.sleep(delay)
    await query.message.answer(
        "✅ **Рассылка завершена**\n\n"
        f"• Успешно: **{ok}**\n"
        f"• Ошибок: **{fail}** (часто — пользователь не нажимал /start или заблокировал бота)\n"
        f"• В выборке: **{len(ids)}**",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


@dp.callback_query(F.data == f"{CB}:menu")
async def cb_menu(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await safe_edit_text(query.message,
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
    await safe_edit_text(query.message,
        "📖 **Справка**\n\n"
        "• **Пользователи** — список из базы, пагинация.\n"
        "• **+7 / +30 / +365** — подписка приложения от текущего момента.\n"
        "• **Свой срок** — дни подписки приложения (`0` — сразу истекает).\n"
        "• **🎯 +7 / +30 / +365** и **Уник. свой срок** — доступ к Уникализатору в боте (то же правило, отдельная колонка).\n"
        "• **Сброс HWID** — пользователь сможет войти с нового ПК.\n"
        "• **Задать HWID** — вставьте **64-символьный** hex (как в приложении).\n"
        "• **Реферальный %** — доля с каждой оплаты приглашённого; там же **мин. вывод** реф. баланса.\n"
        "• **Реф. баланс** в карточке пользователя — задать **доступную к выводу** сумму (USD).\n"
        "• **Описания тарифов** — HTML карточки STANDART/PRO/MAX/TEAM/UNIQUEIZER (цены в тексте); списание в Crypto Pay — .env.\n"
        "• **Рассылка** — одно сообщение всем из `users` (копирование: текст, фото, документ и т.д.).\n\n"
        "/cancel — отменить ввод.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{CB}:menu")]]
        ),
    )
    await query.answer()


@dp.callback_query(F.data == f"{CB}:tpl")
async def cb_admin_tariff_plans_menu(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    intro = (
        "💳 <b>Описания тарифов</b>\n\n"
        "Текст карточки тарифа в боте — <b>HTML</b> (как в коде: <code>&lt;b&gt;</code>, "
        "<code>&lt;i&gt;</code>, списки). "
        "Цены в карточке задаёте в тексте; реальная сумма списания в Crypto Pay — переменные "
        "<code>CRYPTO_PAY_*</code> на сервере.\n\n"
        "Выберите тариф:"
    )
    await safe_edit_text(
        query.message,
        intro,
        parse_mode="HTML",
        reply_markup=kb_admin_tariff_plans(),
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:tpe:"))
async def cb_admin_tariff_plan_edit_start(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) < 3:
        await query.answer("Ошибка")
        return
    code = parts[-1]
    if code not in _TARIFF_PLAN_ENV:
        await query.answer()
        return
    label = _PLAN_INVOICE_LABEL.get(code, code.upper())
    await state.set_state(AdminStates.tariff_plan_text)
    await state.update_data(tariff_plan_code=code)
    full = tariff_plan_body_html(code)
    preview = esc_html(full[:4000]) + ("…" if len(full) > 4000 else "")
    await safe_edit_text(
        query.message,
        f"✏️ <b>{label}</b>\n\n"
        "Отправьте <b>следующим сообщением</b> полный текст карточки в HTML.\n"
        "Цены в карточке — только в тексте; списание в Crypto Pay отсюда не меняется.\n\n"
        "/cancel — отмена.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="♻️ Сброс к .env/дефолту",
                        callback_data=f"{CB}:tpr:{code}",
                    )
                ],
                [InlineKeyboardButton(text="📋 К списку тарифов", callback_data=f"{CB}:tpl")],
                [InlineKeyboardButton(text="⬅️ Админ-меню", callback_data=f"{CB}:menu")],
            ]
        ),
    )
    await query.message.answer(
        f"<b>Текущий текст (превью, экранировано):</b>\n<pre>{preview}</pre>",
        parse_mode="HTML",
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:tpr:"))
async def cb_admin_tariff_plan_reset(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    code = (query.data or "").split(":")[-1]
    if code not in _TARIFF_PLAN_SETTINGS_KEYS:
        await query.answer()
        return
    try:
        app_setting_delete_key(_TARIFF_PLAN_SETTINGS_KEYS[code])
    except Exception:
        await query.answer("Ошибка БД", show_alert=True)
        return
    await state.clear()
    label = _PLAN_INVOICE_LABEL.get(code, code.upper())
    await query.answer(f"Сброшено: {label}")
    await safe_edit_text(
        query.message,
        "💳 <b>Описания тарифов</b>\n\n"
        f"Для <b>{label}</b> снова используются <code>TEXT_PLAN_*</code> из окружения или встроенный дефолт.\n\n"
        "Выберите тариф:",
        parse_mode="HTML",
        reply_markup=kb_admin_tariff_plans(),
    )


@dp.message(AdminStates.tariff_plan_text, F.text)
async def process_admin_tariff_plan_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    code = data.get("tariff_plan_code")
    if code not in _TARIFF_PLAN_SETTINGS_KEYS:
        await state.clear()
        return
    raw = (message.text or "").strip()
    if not raw:
        await message.answer("Пустой текст не сохранён.")
        return
    if len(raw) > 12000:
        await message.answer("Слишком длинно (макс. 12000 символов).")
        return
    key = _TARIFF_PLAN_SETTINGS_KEYS[code]
    try:
        app_setting_upsert_value(key, raw)
    except Exception:
        await message.answer("Не удалось сохранить в Supabase (app_settings).")
        return
    await state.clear()
    label = _PLAN_INVOICE_LABEL.get(code, code.upper())
    await message.answer(
        f"✅ Описание тарифа <b>{label}</b> сохранено в базе.",
        parse_mode="HTML",
        reply_markup=kb_main_admin(),
    )


@dp.message(AdminStates.tariff_plan_text)
async def process_admin_tariff_plan_text_other(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("Нужен текст одним сообщением (HTML). /cancel — отмена.")


@dp.callback_query(F.data == f"{CB}:refpct")
async def cb_admin_referral_menu(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await safe_edit_text(query.message,
        referral_admin_settings_markdown(),
        parse_mode="Markdown",
        reply_markup=kb_referral_admin(),
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:refset:"))
async def cb_admin_referral_set(query: CallbackQuery):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) < 3:
        await query.answer("Ошибка")
        return
    try:
        pct = float(":".join(parts[2:]))
    except ValueError:
        await query.answer("Ошибка")
        return
    set_referral_percent(pct)
    await query.answer(f"Установлено {pct:g}%")
    await safe_edit_text(query.message,
        referral_admin_settings_markdown(),
        parse_mode="Markdown",
        reply_markup=kb_referral_admin(),
    )


@dp.callback_query(F.data.startswith(f"{CB_REF_MIN}:"))
async def cb_admin_referral_min_preset(query: CallbackQuery):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) < 3:
        await query.answer("Ошибка")
        return
    try:
        amt = float(":".join(parts[2:]))
    except ValueError:
        await query.answer("Ошибка")
        return
    set_referral_min_withdraw_usd(amt)
    await query.answer(f"Мин. вывод {amt:g} USD")
    await safe_edit_text(query.message,
        referral_admin_settings_markdown(),
        parse_mode="Markdown",
        reply_markup=kb_referral_admin(),
    )


@dp.callback_query(F.data == f"{CB}:rwmincust")
async def cb_admin_referral_min_custom(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.referral_min_withdraw)
    await query.message.answer(
        "Введите **минимум вывода** реферального баланса в **USD** "
        f"(сейчас **{get_referral_min_withdraw_usd():g}**), например `10` или `25.5`.\n"
        "Допустимо от **0.01** до **1 000 000**.\n/cancel — отмена.",
        parse_mode="Markdown",
    )
    await query.answer()


@dp.callback_query(F.data == f"{CB}:refcust")
async def cb_admin_referral_custom(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.referral_percent)
    await query.message.answer(
        "Введите процент от **0** до **100** (можно `12.5`).\n/cancel — отмена.",
        parse_mode="Markdown",
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

    await safe_edit_text(query.message,text, parse_mode="HTML", reply_markup=markup)
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


@dp.callback_query(F.data.startswith(f"{CB}:uzq:"))
async def cb_quick_uniqueizer_sub(query: CallbackQuery, state: FSMContext):
    """Выдача Уникализатора (бот): a:uzq:telegram_id:days — срок от _сейчас_, как a:q: для приложения."""
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
        supabase.table("users").update({"uniqueizer_until": until}).eq("telegram_id", tid).execute()
    else:
        supabase.table("users").insert(
            {
                "telegram_id": tid,
                "subscription_until": 0,
                "uniqueizer_until": until,
                "hwid": None,
            }
        ).execute()

    await edit_user_card(query, tid, f"✅ Уник. +{days} дн.")


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


@dp.callback_query(F.data.startswith(f"{CB}:uzs:"))
async def cb_uniqueizer_sub_prompt(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return

    await state.set_state(AdminStates.uniqueizer_sub_days)
    await state.update_data(telegram_id=tid)
    await query.message.answer(
        f"Введите **количество дней** доступа к **Уникализатору** (бот) от _сейчас_ для `{tid}`.\n"
        f"`0` — доступ сразу истекает.\n/cancel — отмена.",
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


@dp.message(AdminStates.uniqueizer_sub_days, F.text)
async def process_uniqueizer_sub_days(message: Message, state: FSMContext):
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
        supabase.table("users").update({"uniqueizer_until": until}).eq("telegram_id", tid).execute()
    else:
        supabase.table("users").insert(
            {
                "telegram_id": tid,
                "subscription_until": 0,
                "uniqueizer_until": until,
                "hwid": None,
            }
        ).execute()

    await state.clear()
    await message.answer(
        f"✅ Уникализатор для `{tid}` до: **{fmt_ts(until)}** ({days} дн.)",
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


@dp.callback_query(F.data.startswith(f"{CB}:urp:"))
async def cb_user_referral_percent_prompt(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return
    pct, is_override = get_user_referral_percent(tid)
    origin = "персональный" if is_override else "по умолчанию"
    await state.set_state(AdminStates.referral_percent)
    await state.update_data(referral_scope="user", telegram_id=tid)
    await query.message.answer(
        f"Введите персональный реферальный % для `{tid}` (0..100).\n"
        f"Сейчас: **{pct:g}%** ({origin}).\n"
        "Чтобы убрать персональный % и вернуть значение по умолчанию — кнопка «♻️ По умолчанию» в карточке.\n"
        "/cancel — отмена.",
        parse_mode="Markdown",
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:urc:"))
async def cb_user_referral_percent_clear(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return
    try:
        set_user_referral_percent(tid, None)
    except Exception as e:
        await query.answer(f"Ошибка: {e}", show_alert=True)
        return
    await edit_user_card(query, tid, "♻️ Возвращён % по умолчанию")


@dp.callback_query(F.data.startswith(f"{CB}:urb:"))
async def cb_user_referral_balance_prompt(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return
    if not user_get(tid):
        await query.answer("Пользователя нет в users", show_alert=True)
        return
    earned = await referral_rewards_total_usd(tid)
    withdrawn = await referral_withdrawals_total_usd(tid)
    adj = get_user_referral_balance_adjustment_usd(tid)
    base = earned - withdrawn
    cur_avail = max(0.0, base + adj)
    await state.set_state(AdminStates.referral_balance_set)
    await state.update_data(telegram_id=tid)
    await query.message.answer(
        f"Укажите **сумму в USD, доступную к выводу** для `{tid}` (как увидит рефовод).\n\n"
        f"Сейчас: **{cur_avail:.2f}** USD  "
        f"(из начислений и выводов: **{base:.2f}**, корр.: **{adj:+.2f}**).\n\n"
        "**0** — цель «доступно к выводу» = 0 USD.\n"
        "Сбросить только корректировку (вернуться к сумме из начислений и выводов) — **↩️ Корр. 0** в карточке.\n\n"
        "/cancel — отмена.",
        parse_mode="Markdown",
    )
    await query.answer()


@dp.callback_query(F.data.startswith(f"{CB}:urbr:"))
async def cb_user_referral_balance_adjustment_reset(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    try:
        tid = int(query.data.split(":")[2])
    except (IndexError, ValueError):
        await query.answer("Ошибка ID")
        return
    if not user_get(tid):
        await query.answer("Пользователя нет в users", show_alert=True)
        return
    try:
        set_user_referral_balance_adjustment_usd(tid, 0.0)
    except Exception as e:
        await query.answer(str(e), show_alert=True)
        return
    await edit_user_card(query, tid, "↩️ Корректировка реф. баланса = 0")


@dp.message(AdminStates.referral_balance_set, F.text)
async def process_referral_balance_set(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    tid = data.get("telegram_id")
    if not tid:
        await state.clear()
        await message.answer("Сессия сброшена. /admin")
        return
    raw = message.text.strip().replace(",", ".")
    try:
        target = float(raw)
    except ValueError:
        await message.answer("Нужно число USD, например `100` или `25.5`.")
        return
    if target < 0 or target > 10_000_000:
        await message.answer("Допустимо 0 … 10 000 000 USD.")
        return
    try:
        earned = await referral_rewards_total_usd(int(tid))
        withdrawn = await referral_withdrawals_total_usd(int(tid))
        adj = target - (earned - withdrawn)
        set_user_referral_balance_adjustment_usd(int(tid), adj)
    except Exception as e:
        await message.answer(f"Ошибка: {e}")
        return
    shown = max(0.0, earned - withdrawn + adj)
    await state.clear()
    await message.answer(
        f"✅ Пользователь `{tid}`: **доступно к выводу {shown:.2f}** USD.\n"
        f"Корректировка к базе: **{adj:+.2f}** USD.",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


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


@dp.message(AdminStates.referral_percent, F.text)
async def process_referral_percent_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = message.text.strip().replace(",", ".")
    try:
        pct = float(raw)
    except ValueError:
        await message.answer("Нужно число от 0 до 100.")
        return
    if pct < 0 or pct > 100:
        await message.answer("Допустимо 0…100.")
        return
    data = await state.get_data()
    scope = data.get("referral_scope")
    if scope == "user":
        tid = data.get("telegram_id")
        if not tid:
            await state.clear()
            await message.answer("Сессия сброшена. /admin")
            return
        try:
            set_user_referral_percent(int(tid), pct)
        except Exception as e:
            await message.answer(f"Не удалось сохранить персональный %: {e}")
            return
        await state.clear()
        await message.answer(
            f"✅ Персональный реферальный % для `{tid}`: **{pct:g}%**",
            parse_mode="Markdown",
            reply_markup=kb_main_admin(),
        )
        return

    set_referral_percent(pct)
    await state.clear()
    await message.answer(
        f"✅ Процент реферальной программы: **{pct:g}%**",
        parse_mode="Markdown",
        reply_markup=kb_main_admin(),
    )


@dp.message(AdminStates.referral_min_withdraw, F.text)
async def process_referral_min_withdraw_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = message.text.strip().replace(",", ".")
    try:
        amt = float(raw)
    except ValueError:
        await message.answer("Нужно число (USD), например `10` или `12.5`.")
        return
    if amt < 0.01 or amt > 1_000_000:
        await message.answer("Допустимо 0.01 … 1 000 000 USD.")
        return
    try:
        set_referral_min_withdraw_usd(amt)
    except Exception as e:
        await message.answer(f"Не удалось сохранить: {e}")
        return
    await state.clear()
    await message.answer(
        f"✅ Минимальная сумма вывода рефералов: **{get_referral_min_withdraw_usd():g}** USD",
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
