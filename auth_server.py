"""
Сервер авторизации + Telegram-бот Neuro Uploader.
API для приложения + админ-панель в Telegram (только ADMIN_ID).
"""
import asyncio
import html
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from hmac import HMAC, compare_digest
from typing import Optional

import httpx
import uvicorn
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
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
# Картинка раздела «Тарифы»: локальный файл (приоритет) или URL по HTTPS
TARIFFS_IMAGE_PATH = (os.environ.get("TARIFFS_IMAGE_PATH") or "").strip()
TARIFFS_IMAGE_URL = (os.environ.get("TARIFFS_IMAGE_URL") or "").strip()
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
REFERRAL_PERCENT_DEFAULT = float((os.environ.get("REFERRAL_PERCENT_DEFAULT") or "10").strip())

# Прокси для десктопа Neuro Uploader: ключи не в клиенте, только на сервере (.env)
SADCAPTCHA_LICENSE_KEY = (os.environ.get("SADCAPTCHA_LICENSE_KEY") or "").strip()
ANTHROPIC_API_KEY = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
NU_CLAUDE_MODEL = (os.environ.get("NU_CLAUDE_MODEL") or "claude-3-5-sonnet-20241022").strip()

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
    hwid_value = State()
    referral_percent = State()


def is_admin(user_id: int) -> bool:
    return bool(ADMIN_ID) and user_id == ADMIN_ID


def nu_require_active_subscription(telegram_id: int) -> None:
    """Доступ к прокси SadCaptcha / Claude только при активной подписке."""
    user_res = supabase.table("users").select("subscription_until").eq("telegram_id", telegram_id).execute()
    now = int(time.time())
    if not user_res.data or int(user_res.data[0].get("subscription_until") or 0) < now:
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
    if is_renewal_price:
        return
    try:
        chk = (
            supabase.table("referral_rewards")
            .select("referred_telegram_id")
            .eq("referred_telegram_id", buyer_id)
            .limit(1)
            .execute()
        )
        if chk.data:
            return
    except Exception as e:
        print(f"referral_rewards check: {e}")
        return

    buyer = user_get(buyer_id)
    if not buyer:
        return
    ref_uid = buyer.get("referred_by")
    if not ref_uid or int(ref_uid) == int(buyer_id):
        return

    pct = get_referral_percent()
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
    invoice_id = int(inv.get("invoice_id") or 0)
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
                f"С первой оплаты вашего реферала: `{reward:.6f}` {asset}\n"
                f"({pct:g}% от суммы счёта).\n\n"
                "_Продления и следующие оплаты не участвуют._",
                parse_mode="Markdown",
            )
        except Exception as e:
            print(f"referral notify referrer {ref_uid}: {e}")


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
            [InlineKeyboardButton(text="🎁 Реферальный %", callback_data=f"{CB}:refpct")],
            [InlineKeyboardButton(text="ℹ️ Справка", callback_data=f"{CB}:help")],
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
    """Главное меню пользователя: Профиль|Отзывы, Тарифы|Рефералы, Поддержка."""
    profile_btn = InlineKeyboardButton(text="👤 Профиль", callback_data=f"{UCB}:profile")
    if LINK_REVIEWS:
        reviews_btn = InlineKeyboardButton(text="⭐ Отзывы", url=LINK_REVIEWS)
    else:
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

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [profile_btn, reviews_btn],
            [tariffs_btn, referrals_btn],
            [support_btn],
        ]
    )


def kb_user_back_main():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data=f"{UCB}:main")],
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


def kb_tariffs():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="STANDART", callback_data=f"{UCB}:tariff:s")],
            [InlineKeyboardButton(text="PRO", callback_data=f"{UCB}:tariff:p")],
            [InlineKeyboardButton(text="MAX", callback_data=f"{UCB}:tariff:m")],
            [InlineKeyboardButton(text="TEAM (soon)", callback_data=f"{UCB}:tariff:t")],
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
        [InlineKeyboardButton(text="⬅️ К тарифам", callback_data=f"{UCB}:tariffs_menu")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"{UCB}:main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    until = extend_user_subscription_days(telegram_id, days, plan_code=plan_code)

    await referral_process_paid_invoice(inv, telegram_id, is_renewal_price)

    if bot:
        try:
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
    user_res = supabase.table("users").select("*").eq("telegram_id", telegram_id).execute()
    current_time = int(time.time())
    if not user_res.data or user_res.data[0]["subscription_until"] < current_time:
        return {"status": "failed", "message": "Subscription expired or user not found"}
    u = user_res.data[0]
    plan = (u.get("subscription_plan") or "m")
    if isinstance(plan, str):
        plan = plan.strip().lower()
    else:
        plan = "m"
    if plan not in ("s", "p", "m"):
        plan = "m"
    return {"status": "success", "message": "Subscription active", "subscription_plan": plan}


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


async def _show_tariffs_text_fallback(
    query: CallbackQuery, caption: str, markup: InlineKeyboardMarkup
) -> None:
    chat_id = query.message.chat.id
    if query.message.photo:
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        await query.message.edit_text(
            text=caption,
            parse_mode="HTML",
            reply_markup=markup,
        )


async def show_user_tariffs_screen(query: CallbackQuery) -> None:
    caption = text_tariffs_caption_html()
    markup = kb_tariffs()
    img = tariffs_photo_for_new_message()

    if img:
        try:
            if query.message.photo:
                await query.message.edit_caption(
                    caption=caption,
                    reply_markup=markup,
                    parse_mode="HTML",
                )
            else:
                chat_id = query.message.chat.id
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await query.bot.send_photo(
                    chat_id=chat_id,
                    photo=img,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=markup,
                )
        except Exception as e:
            print(f"tariffs photo failed: {e}")
            await _show_tariffs_text_fallback(query, caption, markup)
    else:
        await _show_tariffs_text_fallback(query, caption, markup)
    await query.answer()


async def require_policies_or_block(query: CallbackQuery) -> bool:
    """False = показан экран принятия, дальше не идём."""
    uid = query.from_user.id
    if is_admin(uid) or policies_user_has_accepted(uid):
        return True
    chat_id = query.message.chat.id
    prompt = text_policies_prompt_html()
    markup = kb_policies_accept()
    if query.message.photo:
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.bot.send_message(
            chat_id=chat_id,
            text=prompt,
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        await query.message.edit_text(
            text=prompt,
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
    chat_id = query.message.chat.id
    if not is_admin(uid) and not policies_user_has_accepted(uid):
        prompt = text_policies_prompt_html()
        markup = kb_policies_accept()
        if query.message.photo:
            try:
                await query.message.delete()
            except Exception:
                pass
            await query.bot.send_message(
                chat_id=chat_id,
                text=prompt,
                parse_mode="HTML",
                reply_markup=markup,
            )
        else:
            await query.message.edit_text(
                text=prompt,
                parse_mode="HTML",
                reply_markup=markup,
            )
        await query.answer()
        return
    extra = "\n\n🔐 /admin — панель администратора." if is_admin(uid) else ""
    menu_text = text_user_main_menu() + extra
    menu_markup = kb_user_main_menu()
    if query.message.photo:
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.bot.send_message(
            chat_id=chat_id,
            text=menu_text,
            parse_mode="Markdown",
            reply_markup=menu_markup,
        )
    else:
        await query.message.edit_text(
            menu_text,
            parse_mode="Markdown",
            reply_markup=menu_markup,
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
}
_TARIFF_PLAN_DEFAULT = {
    "s": (
        "<b>STANDART</b>\n\n"
        "• Интеграция с Dolphin{anty}\n"
        "• Статистика аккаунтов\n"
        "• Добавление до 20 аккаунтов\n"
        "• Базовая уникализация\n"
        "• Прогрев аккаунтов\n"
        "• Стандартная поддержка"
    ),
    "p": (
        "<b>PRO</b>\n\n"
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
        "• Интеграция с Dolphin{anty}\n"
        "• Статистика аккаунтов\n"
        "• Добавление до 100 аккаунтов\n"
        "• Базовая уникализация\n"
        "• Расширенная уникализация\n"
        "• Прогрев аккаунтов\n"
        "• Приоритетная поддержка\n"
        "• Доступ к ИИ"
    ),
}


def tariff_plan_body_html(code: str) -> str:
    if code not in _TARIFF_PLAN_ENV:
        return ""
    env_key = _TARIFF_PLAN_ENV[code]
    return (os.environ.get(env_key) or "").strip() or _TARIFF_PLAN_DEFAULT[code]


_PLAN_INVOICE_LABEL = {"s": "STANDART", "p": "PRO", "m": "MAX"}
# Имена без суффикса _P (в Railway «сырой» ввод переменных ломает строки вроде ..._P=...).
_PLAN_AMOUNT_ENV_KEYS = {
    "s": ("CRYPTO_PAY_AMOUNT_STANDART", "CRYPTO_PAY_AMOUNT_S"),
    "p": ("CRYPTO_PAY_AMOUNT_PRO", "CRYPTO_PAY_AMOUNT_P"),
    "m": ("CRYPTO_PAY_AMOUNT_MAX", "CRYPTO_PAY_AMOUNT_M"),
}
_PLAN_AMOUNT_DEFAULT = {"s": "0.1", "p": "0.15", "m": "0.2"}
# Продление после истечения подписки (см. user_eligible_for_renewal_price).
_PLAN_RENEW_AMOUNT_ENV_KEYS = {
    "s": ("CRYPTO_PAY_RENEW_AMOUNT_STANDART", "CRYPTO_PAY_RENEW_AMOUNT_S"),
    "p": ("CRYPTO_PAY_RENEW_AMOUNT_PRO", "CRYPTO_PAY_RENEW_AMOUNT_P"),
    "m": ("CRYPTO_PAY_RENEW_AMOUNT_MAX", "CRYPTO_PAY_RENEW_AMOUNT_M"),
}
_PLAN_RENEW_AMOUNT_DEFAULT = {"s": "0.08", "p": "0.12", "m": "0.18"}


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
}
_PLAN_SUB_DAYS_DEFAULT = {"s": 30, "p": 30, "m": 30}


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
    if code == "t":
        await query.answer("Тариф TEAM скоро будет доступен.", show_alert=True)
        return
    if code not in _TARIFF_PLAN_ENV:
        await query.answer()
        return
    sub_text = tariff_plan_body_html(code)
    uid = query.from_user.id
    if user_eligible_for_renewal_price(uid, code):
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
    if query.message.photo:
        await query.message.edit_caption(
            caption=sub_text,
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        await query.message.edit_text(
            text=sub_text,
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
    renewal = user_eligible_for_renewal_price(tid, code)
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
            await query.message.edit_text(
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
    pay_text = (
        f"{body}\n\n"
        f"<b>Счёт Crypto Pay</b>: {html.escape(asset)} {html.escape(amount)}\n"
        f"{price_note}\n"
        f"{net_hint}\n"
        "Нажмите кнопку ниже, чтобы открыть оплату."
    )
    markup = kb_tariff_after_invoice(code, pay_url)
    if query.message.photo:
        await query.message.edit_caption(
            caption=pay_text,
            parse_mode="HTML",
            reply_markup=markup,
        )
    else:
        await query.message.edit_text(
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
    pct = get_referral_percent()
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
        f"Вознаграждение: <b>{pct:g}%</b> от <b>первой</b> оплаты каждого приглашённого по ссылке "
        "(оплаты по тарифу продления не дают бонус пригласившему).",
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
    return "\n".join(lines)


@dp.callback_query(F.data == f"{UCB}:referrals")
async def cb_user_referrals(query: CallbackQuery):
    if not await require_policies_or_block(query):
        return
    text = await build_referrals_user_html(query.from_user.id)
    await query.message.edit_text(
        text,
        parse_mode="HTML",
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
        "• **Задать HWID** — вставьте **64-символьный** hex (как в приложении).\n"
        "• **Реферальный %** — доля с первой оплаты приглашённого (не продления).\n\n"
        "/cancel — отменить ввод.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{CB}:menu")]]
        ),
    )
    await query.answer()


@dp.callback_query(F.data == f"{CB}:refpct")
async def cb_admin_referral_menu(query: CallbackQuery, state: FSMContext):
    if not is_admin(query.from_user.id):
        await query.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    pct = get_referral_percent()
    await query.message.edit_text(
        f"🎁 **Реферальный процент**\n\n"
        f"Сейчас: **{pct:g}%** от первой оплаты приглашённого (не продления).\n\n"
        "Выберите значение или «Свой %».",
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
    await query.message.edit_text(
        f"🎁 **Реферальный процент**\n\n"
        f"Сейчас: **{pct:g}%**",
        parse_mode="Markdown",
        reply_markup=kb_referral_admin(),
    )


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
    set_referral_percent(pct)
    await state.clear()
    await message.answer(
        f"✅ Процент реферальной программы: **{pct:g}%**",
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
