import asyncio
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.types import Message
import uvicorn
from supabase import create_client, Client

# ==========================================
# НАСТРОЙКИ
# ==========================================
BOT_TOKEN = "8678565790:AAEWlfu_CD1EFS9Uti04q7hi3WO1O_BBvyU"
ADMIN_ID = 6699203478

SUPABASE_URL = "https://ofvtnnsdoevytvyzoooq.supabase.co"
SUPABASE_KEY = "sb_publishable_S83DLCIn2-bpUzu_mieEig_PZePSNB0"
# ==========================================

app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class AuthRequest(BaseModel):
    session_id: str
    hwid: str

@app.post("/api/auth/start")
async def start_auth(req: AuthRequest):
    supabase.table('auth_sessions').upsert({
        'session_id': req.session_id,
        'hwid': req.hwid,
        'status': 'pending',
        'created_at': int(time.time())
    }).execute()
    
    bot_info = await bot.get_me()
    return {"status": "ok", "login_url": f"https://t.me/{bot_info.username}?start={req.session_id}"}

@app.get("/api/auth/check/{session_id}")
async def check_auth(session_id: str):
    response = supabase.table('auth_sessions').select('*').eq('session_id', session_id).execute()
    if not response.data:
        raise HTTPException(status_code=404, detail="Session not found")
    session = response.data[0]
    return {"status": session['status'], "telegram_id": session.get('telegram_id')}

@app.get("/api/auth/verify/{telegram_id}")
async def verify_subscription(telegram_id: int):
    user_res = supabase.table('users').select('*').eq('telegram_id', telegram_id).execute()
    current_time = int(time.time())
    if not user_res.data or user_res.data[0]['subscription_until'] < current_time:
        return {"status": "failed", "message": "Subscription expired or user not found"}
    return {"status": "success", "message": "Subscription active"}

# ==========================================
# ТЕЛЕГРАМ БОТ (ОБНОВЛЕННАЯ ЛОГИКА)
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    args = message.text.split()
    if len(args) == 1:
        await message.answer("👋 Привет! Я бот для авторизации.\n\n"
                             "Нажмите кнопку входа на сайте или в приложении.")
        return

    session_id = args[1]
    telegram_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else str(telegram_id)
    
    session_res = supabase.table('auth_sessions').select('*').eq('session_id', session_id).eq('status', 'pending').execute()
    if not session_res.data:
        await message.answer("❌ Ссылка недействительна.")
        return
        
    session = session_res.data[0]
    hwid_from_app = session['hwid']
    is_web_login = hwid_from_app.startswith("WEB_BROWSER_") # Проверяем, сайт это или приложение
    
    user_res = supabase.table('users').select('*').eq('telegram_id', telegram_id).execute()
    current_time = int(time.time())
    
    if not user_res.data or user_res.data[0]['subscription_until'] < current_time:
        supabase.table('auth_sessions').update({'status': 'failed'}).eq('session_id', session_id).execute()
        await message.answer("❌ У вас нет активной подписки.")
        return
        
    user = user_res.data[0]
    
    # ЛОГИКА ДЛЯ САЙТА (БЕЗ ПРИВЯЗКИ HWID)
    if is_web_login:
        supabase.table('users').update({'username': username}).eq('telegram_id', telegram_id).execute()
        supabase.table('auth_sessions').update({'status': 'success', 'telegram_id': telegram_id}).eq('session_id', session_id).execute()
        await message.answer("✅ Вход на сайт выполнен успешно!")
        return

    # ЛОГИКА ДЛЯ ПРИЛОЖЕНИЯ (С ПРИВЯЗКОЙ HWID)
    saved_hwid = user.get('hwid')
    if not saved_hwid:
        supabase.table('users').update({'hwid': hwid_from_app, 'username': username}).eq('telegram_id', telegram_id).execute()
        supabase.table('auth_sessions').update({'status': 'success', 'telegram_id': telegram_id}).eq('session_id', session_id).execute()
        await message.answer("✅ Успешный вход! Аккаунт привязан к этому ПК.")
    elif saved_hwid == hwid_from_app:
        supabase.table('users').update({'username': username}).eq('telegram_id', telegram_id).execute()
        supabase.table('auth_sessions').update({'status': 'success', 'telegram_id': telegram_id}).eq('session_id', session_id).execute()
        await message.answer("✅ Успешный вход! Возвращайтесь в программу.")
    else:
        supabase.table('auth_sessions').update({'status': 'failed'}).eq('session_id', session_id).execute()
        await message.answer("❌ Ошибка! Подписка уже используется на другом ПК.")

# Админские команды остаются без изменений...
@dp.message(Command("add"))
async def cmd_add_sub(message: Message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split()
    if len(args) != 3: return
    target_id, days = int(args[1]), int(args[2])
    until_time = int(time.time()) + (days * 24 * 60 * 60)
    user_res = supabase.table('users').select('*').eq('telegram_id', target_id).execute()
    if user_res.data:
        supabase.table('users').update({'subscription_until': until_time}).eq('telegram_id', target_id).execute()
    else:
        supabase.table('users').insert({'telegram_id': target_id, 'subscription_until': until_time, 'hwid': None}).execute()
    await message.answer(f"✅ Подписка выдана на {days} дней.")

@dp.message(Command("reset"))
async def cmd_reset_hwid(message: Message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split()
    if len(args) != 2: return
    target_id = int(args[1])
    supabase.table('users').update({'hwid': None}).eq('telegram_id', target_id).execute()
    await message.answer(f"✅ Привязка сброшена.")

async def main():
    config = uvicorn.Config(app, host="0.0.0.0", port=5000, log_level="info")
    server = uvicorn.Server(config)
    await asyncio.gather(server.serve(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())