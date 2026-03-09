import asyncio
import logging
import time
import random
import string
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
import sqlite3
from contextlib import contextmanager
from functools import wraps
from collections import defaultdict
import threading
import os
import shutil
import re

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, ChatPermissions, InlineKeyboardButton, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = "8557190026:AAHvuKIR0yGUWnruUap0Qw4bwAmlQtOKM-c"
BOT_USERNAME = "PulsOfficialManager_bot"
ADMIN_IDS = [6708209142]  # Ваш ID

# Максимальное количество триггеров
MAX_TRIGGERS = 30
MAX_TRIGGER_LENGTH = 50
MAX_RESPONSE_LENGTH = 1000

# Автоопределение часового пояса сервера
SERVER_TZ = datetime.now().astimezone().tzinfo

# Хранилище для антифлуда
flood_control = defaultdict(list)

# Блокировка для статистики
stats_lock = threading.Lock()
stats_updating = False

# Режим техработ
technical_maintenance = False
maintenance_message = "🛠 Бот временно остановлен на технические работы."

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== ФУНКЦИИ ==========
def generate_user_id() -> str:
    return ''.join(random.choices(string.digits, k=9))

def clean_text(text: str) -> str:
    """Очищает текст от эмодзи и специальных символов для проверки триггеров"""
    if not text:
        return ""
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u200d"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text).lower().strip()

def is_media_message(message: Message) -> bool:
    """Проверяет, является ли сообщение медиа"""
    return bool(message.photo or message.video or message.animation or message.sticker)

# ========== КЛАССЫ СОСТОЯНИЙ ==========
class RulesStates(StatesGroup):
    waiting_for_rules_text = State()
    waiting_for_interval = State()
    waiting_for_new_rules_text = State()

class WelcomeStates(StatesGroup):
    waiting_for_welcome_text = State()
    waiting_for_welcome_photo = State()

class AntiFloodStates(StatesGroup):
    waiting_for_message_limit = State()
    waiting_for_media_limit = State()
    waiting_for_window = State()
    waiting_for_warn_count = State()
    waiting_for_first_punish = State()
    waiting_for_first_duration = State()
    waiting_for_repeat_punish = State()
    waiting_for_repeat_duration = State()
    waiting_for_punish_after_warn = State()

class AutoResponseStates(StatesGroup):
    waiting_for_trigger = State()
    waiting_for_response = State()
    waiting_for_remove_trigger = State()

class LinksStates(StatesGroup):
    waiting_for_duration = State()
    waiting_for_max_mentions = State()
    waiting_for_mention_window = State()

class MaintenanceStates(StatesGroup):
    waiting_for_message = State()

class AdminBroadcastStates(StatesGroup):
    waiting_for_text = State()

# ========== ДЕКОРАТОРЫ ==========
def check_owner():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id
            state: FSMContext = kwargs.get('state')
            if state:
                data = await state.get_data()
                msg_owner = data.get(f"msg_owner_{callback.message.message_id}")
                if msg_owner and msg_owner != user_id:
                    await callback.answer("⚠️ Эта кнопка только для того, кто вызвал команду!", show_alert=True)
                    return
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

def check_public():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

def check_bot_admin():
    def decorator(func):
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            if message.from_user.id not in ADMIN_IDS:
                await message.answer("❌ Эта команда доступна только администраторам бота!")
                return
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

def group_only():
    def decorator(func):
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            if message.chat.type == 'private':
                await message.answer("❌ Эта команда работает только в группах!")
                return
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

def pm_only():
    def decorator(func):
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            if message.chat.type != 'private':
                await message.answer("❌ Эта команда работает только в личных сообщениях!")
                return
            return await func(message, *args, **kwargs)
        return wrapper
    return decorator

# ========== ТЕКСТЫ ==========
DEFAULT_RULES = """
Правила чата:

<blockquote expandable>
1. Запрещено спамить, флудить и писать капсом.
2. Уважайте других участников группы.
3. Реклама, ссылки и призывы к действию - только с разрешения админов.
4. Запрещены оскорбления, угрозы, дискриминация.
5. Нельзя распространять запрещённый контент.
6. Администрация имеет право мута/бана без объяснения причин.
7. Если не согласны с правилами - покиньте группу.
8. При нарушении правил - пишите админам в ЛС.
</blockquote>

Спасибо за внимание!
"""

def get_text(key: str = None, **kwargs) -> str:
    text = TEXTS.get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except:
            pass
    return text

TEXTS = {
    'welcome': "Добро пожаловать, <b>{name}</b>!",
    'no_username': "нет",
    'username': "Username",
    'id': "ID",
    'joined': "Вошёл",
    'last_active': "Последняя активность",
    'place_in_top': "Место в топе",
    'user_id': "ID пользователя",
    'first_seen': "Впервые замечен",
    'confirm_not_bot': "Я не бот",
    'agree_rules': "✅ Согласен с правилами",
    'go_to_pm': "📜 Перейти в ЛС",
    'confirmed_not_bot': "✅ {name} подтвердил, что не бот",
    'confirmed_rules': "✅ {name} согласился с правилами",
    'thanks_confirmation': "Спасибо! Теперь вы можете писать в чат.",
    'need_confirm_both': "Выполните два шага:\n1. Подтвердите, что не бот\n2. Прочитайте правила",
    'step_1_completed': "✅ Шаг 1 выполнен! Теперь шаг 2.",
    'step_2_completed': "✅ Шаг 2 выполнен! Теперь шаг 1.",
    'confirmation_disabled': "✅ Подтверждение отключено",
    'user_joined': "👋 <b>{name}</b> зашёл в чат!",
    'need_confirm_rules': "Вы замьючены, пока не подтвердите правила.",
    'need_confirm_not_bot': "Вы замьючены, пока не подтвердите, что не бот.",
    'stats_empty': "📊 Статистики пока нет",
    'stats_updating': "📊 Статистика обновляется...",
    'top_active': "🏆 Топ активных:",
    'profile': "Профиль {name}",
    'per_day': "За день",
    'per_week': "За неделю",
    'per_month': "За месяц",
    'total': "Всего",
    'messages': "сообщ.",
    'ping': "Пинг: {ping} мс\nВремя: {response} сек",
    'user_left': "👋 {name} вышел",
    'error_no_group': "❌ Сначала выберите группу!",
    'error_not_creator': "❌ Вы не создатель этой группы!",
    'error_not_yours': "⚠️ Не ваше подтверждение!",
    'error_no_rules': "❌ Правила не установлены",
    'error_rules_short': "❌ Правила слишком короткие!",
    'group_only': "❌ Только в группах!",
    'pm_only': "❌ Только в ЛС!",
    'rules_reminder': "📢 Напоминание правил",
    'about': "📋 О боте",
    'help': "🆘 Помощь",
    'add_to_group': "➕ Добавить в группу",
    'group_manage': "⚙️ Управление группой",
    'back': "◀️ Назад",
    'group_not_linked': "❌ Группа не привязана",
    'want_to_link': "Хотите привязать группу?",
    'link_group': "✅ Привязать группу",
    'unlink_group': "❌ Отвязать группу",
    'confirm_unlink': "Вы уверены?",
    'cancel': "🚫 Отмена",
    'group_linked': "✅ Группа привязана!",
    'group_unlinked': "✅ Группа отвязана",
    'settings_in_pm': "⚙️ Настройка только в ЛС",
    'go_to_pm_settings': "📱 Перейти в ЛС",
    'select_group': "📱 Выберите группу:",
    'cant_use_rules': "❌ Сначала установите правила!",
    'cant_use_both': "❌ Для этого сначала установите правила!",
    'rules_management': "📝 Управление правилами",
    'set_rules': "📝 Установить",
    'set_default_rules': "📋 Готовые",
    'edit_rules': "✏️ Изменить",
    'delete_rules': "🗑 Удалить",
    'toggle_rules': "🔄 Вкл/Выкл",
    'rules_enabled': "✅ Включены",
    'rules_disabled': "❌ Выключены",
    'rules_deleted': "✅ Удалены",
    'rules_updated': "✅ Обновлены",
    'rules_set': "✅ Установлены",
    'default_rules_set': "✅ Готовые установлены",
    'enter_new_rules': "📝 Отправьте текст правил",
    'status_enabled': "✅ Вкл",
    'status_disabled': "❌ Выкл",
    'set_text': "📝 Текст",
    'set_photo': "🖼 Фото",
    'view': "👁 Посмотреть",
    'rules': "📜 Правила",
    'my_stats': "📊 Моя статистика",
    'top_active_btn': "🏆 Топ",
    'interval': "⏱ Интервал",
    'limit': "📊 Лимит",
    'window': "⏱ Окно",
    'warn_count': "⚠️ Предупреждений",
    'first_punish': "🔇 Первое",
    'repeat_punish': "🔊 Повторное",
    'enable': "✅ Включить",
    'disable': "❌ Выключить",
    'duration': "Длительность",
    'minutes': "мин",
    'forever': "навсегда",
}

# ========== БАЗА ДАННЫХ ==========
class Database:
    def __init__(self, db_path="puls_manager.db"):
        self.db_path = db_path
        self.init_db()
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            
            c.execute('''CREATE TABLE IF NOT EXISTS group_rules
                         (chat_id INTEGER PRIMARY KEY,
                          owner_id INTEGER,
                          rules_html TEXT,
                          rules_enabled INTEGER DEFAULT 1,
                          welcome_enabled INTEGER DEFAULT 0,
                          welcome_text TEXT,
                          welcome_photo_id TEXT,
                          rules_auto_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300,
                          last_rules_message_id INTEGER,
                          last_rules_time INTEGER,
                          chat_title TEXT,
                          chat_username TEXT,
                          report_group_id INTEGER,
                          confirmation_type TEXT DEFAULT 'not_bot')''')  # По умолчанию только не бот
            
            c.execute('''CREATE TABLE IF NOT EXISTS global_users
                         (user_id INTEGER PRIMARY KEY,
                          global_id TEXT UNIQUE,
                          first_seen INTEGER,
                          username TEXT,
                          full_name TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS auto_responses
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          trigger TEXT,
                          response TEXT,
                          created_at INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS rules_agreed
                         (chat_id INTEGER,
                          user_id INTEGER,
                          agreed_at INTEGER,
                          not_bot_confirmed INTEGER DEFAULT 0,
                          rules_confirmed INTEGER DEFAULT 0,
                          PRIMARY KEY (chat_id, user_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS user_stats
                         (chat_id INTEGER,
                          user_id INTEGER,
                          join_date INTEGER,
                          all_messages INTEGER DEFAULT 0,
                          month_messages INTEGER DEFAULT 0,
                          week_messages INTEGER DEFAULT 0,
                          day_messages INTEGER DEFAULT 0,
                          last_active INTEGER,
                          left_chat INTEGER DEFAULT 0,
                          PRIMARY KEY (chat_id, user_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS antiflood_settings
                         (chat_id INTEGER PRIMARY KEY,
                          enabled INTEGER DEFAULT 0,
                          msg_limit INTEGER DEFAULT 5,
                          media_limit INTEGER DEFAULT 3,
                          time_window INTEGER DEFAULT 10,
                          warn_count INTEGER DEFAULT 3,
                          first_punish TEXT DEFAULT 'mute',
                          first_duration INTEGER DEFAULT 60,
                          repeat_punish TEXT DEFAULT 'ban',
                          repeat_duration INTEGER DEFAULT 3600,
                          punish_after_warn TEXT DEFAULT 'mute',
                          links_enabled INTEGER DEFAULT 0,
                          links_punish TEXT DEFAULT 'mute',
                          links_duration INTEGER DEFAULT 3600,
                          max_mentions INTEGER DEFAULT 3,
                          mention_window INTEGER DEFAULT 60)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS violation_logs
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          user_id INTEGER,
                          user_name TEXT,
                          reason TEXT,
                          punishment TEXT,
                          message_id INTEGER,
                          message_link TEXT,
                          timestamp INTEGER)''')
            conn.commit()
    
    def save_rules(self, chat_id, rules_html=None, owner_id=None, chat_title=None, chat_username=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,))
            existing = c.fetchone()
            if existing:
                updates, params = [], []
                if rules_html is not None:
                    updates.append("rules_html = ?")
                    params.append(rules_html)
                if owner_id is not None:
                    updates.append("owner_id = ?")
                    params.append(owner_id)
                if chat_title is not None:
                    updates.append("chat_title = ?")
                    params.append(chat_title)
                if chat_username is not None:
                    updates.append("chat_username = ?")
                    params.append(chat_username)
                if updates:
                    query = f"UPDATE group_rules SET {', '.join(updates)} WHERE chat_id = ?"
                    params.append(chat_id)
                    c.execute(query, params)
            else:
                c.execute('''INSERT INTO group_rules (chat_id, owner_id, rules_html, chat_title, chat_username, confirmation_type) 
                             VALUES (?, ?, ?, ?, ?, ?)''', (chat_id, owner_id, rules_html, chat_title, chat_username, 'not_bot'))
            conn.commit()
    
    def set_rules_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET rules_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_rules_enabled(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_enabled FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return bool(result[0]) if result else True
    
    def delete_rules(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET rules_html = NULL WHERE chat_id = ?', (chat_id,))
            conn.commit()
    
    def save_welcome(self, chat_id, welcome_text=None, welcome_photo_id=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            if welcome_text is not None:
                c.execute('UPDATE group_rules SET welcome_text = ? WHERE chat_id = ?', (welcome_text, chat_id))
            if welcome_photo_id is not None:
                c.execute('UPDATE group_rules SET welcome_photo_id = ? WHERE chat_id = ?', (welcome_photo_id, chat_id))
            conn.commit()
    
    def get_welcome(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT welcome_text, welcome_photo_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return (result[0], result[1]) if result else (None, None)
    
    def set_welcome_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET welcome_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_welcome_enabled(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT welcome_enabled FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return bool(result[0]) if result else False
    
    def get_rules_html(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_html FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    def set_rules_auto_settings(self, chat_id, enabled, interval):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,))
            existing = c.fetchone()
            if existing:
                c.execute('UPDATE group_rules SET rules_auto_enabled = ?, rules_interval = ? WHERE chat_id = ?', (1 if enabled else 0, interval, chat_id))
            else:
                c.execute('INSERT INTO group_rules (chat_id, rules_auto_enabled, rules_interval) VALUES (?, ?, ?)', (chat_id, 1 if enabled else 0, interval))
            conn.commit()
    
    def get_rules_auto_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_auto_enabled, rules_interval, last_rules_message_id, last_rules_time FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result if result else (0, 300, None, None)
    
    def update_last_rules(self, chat_id, message_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET last_rules_message_id = ?, last_rules_time = ? WHERE chat_id = ?', (message_id, int(time.time()), chat_id))
            conn.commit()
    
    def get_user_groups(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id, chat_title FROM group_rules WHERE owner_id = ?', (user_id,))
            return c.fetchall()
    
    def get_all_chats(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id, chat_title, chat_username, rules_enabled, welcome_enabled FROM group_rules ORDER BY chat_id')
            return c.fetchall()
    
    def set_report_group(self, chat_id, report_group_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET report_group_id = ? WHERE chat_id = ?', (report_group_id, chat_id))
            conn.commit()
    
    def get_report_group(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT report_group_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    def get_confirmation_type(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT confirmation_type FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else 'not_bot'
    
    def set_confirmation_type(self, chat_id, conf_type):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET confirmation_type = ? WHERE chat_id = ?', (conf_type, chat_id))
            conn.commit()
    
    def add_auto_response(self, chat_id, trigger, response):
        # Проверяем количество триггеров
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM auto_responses WHERE chat_id = ?', (chat_id,))
            count = c.fetchone()[0]
            if count >= MAX_TRIGGERS:
                return False, f"❌ Достигнут лимит триггеров ({MAX_TRIGGERS})"
            
            # Проверяем, нет ли уже такого триггера
            c.execute('SELECT 1 FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger.lower()))
            if c.fetchone():
                return False, f"❌ Триггер '{trigger}' уже существует"
            
            c.execute('INSERT INTO auto_responses (chat_id, trigger, response, created_at) VALUES (?, ?, ?, ?)', 
                     (chat_id, trigger.lower(), response, int(time.time())))
            conn.commit()
            return True, f"✅ Триггер '{trigger}' добавлен ({count+1}/{MAX_TRIGGERS})"
    
    def get_auto_responses(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT trigger, response FROM auto_responses WHERE chat_id = ? ORDER BY created_at', (chat_id,))
            return c.fetchall()
    
    def remove_auto_response(self, chat_id, trigger):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger.lower()))
            conn.commit()
            return c.rowcount > 0
    
    def mark_user_confirmed(self, chat_id, user_id, not_bot=False, rules=False):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            result = c.fetchone()
            if result:
                not_bot_confirmed = result[0] or not_bot
                rules_confirmed = result[1] or rules
                c.execute('UPDATE rules_agreed SET not_bot_confirmed = ?, rules_confirmed = ?, agreed_at = ? WHERE chat_id = ? AND user_id = ?', 
                         (1 if not_bot_confirmed else 0, 1 if rules_confirmed else 0, int(time.time()), chat_id, user_id))
            else:
                c.execute('INSERT INTO rules_agreed (chat_id, user_id, agreed_at, not_bot_confirmed, rules_confirmed) VALUES (?, ?, ?, ?, ?)', 
                         (chat_id, user_id, int(time.time()), 1 if not_bot else 0, 1 if rules else 0))
            conn.commit()
    
    def has_user_confirmed(self, chat_id, user_id, conf_type=None):
        if conf_type is None:
            conf_type = self.get_confirmation_type(chat_id)
        if conf_type == 'disabled':
            return True
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            result = c.fetchone()
            if not result:
                return False
            not_bot_confirmed, rules_confirmed = result
            if conf_type == 'not_bot':
                return bool(not_bot_confirmed)
            elif conf_type == 'rules':
                return bool(rules_confirmed) and self.get_rules_html(chat_id) is not None
            else:
                return bool(not_bot_confirmed) and bool(rules_confirmed)
    
    def get_user_confirmation_status(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            result = c.fetchone()
            if not result:
                return (False, False)
            return (bool(result[0]), bool(result[1]))
    
    def get_or_create_global_user(self, user_id, username, full_name):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            if result:
                return result[0]
            global_id = generate_user_id()
            c.execute('INSERT INTO global_users (user_id, global_id, first_seen, username, full_name) VALUES (?, ?, ?, ?, ?)', 
                     (user_id, global_id, int(time.time()), username, full_name))
            conn.commit()
            return global_id
    
    def get_global_user(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id, first_seen, username, full_name FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            if result:
                return {'global_id': result[0], 'first_seen': result[1], 'username': result[2], 'full_name': result[3]}
            return None
    
    def add_user_stat(self, chat_id, user_id, join_date):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO user_stats (chat_id, user_id, join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat) VALUES (?, ?, ?, 0, 0, 0, 0, ?, 0)', 
                     (chat_id, user_id, join_date, join_date))
            conn.commit()
    
    def update_message_count(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE user_stats SET all_messages = all_messages + 1, month_messages = month_messages + 1, week_messages = week_messages + 1, day_messages = day_messages + 1, last_active = ? WHERE chat_id = ? AND user_id = ?', 
                     (int(time.time()), chat_id, user_id))
            conn.commit()
    
    def set_left_chat(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE user_stats SET left_chat = 1 WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            conn.commit()
    
    def get_user_stat(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat FROM user_stats WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            row = c.fetchone()
            if row:
                return {'join_date': row[0], 'all_messages': row[1], 'month_messages': row[2], 'week_messages': row[3], 'day_messages': row[4], 'last_active': row[5], 'left_chat': bool(row[6])}
            return None
    
    def get_top_messages(self, chat_id, period='all', limit=10):
        field = {'day': 'day_messages', 'week': 'week_messages', 'month': 'month_messages', 'all': 'all_messages'}.get(period, 'all_messages')
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute(f'SELECT user_id, {field} FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY {field} DESC LIMIT ?', (chat_id, limit))
            return c.fetchall()
    
    def get_user_position(self, chat_id, user_id, period='all'):
        field = {'day': 'day_messages', 'week': 'week_messages', 'month': 'month_messages', 'all': 'all_messages'}.get(period, 'all_messages')
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute(f'SELECT user_id FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY {field} DESC', (chat_id,))
            users = c.fetchall()
            for i, (uid,) in enumerate(users, 1):
                if uid == user_id:
                    return i
            return 0
    
    def get_antiflood_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT enabled, msg_limit, media_limit, time_window, warn_count, 
                                first_punish, first_duration, repeat_punish, repeat_duration, punish_after_warn,
                                links_enabled, links_punish, links_duration, max_mentions, mention_window
                         FROM antiflood_settings WHERE chat_id = ?''', (chat_id,))
            row = c.fetchone()
            if row:
                return {'enabled': bool(row[0]), 'msg_limit': row[1] or 5, 'media_limit': row[2] or 3, 
                        'time_window': row[3] or 10, 'warn_count': row[4] or 3,
                        'first_punish': row[5] or 'mute', 'first_duration': row[6] or 60,
                        'repeat_punish': row[7] or 'ban', 'repeat_duration': row[8] or 3600,
                        'punish_after_warn': row[9] or 'mute',
                        'links_enabled': bool(row[10]), 'links_punish': row[11] or 'mute',
                        'links_duration': row[12] or 3600, 'max_mentions': row[13] or 3, 'mention_window': row[14] or 60}
            return {'enabled': False, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3,
                    'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600,
                    'punish_after_warn': 'mute', 'links_enabled': False, 'links_punish': 'mute',
                    'links_duration': 3600, 'max_mentions': 3, 'mention_window': 60}
    
    def set_antiflood_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO antiflood_settings (chat_id, enabled) VALUES (?, ?)', (chat_id, 1 if enabled else 0))
            conn.commit()
    
    def save_antiflood_settings(self, chat_id, **kwargs):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM antiflood_settings WHERE chat_id = ?', (chat_id,))
            exists = c.fetchone()
            if exists:
                if kwargs:
                    fields = ', '.join(f"{k}=?" for k in kwargs)
                    values = list(kwargs.values()) + [chat_id]
                    c.execute(f'UPDATE antiflood_settings SET {fields} WHERE chat_id = ?', values)
            else:
                defaults = {'enabled': 0, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3,
                            'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600,
                            'punish_after_warn': 'mute', 'links_enabled': 0, 'links_punish': 'mute',
                            'links_duration': 3600, 'max_mentions': 3, 'mention_window': 60}
                defaults.update(kwargs)
                c.execute('''INSERT INTO antiflood_settings 
                             (chat_id, enabled, msg_limit, media_limit, time_window, warn_count, 
                              first_punish, first_duration, repeat_punish, repeat_duration, punish_after_warn,
                              links_enabled, links_punish, links_duration, max_mentions, mention_window) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (chat_id, defaults['enabled'], defaults['msg_limit'], defaults['media_limit'],
                           defaults['time_window'], defaults['warn_count'],
                           defaults['first_punish'], defaults['first_duration'],
                           defaults['repeat_punish'], defaults['repeat_duration'],
                           defaults['punish_after_warn'], defaults['links_enabled'], defaults['links_punish'],
                           defaults['links_duration'], defaults['max_mentions'], defaults['mention_window']))
            conn.commit()
    
    def log_violation(self, chat_id, user_id, user_name, reason, punishment, message_id, message_link):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO violation_logs (chat_id, user_id, user_name, reason, punishment, message_id, message_link, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', 
                     (chat_id, user_id, user_name, reason, punishment, message_id, message_link, int(time.time())))
            conn.commit()

db = Database()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
async def is_creator(chat_id, user_id):
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status == 'creator'
    except:
        return False

async def is_admin(chat_id, user_id):
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ['creator', 'administrator']
    except:
        return False

def format_datetime(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def format_interval(seconds):
    if seconds < 60:
        return f"{seconds} сек"
    elif seconds < 3600:
        return f"{seconds // 60} мин"
    elif seconds < 86400:
        return f"{seconds // 3600} ч"
    else:
        return f"{seconds // 86400} дн"

def format_duration(minutes):
    if minutes == 0:
        return "навсегда"
    return f"{minutes} мин"

def get_message_link(chat_id, message_id):
    chat_id_str = str(chat_id)
    if chat_id_str.startswith('-100'):
        chat_id_str = chat_id_str[4:]
    return f"https://t.me/c/{chat_id_str}/{message_id}"

# ========== КЛАВИАТУРЫ ==========
def create_button(text: str, callback_data: str, color: str = None):
    if color:
        return InlineKeyboardButton(text=text, callback_data=callback_data, color=color)
    return InlineKeyboardButton(text=text, callback_data=callback_data)

def get_back_keyboard(callback_data):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", callback_data, "secondary"))
    return builder.as_markup()

def get_main_keyboard(is_group: bool = False):
    """Главное меню - разное для групп и ЛС"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📋 О боте", "about", "primary"))
    builder.add(create_button("🆘 Помощь", "help", "danger"))
    
    if not is_group:
        # В ЛС показываем все кнопки
        builder.add(create_button("➕ Добавить в группу", f"add_to_group_{BOT_USERNAME}", "success"))
        builder.add(create_button("⚙️ Управление группой", "group_manage_main", "primary"))
        builder.adjust(1)
    else:
        # В группе только основные кнопки
        builder.adjust(2)
    
    return builder.as_markup()

def get_group_main_keyboard():
    """Клавиатура для группы"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📋 О боте", "about", "primary"))
    builder.add(create_button("🆘 Помощь", "help", "danger"))
    builder.add(create_button("⚙️ Управление", "group_manage_group", "primary"))
    builder.adjust(2)
    return builder.as_markup()

def get_group_manage_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚙️ Управление группой", "group_manage", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Правила", "manage_rules", "primary"))
    builder.add(create_button("👋 Приветствие", "manage_welcome", "secondary"))
    builder.add(create_button("🔄 Авто-рассылка", "rules_auto", "secondary"))
    builder.add(create_button("🚫 Антифлуд", "antiflood_manage", "primary"))
    builder.add(create_button("📋 Репорты", "set_report_group", "secondary"))
    builder.add(create_button("🤖 Автоответчик", "auto_response_manage", "success"))
    builder.add(create_button("🔗 Ссылки", "links_manage", "secondary"))
    builder.add(create_button("✅ Подтверждение", "confirmation_manage", "primary"))
    builder.add(create_button("❌ Отвязать", "unlink_group_confirm", "danger"))
    builder.add(create_button("◀️ Назад", "back_to_groups", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard(has_rules, rules_enabled):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Установить", "set_rules", "success"))
    builder.add(create_button("📋 Готовые", "set_default_rules", "primary"))
    if has_rules:
        builder.add(create_button("👁 Посмотреть", "show_rules", "secondary"))
        builder.add(create_button("✏️ Изменить", "edit_rules", "secondary"))
        builder.add(create_button("🗑 Удалить", "delete_rules_confirm", "danger"))
        status_text = "✅ Включить" if not rules_enabled else "❌ Выключить"
        status_color = "success" if not rules_enabled else "danger"
        builder.add(create_button(status_text, "toggle_rules", status_color))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled=False):
    toggle_color = "danger" if enabled else "success"
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'Выключить' if enabled else 'Включить'}", "toggle_welcome", toggle_color))
    builder.add(create_button("📝 Текст", "set_welcome_text", "primary"))
    builder.add(create_button("🖼 Фото", "set_welcome_photo", "primary"))
    builder.add(create_button("👁 Посмотреть", "show_welcome", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled):
    toggle_color = "danger" if enabled else "success"
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'Выключить' if enabled else 'Включить'}", "toggle_rules_auto", toggle_color))
    builder.add(create_button("⏱ Интервал", "set_interval", "primary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(f"{'Выключить' if settings['enabled'] else 'Включить'}", "toggle_antiflood", toggle_color))
    builder.add(create_button(f"📝 Текст: {settings['msg_limit']}", "set_msg_limit", "secondary"))
    builder.add(create_button(f"🎬 Медиа: {settings['media_limit']}", "set_media_limit", "secondary"))
    builder.add(create_button(f"⏱ Окно: {settings['time_window']} сек", "set_window", "secondary"))
    builder.add(create_button(f"⚠️ Предупреждений: {settings['warn_count']}", "set_warn_count", "secondary"))
    builder.add(create_button("🔇 Первое наказание", "set_first_punish", "primary"))
    builder.add(create_button("🔊 Повторное", "set_repeat_punish", "primary"))
    builder.add(create_button("⚠️ После варнов", "set_punish_after_warn", "primary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(is_first=True, prefix=""):
    pre = prefix + ("first" if is_first else "repeat")
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"punish_warn_{pre}", "secondary"))
    builder.add(create_button("🔇 Мут", f"punish_mute_{pre}", "primary"))
    builder.add(create_button("👢 Кик", f"punish_kick_{pre}", "danger"))
    builder.add(create_button("⛔️ Бан", f"punish_ban_{pre}", "danger"))
    builder.add(create_button("◀️ Назад", "antiflood_manage", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_welcome_buttons(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📜 Правила", f"show_group_rules_{chat_id}", "primary"))
    builder.add(create_button("📊 Моя статистика", f"my_stats_{chat_id}", "secondary"))
    builder.add(create_button("🏆 Топ", f"top_active_{chat_id}", "success"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id, user_id, msg_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}", "success"))
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id, user_id, msg_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Согласен", f"agree_rules_{chat_id}_{user_id}_{msg_id}", "success"))
    return builder.as_markup()

def get_link_group_keyboard(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Привязать", f"link_group_{chat_id}", "success"))
    builder.add(create_button("🚫 Отмена", "cancel_link", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("❌ Отвязать", f"unlink_group_{chat_id}", "danger"))
    builder.add(create_button("🚫 Отмена", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_pm_link_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📱 Перейти в ЛС", "go_to_pm", "primary"))
    return builder.as_markup()

def get_report_group_keyboard(groups):
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"set_report_group_{chat_id}", "secondary"))
    builder.add(create_button("❌ Удалить", "remove_report_group", "danger"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_keyboard(responses):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "add_auto_trigger", "success"))
    if responses:
        builder.add(create_button("🗑 Удалить", "remove_auto_trigger", "danger"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_remove_keyboard(responses):
    builder = InlineKeyboardBuilder()
    for i, (trigger, _) in enumerate(responses):
        short = trigger[:15] + "..." if len(trigger) > 15 else trigger
        builder.add(create_button(short, f"rem_trig_{i}", "danger"))
    builder.add(create_button("◀️ Назад", "auto_response_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_manage_keyboard(settings):
    toggle_color = "danger" if settings['links_enabled'] else "success"
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'Выключить' if settings['links_enabled'] else 'Включить'}", "toggle_links", toggle_color))
    builder.add(create_button("Наказание", "set_links_punish", "primary"))
    builder.add(create_button("Макс упоминаний", "set_max_mentions", "secondary"))
    builder.add(create_button("Окно", "set_mention_window", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_punish_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", "links_punish_warn", "secondary"))
    builder.add(create_button("🔇 Мут", "links_punish_mute", "primary"))
    builder.add(create_button("👢 Кик", "links_punish_kick", "danger"))
    builder.add(create_button("⛔️ Бан", "links_punish_ban", "danger"))
    builder.add(create_button("◀️ Назад", "links_manage", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirmation_keyboard(current_type, has_rules):
    builder = InlineKeyboardBuilder()
    
    disabled = "🚫 Отключено"
    if current_type == 'disabled':
        disabled += " ✅"
    builder.add(create_button(disabled, "confirmation_disabled", "secondary"))
    
    not_bot = "🤖 Только не бот"
    if current_type == 'not_bot':
        not_bot += " ✅"
    builder.add(create_button(not_bot, "confirmation_not_bot", "primary"))
    
    rules = "📜 Только правила"
    if not has_rules:
        rules = "❌ " + rules
    elif current_type == 'rules':
        rules += " ✅"
    builder.add(create_button(rules, "confirmation_rules" if has_rules else "confirmation_disabled", "success" if has_rules else "secondary"))
    
    both = "2️⃣ Оба шага"
    if not has_rules:
        both = "❌ " + both
    elif current_type == 'both':
        both += " ✅"
    builder.add(create_button(both, "confirmation_both" if has_rules else "confirmation_disabled", "success" if has_rules else "secondary"))
    
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

# ========== MIDDLEWARE ==========
class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if not isinstance(event, Message) or event.chat.type not in {'group', 'supergroup'}:
            return await handler(event, data)
        
        chat_id = event.chat.id
        user = event.from_user
        
        if user.is_bot:
            return await handler(event, data)
        
        # Админы и создатели тоже считаются в статистике
        # Но не наказываются антифлудом
        
        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            return await handler(event, data)
        
        settings = db.get_antiflood_settings(chat_id)
        
        if not settings['enabled']:
            return await handler(event, data)
        
        now = time.time()
        key = f"{chat_id}_{user.id}"
        is_media = is_media_message(event)
        
        # Проверяем флуд
        if key not in flood_control:
            flood_control[key] = []
        
        # Очищаем старые записи
        flood_control[key] = [t for t in flood_control[key] if now - t < settings['time_window']]
        
        # Считаем сообщения
        msg_count = len(flood_control[key])
        
        # Определяем лимит в зависимости от типа сообщения
        limit = settings['media_limit'] if is_media else settings['msg_limit']
        
        if msg_count >= limit:
            # Нарушение
            violations = len([v for v in flood_control[key] if v > now - settings['time_window']])
            
            if violations < settings['warn_count']:
                # Предупреждение
                await event.reply(f"⚠️ {user.full_name}, не флуди! ({violations+1}/{settings['warn_count']})")
                flood_control[key].append(now)
                return
            else:
                # Наказание
                punish_type = settings['first_punish'] if violations == settings['warn_count'] else settings['repeat_punish']
                duration = settings['first_duration'] if violations == settings['warn_count'] else settings['repeat_duration']
                
                # Применяем наказание
                message_link = get_message_link(chat_id, event.message_id)
                db.log_violation(chat_id, user.id, user.full_name, "Флуд", punish_type, event.message_id, message_link)
                
                report_group = db.get_report_group(chat_id)
                if report_group:
                    try:
                        await bot.send_message(report_group, 
                            f"<b>🚫 Нарушение</b>\n\nПользователь: {user.full_name}\nПричина: Флуд\nНаказание: {punish_type}\n<a href='{message_link}'>Сообщение</a>",
                            parse_mode="HTML")
                    except:
                        pass
                
                try:
                    if punish_type == 'mute':
                        until = int(now + duration) if duration > 0 else None
                        await bot.restrict_chat_member(chat_id, user.id, 
                            permissions=ChatPermissions(can_send_messages=False), until_date=until)
                        await event.reply(f"🔇 {user.full_name} замьючен")
                    elif punish_type == 'ban':
                        until = int(now + duration) if duration > 0 else None
                        await bot.ban_chat_member(chat_id, user.id, until_date=until)
                        await event.reply(f"⛔️ {user.full_name} забанен")
                    elif punish_type == 'kick':
                        await bot.ban_chat_member(chat_id, user.id)
                        await bot.unban_chat_member(chat_id, user.id)
                        await event.reply(f"👢 {user.full_name} кикнут")
                except Exception as e:
                    logger.warning(f"Ошибка наказания: {e}")
                
                flood_control[key] = []
                return
        
        flood_control[key].append(now)
        
        # Считаем статистику для всех, включая админов
        db.update_message_count(chat_id, user.id)
        
        return await handler(event, data)

class MaintenanceMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        global technical_maintenance
        if isinstance(event, (Message, CallbackQuery)):
            user_id = event.from_user.id
            if user_id in ADMIN_IDS:
                return await handler(event, data)
        if technical_maintenance:
            if isinstance(event, Message):
                await event.reply(maintenance_message)
                return
            if isinstance(event, CallbackQuery):
                await event.answer("🛠 Бот на техработах", show_alert=True)
                return
        return await handler(event, data)

# ========== ФОНОВЫЕ ЗАДАЧИ ==========
async def reset_periodic_counters():
    global stats_updating
    while True:
        now = datetime.now(SERVER_TZ)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        with stats_lock:
            stats_updating = True
            try:
                with db.get_connection() as conn:
                    c = conn.cursor()
                    c.execute('UPDATE user_stats SET day_messages = 0 WHERE last_active < ?', (day_start.timestamp(),))
                    c.execute('UPDATE user_stats SET week_messages = 0 WHERE last_active < ?', (week_start.timestamp(),))
                    c.execute('UPDATE user_stats SET month_messages = 0 WHERE last_active < ?', (month_start.timestamp(),))
                    conn.commit()
                    logger.info("Счетчики сброшены")
            except Exception as e:
                logger.error(f"Ошибка сброса: {e}")
            stats_updating = False
        await asyncio.sleep(3600)

async def rules_broadcast_task():
    while True:
        try:
            with db.get_connection() as conn:
                c = conn.cursor()
                c.execute('SELECT chat_id, rules_auto_enabled, rules_interval, last_rules_time, rules_html FROM group_rules WHERE rules_auto_enabled = 1 AND rules_html IS NOT NULL')
                for chat_id, enabled, interval, last_time, rules_html in c.fetchall():
                    if last_time and int(time.time()) - last_time < interval:
                        continue
                    try:
                        msg = await bot.send_message(chat_id, f"<b>📢 Напоминание правил</b>\n\n{rules_html}", parse_mode="HTML")
                        try:
                            await bot.pin_chat_message(chat_id, msg.message_id)
                        except:
                            pass
                        db.update_last_rules(chat_id, msg.message_id)
                    except Exception as e:
                        logger.error(f"Ошибка отправки правил в {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче: {e}")
        await asyncio.sleep(60)

# ========== КОМАНДЫ ==========
@dp.message(CommandStart())
@pm_only()
async def cmd_start_pm(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    await message.answer(
        "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\n"
        "Я помогу вам управлять чатами, следить за порядком и автоматизировать модерацию.\n\n"
        "Выберите раздел в меню ниже 👇",
        reply_markup=get_main_keyboard(is_group=False)
    )

@dp.message(Command("start"))
@group_only()
async def cmd_start_group(message: Message):
    await message.reply(
        "👋 <b>Puls Chat Manager</b>\n\n"
        "• /rules - Правила\n"
        "• /stats - Моя статистика\n"
        "• /top - Топ активных\n"
        "• /profile - Профиль пользователя\n"
        "• /group - Управление группой\n"
        "• /puls - Проверка пинга",
        reply_markup=get_group_main_keyboard(),
        parse_mode="HTML"
    )

@dp.message(Command("groupsettings"))
@pm_only()
async def cmd_group_settings(message: Message, state: FSMContext):
    """Открывает управление группами"""
    await state.clear()
    groups = db.get_user_groups(message.from_user.id)
    
    if not groups:
        await message.answer(
            "❌ У вас нет привязанных групп.\n\n"
            "Добавьте бота в группу и привяжите её командой /group в той группе."
        )
        return
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    
    await message.answer("📱 <b>Ваши группы</b>\n\nВыберите группу для настройки:", reply_markup=builder.as_markup())

@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["пульс", "понг"]))
async def cmd_ping(message: Message):
    start = time.time()
    msg = await message.reply("⏳ ...")
    ping = round((time.time() - start) * 1000)
    await msg.edit_text(f"📊 <b>Пинг:</b> {ping} мс\n<b>Статус:</b> ✅ Работаю", parse_mode="HTML")

@dp.message(Command("stats"))
@group_only()
async def cmd_stats(message: Message):
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply("📊 Статистика обновляется...")
        return
    
    chat_id, user = message.chat.id, message.from_user
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    position = db.get_user_position(chat_id, user.id, 'all')
    
    if not stat:
        text = "📊 У вас пока нет сообщений в этом чате"
    else:
        text = (
            f"<b>Профиль {user.full_name}</b>\n\n"
            f"🆔 <b>ID:</b> <code>{global_user['global_id']}</code>\n"
            f"📅 <b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"📊 <b>Статистика в этом чате:</b>\n"
            f"• За день: {stat['day_messages']} сообщ.\n"
            f"• За неделю: {stat['week_messages']} сообщ.\n"
            f"• За месяц: {stat['month_messages']} сообщ.\n"
            f"• Всего: {stat['all_messages']} сообщ.\n"
            f"• Место в топе: {position}"
        )
    
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("top"))
@group_only()
async def cmd_top(message: Message):
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply("📊 Статистика обновляется...")
        return
    
    top = db.get_top_messages(message.chat.id, limit=10)
    
    if not top:
        await message.reply("📊 В этом чате пока нет сообщений")
        return
    
    text = "<b>🏆 Топ активных (всего сообщений):</b>\n\n"
    for i, (user_id, count) in enumerate(top, 1):
        try:
            name = (await bot.get_chat_member(message.chat.id, user_id)).user.full_name
        except:
            name = f"ID {user_id}"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        text += f"{medal} {name} — {count} сообщ.\n"
    
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("profile"))
@group_only()
async def cmd_profile(message: Message):
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply("📊 Статистика обновляется...")
        return
    
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя, чтобы увидеть его профиль")
        return
    
    target_user = message.reply_to_message.from_user
    chat_id = message.chat.id
    
    global_user = db.get_global_user(target_user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(target_user.id, target_user.username or "", target_user.full_name or "")
        global_user = db.get_global_user(target_user.id)
    
    stat = db.get_user_stat(chat_id, target_user.id)
    position = db.get_user_position(chat_id, target_user.id, 'all')
    
    if not stat:
        text = f"👤 <b>{target_user.full_name}</b>\n\nУ пользователя пока нет сообщений в этом чате"
    else:
        text = (
            f"<b>Профиль {target_user.full_name}</b>\n\n"
            f"🆔 <b>ID:</b> <code>{global_user['global_id']}</code>\n"
            f"📅 <b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"📊 <b>Статистика в этом чате:</b>\n"
            f"• За день: {stat['day_messages']} сообщ.\n"
            f"• За неделю: {stat['week_messages']} сообщ.\n"
            f"• За месяц: {stat['month_messages']} сообщ.\n"
            f"• Всего: {stat['all_messages']} сообщ.\n"
            f"• Место в топе: {position}"
        )
    
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("rules"))
@group_only()
async def cmd_rules(message: Message):
    rules = db.get_rules_html(message.chat.id)
    if rules and db.get_rules_enabled(message.chat.id):
        await message.reply(f"<b>📢 Правила чата</b>\n\n{rules}", parse_mode="HTML")
    else:
        await message.answer("❌ В этом чате ещё не установлены правила")

@dp.message(Command("group"))
@group_only()
async def cmd_group(message: Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может настраивать бота!")
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        owner_id = result[0] if result else None
    
    if owner_id == user_id:
        await message.answer(
            "✅ Группа уже привязана к вашему аккаунту.\n\n"
            "Все настройки доступны в личных сообщениях с ботом.\n"
            "Нажмите кнопку ниже, чтобы перейти в ЛС.",
            reply_markup=get_pm_link_keyboard()
        )
    else:
        await message.answer(
            "❌ Группа ещё не привязана к вашему аккаунту.\n\n"
            "Нажмите кнопку ниже, чтобы привязать группу.\n"
            "После привязки вы сможете настраивать бота в ЛС.",
            reply_markup=get_link_group_keyboard(chat_id)
        )

@dp.message(Command("adminstats"))
@check_bot_admin()
@pm_only()
async def cmd_admin_stats(message: Message):
    chats = db.get_all_chats()
    text = f"📊 <b>Статистика бота</b>\n\n📱 Всего групп: {len(chats)}\n\n"
    
    if chats:
        text += "<b>📋 Список групп:</b>\n"
        for chat_id, title, username, rules_enabled, welcome_enabled in chats:
            status = []
            if rules_enabled:
                status.append("📜✅")
            if welcome_enabled:
                status.append("👋✅")
            status_text = f" [{''.join(status)}]" if status else ""
            
            if username:
                link = f"https://t.me/{username}"
                group_info = f"<a href='{link}'>{title or 'Без названия'}</a>"
            else:
                group_info = title or 'Без названия'
            
            text += f"• {group_info}{status_text} | ID: <code>{chat_id}</code>\n"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.new_chat_members)
async def on_bot_added(message: Message):
    bot_info = await bot.get_me()
    if any(member.id == bot_info.id for member in message.new_chat_members):
        logger.info(f"Бот добавлен в группу {message.chat.id}")

# ========== ПРИВЯЗКА ГРУППЫ ==========
@dp.callback_query(F.data.startswith("link_group_"))
@check_owner()
async def link_group(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    if not await is_creator(chat_id, user_id):
        await callback.answer("❌ Вы не создатель этой группы!", show_alert=True)
        return
    
    try:
        chat = await bot.get_chat(chat_id)
        db.save_rules(chat_id, owner_id=user_id, chat_title=chat.title, chat_username=chat.username)
    except:
        db.save_rules(chat_id, owner_id=user_id, chat_title="Группа", chat_username=None)
    
    await callback.message.edit_text("✅ Группа успешно привязана! Теперь вы можете настроить её в ЛС.")
    await callback.answer("✅ Группа привязана!")
    
    try:
        await bot.send_message(
            user_id,
            f"✅ Группа привязана! Теперь она доступна в меню настроек.",
            reply_markup=get_main_keyboard(is_group=False)
        )
    except:
        pass

@dp.callback_query(F.data == "cancel_link")
@check_owner()
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

# ========== ВХОД/ВЫХОД ==========
@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id, user = update.chat.id, update.new_chat_member.user
        
        # Регистрируем пользователя
        db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        db.add_user_stat(chat_id, user.id, int(time.time()))
        
        # Проверяем, есть ли владелец у группы
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            owner_id = result[0] if result else None
        
        # Если нет владельца - просто приветствуем
        if not owner_id:
            await send_simple_welcome(chat_id, user)
            return
        
        conf_type = db.get_confirmation_type(chat_id)
        
        # Если подтверждение отключено - просто приветствуем
        if conf_type == 'disabled':
            await send_simple_welcome(chat_id, user)
            return
        
        # Проверяем, подтверждён ли уже пользователь
        not_bot, rules = db.get_user_confirmation_status(chat_id, user.id)
        
        if (conf_type == 'both' and not_bot and rules) or \
           (conf_type == 'not_bot' and not_bot) or \
           (conf_type == 'rules' and rules):
            await send_simple_welcome(chat_id, user)
            return
        
        # Мутим пользователя
        try:
            await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False))
        except:
            pass
        
        rules_html = db.get_rules_html(chat_id)
        rules_enabled = db.get_rules_enabled(chat_id)
        builder = InlineKeyboardBuilder()
        msg_text = ""
        
        if conf_type == 'both':
            msg_text = f"👋 <b>{user.full_name}</b>, выполните два шага:\n1. Подтвердите, что вы не бот\n2. Прочитайте правила"
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {update.chat.title}!\n\nШаг 1: Подтвердите, что вы не бот",
                    reply_markup=get_confirm_not_bot_keyboard(chat_id, user.id, 0)
                )
                if rules_html and rules_enabled:
                    await bot.send_message(
                        user.id,
                        f"Шаг 2: Прочитайте правила:\n\n{rules_html}",
                        reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0),
                        parse_mode="HTML"
                    )
            except:
                await bot.send_message(chat_id, "⚠️ Не удалось отправить подтверждение в ЛС")
            
            builder.add(create_button("📜 Перейти в ЛС", f"go_to_pm_{chat_id}_{user.id}", "primary"))
            
        elif conf_type == 'not_bot':
            msg_text = f"👋 <b>{user.full_name}</b>, подтвердите, что вы не бот"
            builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user.id}_0", "success"))
            
        elif conf_type == 'rules' and rules_html and rules_enabled:
            msg_text = f"👋 <b>{user.full_name}</b>, прочитайте правила"
            builder.add(create_button("📜 Перейти в ЛС", f"go_to_pm_{chat_id}_{user.id}", "primary"))
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {update.chat.title}!\n\nПрочитайте правила:\n\n{rules_html}",
                    reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0),
                    parse_mode="HTML"
                )
            except:
                await bot.send_message(chat_id, "⚠️ Не удалось отправить правила в ЛС")
        
        if msg_text:
            await bot.send_message(chat_id, msg_text, reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    db.set_left_chat(update.chat.id, update.from_user.id)
    await bot.send_message(update.chat.id, f"👋 {update.from_user.full_name} вышел из чата")

async def send_simple_welcome(chat_id, user):
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    join_dt = format_datetime(stat['join_date']) if stat else format_datetime(time.time())
    position = db.get_user_position(chat_id, user.id, 'all')
    
    text = (
        f"Добро пожаловать, <b>{user.full_name}</b>!\n\n"
        f"🆔 <b>ID:</b> <code>{global_user['global_id']}</code>\n"
        f"📅 <b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
        f"• Username: @{user.username or 'нет'}\n"
        f"• Telegram ID: <code>{user.id}</code>\n"
        f"• Вошёл: {join_dt}\n"
        f"• Место в топе: {position}"
    )
    
    welcome_text, welcome_photo = db.get_welcome(chat_id)
    
    if welcome_photo:
        await bot.send_photo(
            chat_id,
            photo=welcome_photo,
            caption=text + (f"\n\n{welcome_text}" if welcome_text else ""),
            reply_markup=get_welcome_buttons(chat_id),
            parse_mode="HTML"
        )
    else:
        await bot.send_message(
            chat_id,
            text + (f"\n\n{welcome_text}" if welcome_text else ""),
            reply_markup=get_welcome_buttons(chat_id),
            parse_mode="HTML"
        )

# ========== ПОДТВЕРЖДЕНИЯ ==========
@dp.callback_query(F.data.startswith("confirm_not_bot_"))
@check_public()
async def process_confirm_not_bot(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id, user_id = int(parts[3]), int(parts[4])
    msg_id = int(parts[5]) if len(parts) > 5 else 0
    
    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не ваше подтверждение!", show_alert=True)
        return
    
    db.mark_user_confirmed(chat_id, user_id, not_bot=True, rules=False)
    
    conf_type = db.get_confirmation_type(chat_id)
    not_bot, rules = db.get_user_confirmation_status(chat_id, user_id)
    
    if conf_type == 'both' and not rules:
        await callback.message.edit_text("✅ Шаг 1 выполнен! Теперь выполните шаг 2.")
        await callback.answer()
        return
    
    # Снимаем мут
    try:
        await bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=True))
    except:
        pass
    
    if msg_id > 0:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"✅ {callback.from_user.full_name} подтвердил, что не бот"
            )
        except:
            pass
    
    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("✅ Спасибо за подтверждение! Теперь вы можете писать в чат.")
    await callback.answer()

@dp.callback_query(F.data.startswith("agree_rules_"))
@check_public()
async def process_agree_rules(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id, user_id, msg_id = int(parts[2]), int(parts[3]), int(parts[4])
    
    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не ваше подтверждение!", show_alert=True)
        return
    
    db.mark_user_confirmed(chat_id, user_id, not_bot=False, rules=True)
    
    conf_type = db.get_confirmation_type(chat_id)
    not_bot, rules = db.get_user_confirmation_status(chat_id, user_id)
    
    if conf_type == 'both' and not not_bot:
        await callback.message.edit_text("✅ Шаг 2 выполнен! Теперь выполните шаг 1.")
        await callback.answer()
        return
    
    # Снимаем мут
    try:
        await bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=True))
    except:
        pass
    
    if msg_id > 0:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"✅ {callback.from_user.full_name} согласился с правилами"
            )
        except:
            pass
    
    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("✅ Спасибо! Теперь вы можете писать в чат.")
    await callback.answer()

@dp.callback_query(F.data.startswith("go_to_pm_"))
@check_public()
async def go_to_pm(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id, user_id = int(parts[3]), int(parts[4])
    
    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не для вас!", show_alert=True)
        return
    
    await callback.message.answer(
        "📱 Откройте личные сообщения с ботом и завершите подтверждение.",
        reply_markup=get_pm_link_keyboard()
    )
    await callback.answer()

# ========== ОБРАБОТЧИК СООБЩЕНИЙ ==========
@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    text = message.text or message.caption or ""
    
    # АВТООТВЕТЧИК
    if text:
        cleaned_text = clean_text(text)
        responses = db.get_auto_responses(chat_id)
        
        for trigger, response in responses:
            if trigger in cleaned_text or trigger in text.lower():
                try:
                    await message.reply(response, parse_mode="HTML", disable_notification=True)
                except:
                    await message.reply(response, disable_notification=True)
                break

# ========== ОБРАБОТЧИКИ НАСТРОЕК В ЛС ==========
@dp.callback_query(F.data == "back_to_main")
@check_owner()
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "👋 <b>Главное меню</b>\n\nВыберите раздел:",
        reply_markup=get_main_keyboard(is_group=False)
    )
    await callback.answer()

@dp.callback_query(F.data == "group_manage_main")
@check_owner()
async def group_manage_main(callback: CallbackQuery, state: FSMContext):
    groups = db.get_user_groups(callback.from_user.id)
    
    if not groups:
        await callback.answer("❌ У вас нет привязанных групп!", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text("📱 <b>Ваши группы</b>\n\nВыберите группу:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "group_manage_group")
@group_only()
async def group_manage_group(callback: CallbackQuery):
    """Обработка нажатия на кнопку управления в группе"""
    await callback.message.answer(
        "⚙️ Настраивать группу можно только в личных сообщениях с ботом.\n\n"
        "Нажмите кнопку ниже, чтобы перейти в ЛС.",
        reply_markup=get_pm_link_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("select_group_"))
@check_owner()
async def select_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    
    if not await is_creator(chat_id, callback.from_user.id):
        await callback.answer("❌ Вы больше не являетесь создателем этой группы!", show_alert=True)
        return
    
    await state.update_data(
        selected_chat_id=chat_id,
        **{f"msg_owner_{callback.message.message_id}": callback.from_user.id}
    )
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Группа"
    
    await callback.message.edit_text(
        f"⚙️ <b>Настройка группы:</b> {chat_title}\n\nВыберите действие:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_groups")
@check_owner()
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    groups = db.get_user_groups(callback.from_user.id)
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text("📱 <b>Ваши группы</b>\n\nВыберите группу:", reply_markup=builder.as_markup())
    await callback.answer()

# ========== УПРАВЛЕНИЕ ПРАВИЛАМИ ==========
@dp.callback_query(F.data == "manage_rules")
@check_owner()
async def manage_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    has_rules = db.get_rules_html(chat_id) is not None
    rules_enabled = db.get_rules_enabled(chat_id)
    status = "✅ Включены" if rules_enabled else "❌ Выключены"
    
    await callback.message.edit_text(
        f"<b>📝 Управление правилами</b>\n\nСтатус: {status}",
        reply_markup=get_rules_manage_keyboard(has_rules, rules_enabled),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "set_rules")
@check_owner()
async def set_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте текст правил для этой группы.\n\n"
        "Вы можете использовать форматирование:\n"
        "• <b>Жирный</b> - &lt;b&gt;текст&lt;/b&gt;\n"
        "• <i>Курсив</i> - &lt;i&gt;текст&lt;/i&gt;\n"
        "• <tg-spoiler>Спойлер</tg-spoiler> - &lt;tg-spoiler&gt;текст&lt;/tg-spoiler&gt;\n"
        "• <blockquote>Цитата</blockquote> - &lt;blockquote&gt;текст&lt;/blockquote&gt;\n"
        "• <blockquote expandable>Свернутая цитата\nСтрока 2\nСтрока 3</blockquote> - &lt;blockquote expandable&gt;текст\nстроки&lt;/blockquote&gt;",
        reply_markup=get_back_keyboard("manage_rules")
    )
    await state.set_state(RulesStates.waiting_for_rules_text)
    await callback.answer()

@dp.message(RulesStates.waiting_for_rules_text)
async def process_rules_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    rules_html = message.html_text.strip()
    
    if not rules_html or len(rules_html) < 10:
        await message.answer("❌ Правила слишком короткие!")
        return
    
    db.save_rules(chat_id, rules_html=rules_html)
    db.set_rules_enabled(chat_id, True)
    
    await message.reply("✅ <b>Правила сохранены!</b>", parse_mode="HTML")
    await state.clear()

@dp.callback_query(F.data == "set_default_rules")
@check_owner()
async def set_default_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_rules(chat_id, rules_html=DEFAULT_RULES)
    db.set_rules_enabled(chat_id, True)
    
    await callback.answer("✅ Готовые правила установлены!", show_alert=True)
    await manage_rules(callback, state)

@dp.callback_query(F.data == "show_rules")
@check_owner()
async def show_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    rules_html = db.get_rules_html(chat_id)
    
    if rules_html:
        await callback.message.edit_text(
            f"📜 <b>Текущие правила:</b>\n\n{rules_html}",
            parse_mode="HTML",
            reply_markup=get_back_keyboard("manage_rules")
        )
    else:
        await callback.message.edit_text(
            "❌ Правила ещё не установлены",
            reply_markup=get_back_keyboard("manage_rules")
        )
    
    await callback.answer()

@dp.callback_query(F.data == "edit_rules")
@check_owner()
async def edit_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте новый текст правил:",
        reply_markup=get_back_keyboard("manage_rules")
    )
    await state.set_state(RulesStates.waiting_for_new_rules_text)
    await callback.answer()

@dp.message(RulesStates.waiting_for_new_rules_text)
async def process_edit_rules_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    rules_html = message.html_text.strip()
    
    if not rules_html or len(rules_html) < 10:
        await message.answer("❌ Правила слишком короткие!")
        return
    
    db.save_rules(chat_id, rules_html=rules_html)
    
    await message.reply("✅ <b>Правила обновлены!</b>", parse_mode="HTML")
    await state.clear()

@dp.callback_query(F.data == "delete_rules_confirm")
@check_owner()
async def delete_rules_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, удалить", "delete_rules", "danger"))
    builder.add(create_button("🚫 Нет", "manage_rules", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text("❓ Вы уверены, что хотите удалить правила?", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "delete_rules")
@check_owner()
async def delete_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.delete_rules(chat_id)
    
    await callback.answer("✅ Правила удалены!", show_alert=True)
    await manage_rules(callback, state)

@dp.callback_query(F.data == "toggle_rules")
@check_owner()
async def toggle_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    current = db.get_rules_enabled(chat_id)
    db.set_rules_enabled(chat_id, not current)
    
    status = "включены" if not current else "выключены"
    await callback.answer(f"✅ Правила {status}!", show_alert=True)
    await manage_rules(callback, state)

# ========== УПРАВЛЕНИЕ ПРИВЕТСТВИЕМ ==========
@dp.callback_query(F.data == "manage_welcome")
@check_owner()
async def manage_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    enabled = db.get_welcome_enabled(chat_id)
    await callback.message.edit_text(
        "👋 <b>Управление приветствием</b>\n\nНастройте приветствие для новых участников.",
        reply_markup=get_welcome_manage_keyboard(enabled)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_welcome")
@check_owner()
async def toggle_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    current = db.get_welcome_enabled(chat_id)
    db.set_welcome_enabled(chat_id, not current)
    
    await callback.answer(f"✅ Приветствие {'включено' if not current else 'выключено'}!", show_alert=True)
    await manage_welcome(callback, state)

@dp.callback_query(F.data == "set_welcome_text")
@check_owner()
async def set_welcome_text(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте текст приветствия.\n\n"
        "Можно использовать:\n"
        "• {name} - имя\n"
        "• {username} - юзернейм\n"
        "• {chat} - название группы\n\n"
        "Пример: Добро пожаловать, {name}!",
        reply_markup=get_back_keyboard("manage_welcome")
    )
    await state.set_state(WelcomeStates.waiting_for_welcome_text)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    welcome_text = message.html_text.strip()
    
    if not welcome_text:
        await message.answer("❌ Текст не может быть пустым!")
        return
    
    db.save_welcome(chat_id, welcome_text=welcome_text)
    
    await message.reply("✅ Текст приветствия сохранён!")
    await state.clear()

@dp.callback_query(F.data == "set_welcome_photo")
@check_owner()
async def set_welcome_photo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🖼 Отправьте фото для приветствия.\n\nОно будет отправляться вместе с текстом.",
        reply_markup=get_back_keyboard("manage_welcome")
    )
    await state.set_state(WelcomeStates.waiting_for_welcome_photo)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_photo, F.photo)
async def process_welcome_photo(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    photo_id = message.photo[-1].file_id
    db.save_welcome(chat_id, welcome_photo_id=photo_id)
    
    await message.reply("✅ Фото сохранено!")
    await state.clear()

@dp.message(WelcomeStates.waiting_for_welcome_photo)
async def process_welcome_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Пожалуйста, отправьте фото!")

@dp.callback_query(F.data == "show_welcome")
@check_owner()
async def show_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    text, photo_id = db.get_welcome(chat_id)
    
    if not text and not photo_id:
        await callback.message.edit_text(
            "❌ Приветствие ещё не настроено",
            reply_markup=get_back_keyboard("manage_welcome")
        )
        await callback.answer()
        return
    
    await callback.message.delete()
    
    if photo_id:
        await callback.message.answer_photo(
            photo_id,
            caption=f"👋 <b>Текущее приветствие:</b>\n\n{text}" if text else None,
            reply_markup=get_back_keyboard("manage_welcome"),
            parse_mode="HTML"
        )
    else:
        await callback.message.answer(
            f"👋 <b>Текущее приветствие:</b>\n\n{text}",
            reply_markup=get_back_keyboard("manage_welcome"),
            parse_mode="HTML"
        )
    
    await callback.answer()

# ========== АВТО-РАССЫЛКА ==========
@dp.callback_query(F.data == "rules_auto")
@check_owner()
async def rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    
    await callback.message.edit_text(
        f"🔄 <b>Авто-рассылка правил</b>\n\n"
        f"Статус: {'✅ Включена' if enabled else '❌ Выключена'}\n"
        f"Интервал: {format_interval(interval)}",
        reply_markup=get_rules_auto_keyboard(bool(enabled))
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_rules_auto")
@check_owner()
async def toggle_rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    new_enabled = not enabled
    db.set_rules_auto_settings(chat_id, new_enabled, interval)
    
    await callback.answer(f"✅ Авто-рассылка {'включена' if new_enabled else 'выключена'}!", show_alert=True)
    await rules_auto(callback, state)

@dp.callback_query(F.data == "set_interval")
@check_owner()
async def set_interval(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите интервал в минутах (от 5 до 525600):",
        reply_markup=get_back_keyboard("rules_auto")
    )
    await state.set_state(RulesStates.waiting_for_interval)
    await callback.answer()

@dp.message(RulesStates.waiting_for_interval)
async def process_interval(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        interval = int(message.text)
        if interval < 5 or interval > 525600:
            await message.answer("❌ Интервал должен быть от 5 до 525600 минут!")
            return
        
        enabled, _, _, _ = db.get_rules_auto_settings(chat_id)
        db.set_rules_auto_settings(chat_id, bool(enabled), interval * 60)
        
        await message.reply(f"✅ Интервал установлен: {format_interval(interval * 60)}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

# ========== АНТИФЛУД ==========
@dp.callback_query(F.data == "antiflood_manage")
@check_owner()
async def antiflood_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    
    await callback.message.edit_text(
        f"🚫 <b>Антифлуд</b>\n\n"
        f"Статус: {'✅ Включён' if settings['enabled'] else '❌ Выключен'}\n"
        f"• Текст: {settings['msg_limit']} сообщ.\n"
        f"• Медиа: {settings['media_limit']} сообщ.\n"
        f"• Окно: {settings['time_window']} сек\n"
        f"• Предупреждений: {settings['warn_count']}\n"
        f"• Первое: {settings['first_punish']} ({settings['first_duration']} сек)\n"
        f"• Повторное: {settings['repeat_punish']} ({settings['repeat_duration']} сек)\n"
        f"• После варнов: {settings['punish_after_warn']}",
        reply_markup=get_antiflood_manage_keyboard(settings)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_antiflood")
@check_owner()
async def toggle_antiflood(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    new_enabled = not settings['enabled']
    db.set_antiflood_enabled(chat_id, new_enabled)
    
    await callback.answer(f"✅ Антифлуд {'включён' if new_enabled else 'выключен'}!", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_msg_limit")
@check_owner()
async def set_msg_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📊 Введите лимит текстовых сообщений (3-50):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.set_state(AntiFloodStates.waiting_for_message_limit)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_message_limit)
async def process_msg_limit(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        limit = int(message.text)
        if limit < 3 or limit > 50:
            await message.answer("❌ Лимит должен быть от 3 до 50!")
            return
        
        db.save_antiflood_settings(chat_id, msg_limit=limit)
        await message.reply(f"✅ Лимит текстовых сообщений установлен: {limit}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_media_limit")
@check_owner()
async def set_media_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🎬 Введите лимит медиа-сообщений (2-20):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.set_state(AntiFloodStates.waiting_for_media_limit)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_media_limit)
async def process_media_limit(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        limit = int(message.text)
        if limit < 2 or limit > 20:
            await message.answer("❌ Лимит должен быть от 2 до 20!")
            return
        
        db.save_antiflood_settings(chat_id, media_limit=limit)
        await message.reply(f"✅ Лимит медиа установлен: {limit}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_window")
@check_owner()
async def set_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите временное окно в секундах (5-300):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.set_state(AntiFloodStates.waiting_for_window)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_window)
async def process_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        window = int(message.text)
        if window < 5 or window > 300:
            await message.answer("❌ Окно должно быть от 5 до 300 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, time_window=window)
        await message.reply(f"✅ Окно установлено: {window} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_warn_count")
@check_owner()
async def set_warn_count(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⚠️ Введите количество предупреждений перед наказанием (1-10):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.set_state(AntiFloodStates.waiting_for_warn_count)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_warn_count)
async def process_warn_count(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        count = int(message.text)
        if count < 1 or count > 10:
            await message.answer("❌ Количество предупреждений должно быть от 1 до 10!")
            return
        
        db.save_antiflood_settings(chat_id, warn_count=count)
        await message.reply(f"✅ Предупреждений: {count}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_first_punish")
@check_owner()
async def set_first_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔇 Выберите наказание для первого нарушения:",
        reply_markup=get_punish_type_keyboard(is_first=True, prefix="first_")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_repeat_punish")
@check_owner()
async def set_repeat_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔊 Выберите наказание для повторных нарушений:",
        reply_markup=get_punish_type_keyboard(is_first=False, prefix="repeat_")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_punish_after_warn")
@check_owner()
async def set_punish_after_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⚠️ Выберите наказание после достижения лимита предупреждений:",
        reply_markup=get_punish_type_keyboard(is_first=False, prefix="after_")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_warn_first_"))
@check_owner()
async def punish_first_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, first_punish='warn')
    await callback.answer("✅ Наказание: предупреждение", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_mute_first_"))
@check_owner()
async def punish_first_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность мута в секундах (30-86400):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.update_data(setting_punish='first_punish', punish_type='mute')
    await state.set_state(AntiFloodStates.waiting_for_first_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_kick_first_"))
@check_owner()
async def punish_first_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, first_punish='kick')
    await callback.answer("✅ Наказание: кик", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_ban_first_"))
@check_owner()
async def punish_first_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность бана в секундах (60-604800):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.update_data(setting_punish='first_punish', punish_type='ban')
    await state.set_state(AntiFloodStates.waiting_for_first_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_warn_repeat_"))
@check_owner()
async def punish_repeat_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, repeat_punish='warn')
    await callback.answer("✅ Наказание: предупреждение", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_mute_repeat_"))
@check_owner()
async def punish_repeat_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность мута в секундах (60-604800):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.update_data(setting_punish='repeat_punish', punish_type='mute')
    await state.set_state(AntiFloodStates.waiting_for_repeat_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_kick_repeat_"))
@check_owner()
async def punish_repeat_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, repeat_punish='kick')
    await callback.answer("✅ Наказание: кик", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_ban_repeat_"))
@check_owner()
async def punish_repeat_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность бана в секундах (120-1209600):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.update_data(setting_punish='repeat_punish', punish_type='ban')
    await state.set_state(AntiFloodStates.waiting_for_repeat_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_warn_after_"))
@check_owner()
async def punish_after_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, punish_after_warn='warn')
    await callback.answer("✅ Наказание после варнов: предупреждение", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_mute_after_"))
@check_owner()
async def punish_after_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность мута в секундах (30-86400):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.update_data(setting_punish='punish_after_warn', punish_type='mute')
    await state.set_state(AntiFloodStates.waiting_for_repeat_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_kick_after_"))
@check_owner()
async def punish_after_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, punish_after_warn='kick')
    await callback.answer("✅ Наказание после варнов: кик", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_ban_after_"))
@check_owner()
async def punish_after_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность бана в секундах (60-604800):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
    await state.update_data(setting_punish='punish_after_warn', punish_type='ban')
    await state.set_state(AntiFloodStates.waiting_for_repeat_duration)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_first_duration)
async def process_first_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    setting = data.get('setting_punish')
    p_type = data.get('punish_type')
    
    if not chat_id or not setting or not p_type or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        if p_type == 'mute' and (duration < 30 or duration > 86400):
            await message.answer("❌ Длительность мута должна быть от 30 до 86400 секунд!")
            return
        elif p_type == 'ban' and (duration < 60 or duration > 604800):
            await message.answer("❌ Длительность бана должна быть от 60 до 604800 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, **{setting: p_type, f"{setting.replace('punish', 'duration')}": duration})
        await message.reply(f"✅ Настройки сохранены!")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.message(AntiFloodStates.waiting_for_repeat_duration)
async def process_repeat_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    setting = data.get('setting_punish')
    p_type = data.get('punish_type')
    
    if not chat_id or not setting or not p_type or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        if setting == 'repeat_punish':
            if p_type == 'mute' and (duration < 60 or duration > 604800):
                await message.answer("❌ Длительность мута должна быть от 60 до 604800 секунд!")
                return
            elif p_type == 'ban' and (duration < 120 or duration > 1209600):
                await message.answer("❌ Длительность бана должна быть от 120 до 1209600 секунд!")
                return
        else:
            if p_type == 'mute' and (duration < 30 or duration > 86400):
                await message.answer("❌ Длительность мута должна быть от 30 до 86400 секунд!")
                return
            elif p_type == 'ban' and (duration < 60 or duration > 604800):
                await message.answer("❌ Длительность бана должна быть от 60 до 604800 секунд!")
                return
        
        db.save_antiflood_settings(chat_id, **{setting: p_type, f"{setting.replace('punish', 'duration')}": duration})
        await message.reply(f"✅ Настройки сохранены!")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

# ========== ГРУППА РЕПОРТОВ ==========
@dp.callback_query(F.data == "set_report_group")
@check_owner()
async def set_report_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    groups = db.get_user_groups(callback.from_user.id)
    
    if not groups:
        await callback.answer("❌ У вас нет привязанных групп!", show_alert=True)
        return
    
    current = db.get_report_group(chat_id)
    current_name = ""
    if current:
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (current,))
            res = c.fetchone()
            current_name = res[0] if res else str(current)
    
    text = f"📋 <b>Группа репортов</b>\n\n"
    if current:
        text += f"Текущая группа: {current_name} (ID: {current})\n\n"
    text += "Выберите группу для отправки логов нарушений:"
    
    await callback.message.edit_text(text, reply_markup=get_report_group_keyboard(groups), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data.startswith("set_report_group_"))
@check_owner()
async def process_set_report_group(callback: CallbackQuery, state: FSMContext):
    report_group_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.set_report_group(chat_id, report_group_id)
    
    await callback.answer("✅ Группа репортов установлена!", show_alert=True)
    await set_report_group(callback, state)

@dp.callback_query(F.data == "remove_report_group")
@check_owner()
async def remove_report_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.set_report_group(chat_id, None)
    
    await callback.answer("✅ Группа репортов удалена!", show_alert=True)
    await set_report_group(callback, state)

# ========== АВТООТВЕТЧИК ==========
@dp.callback_query(F.data == "auto_response_manage")
@check_owner()
async def auto_response_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    responses = db.get_auto_responses(chat_id)
    count = len(responses)
    
    if not responses:
        text = f"🤖 <b>Автоответчик</b>\n\nСписок триггеров пуст.\nМаксимум: {MAX_TRIGGERS} триггеров"
    else:
        text = f"🤖 <b>Автоответчик</b> ({count}/{MAX_TRIGGERS})\n\n"
        for trigger, resp in responses:
            short_resp = resp[:30] + "..." if len(resp) > 30 else resp
            text += f"• <code>{trigger}</code> → {short_resp}\n"
    
    await callback.message.edit_text(text, reply_markup=get_auto_response_keyboard(responses), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "add_auto_trigger")
@check_owner()
async def add_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    # Проверяем лимит
    responses = db.get_auto_responses(data.get('selected_chat_id'))
    if len(responses) >= MAX_TRIGGERS:
        await callback.answer(f"❌ Достигнут лимит триггеров ({MAX_TRIGGERS})!", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"📝 Введите ключевое слово (триггер).\nМакс. длина: {MAX_TRIGGER_LENGTH} символов",
        reply_markup=get_back_keyboard("auto_response_manage")
    )
    await state.set_state(AutoResponseStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponseStates.waiting_for_trigger)
async def process_auto_trigger(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    trigger = clean_text(message.text)
    
    if not trigger:
        await message.answer("❌ Триггер не может быть пустым!")
        return
    
    if len(trigger) > MAX_TRIGGER_LENGTH:
        await message.answer(f"❌ Триггер слишком длинный! Максимум {MAX_TRIGGER_LENGTH} символов")
        return
    
    await state.update_data(auto_trigger=trigger)
    await message.reply(
        f"📝 Введите ответ для триггера '{trigger}'.\nМакс. длина: {MAX_RESPONSE_LENGTH} символов",
        reply_markup=get_back_keyboard("auto_response_manage")
    )
    await state.set_state(AutoResponseStates.waiting_for_response)

@dp.message(AutoResponseStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    response = message.html_text.strip()
    
    if not response:
        await message.answer("❌ Ответ не может быть пустым!")
        return
    
    if len(response) > MAX_RESPONSE_LENGTH:
        await message.answer(f"❌ Ответ слишком длинный! Максимум {MAX_RESPONSE_LENGTH} символов")
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    trigger = data.get('auto_trigger')
    
    if not chat_id or not trigger or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    success, msg = db.add_auto_response(chat_id, trigger, response)
    await message.reply(msg)
    await state.clear()

@dp.callback_query(F.data == "remove_auto_trigger")
@check_owner()
async def remove_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    responses = db.get_auto_responses(chat_id)
    
    if not responses:
        await callback.answer("❌ Нет триггеров для удаления!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🗑 Выберите триггер для удаления:",
        reply_markup=get_auto_response_remove_keyboard(responses)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("rem_trig_"))
@check_owner()
async def process_remove_trigger(callback: CallbackQuery, state: FSMContext):
    index = int(callback.data.split('_')[-1])
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    responses = db.get_auto_responses(chat_id)
    
    if index < 0 or index >= len(responses):
        await callback.answer("❌ Триггер не найден!", show_alert=True)
        return
    
    trigger = responses[index][0]
    db.remove_auto_response(chat_id, trigger)
    
    await callback.answer(f"✅ Триггер '{trigger}' удалён!", show_alert=True)
    await auto_response_manage(callback, state)

# ========== ССЫЛКИ И УПОМИНАНИЯ ==========
@dp.callback_query(F.data == "links_manage")
@check_owner()
async def links_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    
    await callback.message.edit_text(
        f"🔗 <b>Ссылки и упоминания</b>\n\n"
        f"Фильтр ссылок: {'✅ Вкл' if settings['links_enabled'] else '❌ Выкл'}\n"
        f"Наказание: {settings['links_punish']}\n"
        f"Макс упоминаний: {settings['max_mentions']} за {settings['mention_window']} сек",
        reply_markup=get_links_manage_keyboard(settings)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_links")
@check_owner()
async def toggle_links(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    new_enabled = not settings['links_enabled']
    db.save_antiflood_settings(chat_id, links_enabled=int(new_enabled))
    
    await callback.answer(f"✅ Фильтр ссылок {'включён' if new_enabled else 'выключен'}!", show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "set_links_punish")
@check_owner()
async def set_links_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Выберите наказание для ссылок и упоминаний:",
        reply_markup=get_links_punish_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "links_punish_warn")
@check_owner()
async def links_punish_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, links_punish='warn')
    await callback.answer("✅ Наказание: предупреждение", show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "links_punish_mute")
@check_owner()
async def links_punish_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность мута в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.update_data(links_punish='mute')
    await state.set_state(LinksStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "links_punish_kick")
@check_owner()
async def links_punish_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.save_antiflood_settings(chat_id, links_punish='kick')
    await callback.answer("✅ Наказание: кик", show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "links_punish_ban")
@check_owner()
async def links_punish_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность бана в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.update_data(links_punish='ban')
    await state.set_state(LinksStates.waiting_for_duration)
    await callback.answer()

@dp.message(LinksStates.waiting_for_duration)
async def process_links_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    punish = data.get('links_punish')
    
    if not chat_id or not punish or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Введите положительное число или 0!")
            return
        
        duration = minutes * 60
        db.save_antiflood_settings(chat_id, links_punish=punish, links_duration=duration)
        
        time_str = "навсегда" if minutes == 0 else f"{minutes} мин"
        await message.reply(f"✅ Наказание: {punish} на {time_str}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_max_mentions")
@check_owner()
async def set_max_mentions(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📊 Введите максимальное количество упоминаний (1-50):",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.set_state(LinksStates.waiting_for_max_mentions)
    await callback.answer()

@dp.message(LinksStates.waiting_for_max_mentions)
async def process_max_mentions(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        count = int(message.text)
        if count < 1 or count > 50:
            await message.answer("❌ Значение должно быть от 1 до 50!")
            return
        
        db.save_antiflood_settings(chat_id, max_mentions=count)
        await message.reply(f"✅ Макс упоминаний: {count}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_mention_window")
@check_owner()
async def set_mention_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите окно упоминаний в секундах (10-3600):",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.set_state(LinksStates.waiting_for_mention_window)
    await callback.answer()

@dp.message(LinksStates.waiting_for_mention_window)
async def process_mention_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    try:
        window = int(message.text)
        if window < 10 or window > 3600:
            await message.answer("❌ Окно должно быть от 10 до 3600 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, mention_window=window)
        await message.reply(f"✅ Окно упоминаний: {window} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

# ========== НАСТРОЙКИ ПОДТВЕРЖДЕНИЯ ==========
@dp.callback_query(F.data == "confirmation_manage")
@check_owner()
async def confirmation_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    conf_type = db.get_confirmation_type(chat_id)
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    
    type_names = {
        'disabled': '🚫 Отключено',
        'not_bot': '🤖 Только не бот',
        'rules': '📜 Только правила',
        'both': '2️⃣ Оба шага'
    }
    
    warning = ""
    if (conf_type in ['rules', 'both']) and not has_rules:
        warning = "\n\n⚠️ <b>Внимание:</b> Правила не установлены. Эта настройка не будет работать."
    
    await callback.message.edit_text(
        f"✅ <b>Настройки подтверждения</b>\n\n"
        f"Тип: {type_names.get(conf_type, conf_type)}{warning}",
        reply_markup=get_confirmation_keyboard(conf_type, has_rules),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirmation_"))
@check_owner()
async def process_confirmation_type(callback: CallbackQuery, state: FSMContext):
    conf_type = callback.data.replace("confirmation_", "")
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    
    if conf_type in ['rules', 'both'] and not has_rules:
        error = "❌ Нельзя выбрать 'Только правила' - сначала установите правила!" if conf_type == 'rules' else "❌ Нельзя выбрать 'Оба шага' - сначала установите правила!"
        await callback.answer(error, show_alert=True)
        return
    
    db.set_confirmation_type(chat_id, conf_type)
    
    names = {
        'disabled': '🚫 Отключено',
        'not_bot': '🤖 Только не бот',
        'rules': '📜 Только правила',
        'both': '2️⃣ Оба шага'
    }
    
    await callback.answer(f"✅ Установлено: {names.get(conf_type)}", show_alert=True)
    await confirmation_manage(callback, state)

# ========== ОТВЯЗКА ГРУППЫ ==========
@dp.callback_query(F.data == "unlink_group_confirm")
@check_owner()
async def unlink_group_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "❓ Вы уверены, что хотите отвязать группу?\n\nВсе настройки будут сохранены, но вы больше не сможете управлять ей.",
        reply_markup=get_unlink_confirm_keyboard(chat_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("unlink_group_"))
@check_owner()
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET owner_id = NULL WHERE chat_id = ?', (chat_id,))
        conn.commit()
    
    await callback.message.edit_text("✅ Группа отвязана от вашего аккаунта.")
    await callback.answer("✅ Группа отвязана!")
    
    await state.clear()
    await cmd_start_pm(callback.message, state)

@dp.callback_query(F.data == "group_manage")
@check_owner()
async def back_to_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.message.edit_text("❌ Ошибка! Начните заново.")
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Группа"
    
    await callback.message.edit_text(
        f"⚙️ <b>Настройка группы:</b> {chat_title}\n\nВыберите действие:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

# ========== ПУБЛИЧНЫЕ КНОПКИ ==========
@dp.callback_query(F.data.startswith("show_group_rules_"))
@check_public()
async def show_group_rules(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    rules = db.get_rules_html(chat_id)
    
    if rules and db.get_rules_enabled(chat_id):
        await callback.message.answer(f"📜 <b>Правила чата</b>\n\n{rules}", parse_mode="HTML")
    else:
        await callback.message.answer("❌ В этом чате ещё не установлены правила.")
    
    await callback.answer()

@dp.callback_query(F.data.startswith("my_stats_"))
@check_public()
async def my_stats(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user = callback.from_user
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    position = db.get_user_position(chat_id, user.id, 'all')
    
    if not stat:
        text = f"<b>Ваш профиль</b>\n\n🆔 ID: <code>{global_user['global_id']}</code>\n📅 Зарегистрирован: {format_datetime(global_user['first_seen'])}\n\n📊 В этом чате у вас пока нет сообщений"
    else:
        text = (
            f"<b>Ваш профиль</b>\n\n"
            f"🆔 ID: <code>{global_user['global_id']}</code>\n"
            f"📅 Зарегистрирован: {format_datetime(global_user['first_seen'])}\n\n"
            f"📊 <b>Статистика в этом чате:</b>\n"
            f"• За день: {stat['day_messages']} сообщ.\n"
            f"• За неделю: {stat['week_messages']} сообщ.\n"
            f"• За месяц: {stat['month_messages']} сообщ.\n"
            f"• Всего: {stat['all_messages']} сообщ.\n"
            f"• Место в топе: {position}"
        )
    
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data.startswith("top_active_"))
@check_public()
async def top_active(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    
    top = db.get_top_messages(chat_id, limit=10)
    
    if not top:
        await callback.message.answer("📊 В этом чате пока нет сообщений")
        await callback.answer()
        return
    
    text = "<b>🏆 Топ активных (всего сообщений):</b>\n\n"
    for i, (uid, count) in enumerate(top, 1):
        try:
            name = (await bot.get_chat_member(chat_id, uid)).user.full_name
        except:
            name = f"ID {uid}"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        text += f"{medal} {name} — {count} сообщ.\n"
    
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "about")
@check_public()
async def about(callback: CallbackQuery):
    await callback.message.edit_text(
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "Версия: 3.26.0\n\n"
        "📌 <b>Возможности:</b>\n"
        "• Управление правилами\n"
        "• Авто-рассылка\n"
        "• Антифлуд (текст/медиа)\n"
        "• Автоответчик (до 30 триггеров)\n"
        "• Статистика сообщений\n"
        "• Приветствия\n"
        "• Группа репортов\n"
        "• Подтверждение входа\n\n"
        "➕ Нажмите «Добавить в группу» чтобы пригласить меня",
        reply_markup=get_main_keyboard(is_group=False)
    )
    await callback.answer()

@dp.callback_query(F.data == "help")
@check_public()
async def help(callback: CallbackQuery):
    await callback.message.edit_text(
        "🆘 <b>Помощь</b>\n\n"
        "🔹 <b>Команды в группе:</b>\n"
        "• /rules - показать правила\n"
        "• /stats - моя статистика\n"
        "• /top - топ активных\n"
        "• /profile - профиль пользователя\n"
        "• /group - управление группой\n"
        "• /puls - проверка пинга\n\n"
        "🔹 <b>В ЛС:</b>\n"
        "• /start - главное меню\n"
        "• /groupsettings - управление группами\n"
        "• /adminstats - статистика бота (для админов)\n\n"
        "🔹 <b>Для новых участников:</b>\n"
        "• Нужно подтвердить, что не бот\n"
        "• Если есть правила - согласиться с ними\n"
        "• Мут снимается после подтверждения",
        reply_markup=get_main_keyboard(is_group=False)
    )
    await callback.answer()

# ========== АДМИН ПАНЕЛЬ ==========
@dp.message(Command("admin"))
@check_bot_admin()
@pm_only()
async def admin_panel(message: Message, state: FSMContext):
    await state.clear()
    
    status = "🟢 РАБОТАЕТ" if not technical_maintenance else "🔴 ТЕХРАБОТЫ"
    color = "success" if not technical_maintenance else "danger"
    
    text = (
        "👑 <b>Панель администратора</b>\n\n"
        f"Статус бота: {status}\n"
        f"Сообщение: {maintenance_message}\n\n"
        "Выберите действие:"
    )
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📊 Статистика", "admin_stats", "primary"))
    builder.add(create_button("📱 Группы", "admin_groups", "primary"))
    builder.add(create_button("👥 Пользователи", "admin_users", "primary"))
    builder.add(create_button("📋 Логи", "admin_logs", "primary"))
    builder.add(create_button("🛠 Техработы", "admin_maintenance", color))
    builder.add(create_button("📢 Рассылка", "admin_broadcast", "success"))
    builder.add(create_button("📦 Бэкап", "admin_backup", "secondary"))
    builder.add(create_button("❌ Выключить", "admin_shutdown", "danger"))
    builder.adjust(2)
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_maintenance")
@check_bot_admin()
async def admin_maintenance(callback: CallbackQuery):
    global technical_maintenance, maintenance_message
    
    text = (
        f"🛠 <b>Режим технических работ</b>\n\n"
        f"Статус: {'🔴 ВКЛ' if technical_maintenance else '🟢 ВЫКЛ'}\n"
        f"Сообщение: {maintenance_message}"
    )
    
    builder = InlineKeyboardBuilder()
    if technical_maintenance:
        builder.add(create_button("🟢 Выключить", "maintenance_off", "success"))
    else:
        builder.add(create_button("🔴 Включить", "maintenance_on", "danger"))
    builder.add(create_button("✏️ Изменить сообщение", "maintenance_message", "primary"))
    builder.add(create_button("◀️ Назад", "admin_back", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "maintenance_on")
@check_bot_admin()
async def maintenance_on(callback: CallbackQuery):
    global technical_maintenance
    technical_maintenance = True
    await notify_all_groups(maintenance_message)
    await callback.answer("🛠 Техработы ВКЛЮЧЕНЫ!", show_alert=True)
    await admin_maintenance(callback)

@dp.callback_query(F.data == "maintenance_off")
@check_bot_admin()
async def maintenance_off(callback: CallbackQuery):
    global technical_maintenance
    technical_maintenance = False
    await notify_all_groups("✅ Бот снова в работе!")
    await callback.answer("🟢 Техработы ВЫКЛЮЧЕНЫ!", show_alert=True)
    await admin_maintenance(callback)

@dp.callback_query(F.data == "maintenance_message")
@check_bot_admin()
async def maintenance_message(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📝 Отправьте новое сообщение для режима техработ:"
    )
    await state.set_state(MaintenanceStates.waiting_for_message)
    await callback.answer()

@dp.message(Command("cancel"))
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено")

@dp.message(MaintenanceStates.waiting_for_message)
async def process_maintenance_message(message: Message, state: FSMContext):
    global maintenance_message
    maintenance_message = message.text
    await state.clear()
    await message.reply(f"✅ Сообщение сохранено: {maintenance_message}")

async def notify_all_groups(text):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM group_rules')
        for chat_id, in c.fetchall():
            try:
                await bot.send_message(chat_id, text)
                await asyncio.sleep(0.05)
            except:
                pass

@dp.callback_query(F.data == "admin_stats")
@check_bot_admin()
async def admin_stats(callback: CallbackQuery):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM group_rules')
        total_groups = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM global_users')
        total_users = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM violation_logs')
        total_violations = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM auto_responses')
        total_triggers = c.fetchone()[0] or 0
    
    text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"📱 Групп: {total_groups}\n"
        f"👥 Пользователей: {total_users}\n"
        f"🚫 Нарушений: {total_violations}\n"
        f"🤖 Триггеров: {total_triggers}\n\n"
        f"🕐 Время сервера: {datetime.now(SERVER_TZ).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔄 Обновить", "admin_stats", "primary"))
    builder.add(create_button("◀️ Назад", "admin_back", "secondary"))
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_groups")
@check_bot_admin()
async def admin_groups(callback: CallbackQuery):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id, chat_title, rules_enabled, welcome_enabled FROM group_rules LIMIT 20')
        groups = c.fetchall()
    
    text = "📱 <b>Группы (первые 20):</b>\n\n"
    
    for chat_id, title, rules_enabled, welcome_enabled in groups:
        status = []
        if rules_enabled:
            status.append("📜✅")
        if welcome_enabled:
            status.append("👋✅")
        status_text = f" [{''.join(status)}]" if status else ""
        text += f"• {title or 'Без названия'}{status_text} | ID: <code>{chat_id}</code>\n"
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", "admin_back", "secondary"))
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
@check_bot_admin()
async def admin_users(callback: CallbackQuery):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT full_name, global_id, first_seen FROM global_users ORDER BY first_seen DESC LIMIT 20')
        users = c.fetchall()
    
    text = "👥 <b>Последние пользователи:</b>\n\n"
    
    for name, gid, ts in users:
        date = format_datetime(ts)
        text += f"• {name}\n  ID: <code>{gid}</code> | {date}\n\n"
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", "admin_back", "secondary"))
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_logs")
@check_bot_admin()
async def admin_logs(callback: CallbackQuery):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user_name, reason, punishment, timestamp FROM violation_logs ORDER BY timestamp DESC LIMIT 20')
        logs = c.fetchall()
    
    text = "📋 <b>Последние нарушения:</b>\n\n"
    
    if logs:
        for name, reason, punishment, ts in logs:
            date = format_datetime(ts)
            text += f"• <b>{name}</b>\n  {reason} → {punishment} | {date}\n\n"
    else:
        text += "Нарушений пока нет."
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🗑 Очистить", "admin_logs_clear", "danger"))
    builder.add(create_button("◀️ Назад", "admin_back", "secondary"))
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_logs_clear")
@check_bot_admin()
async def admin_logs_clear(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, очистить", "admin_logs_clear_confirm", "danger"))
    builder.add(create_button("🚫 Нет", "admin_logs", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите очистить все логи?</b>\n\nЭто действие нельзя отменить!",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_logs_clear_confirm")
@check_bot_admin()
async def admin_logs_clear_confirm(callback: CallbackQuery):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM violation_logs')
        conn.commit()
    
    await callback.answer("✅ Все логи очищены!", show_alert=True)
    await admin_logs(callback)

@dp.callback_query(F.data == "admin_broadcast")
@check_bot_admin()
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📢 <b>Рассылка сообщений</b>\n\n"
        "Отправьте текст для рассылки во все группы.\n\n"
        "Или отправьте /cancel для отмены."
    )
    await state.set_state(AdminBroadcastStates.waiting_for_text)
    await callback.answer()

@dp.message(AdminBroadcastStates.waiting_for_text)
async def process_broadcast_text(message: Message, state: FSMContext):
    text = message.text
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM group_rules')
        groups = c.fetchall()
    
    if not groups:
        await message.answer("❌ Нет групп для рассылки")
        await state.clear()
        return
    
    sent, failed = 0, 0
    status_msg = await message.answer(f"📤 Начинаю рассылку...\nВсего групп: {len(groups)}")
    
    for chat_id, in groups:
        try:
            await bot.send_message(chat_id, text)
            sent += 1
        except:
            failed += 1
        
        if (sent + failed) % 5 == 0:
            await status_msg.edit_text(f"📤 Прогресс: {sent + failed}/{len(groups)}\n✅ {sent}\n❌ {failed}")
        await asyncio.sleep(0.05)
    
    await status_msg.edit_text(f"✅ Рассылка завершена!\n✅ Успешно: {sent}\n❌ Ошибок: {failed}")
    await state.clear()

@dp.callback_query(F.data == "admin_backup")
@check_bot_admin()
async def admin_backup(callback: CallbackQuery):
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2("puls_manager.db", backup_name)
        
        await callback.message.answer_document(
            FSInputFile(backup_name),
            caption=f"✅ Бэкап создан: {backup_name}"
        )
        
        os.remove(backup_name)
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

@dp.callback_query(F.data == "admin_shutdown")
@check_bot_admin()
async def admin_shutdown(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, выключить", "admin_shutdown_confirm", "danger"))
    builder.add(create_button("🚫 Нет", "admin_back", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите выключить бота?</b>\n\n"
        "Администраторы всё ещё будут иметь доступ.",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_shutdown_confirm")
@check_bot_admin()
async def admin_shutdown_confirm(callback: CallbackQuery):
    global technical_maintenance, maintenance_message
    technical_maintenance = True
    maintenance_message = "🛑 Бот остановлен администратором"
    
    await callback.message.edit_text(
        "🛑 <b>Бот остановлен</b>\n\n"
        "Администраторы всё ещё имеют доступ."
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_back")
@check_bot_admin()
async def admin_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await admin_panel(callback.message, state)

# ========== ЗАПУСК ==========
async def main():
    dp.message.middleware(AntiFloodMiddleware())
    dp.message.middleware(MaintenanceMiddleware())
    dp.callback_query.middleware(MaintenanceMiddleware())
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
