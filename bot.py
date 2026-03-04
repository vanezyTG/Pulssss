import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
import sqlite3
from contextlib import contextmanager
from functools import wraps
from collections import defaultdict
import threading

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, ChatPermissions
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

# Конфигурация
BOT_TOKEN = "8557190026:AAHJ9GdxvUtIy8O0DQAC0bL-E_dLSzwgtgE"  # Замените на ваш токен
BOT_USERNAME = "PulsOfficialManager_bot"  # Замените на username вашего бота (без @)
ADMIN_IDS = [6708209142]  # Замените на ID администраторов бота

# Хранилище для антифлуда
flood_control = defaultdict(list)

# Блокировка для статистики
stats_lock = threading.Lock()
stats_updating = False

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Классы состояний
class RulesStates(StatesGroup):
    waiting_for_rules_text = State()
    waiting_for_interval = State()

class WelcomeStates(StatesGroup):
    waiting_for_welcome_text = State()
    waiting_for_welcome_photo = State()

class AntiFloodStates(StatesGroup):
    waiting_for_limit = State()
    waiting_for_window = State()
    waiting_for_warn_count = State()
    waiting_for_first_punish = State()
    waiting_for_first_duration = State()
    waiting_for_repeat_punish = State()
    waiting_for_repeat_duration = State()

# Словарь переводов
TRANSLATIONS = {
    'ru': {
        # Приветствия и общее
        'welcome': "Добро пожаловать, <b>{name}</b>!",
        'no_username': "нет",
        'username': "Username",
        'id': "ID",
        'joined': "Вошёл",
        'last_active': "Последняя активность",
        'place_in_top': "Место в топе",
        
        # Подтверждение
        'confirm_not_bot': "Я не бот",
        'agree_rules': "✅ Согласен с правилами",
        'muted_forever': "Вы замьючены **навсегда**, пока не подтвердите правила",
        'go_to_pm': "📜 Перейти в ЛС",
        'rules_sent': "Правила отправлены в личные сообщения",
        'confirmed_not_bot': "✅ {name} подтвердил, что не бот и теперь может писать в чат.",
        'confirmed_rules': "✅ {name} согласился с правилами и теперь может писать в чат.",
        'thanks_confirmation': "Спасибо за подтверждение! Теперь вы можете писать в чат.",
        
        # Сообщения при входе
        'user_joined': "👋 <b>{name}</b> зашёл в чат!",
        'need_confirm_rules': "Вы замьючены **навсегда**, пока не подтвердите правила.\nПерейдите в ЛС бота, прочитайте правила и подтвердите согласие — мут снимется.",
        'need_confirm_not_bot': "Вы замьючены **навсегда**, пока не подтвердите, что вы не бот.\nНажмите кнопку ниже — мут снимется.",
        
        # Статистика
        'stats_empty': "📊 Статистика ещё не собрана",
        'stats_updating': "📊 Статистика обновляется, подождите 5–10 секунд",
        'top_active': "🏆 Топ активных (всего сообщений):",
        'profile': "Профиль {name}",
        'per_day': "За день",
        'per_week': "За неделю",
        'per_month': "За месяц",
        'total': "Всего",
        'messages': "сообщ.",
        
        # Настройки
        'language': "🌐 Язык бота",
        'current_language': "Текущий язык: {lang}",
        'choose_language': "Выберите язык бота для этой группы:",
        'language_changed': "✅ Язык изменён на {lang}",
        'russian': "🇷🇺 Русский",
        'ukrainian': "🇺🇦 Українська",
        'english': "🇬🇧 English",
        
        # Команды
        'pulse': "пульс",
        'pong': "понг",
        'ping': "Пинг: {ping} мс\nВремя ответа: {response} сек",
        
        # Выход
        'user_left': "👋 Пользователь {name} вышел из чата.",
        
        # Ошибки
        'error_no_group': "❌ Сначала выберите группу!",
        'error_not_creator': "❌ Вы не являетесь создателем этой группы!",
        'error_not_yours': "⚠️ Это не ваше подтверждение!",
        'error_no_rules': "❌ В этом чате еще не установлены правила.",
        'error_rules_short': "❌ Правила слишком короткие! Отправьте более содержательный текст.",
        
        # Авто-рассылка
        'rules_reminder': "📢 Напоминание правил чата",
        
        # Главное меню
        'about': "📋 О боте",
        'help': "🆘 Помощь",
        'add_to_group': "➕ Добавить в группу",
        'group_manage': "⚙️ Управление группой",
        'back': "◀️ Назад",
        
        # Привязка группы
        'group_not_linked': "❌ Группа еще не привязана к вашему аккаунту.",
        'want_to_link': "Хотите привязать эту группу?",
        'link_group': "✅ Привязать группу",
        'unlink_group': "❌ Отвязать группу",
        'confirm_unlink': "Вы уверены, что хотите отвязать группу?",
        'cancel': "🚫 Отмена",
        'group_linked': "✅ Группа успешно привязана! Теперь вы можете настроить её в ЛС.",
        'group_unlinked': "✅ Группа отвязана от вашего аккаунта.",
        'settings_in_pm': "Настраивать группу можно только в личных сообщениях с ботом.",
        'go_to_pm_settings': "📱 Перейти в ЛС для настройки",
    },
    'uk': {
        # Привітання та загальне
        'welcome': "Ласкаво просимо, <b>{name}</b>!",
        'no_username': "немає",
        'username': "Юзернейм",
        'id': "ID",
        'joined': "Увійшов",
        'last_active': "Остання активність",
        'place_in_top': "Місце в топі",
        
        # Підтвердження
        'confirm_not_bot': "Я не бот",
        'agree_rules': "✅ Згоден з правилами",
        'muted_forever': "Ви зам'ючені **назавжди**, поки не підтвердите правила",
        'go_to_pm': "📜 Перейти в ЛС",
        'rules_sent': "Правила надіслано в особисті повідомлення",
        'confirmed_not_bot': "✅ {name} підтвердив, що не бот і тепер може писати в чат.",
        'confirmed_rules': "✅ {name} погодився з правилами і тепер може писати в чат.",
        'thanks_confirmation': "Дякую за підтвердження! Тепер ви можете писати в чат.",
        
        # Повідомлення при вході
        'user_joined': "👋 <b>{name}</b> зайшов у чат!",
        'need_confirm_rules': "Ви зам'ючені **назавжди**, поки не підтвердите правила.\nПерейдіть в ЛС бота, прочитайте правила і підтвердьте згоду — мут зніметься.",
        'need_confirm_not_bot': "Ви зам'ючені **назавжди**, поки не підтвердите, що ви не бот.\nНатисніть кнопку нижче — мут зніметься.",
        
        # Статистика
        'stats_empty': "📊 Статистика ще не зібрана",
        'stats_updating': "📊 Статистика оновлюється, зачекайте 5–10 секунд",
        'top_active': "🏆 Топ активних (всього повідомлень):",
        'profile': "Профіль {name}",
        'per_day': "За день",
        'per_week': "За тиждень",
        'per_month': "За місяць",
        'total': "Всього",
        'messages': "повідом.",
        
        # Налаштування
        'language': "🌐 Мова бота",
        'current_language': "Поточна мова: {lang}",
        'choose_language': "Виберіть мову бота для цієї групи:",
        'language_changed': "✅ Мову змінено на {lang}",
        'russian': "🇷🇺 Русский",
        'ukrainian': "🇺🇦 Українська",
        'english': "🇬🇧 English",
        
        # Команди
        'pulse': "пульс",
        'pong': "понг",
        'ping': "Пінг: {ping} мс\nЧас відповіді: {response} сек",
        
        # Вихід
        'user_left': "👋 Користувач {name} вийшов з чату.",
        
        # Помилки
        'error_no_group': "❌ Спочатку виберіть групу!",
        'error_not_creator': "❌ Ви не є творцем цієї групи!",
        'error_not_yours': "⚠️ Це не ваше підтвердження!",
        'error_no_rules': "❌ У цьому чаті ще не встановлені правила.",
        'error_rules_short': "❌ Правила занадто короткі! Відправте більш змістовний текст.",
        
        # Авторозсилка
        'rules_reminder': "📢 Нагадування правил чату",
        
        # Головне меню
        'about': "📋 Про бота",
        'help': "🆘 Допомога",
        'add_to_group': "➕ Додати в групу",
        'group_manage': "⚙️ Керування групою",
        'back': "◀️ Назад",
        
        # Прив'язка групи
        'group_not_linked': "❌ Група ще не прив'язана до вашого акаунту.",
        'want_to_link': "Хочете прив'язати цю групу?",
        'link_group': "✅ Прив'язати групу",
        'unlink_group': "❌ Відв'язати групу",
        'confirm_unlink': "Ви впевнені, що хочете відв'язати групу?",
        'cancel': "🚫 Скасування",
        'group_linked': "✅ Групу успішно прив'язано! Тепер ви можете налаштувати її в ЛС.",
        'group_unlinked': "✅ Групу відв'язано від вашого акаунту.",
        'settings_in_pm': "Налаштовувати групу можна тільки в особистих повідомленнях з ботом.",
        'go_to_pm_settings': "📱 Перейти в ЛС для налаштування",
    },
    'en': {
        # Greetings and general
        'welcome': "Welcome, <b>{name}</b>!",
        'no_username': "none",
        'username': "Username",
        'id': "ID",
        'joined': "Joined",
        'last_active': "Last active",
        'place_in_top': "Place in top",
        
        # Confirmation
        'confirm_not_bot': "I'm not a bot",
        'agree_rules': "✅ I agree with the rules",
        'muted_forever': "You are muted **forever** until you confirm the rules",
        'go_to_pm': "📜 Go to PM",
        'rules_sent': "Rules sent to private messages",
        'confirmed_not_bot': "✅ {name} confirmed they're not a bot and can now write in the chat.",
        'confirmed_rules': "✅ {name} agreed to the rules and can now write in the chat.",
        'thanks_confirmation': "Thank you for confirmation! You can now write in the chat.",
        
        # Join messages
        'user_joined': "👋 <b>{name}</b> joined the chat!",
        'need_confirm_rules': "You are muted **forever** until you confirm the rules.\nGo to bot's PM, read the rules and confirm — mute will be removed.",
        'need_confirm_not_bot': "You are muted **forever** until you confirm you're not a bot.\nClick the button below — mute will be removed.",
        
        # Statistics
        'stats_empty': "📊 No statistics yet",
        'stats_updating': "📊 Statistics are updating, wait 5–10 seconds",
        'top_active': "🏆 Top active (total messages):",
        'profile': "Profile {name}",
        'per_day': "Per day",
        'per_week': "Per week",
        'per_month': "Per month",
        'total': "Total",
        'messages': "msg",
        
        # Settings
        'language': "🌐 Bot language",
        'current_language': "Current language: {lang}",
        'choose_language': "Choose bot language for this group:",
        'language_changed': "✅ Language changed to {lang}",
        'russian': "🇷🇺 Russian",
        'ukrainian': "🇺🇦 Ukrainian",
        'english': "🇬🇧 English",
        
        # Commands
        'pulse': "pulse",
        'pong': "pong",
        'ping': "Ping: {ping} ms\nResponse time: {response} sec",
        
        # Exit
        'user_left': "👋 User {name} left the chat.",
        
        # Errors
        'error_no_group': "❌ Select a group first!",
        'error_not_creator': "❌ You are not the creator of this group!",
        'error_not_yours': "⚠️ This is not your confirmation!",
        'error_no_rules': "❌ No rules set in this chat yet.",
        'error_rules_short': "❌ Rules are too short! Send a more meaningful text.",
        
        # Auto broadcast
        'rules_reminder': "📢 Reminder: chat rules",
        
        # Main menu
        'about': "📋 About",
        'help': "🆘 Help",
        'add_to_group': "➕ Add to group",
        'group_manage': "⚙️ Group management",
        'back': "◀️ Back",
        
        # Group linking
        'group_not_linked': "❌ Group is not linked to your account yet.",
        'want_to_link': "Do you want to link this group?",
        'link_group': "✅ Link group",
        'unlink_group': "❌ Unlink group",
        'confirm_unlink': "Are you sure you want to unlink the group?",
        'cancel': "🚫 Cancel",
        'group_linked': "✅ Group successfully linked! Now you can configure it in PM.",
        'group_unlinked': "✅ Group unlinked from your account.",
        'settings_in_pm': "You can only configure the group in private messages with the bot.",
        'go_to_pm_settings': "📱 Go to PM for settings",
    }
}

# Декораторы проверки прав
def check_owner():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id
            state: FSMContext = kwargs.get('state')
            
            if state:
                data = await state.get_data()
                for key in data:
                    if key.startswith('msg_owner_'):
                        if str(callback.message.message_id) in key:
                            if data[key] != user_id:
                                await callback.answer("⚠️ Эта кнопка не для вас!", show_alert=True)
                                return
                            break
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

# Класс базы данных
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
            
            # Таблица для правил групп
            c.execute('''CREATE TABLE IF NOT EXISTS group_rules
                         (chat_id INTEGER PRIMARY KEY,
                          owner_id INTEGER,
                          rules_html TEXT,
                          welcome_enabled INTEGER DEFAULT 0,
                          welcome_text TEXT,
                          welcome_photo_id TEXT,
                          rules_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300,
                          last_rules_message_id INTEGER,
                          last_rules_time INTEGER,
                          chat_title TEXT,
                          chat_username TEXT)''')
            
            # Добавляем поле language, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN language TEXT DEFAULT "ru"')
            except sqlite3.OperationalError:
                pass  # колонка уже есть
            
            # Таблица для согласившихся с правилами
            c.execute('''CREATE TABLE IF NOT EXISTS rules_agreed
                         (chat_id INTEGER,
                          user_id INTEGER,
                          agreed_at INTEGER,
                          PRIMARY KEY (chat_id, user_id))''')
            
            # Таблица для статистики пользователей
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
            
            # Таблица для антифлуда
            c.execute('''CREATE TABLE IF NOT EXISTS antiflood_settings
                         (chat_id INTEGER PRIMARY KEY,
                          enabled INTEGER DEFAULT 0,
                          msg_limit INTEGER DEFAULT 5,
                          time_window INTEGER DEFAULT 10,
                          warn_count INTEGER DEFAULT 2,
                          first_punish TEXT DEFAULT 'mute',
                          first_duration INTEGER DEFAULT 60,
                          repeat_punish TEXT DEFAULT 'mute',
                          repeat_duration INTEGER DEFAULT 300)''')
            
            conn.commit()
    
    def save_rules(self, chat_id: int, rules_html: str = None, owner_id: int = None, 
                   chat_title: str = None, chat_username: str = None):
        with self.get_connection() as conn:
            c = conn.cursor()
            
            c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,))
            existing = c.fetchone()
            
            if existing:
                updates = []
                params = []
                
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
                c.execute('''INSERT INTO group_rules 
                             (chat_id, owner_id, rules_html, chat_title, chat_username) 
                             VALUES (?, ?, ?, ?, ?)''', 
                             (chat_id, owner_id, rules_html, chat_title, chat_username))
            
            conn.commit()
    
    def save_welcome(self, chat_id: int, welcome_text: str = None, welcome_photo_id: str = None):
        with self.get_connection() as conn:
            c = conn.cursor()
            if welcome_text is not None:
                c.execute('UPDATE group_rules SET welcome_text = ? WHERE chat_id = ?', 
                         (welcome_text, chat_id))
            if welcome_photo_id is not None:
                c.execute('UPDATE group_rules SET welcome_photo_id = ? WHERE chat_id = ?', 
                         (welcome_photo_id, chat_id))
            conn.commit()
    
    def get_welcome(self, chat_id: int) -> Tuple[Optional[str], Optional[str]]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT welcome_text, welcome_photo_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return (result[0], result[1]) if result else (None, None)
    
    def set_welcome_enabled(self, chat_id: int, enabled: bool):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET welcome_enabled = ? WHERE chat_id = ?', 
                     (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_welcome_enabled(self, chat_id: int) -> bool:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT welcome_enabled FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return bool(result[0]) if result else False
    
    def get_rules_html(self, chat_id: int) -> Optional[str]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_html FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    def set_rules_settings(self, chat_id: int, enabled: bool, interval: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,))
            existing = c.fetchone()
            
            if existing:
                c.execute('''UPDATE group_rules 
                             SET rules_enabled = ?, rules_interval = ? 
                             WHERE chat_id = ?''', (1 if enabled else 0, interval, chat_id))
            else:
                c.execute('''INSERT INTO group_rules (chat_id, rules_enabled, rules_interval) 
                             VALUES (?, ?, ?)''', (chat_id, 1 if enabled else 0, interval))
            conn.commit()
    
    def get_rules_settings(self, chat_id: int) -> Tuple[int, int, Optional[int], Optional[int]]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT rules_enabled, rules_interval, last_rules_message_id, last_rules_time 
                         FROM group_rules WHERE chat_id = ?''', (chat_id,))
            result = c.fetchone()
            return result if result else (0, 300, None, None)
    
    def update_last_rules(self, chat_id: int, message_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''UPDATE group_rules 
                         SET last_rules_message_id = ?, last_rules_time = ? 
                         WHERE chat_id = ?''', (message_id, int(time.time()), chat_id))
            conn.commit()
    
    def get_user_groups(self, user_id: int) -> List[Tuple[int, str]]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id, chat_title FROM group_rules WHERE owner_id = ?', (user_id,))
            return c.fetchall()
    
    def get_all_chats(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT chat_id, chat_title, chat_username, rules_enabled, welcome_enabled
                         FROM group_rules 
                         ORDER BY chat_id''')
            return c.fetchall()
    
    # Метод для получения языка группы
    def get_group_language(self, chat_id: int) -> str:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT language FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else 'ru'
    
    # Методы для согласия с правилами
    def mark_user_confirmed(self, chat_id: int, user_id: int, agreed: bool = True):
        with self.get_connection() as conn:
            c = conn.cursor()
            value = int(time.time()) if agreed else -1
            c.execute('INSERT OR REPLACE INTO rules_agreed (chat_id, user_id, agreed_at) VALUES (?, ?, ?)',
                      (chat_id, user_id, value))
            conn.commit()
    
    def has_user_confirmed(self, chat_id: int, user_id: int) -> bool:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT agreed_at FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            result = c.fetchone()
            return result and result[0] > 0 if result else False
    
    # Методы для статистики
    def add_user_stat(self, chat_id: int, user_id: int, join_date: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO user_stats 
                         (chat_id, user_id, join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat) 
                         VALUES (?, ?, ?, 0, 0, 0, 0, ?, 0)''', (chat_id, user_id, join_date, join_date))
            conn.commit()
    
    def update_message_count(self, chat_id: int, user_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''UPDATE user_stats 
                         SET all_messages = all_messages + 1, 
                             month_messages = month_messages + 1, 
                             week_messages = week_messages + 1, 
                             day_messages = day_messages + 1, 
                             last_active = ? 
                         WHERE chat_id = ? AND user_id = ?''', 
                     (int(time.time()), chat_id, user_id))
            conn.commit()
    
    def set_left_chat(self, chat_id: int, user_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE user_stats SET left_chat = 1 WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            conn.commit()
    
    def get_user_stat(self, chat_id: int, user_id: int) -> Optional[dict]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat 
                         FROM user_stats WHERE chat_id = ? AND user_id = ?''', (chat_id, user_id))
            row = c.fetchone()
            if row:
                return {
                    'join_date': row[0],
                    'all_messages': row[1],
                    'month_messages': row[2],
                    'week_messages': row[3],
                    'day_messages': row[4],
                    'last_active': row[5],
                    'left_chat': bool(row[6])
                }
            return None
    
    def get_top_messages(self, chat_id: int, period: str = 'all', limit: int = 10) -> List[Tuple[int, int]]:
        field = {
            'day': 'day_messages',
            'week': 'week_messages',
            'month': 'month_messages',
            'all': 'all_messages'
        }.get(period, 'all_messages')
        
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute(f'SELECT user_id, {field} FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY {field} DESC LIMIT ?', (chat_id, limit))
            return c.fetchall()
    
    def get_user_position(self, chat_id: int, user_id: int, period: str = 'all') -> int:
        field = {
            'day': 'day_messages',
            'week': 'week_messages',
            'month': 'month_messages',
            'all': 'all_messages'
        }.get(period, 'all_messages')
        
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute(f'SELECT user_id FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY {field} DESC', (chat_id,))
            users = c.fetchall()
            for i, (uid,) in enumerate(users, 1):
                if uid == user_id:
                    return i
            return 0
    
    # Методы антифлуда
    def get_antiflood_settings(self, chat_id: int) -> dict:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT enabled, msg_limit, time_window, warn_count, 
                                first_punish, first_duration, repeat_punish, repeat_duration 
                         FROM antiflood_settings WHERE chat_id = ?''', (chat_id,))
            row = c.fetchone()
            if row:
                return {
                    'enabled': bool(row[0]),
                    'msg_limit': row[1] or 5,
                    'time_window': row[2] or 10,
                    'warn_count': row[3] or 2,
                    'first_punish': row[4] or 'mute',
                    'first_duration': row[5] or 60,
                    'repeat_punish': row[6] or 'mute',
                    'repeat_duration': row[7] or 300
                }
            return {
                'enabled': False,
                'msg_limit': 5,
                'time_window': 10,
                'warn_count': 2,
                'first_punish': 'mute',
                'first_duration': 60,
                'repeat_punish': 'mute',
                'repeat_duration': 300
            }
    
    def set_antiflood_enabled(self, chat_id: int, enabled: bool):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO antiflood_settings (chat_id, enabled) VALUES (?, ?)',
                      (chat_id, 1 if enabled else 0))
            conn.commit()
    
    def save_antiflood_settings(self, chat_id: int, **kwargs):
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
                defaults = {
                    'enabled': 0,
                    'msg_limit': 5,
                    'time_window': 10,
                    'warn_count': 2,
                    'first_punish': 'mute',
                    'first_duration': 60,
                    'repeat_punish': 'mute',
                    'repeat_duration': 300
                }
                defaults.update(kwargs)
                c.execute('''INSERT INTO antiflood_settings 
                             (chat_id, enabled, msg_limit, time_window, warn_count, 
                              first_punish, first_duration, repeat_punish, repeat_duration) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (chat_id, defaults['enabled'], defaults['msg_limit'], 
                           defaults['time_window'], defaults['warn_count'],
                           defaults['first_punish'], defaults['first_duration'],
                           defaults['repeat_punish'], defaults['repeat_duration']))
            conn.commit()

db = Database()

# Вспомогательные функции
async def is_creator(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status == 'creator'
    except:
        return False

async def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ['creator', 'administrator']
    except:
        return False

def format_datetime(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def format_interval(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} сек"
    elif seconds < 3600:
        return f"{seconds // 60} мин"
    elif seconds < 86400:
        return f"{seconds // 3600} ч"
    else:
        return f"{seconds // 86400} дн"

# Клавиатуры
def get_main_keyboard(lang: str = 'ru'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['about'], callback_data="about")
    builder.button(text=tr['help'], callback_data="help")
    builder.button(text=tr['add_to_group'], url=f"https://t.me/{BOT_USERNAME}?startgroup=true")
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Правила чата", callback_data="manage_rules")
    builder.button(text="👋 Приветствие", callback_data="manage_welcome")
    builder.button(text="🔄 Авто-рассылка правил", callback_data="rules_auto")
    builder.button(text="🚫 Антифлуд", callback_data="antiflood_manage")
    builder.button(text="🌐 Язык бота", callback_data="set_language")
    builder.button(text="❌ Отвязать группу", callback_data="unlink_group_confirm")
    builder.button(text="◀️ Назад к группам", callback_data="back_to_groups")
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Установить правила", callback_data="set_rules")
    builder.button(text="👁 Показать правила", callback_data="show_rules")
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled: bool = False):
    builder = InlineKeyboardBuilder()
    status = "✅ Включено" if enabled else "❌ Выключено"
    builder.button(text=f"Статус: {status}", callback_data="toggle_welcome")
    builder.button(text="📝 Установить текст", callback_data="set_welcome_text")
    builder.button(text="🖼 Установить фото", callback_data="set_welcome_photo")
    builder.button(text="👁 Посмотреть", callback_data="show_welcome")
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled: bool):
    builder = InlineKeyboardBuilder()
    status = "✅ Включено" if enabled else "❌ Выключено"
    builder.button(text=f"Статус: {status}", callback_data="toggle_rules_auto")
    builder.button(text="⏱ Установить интервал", callback_data="set_interval")
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings: dict):
    builder = InlineKeyboardBuilder()
    status = "✅ Включено" if settings['enabled'] else "❌ Выключено"
    builder.button(text=f"Статус: {status}", callback_data="toggle_antiflood")
    builder.button(text=f"📊 Лимит: {settings['msg_limit']} сообщ.", callback_data="set_limit")
    builder.button(text=f"⏱ Окно: {settings['time_window']} сек", callback_data="set_window")
    builder.button(text=f"⚠️ Предупреждений: {settings['warn_count']}", callback_data="set_warn_count")
    builder.button(text=f"🔇 Первое: {settings['first_punish']} ({settings['first_duration']} сек)", callback_data="set_first_punish")
    builder.button(text=f"🔊 Повторное: {settings['repeat_punish']} ({settings['repeat_duration']} сек)", callback_data="set_repeat_punish")
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(is_first: bool = True):
    prefix = "first" if is_first else "repeat"
    builder = InlineKeyboardBuilder()
    builder.button(text="⚠️ Предупреждение", callback_data=f"punish_warn_{prefix}")
    builder.button(text="🔇 Мут", callback_data=f"punish_mute_{prefix}")
    builder.button(text="👢 Кик", callback_data=f"punish_kick_{prefix}")
    builder.button(text="⛔️ Бан", callback_data=f"punish_ban_{prefix}")
    builder.button(text="◀️ Назад", callback_data="antiflood_manage")
    builder.adjust(2)
    return builder.as_markup()

def get_language_keyboard(current_lang: str):
    builder = InlineKeyboardBuilder()
    
    # Русский
    ru_text = TRANSLATIONS['ru']['russian']
    if current_lang == 'ru':
        ru_text += " ✅"
    builder.button(text=ru_text, callback_data="lang_ru")
    
    # Украинский
    uk_text = TRANSLATIONS['uk']['ukrainian']
    if current_lang == 'uk':
        uk_text += " ✅"
    builder.button(text=uk_text, callback_data="lang_uk")
    
    # Английский
    en_text = TRANSLATIONS['en']['english']
    if current_lang == 'en':
        en_text += " ✅"
    builder.button(text=en_text, callback_data="lang_en")
    
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_buttons(chat_id: int):
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📜 " + tr['rules_reminder'].replace("📢 ", ""), callback_data=f"show_group_rules_{chat_id}")
    builder.button(text="📊 " + tr['profile'].format(name=""), callback_data=f"my_stats_{chat_id}")
    builder.button(text="🏆 " + tr['top_active'].split("(")[0].strip(), callback_data=f"top_active_{chat_id}")
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id: int, user_id: int, msg_id: int):
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['confirm_not_bot'], callback_data=f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}")
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id: int, user_id: int, msg_id: int):
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['agree_rules'], callback_data=f"agree_rules_{chat_id}_{user_id}_{msg_id}")
    return builder.as_markup()

def get_link_group_keyboard(chat_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['link_group'], callback_data=f"link_group_{chat_id}")
    builder.button(text=tr['cancel'], callback_data="cancel_link")
    builder.adjust(1)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['unlink_group'], callback_data=f"unlink_group_{chat_id}")
    builder.button(text=tr['cancel'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_pm_link_keyboard(lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['go_to_pm_settings'], url=f"https://t.me/{BOT_USERNAME}?start")
    return builder.as_markup()

# Middleware для антифлуда
class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        if not isinstance(event, Message) or event.chat.type not in {'group', 'supergroup'}:
            return await handler(event, data)

        chat_id = event.chat.id
        user = event.from_user

        if user.is_bot:
            return await handler(event, data)

        # Проверяем, является ли пользователь админом или создателем
        if await is_admin(chat_id, user.id):
            return await handler(event, data)

        # Проверяем, согласился ли пользователь с правилами
        if not db.has_user_confirmed(chat_id, user.id):
            return await handler(event, data)

        settings = db.get_antiflood_settings(chat_id)
        if not settings['enabled']:
            return await handler(event, data)

        now = time.time()
        key = f"{chat_id}_{user.id}"
        timestamps = flood_control[key]

        timestamps[:] = [t for t in timestamps if now - t < settings['time_window']]
        timestamps.append(now)

        if len(timestamps) > settings['msg_limit']:
            violations = len(timestamps) - settings['msg_limit']

            punish_type = settings['first_punish'] if violations <= settings['warn_count'] else settings['repeat_punish']
            duration = settings['first_duration'] if violations <= settings['warn_count'] else settings['repeat_duration']

            if punish_type == 'warn':
                await event.reply(f"⚠️ {user.full_name}, не флуди! ({violations}/{settings['warn_count']})")
            else:
                try:
                    permissions = ChatPermissions(can_send_messages=False)
                    until = int(now + duration)
                    
                    if punish_type == 'mute':
                        await bot.restrict_chat_member(chat_id, user.id, permissions=permissions, until_date=until)
                        await event.reply(f"🔇 {user.full_name} замьючен на {duration // 60} мин")
                    elif punish_type == 'kick':
                        await bot.ban_chat_member(chat_id, user.id)
                        await bot.unban_chat_member(chat_id, user.id)
                        await event.reply(f"👢 {user.full_name} кикнут за флуд")
                    elif punish_type == 'ban':
                        await bot.ban_chat_member(chat_id, user.id, until_date=until)
                        await event.reply(f"⛔️ {user.full_name} забанен на {duration // 60} мин")
                except Exception as e:
                    logger.warning(f"Ошибка наказания в {chat_id}: {e}")

            flood_control[key].clear()
            return

        return await handler(event, data)

# Фоновая задача для сброса счетчиков
async def reset_periodic_counters():
    global stats_updating
    
    while True:
        now = datetime.now(timezone.utc)
        
        # Начало текущего дня (00:00 UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Начало текущей недели (понедельник 00:00 UTC)
        week_start = now - timedelta(days=now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Начало текущего месяца
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Блокируем статистику на время сброса
        with stats_lock:
            stats_updating = True
            
            try:
                with db.get_connection() as conn:
                    c = conn.cursor()
                    
                    # Сбрасываем day_messages, если last_active < начало сегодняшнего дня
                    c.execute('UPDATE user_stats SET day_messages = 0 WHERE last_active < ?', (day_start.timestamp(),))
                    
                    # Сбрасываем week_messages, если last_active < начало текущей недели
                    c.execute('UPDATE user_stats SET week_messages = 0 WHERE last_active < ?', (week_start.timestamp(),))
                    
                    # Сбрасываем month_messages, если last_active < начало месяца
                    c.execute('UPDATE user_stats SET month_messages = 0 WHERE last_active < ?', (month_start.timestamp(),))
                    
                    conn.commit()
                    logger.info("Счетчики статистики сброшены")
            except Exception as e:
                logger.error(f"Ошибка при сбросе счетчиков: {e}")
            
            stats_updating = False
        
        # Ждём 1 час
        await asyncio.sleep(3600)

# Фоновая задача для авто-рассылки правил
async def rules_broadcast_task():
    while True:
        try:
            with db.get_connection() as conn:
                c = conn.cursor()
                c.execute('''SELECT chat_id, rules_enabled, rules_interval, 
                                   last_rules_time, rules_html 
                            FROM group_rules 
                            WHERE rules_enabled = 1 AND rules_html IS NOT NULL''')
                
                for chat_id, enabled, interval, last_time, rules_html in c.fetchall():
                    current_time = int(time.time())
                    
                    if last_time and current_time - last_time < interval:
                        continue
                    
                    try:
                        lang = db.get_group_language(chat_id)
                        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
                        
                        msg = await bot.send_message(
                            chat_id,
                            f"<b>{tr['rules_reminder']}</b>\n\n{rules_html}",
                            parse_mode="HTML"
                        )
                        
                        try:
                            await bot.pin_chat_message(chat_id, msg.message_id)
                        except:
                            pass
                        
                        db.update_last_rules(chat_id, msg.message_id)
                        
                    except Exception as e:
                        logger.error(f"Ошибка отправки правил в чат {chat_id}: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче: {e}")
        
        await asyncio.sleep(60)

# Команда для проверки пинга
@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["пульс", "pulse", "пульс бот", "понг"]))
async def cmd_ping(message: Message):
    start_time = time.time()
    msg = await message.reply("⏳ ...")
    end_time = time.time()
    
    ping = round((end_time - start_time) * 1000)  # в миллисекундах
    response_time = round(end_time - start_time, 2)  # в секундах
    
    lang = 'ru'  # по умолчанию
    if message.chat.type in {'group', 'supergroup'}:
        lang = db.get_group_language(message.chat.id)
    
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    await msg.edit_text(
        tr['ping'].format(ping=ping, response=response_time),
        parse_mode="HTML"
    )

# Команды админа
@dp.message(Command("adminstats"))
@check_bot_admin()
async def cmd_admin_stats(message: Message):
    chats = db.get_all_chats()
    
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"📱 Всего групп: {len(chats)}\n\n"
    )
    
    if chats:
        text += "<b>📋 Список групп:</b>\n"
        for chat_id, title, username, rules_enabled, welcome_enabled in chats:
            if username:
                link = f"https://t.me/{username}"
                chat_info = f"<a href='{link}'>{title or 'Без названия'}</a>"
            else:
                chat_info = f"{title or 'Без названия'} (частная)"
            
            rules_status = "✅" if rules_enabled else "❌"
            welcome_status = "✅" if welcome_enabled else "❌"
            text += f"• {chat_info} | Правила:{rules_status} Привет:{welcome_status}\n"
    
    await message.answer(text)

# Обработчик добавления бота в группу
@dp.message(F.new_chat_members)
async def on_bot_added(message: Message):
    bot_info = await bot.get_me()
    if any(member.id == bot_info.id for member in message.new_chat_members):
        # Не привязываем автоматически, просто логируем
        logger.info(f"Бот добавлен в группу {message.chat.id}")

# Обработчик команды управления группой в чате
@dp.message(F.chat.type.in_({"group", "supergroup"}), Command("group"))
@dp.message(F.chat.type.in_({"group", "supergroup"}), Command("manage"))
async def cmd_group_manage(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, является ли пользователь создателем
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может настраивать бота!")
        return
    
    # Проверяем, привязана ли группа к пользователю
    owner_id = None
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        owner_id = result[0] if result else None
    
    lang = db.get_group_language(chat_id) if owner_id else 'ru'
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    if owner_id == user_id:
        # Группа уже привязана к этому пользователю
        await message.answer(
            f"{tr['settings_in_pm']}",
            reply_markup=get_pm_link_keyboard(lang)
        )
    else:
        # Группа не привязана или привязана к другому пользователю
        text = f"{tr['group_not_linked']}\n\n{tr['want_to_link']}"
        await message.answer(
            text,
            reply_markup=get_link_group_keyboard(chat_id, lang)
        )

# Привязка группы
@dp.callback_query(F.data.startswith("link_group_"))
async def link_group(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    # Проверяем, что пользователь действительно создатель
    if not await is_creator(chat_id, user_id):
        await callback.answer("❌ Вы не создатель этой группы!", show_alert=True)
        return
    
    # Получаем информацию о группе
    try:
        chat = await bot.get_chat(chat_id)
        chat_title = chat.title
        chat_username = chat.username
    except:
        chat_title = "Группа"
        chat_username = None
    
    # Сохраняем привязку
    db.save_rules(
        chat_id=chat_id,
        owner_id=user_id,
        chat_title=chat_title,
        chat_username=chat_username
    )
    
    # Устанавливаем язык по умолчанию
    lang = 'ru'
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET language = ? WHERE chat_id = ?', (lang, chat_id))
        conn.commit()
    
    tr = TRANSLATIONS[lang]
    
    await callback.message.edit_text(tr['group_linked'])
    await callback.answer("✅ Группа привязана!")
    
    # Отправляем сообщение в ЛС
    try:
        await bot.send_message(
            user_id,
            f"✅ Группа <b>{chat_title}</b> успешно привязана!\n\n"
            f"Теперь вы можете настроить её, выбрав в меню групп.",
            reply_markup=get_main_keyboard(lang)
        )
    except:
        pass

# Отмена привязки
@dp.callback_query(F.data == "cancel_link")
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

# Подтверждение отвязки группы
@dp.callback_query(F.data == "unlink_group_confirm")
@check_owner()
async def unlink_group_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    await callback.message.edit_text(
        tr['confirm_unlink'],
        reply_markup=get_unlink_confirm_keyboard(chat_id, lang)
    )
    await callback.answer()

# Отвязка группы
@dp.callback_query(F.data.startswith("unlink_group_"))
@check_owner()
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    # Удаляем owner_id (отвязываем группу)
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET owner_id = NULL WHERE chat_id = ?', (chat_id,))
        conn.commit()
    
    await callback.message.edit_text(tr['group_unlinked'])
    await callback.answer("✅ Группа отвязана!")
    
    # Возвращаемся к списку групп
    await state.clear()
    await cmd_start(callback.message, state)

# Вход нового участника
@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id = update.chat.id
        user = update.new_chat_member.user
        
        db.add_user_stat(chat_id, user.id, int(time.time()))
        
        # Проверяем, есть ли владелец у группы
        owner_id = None
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            owner_id = result[0] if result else None
        
        # Если нет владельца - не мутим
        if not owner_id:
            await send_simple_welcome(chat_id, user)
            return
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])

        if db.has_user_confirmed(chat_id, user.id):
            await send_simple_welcome(chat_id, user)
            return

        # МУТ НАВСЕГДА
        try:
            await bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user.id,
                permissions=types.ChatPermissions(can_send_messages=False)
            )
        except Exception as e:
            logger.warning(f"Не удалось замутить {user.id}: {e}")

        rules_html = db.get_rules_html(chat_id)
        builder = InlineKeyboardBuilder()
        msg_text = ""

        if rules_html:
            msg_text = (
                f"{tr['user_joined'].format(name=user.full_name)}\n\n"
                f"{tr['need_confirm_rules']}"
            )
            builder.button(
                text=tr['go_to_pm'],
                url=f"https://t.me/{BOT_USERNAME}?start=agree_{chat_id}_{user.id}"
            )
        else:
            msg_text = (
                f"{tr['user_joined'].format(name=user.full_name)}\n\n"
                f"{tr['need_confirm_not_bot']}"
            )
            builder.button(
                text=tr['confirm_not_bot'],
                callback_data=f"confirm_not_bot_{chat_id}_{user.id}_0"
            )
        
        msg = await bot.send_message(chat_id, msg_text, reply_markup=builder.as_markup(), parse_mode="HTML")

        if rules_html:
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {update.chat.title}!\n\n"
                    "Пожалуйста, прочитайте правила ниже и подтвердите согласие.\n"
                    "Без этого вы не сможете писать в чат.\n\n"
                    f"<b>Правила:</b>\n\n{rules_html}",
                    reply_markup=get_rules_agree_keyboard(chat_id, user.id, msg.message_id),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить правила в ЛС {user.id}: {e}")
                await bot.send_message(chat_id, f"Не удалось отправить правила {user.full_name} в ЛС. Попросите открыть ЛС боту.")
        else:
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {update.chat.title}!\n\n"
                    "Просто подтвердите в группе, что вы не бот."
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить сообщение в ЛС {user.id}: {e}")

# Простое приветствие
async def send_simple_welcome(chat_id: int, user: types.User):
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    stat = db.get_user_stat(chat_id, user.id)
    join_dt = format_datetime(stat['join_date']) if stat else format_datetime(time.time())
    
    # Получаем позицию в топе
    position = db.get_user_position(chat_id, user.id, 'all')
    
    text = (
        f"{tr['welcome'].format(name=user.full_name)}\n\n"
        f"• {tr['username']}: @{user.username or tr['no_username']}\n"
        f"• {tr['id']}: <code>{user.id}</code>\n"
        f"• {tr['joined']}: {join_dt}\n"
        f"• {tr['place_in_top']}: {position}"
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

# Подтверждение "Я не бот"
@dp.callback_query(F.data.startswith("confirm_not_bot_"))
async def process_confirm_not_bot(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id = int(parts[3])
    user_id = int(parts[4])
    msg_id = int(parts[5]) if len(parts) > 5 else 0

    if callback.from_user.id != user_id:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await callback.answer(tr['error_not_yours'], show_alert=True)
        return

    db.mark_user_confirmed(chat_id, user_id, agreed=False)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])

    # Снимаем мут
    try:
        await bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions=types.ChatPermissions(can_send_messages=True)
        )
    except Exception as e:
        logger.warning(f"Не удалось снять мут {user_id}: {e}")

    if msg_id > 0:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=tr['confirmed_not_bot'].format(name=callback.from_user.full_name),
                parse_mode="HTML"
            )
        except:
            pass

    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text(tr['thanks_confirmation'])
    await callback.answer("✅")

# Согласие с правилами
@dp.callback_query(F.data.startswith("agree_rules_"))
async def process_agree_rules(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id = int(parts[2])
    user_id = int(parts[3])
    msg_id = int(parts[4])

    if callback.from_user.id != user_id:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await callback.answer(tr['error_not_yours'], show_alert=True)
        return

    db.mark_user_confirmed(chat_id, user_id, agreed=True)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])

    # Снимаем мут
    try:
        await bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions=types.ChatPermissions(can_send_messages=True)
        )
    except Exception as e:
        logger.warning(f"Не удалось снять мут {user_id}: {e}")

    if msg_id > 0:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=tr['confirmed_rules'].format(name=callback.from_user.full_name),
                parse_mode="HTML"
            )
        except:
            pass

    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text(tr['thanks_confirmation'])
    await callback.answer("✅")

# Обновление статистики
@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def update_stats(message: Message):
    if not message.from_user.is_bot and db.has_user_confirmed(message.chat.id, message.from_user.id):
        db.update_message_count(message.chat.id, message.from_user.id)

# Выход из группы
@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    chat_id = update.chat.id
    user = update.from_user
    db.set_left_chat(chat_id, user.id)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    await bot.send_message(chat_id, tr['user_left'].format(name=user.full_name))

# Основные команды в ЛС
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки доступны только в личных сообщениях с ботом!")
        return
    
    await state.clear()
    
    groups = db.get_user_groups(message.from_user.id)
    
    if not groups:
        text = (
            "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\n"
            "У вас пока нет привязанных групп.\n\n"
            "🔹 <b>Как начать:</b>\n"
            "1. Нажмите кнопку «➕ Добавить в группу»\n"
            "2. Выберите чат, куда добавить бота\n"
            "3. Сделайте бота администратором\n"
            "4. В группе напишите /group или нажмите кнопку управления\n"
            "5. Подтвердите привязку группы\n\n"
            "После этого группа появится в списке для настройки."
        )
        await message.answer(text, reply_markup=get_main_keyboard('ru'))
        return
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.button(text=title or f"Группа {chat_id}", callback_data=f"select_group_{chat_id}")
    builder.adjust(1)
    
    await message.answer(
        "📱 <b>Ваши группы:</b>\n\n"
        "Выберите группу для настройки:",
        reply_markup=builder.as_markup()
    )

# Выбор группы
@dp.callback_query(F.data.startswith("select_group_"))
@check_owner()
async def select_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    
    if not await is_creator(chat_id, callback.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await callback.answer(tr['error_not_creator'], show_alert=True)
        return
    
    await state.update_data(selected_chat_id=chat_id)
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Группа"
    
    await callback.message.edit_text(
        f"⚙️ <b>Настройка группы:</b> {chat_title}\n\n"
        f"Выберите, что хотите настроить:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

# Возврат к списку групп
@dp.callback_query(F.data == "back_to_groups")
@check_owner()
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await cmd_start(callback.message, state)

# Управление правилами
@dp.callback_query(F.data == "manage_rules")
@check_owner()
async def manage_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 <b>Управление правилами чата</b>\n\n"
        "Выберите действие:",
        reply_markup=get_rules_manage_keyboard()
    )
    await callback.answer()

# Установка правил
@dp.callback_query(F.data == "set_rules")
@check_owner()
async def set_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте текст правил для этой группы.\n\n"
        "Вы можете использовать любое форматирование:\n"
        "• <b>Жирный</b> - &lt;b&gt;текст&lt;/b&gt;\n"
        "• <i>Курсив</i> - &lt;i&gt;текст&lt;/i&gt;\n"
        "• <tg-spoiler>Спойлер</tg-spoiler> - &lt;tg-spoiler&gt;текст&lt;/tg-spoiler&gt;\n"
        "• <blockquote>Цитата</blockquote> - &lt;blockquote&gt;текст&lt;/blockquote&gt;\n"
        "• <blockquote expandable>Свернутая цитата\nСтрока 2\nСтрока 3</blockquote> - &lt;blockquote expandable&gt;текст\nстроки&lt;/blockquote&gt;\n\n"
        "💡 <b>Важно:</b> Для свернутых цитат нужно минимум 2-3 строки внутри."
    )
    await state.set_state(RulesStates.waiting_for_rules_text)
    await callback.answer()

@dp.message(RulesStates.waiting_for_rules_text)
async def process_rules_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    # ФИКСИРОВАННЫЙ ТЕКСТ ПРАВИЛ С СВЁРНУТОЙ ЦИТАТОЙ
    # НЕ МЕНЯЙТЕ ЭТУ ЧАСТЬ — тег <blockquote expandable> ОБЯЗАТЕЛЬНО ДОЛЖЕН ОСТАТЬСЯ
    rules_html = """
Правила чата:

<blockquote expandable>
1. Запрещено спамить, флудить и писать капсом.<br>
2. Уважайте других участников группы.<br>
3. Реклама, ссылки и призывы к действию — только с разрешения админов.<br>
4. Запрещены оскорбления, угрозы, дискриминация по любому признаку.<br>
5. Нельзя распространять запрещённый контент (порно, насилие, наркотики и т.д.).<br>
6. Администрация имеет право мута/бана без объяснения причин.<br>
7. Если вы не согласны с правилами — покиньте группу.<br>
8. При нарушении правил — пишите админам в ЛС.
</blockquote>

Спасибо за внимание и приятного общения!
"""
    
    # СОХРАНЯЕМ ТОЧНО ТАК, КАК ЕСТЬ — без изменений
    db.save_rules(chat_id, rules_html=rules_html)
    
    await message.reply(
        "✅ <b>Правила успешно сохранены!</b>\n\n"
        "В группе их можно посмотреть командой /rules\n"
        "Цитата с правилами будет свёрнута для удобства.",
        parse_mode="HTML"
    )
    await state.clear()

# Показать правила
@dp.callback_query(F.data == "show_rules")
@check_owner()
async def show_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.message.edit_text("❌ Ошибка! Начните заново через /start.")
        return
    
    rules_html = db.get_rules_html(chat_id)
    
    if rules_html:
        await callback.message.edit_text(
            f"📜 <b>Текущие правила:</b>\n\n{rules_html}",
            parse_mode="HTML"
        )
    else:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await callback.message.edit_text(tr['error_no_rules'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="manage_rules")
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

# Показать правила группы (из приветствия)
@dp.callback_query(F.data.startswith("show_group_rules_"))
async def show_group_rules(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    rules_html = db.get_rules_html(chat_id)
    
    if rules_html:
        await callback.message.answer(
            f"📜 <b>Правила чата:</b>\n\n{rules_html}",
            parse_mode="HTML"
        )
    else:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await callback.message.answer(tr['error_no_rules'])
    
    await callback.answer()

# Управление приветствием
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
        "👋 <b>Управление приветствием</b>\n\n"
        "Настройте приветственное сообщение для новых участников.",
        reply_markup=get_welcome_manage_keyboard(enabled)
    )
    await callback.answer()

# Включение/выключение приветствия
@dp.callback_query(F.data == "toggle_welcome")
@check_owner()
async def toggle_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    current = db.get_welcome_enabled(chat_id)
    db.set_welcome_enabled(chat_id, not current)
    
    new_status = "включено" if not current else "выключено"
    await callback.answer(f"✅ Приветствие {new_status}!", show_alert=True)
    
    await manage_welcome(callback, state)

# Установка текста приветствия
@dp.callback_query(F.data == "set_welcome_text")
@check_owner()
async def set_welcome_text(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте текст приветствия для новых участников.\n\n"
        "Вы можете использовать:\n"
        "• {name} - имя пользователя\n"
        "• {username} - юзернейм\n"
        "• {chat} - название группы\n\n"
        "Пример:\n"
        "<code>Добро пожаловать, {name}!</code>"
    )
    await state.set_state(WelcomeStates.waiting_for_welcome_text)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    welcome_text = message.html_text.strip()
    
    if not welcome_text:
        await message.answer("❌ Текст не может быть пустым!")
        return
    
    db.save_welcome(chat_id, welcome_text=welcome_text)
    
    await message.reply("✅ Текст приветствия сохранен!")
    await state.clear()

# Установка фото для приветствия
@dp.callback_query(F.data == "set_welcome_photo")
@check_owner()
async def set_welcome_photo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🖼 Отправьте фото для приветствия.\n\n"
        "Оно будет отправляться вместе с текстом."
    )
    await state.set_state(WelcomeStates.waiting_for_welcome_photo)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_photo, F.photo)
async def process_welcome_photo(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    photo_id = message.photo[-1].file_id
    
    db.save_welcome(chat_id, welcome_photo_id=photo_id)
    
    await message.reply("✅ Фото для приветствия сохранено!")
    await state.clear()

@dp.message(WelcomeStates.waiting_for_welcome_photo)
async def process_welcome_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Пожалуйста, отправьте фото!")

# Показать приветствие
@dp.callback_query(F.data == "show_welcome")
@check_owner()
async def show_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.message.edit_text("❌ Ошибка! Начните заново через /start.")
        return
    
    text, photo_id = db.get_welcome(chat_id)
    
    if not text and not photo_id:
        await callback.message.edit_text("❌ Приветствие еще не настроено.")
        
        builder = InlineKeyboardBuilder()
        builder.button(text="◀️ Назад", callback_data="manage_welcome")
        await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
        await callback.answer()
        return
    
    await callback.message.delete()
    
    if photo_id:
        await callback.message.answer_photo(
            photo=photo_id,
            caption=f"👋 <b>Текущее приветствие:</b>\n\n{text}" if text else None,
            parse_mode="HTML"
        )
    elif text:
        await callback.message.answer(
            f"👋 <b>Текущее приветствие:</b>\n\n{text}",
            parse_mode="HTML"
        )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="manage_welcome")
    await callback.message.answer("Выберите действие:", reply_markup=builder.as_markup())
    await callback.answer()

# Авто-рассылка правил
@dp.callback_query(F.data == "rules_auto")
@check_owner()
async def rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_settings(chat_id)
    
    text = (
        "🔄 <b>Автоматическая рассылка правил</b>\n\n"
        f"Статус: {'✅ Включено' if enabled else '❌ Выключено'}\n"
        f"Интервал: {format_interval(interval)}\n\n"
        "Бот будет автоматически отправлять и закреплять правила "
        "с заданным интервалом."
    )
    
    await callback.message.edit_text(text, reply_markup=get_rules_auto_keyboard(bool(enabled)))
    await callback.answer()

@dp.callback_query(F.data == "toggle_rules_auto")
@check_owner()
async def toggle_rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_settings(chat_id)
    new_enabled = not bool(enabled)
    
    db.set_rules_settings(chat_id, new_enabled, interval)
    
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
        "⏱ Введите интервал в минутах (от 5 до 525600):\n"
        "Примеры:\n"
        "• 60 = 1 час\n"
        "• 1440 = 1 день\n"
        "• 10080 = 1 неделя\n"
        "• 43200 = 1 месяц"
    )
    await state.set_state(RulesStates.waiting_for_interval)
    await callback.answer()

@dp.message(RulesStates.waiting_for_interval)
async def process_interval(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        interval = int(message.text)
        if interval < 5 or interval > 525600:
            await message.answer("❌ Интервал должен быть от 5 до 525600 минут!")
            return
        
        interval_seconds = interval * 60
        enabled, _, _, _ = db.get_rules_settings(chat_id)
        db.set_rules_settings(chat_id, bool(enabled), interval_seconds)
        
        await message.reply(f"✅ Интервал установлен: {format_interval(interval_seconds)}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

# Управление антифлудом
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
        "🚫 <b>Управление антифлудом</b>\n\n"
        "Настройте защиту от флуда в чате.",
        reply_markup=get_antiflood_manage_keyboard(settings)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_antiflood")
@check_owner()
async def toggle_antiflood(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    db.set_antiflood_enabled(chat_id, not settings['enabled'])
    
    new_status = "включен" if not settings['enabled'] else "выключен"
    await callback.answer(f"✅ Антифлуд {new_status}!", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_limit")
@check_owner()
async def set_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📊 Введите лимит сообщений за интервал (от 3 до 20):"
    )
    await state.set_state(AntiFloodStates.waiting_for_limit)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_limit)
async def process_limit(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        limit = int(message.text)
        if limit < 3 or limit > 20:
            await message.answer("❌ Лимит должен быть от 3 до 20!")
            return
        
        db.save_antiflood_settings(chat_id, msg_limit=limit)
        await message.reply(f"✅ Лимит сообщений установлен: {limit}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "set_window")
@check_owner()
async def set_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите временное окно в секундах (от 5 до 300):"
    )
    await state.set_state(AntiFloodStates.waiting_for_window)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_window)
async def process_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        window = int(message.text)
        if window < 5 or window > 300:
            await message.answer("❌ Окно должно быть от 5 до 300 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, time_window=window)
        await message.reply(f"✅ Временное окно установлено: {window} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "set_warn_count")
@check_owner()
async def set_warn_count(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⚠️ Введите количество предупреждений перед наказанием (от 1 до 5):"
    )
    await state.set_state(AntiFloodStates.waiting_for_warn_count)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_warn_count)
async def process_warn_count(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        count = int(message.text)
        if count < 1 or count > 5:
            await message.answer("❌ Количество предупреждений должно быть от 1 до 5!")
            return
        
        db.save_antiflood_settings(chat_id, warn_count=count)
        await message.reply(f"✅ Количество предупреждений установлено: {count}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "set_first_punish")
@check_owner()
async def set_first_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔇 Выберите наказание для первого нарушения:",
        reply_markup=get_punish_type_keyboard(is_first=True)
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
        reply_markup=get_punish_type_keyboard(is_first=False)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_"))
async def process_punish_type(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split('_')
    punish_type = parts[1]  # warn, mute, kick, ban
    is_first = parts[2] == 'first'
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    if is_first:
        db.save_antiflood_settings(chat_id, first_punish=punish_type)
        await callback.answer(f"✅ Наказание для первого нарушения: {punish_type}", show_alert=True)
    else:
        db.save_antiflood_settings(chat_id, repeat_punish=punish_type)
        await callback.answer(f"✅ Наказание для повторных нарушений: {punish_type}", show_alert=True)
    
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_first_duration")
@check_owner()
async def set_first_duration(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность наказания в секундах для первого нарушения (от 30 до 86400):"
    )
    await state.set_state(AntiFloodStates.waiting_for_first_duration)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_first_duration)
async def process_first_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        if duration < 30 or duration > 86400:
            await message.answer("❌ Длительность должна быть от 30 до 86400 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, first_duration=duration)
        await message.reply(f"✅ Длительность для первого нарушения установлена: {duration} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "set_repeat_duration")
@check_owner()
async def set_repeat_duration(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность наказания в секундах для повторных нарушений (от 60 до 604800):"
    )
    await state.set_state(AntiFloodStates.waiting_for_repeat_duration)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_repeat_duration)
async def process_repeat_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново через /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        if duration < 60 or duration > 604800:
            await message.answer("❌ Длительность должна быть от 60 до 604800 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, repeat_duration=duration)
        await message.reply(f"✅ Длительность для повторных нарушений установлена: {duration} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

# Настройка языка
@dp.callback_query(F.data == "set_language")
@check_owner()
async def set_language(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    current_lang = db.get_group_language(chat_id)
    
    await callback.message.edit_text(
        TRANSLATIONS[current_lang]['current_language'].format(lang=current_lang.upper()) + "\n\n" +
        TRANSLATIONS[current_lang]['choose_language'],
        reply_markup=get_language_keyboard(current_lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("lang_"))
@check_owner()
async def process_language(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split('_')[1]  # ru / uk / en
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Группа не выбрана!", show_alert=True)
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET language = ? WHERE chat_id = ?', (lang, chat_id))
        conn.commit()
    
    await callback.answer(TRANSLATIONS[lang]['language_changed'].format(lang=lang.upper()), show_alert=True)
    await set_language(callback, state)

# Топ активных
@dp.callback_query(F.data.startswith("top_active_"))
@dp.message(Command("top"))
async def show_top_messages(event):
    global stats_updating
    
    # Ждём, пока сброс закончится (максимум 5 секунд)
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        if isinstance(event, CallbackQuery):
            await event.answer("📊 Статистика обновляется, подождите 5–10 секунд", show_alert=True)
            return
        else:
            await event.reply("📊 Статистика обновляется, подождите 5–10 секунд")
            return
    
    if isinstance(event, CallbackQuery):
        msg = event.message
        chat_id = int(event.data.split('_')[-1])
        is_callback = True
    else:
        msg = event
        chat_id = msg.chat.id
        is_callback = False
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    top = db.get_top_messages(chat_id, period='all', limit=10)
    
    if not top:
        text = tr['stats_empty']
    else:
        text = f"<b>{tr['top_active']}</b>\n\n"
        for i, (user_id, count) in enumerate(top, 1):
            try:
                user = await bot.get_chat_member(chat_id, user_id)
                name = user.user.full_name
            except:
                name = f"ID {user_id}"
            text += f"{i}. {name} — {count} {tr['messages']}\n"
    
    if is_callback:
        await event.message.answer(text, parse_mode="HTML")
        await event.answer()
    else:
        await msg.reply(text, parse_mode="HTML")

# Моя статистика
@dp.callback_query(F.data.startswith("my_stats_"))
@dp.message(Command("stats"))
async def show_user_profile(event):
    global stats_updating
    
    # Ждём, пока сброс закончится (максимум 5 секунд)
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        if isinstance(event, CallbackQuery):
            await event.answer("📊 Статистика обновляется, подождите 5–10 секунд", show_alert=True)
            return
        else:
            await event.reply("📊 Статистика обновляется, подождите 5–10 секунд")
            return
    
    if isinstance(event, CallbackQuery):
        msg = event.message
        user = event.from_user
        chat_id = int(event.data.split('_')[-1])
        is_callback = True
    else:
        msg = event
        user = msg.from_user
        chat_id = msg.chat.id
        is_callback = False
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['ru'])
    
    stat = db.get_user_stat(chat_id, user.id)
    if not stat:
        text = tr['stats_empty']
    else:
        join_dt = format_datetime(stat['join_date'])
        last_dt = format_datetime(stat['last_active'])
        position = db.get_user_position(chat_id, user.id, 'all')
        
        text = (
            f"<b>{tr['profile'].format(name=user.full_name)}</b>\n\n"
            f"• {tr['per_day']}: {stat['day_messages']} {tr['messages']}\n"
            f"• {tr['per_week']}: {stat['week_messages']} {tr['messages']}\n"
            f"• {tr['per_month']}: {stat['month_messages']} {tr['messages']}\n"
            f"• {tr['total']}: {stat['all_messages']} {tr['messages']}\n"
            f"• {tr['place_in_top']}: {position}\n"
            f"• {tr['joined']}: {join_dt}\n"
            f"• {tr['last_active']}: {last_dt}"
        )
    
    if is_callback:
        await event.message.answer(text, parse_mode="HTML")
        await event.answer()
    else:
        await msg.reply(text, parse_mode="HTML")

# Возврат в меню группы
@dp.callback_query(F.data == "group_manage")
@check_owner()
async def back_to_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.message.edit_text("❌ Ошибка! Начните заново через /start.")
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Группа"
    
    await callback.message.edit_text(
        f"⚙️ <b>Настройка группы:</b> {chat_title}\n\n"
        f"Выберите, что хотите настроить:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

# О боте
@dp.callback_query(F.data == "about")
@check_owner()
async def callback_about(callback: CallbackQuery, state: FSMContext):
    text = (
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "📌 <b>Что я умею:</b>\n"
        "• Автоматическая модерация новых участников\n"
        "• Подтверждение правил в ЛС\n"
        "• Статистика сообщений (день/неделя/месяц/всего)\n"
        "• Топ активных участников\n"
        "• Антифлуд с настраиваемыми наказаниями\n"
        "• Приветствие с фото/текстом\n"
        "• Авто-рассылка правил\n"
        "• Поддержка 3 языков (русский, украинский, английский)\n"
        "• Проверка пинга (/puls, /startpuls, пульс, pulse, понг)\n\n"
        "👇 Нажмите «➕ Добавить в группу» чтобы пригласить меня в ваш чат"
    )
    
    # Определяем язык для кнопок
    lang = 'ru'
    if callback.message.chat.type in {'group', 'supergroup'}:
        lang = db.get_group_language(callback.message.chat.id)
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(lang))
    await callback.answer()

# Помощь
@dp.callback_query(F.data == "help")
@check_owner()
async def callback_help(callback: CallbackQuery, state: FSMContext):
    text = (
        "🆘 <b>Помощь по Puls Chat Manager</b>\n\n"
        "🔹 <b>Основные команды в группе:</b>\n"
        "• /rules - Показать правила\n"
        "• /stats - Моя статистика\n"
        "• /top - Топ активных\n"
        "• /puls, /startpuls, пульс, pulse - Проверка пинга\n"
        "• /group - Управление группой (для создателя)\n\n"
        "🔹 <b>Как добавить бота в группу:</b>\n"
        "1. Нажмите кнопку «➕ Добавить в группу»\n"
        "2. Выберите чат\n"
        "3. Сделайте бота администратором\n"
        "4. В группе напишите /group и привяжите группу\n"
        "5. Настройте в ЛС через /start\n\n"
        "🔹 <b>Для новых участников:</b>\n"
        "• Бот автоматически мутит до подтверждения\n"
        "• Нужно согласиться с правилами в ЛС\n"
        "• После подтверждения снимается мут\n\n"
        "🔹 <b>Статистика:</b>\n"
        "• Считаются сообщения за день/неделю/месяц\n"
        "• Сброс происходит автоматически\n"
        "• Топ показывает самых активных\n\n"
        "🔹 <b>Языки:</b>\n"
        "• Бот поддерживает русский, украинский и английский\n"
        "• Язык можно изменить в настройках группы"
    )
    
    # Определяем язык для кнопок
    lang = 'ru'
    if callback.message.chat.type in {'group', 'supergroup'}:
        lang = db.get_group_language(callback.message.chat.id)
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(lang))
    await callback.answer()

# Запуск бота
async def main():
    # Подключаем middleware
    dp.message.middleware(AntiFloodMiddleware())
    
    # Запускаем фоновые задачи
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
