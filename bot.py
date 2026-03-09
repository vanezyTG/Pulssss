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

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, ChatPermissions, InlineKeyboardButton
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
BOT_TOKEN = "8557190026:AAENDFgMgIPPUFxhBoxYBr1k-R8et0rL-P8"
BOT_USERNAME = "PulsOfficialManager_bot"
ADMIN_IDS = [6708209142]

# Автоопределение часового пояса сервера
SERVER_TZ = datetime.now().astimezone().tzinfo

# Хранилище для антифлуда
flood_control = defaultdict(list)

# Блокировка для статистики
stats_lock = threading.Lock()
stats_updating = False

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Функция для генерации случайного ID пользователя
def generate_user_id() -> str:
    """Генерирует случайный 9-значный ID для пользователя"""
    return ''.join(random.choices(string.digits, k=9))

# Классы состояний
class RulesStates(StatesGroup):
    waiting_for_rules_text = State()
    waiting_for_interval = State()
    waiting_for_new_rules_text = State()

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

class ReportGroupStates(StatesGroup):
    waiting_for_report_group = State()

class AutoResponseStates(StatesGroup):
    waiting_for_trigger = State()
    waiting_for_response = State()
    waiting_for_remove_trigger = State()

class LinksStates(StatesGroup):
    waiting_for_duration = State()
    waiting_for_max_mentions = State()
    waiting_for_mention_window = State()

class ConfirmationStates(StatesGroup):
    waiting_for_confirmation = State()

# ========== ДЕКОРАТОРЫ ПРОВЕРКИ ПРАВ ==========

def check_owner():
    """
    Декоратор для проверки, что кнопку нажимает тот же пользователь,
    который вызвал команду. Сохраняет owner_id в state при вызове команды.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id
            state: FSMContext = kwargs.get('state')
            
            if state:
                data = await state.get_data()
                # Проверяем наличие сохраненного owner_id для этого сообщения
                msg_owner = data.get(f"msg_owner_{callback.message.message_id}")
                
                if msg_owner and msg_owner != user_id:
                    await callback.answer("⚠️ Эта кнопка только для того, кто вызвал команду!", show_alert=True)
                    return
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

def check_public():
    """
    Декоратор для публичных кнопок, которые может нажимать кто угодно.
    Просто пропускает всех.
    """
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

# Готовый текст правил с expandable цитатой
DEFAULT_RULES = """
Правила чата:

<blockquote expandable>
1. Запрещено спамить, флудить и писать капсом.
2. Уважайте других участников группы.
3. Реклама, ссылки и призывы к действию - только с разрешения админов.
4. Запрещены оскорбления, угрозы, дискриминация по любому признаку.
5. Нельзя распространять запрещённый контент (порно, насилие, наркотики и т.д.).
6. Администрация имеет право мута/бана без объяснения причин.
7. Если вы не согласны с правилами - покиньте группу.
8. При нарушении правил - пишите админам в ЛС.
</blockquote>

Спасибо за внимание и приятного общения!
"""

# Функция для получения текста (всегда русский)
def get_text(key: str = None, **kwargs) -> str:
    """
    Возвращает текст на русском языке
    """
    text = TEXTS.get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except:
            pass
    return text

# Словарь текстов на русском
TEXTS = {
    # ========== ПРИВЕТСТВИЯ И ОБЩЕЕ ==========
    'welcome': "Добро пожаловать, <b>{name}</b>!",
    'no_username': "нет",
    'username': "Username",
    'id': "ID",
    'joined': "Вошёл",
    'last_active': "Последняя активность",
    'place_in_top': "Место в топе",
    'user_id': "ID пользователя",
    'first_seen': "Впервые замечен",
    'messages_count': "Сообщений",
    
    # ========== ПОДТВЕРЖДЕНИЕ ==========
    'confirm_not_bot': "Я не бот",
    'agree_rules': "✅ Согласен с правилами",
    'muted_forever': "Вы замьючены **навсегда**, пока не подтвердите правила",
    'go_to_pm': "📜 Перейти в ЛС",
    'rules_sent': "Правила отправлены в личные сообщения",
    'confirmed_not_bot': "✅ {name} подтвердил, что не бот и теперь может писать в чат.",
    'confirmed_rules': "✅ {name} согласился с правилами и теперь может писать в чат.",
    'thanks_confirmation': "Спасибо за подтверждение! Теперь вы можете писать в чат.",
    'need_confirm_both': "Вам нужно выполнить ДВА шага:\n1. Подтвердить, что вы не бот\n2. Прочитать и согласиться с правилами",
    'step_1_completed': "✅ Шаг 1 выполнен! Теперь выполните шаг 2: согласитесь с правилами.",
    'step_2_completed': "✅ Шаг 2 выполнен! Теперь выполните шаг 1: подтвердите, что вы не бот.",
    'confirmation_disabled': "✅ Подтверждение отключено. Новые участники могут писать сразу.",
    
    # ========== СООБЩЕНИЯ ПРИ ВХОДЕ ==========
    'user_joined': "👋 <b>{name}</b> зашёл в чат!",
    'need_confirm_rules': "Вы замьючены **навсегда**, пока не подтвердите правила.\nПерейдите в ЛС бота, прочитайте правила и подтвердите согласие — мут снимется.",
    'need_confirm_not_bot': "Вы замьючены **навсегда**, пока не подтвердите, что вы не бот.\nНажмите кнопку ниже — мут снимется.",
    
    # ========== СТАТИСТИКА ==========
    'stats_empty': "📊 Статистика ещё не собрана",
    'stats_updating': "📊 Статистика обновляется, подождите 5–10 секунд",
    'top_active': "🏆 Топ активных (всего сообщений):",
    'profile': "Профиль {name}",
    'per_day': "За день",
    'per_week': "За неделю",
    'per_month': "За месяц",
    'total': "Всего",
    'messages': "сообщений",
    
    # ========== НАСТРОЙКИ ==========
    'current_language': "Текущий язык: русский",
    
    # ========== КОМАНДЫ ==========
    'pulse': "пульс",
    'pong': "понг",
    'ping': "Пинг: {ping} мс\nВремя ответа: {response} сек",
    'start': "Старт",
    'main_menu': "Главное меню",
    
    # ========== ВЫХОД ==========
    'user_left': "👋 Пользователь {name} вышел из чата.",
    
    # ========== ОШИБКИ ==========
    'error_no_group': "❌ Сначала выберите группу!",
    'error_not_creator': "❌ Вы не являетесь создателем этой группы!",
    'error_not_yours': "⚠️ Это не ваше подтверждение!",
    'error_no_rules': "❌ В этом чате еще не установлены правила.",
    'error_rules_short': "❌ Правила слишком короткие! Отправьте более содержательный текст.",
    'group_only': "❌ Эта команда работает только в группах!",
    'pm_only': "❌ Эта команда работает только в личных сообщениях!",
    'rules_not_set': "❌ В этом чате не установлены правила!",
    'group_not_found': "❌ Группа не найдена!",
    'user_not_found': "❌ Пользователь не найден!",
    'cant_use_rules_no_rules': "❌ Нельзя выбрать 'Только правила', так как в группе не установлены правила!\nСначала установите правила через меню «Управление правилами».",
    'cant_use_both_no_rules': "❌ Нельзя выбрать 'Оба шага', так как в группе не установлены правила!\nСначала установите правила через меню «Управление правилами».",
    
    # ========== АВТО-РАССЫЛКА ==========
    'rules_reminder': "📢 Напоминание правил чата",
    
    # ========== ГЛАВНОЕ МЕНЮ ==========
    'about': "📋 О боте",
    'help': "🆘 Помощь",
    'add_to_group': "➕ Добавить в группу",
    'group_manage': "⚙️ Управление группой",
    'back': "◀️ Назад",
    
    # ========== ПРИВЯЗКА ГРУППЫ ==========
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
    'select_group': "📱 Выберите группу для настройки:",
    'link_command': "Если хотите привязать группу, напишите команду: /group",
    
    # ========== ГРУППА РЕПОРТОВ ==========
    'report_group': "📋 Группа репортов",
    'current_report_group': "📋 Текущая группа репортов: {group}",
    'set_report_group': "Установить группу репортов",
    'report_group_info': "Выберите группу, куда будут отправляться логи нарушений:",
    'report_group_set': "✅ Группа репортов успешно установлена!",
    'report_group_removed': "❌ Группа репортов удалена.",
    'violation_report': "🚫 Отчёт о нарушении",
    'user': "Пользователь",
    'reason': "Причина",
    'punishment': "Наказание",
    'message_link': "🔗 Перейти к сообщению",
    'time': "Время",
    'violations_list': "📋 Последние нарушения",
    'no_violations': "Нарушений пока нет.",
    
    # ========== ПРОФИЛЬ ==========
    'profile_info': "👤 Профиль пользователя",
    'profile_stats': "Статистика для {name}:",
    'reply_to_user': "Ответьте на сообщение пользователя командой /profile чтобы увидеть его статистику",
    
    # ========== АВТООТВЕТЧИК ==========
    'auto_responder': "🤖 Автоответчик",
    'auto_responder_empty': "Автоответчик пуст.\nДобавьте первое ключевое слово и ответ.",
    'auto_responder_list': "🤖 Автоответчик:\n\n",
    'add_trigger': "➕ Добавить триггер",
    'remove_trigger': "🗑 Удалить триггер",
    'enter_trigger': "Введите ключевое слово (триггер):",
    'enter_response': "Введите текст ответа:",
    'trigger_added': "✅ Триггер '{trigger}' успешно добавлен!",
    'trigger_removed': "✅ Триггер '{trigger}' успешно удалён!",
    'select_trigger_to_remove': "Выберите триггер для удаления:",
    'trigger_exists': "❌ Триггер '{trigger}' уже существует!",
    
    # ========== ССЫЛКИ И УПОМИНАНИЯ ==========
    'links_mentions': "🔗 Ссылки и упоминания",
    'links_enabled': "Фильтр ссылок: {status}",
    'links_punish': "Наказание: {punish}",
    'max_mentions': "Макс упоминаний: {count} за {window} сек",
    'toggle_links': "Вкл/выкл фильтр ссылок",
    'set_links_punish': "Установить наказание за ссылки",
    'set_max_mentions': "Установить макс упоминаний",
    'set_mention_window': "Установить окно упоминаний",
    'choose_punish': "Выберите наказание:",
    'enter_duration': "Введите длительность в минутах (0 = навсегда):",
    'enter_max_mentions': "Введите максимальное количество упоминаний в минуту:",
    'enter_mention_window': "Введите окно упоминаний в секундах:",
    'punish_set': "✅ Наказание установлено: {punish}",
    'max_mentions_set': "✅ Макс упоминаний установлено: {count}",
    'mention_window_set': "✅ Окно упоминаний установлено: {window} сек",
    'filter_enabled': "✅ Фильтр включён",
    'filter_disabled': "❌ Фильтр выключен",
    'punishment_saved': "✅ <b>Настройки сохранены!</b>\n\nНаказание: {punish}\nДлительность: {duration}",
    
    # ========== НАСТРОЙКИ ПОДТВЕРЖДЕНИЯ ==========
    'confirmation_settings': "✅ Настройки подтверждения",
    'confirmation_type': "Тип подтверждения: {type}",
    'disabled': "🚫 Отключено",
    'not_bot_only': "🤖 Только не бот",
    'rules_only': "📜 Только правила",
    'both_steps': "2️⃣ Оба шага",
    'set_confirmation_type': "Установить тип подтверждения",
    'confirmation_updated': "✅ Настройки подтверждения обновлены!",
    'cant_use_rules': "❌ Нельзя выбрать этот вариант, так как в группе не установлены правила! Сначала установите правила.",
    'cant_use_both': "❌ Нельзя выбрать 'Оба шага', так как в группе не установлены правила! Сначала установите правила.",
    'need_rules_first': "⚠️ Правила не установлены. Этот вариант требует наличия правил.",
    
    # ========== УПРАВЛЕНИЕ ПРАВИЛАМИ ==========
    'rules_management': "📝 Управление правилами",
    'set_rules': "📝 Установить правила",
    'set_default_rules': "📋 Установить готовые правила",
    'edit_rules': "✏️ Изменить правила",
    'delete_rules': "🗑 Удалить правила",
    'toggle_rules': "🔄 Вкл/Выкл правила",
    'rules_enabled': "✅ Правила включены",
    'rules_disabled': "❌ Правила выключены",
    'rules_deleted': "✅ Правила успешно удалены!",
    'rules_enabled_status': "Правила включены: {status}",
    'enter_new_rules': "📝 Отправьте новый текст правил:",
    'rules_updated': "✅ Правила успешно обновлены!",
    'rules_set': "✅ Правила успешно установлены!",
    'default_rules_set': "✅ Готовые правила успешно установлены!",
    
    # ========== КНОПКИ ==========
    'status_enabled': "✅ Включено",
    'status_disabled': "❌ Выключено",
    'set_text': "📝 Установить текст",
    'set_photo': "🖼 Установить фото",
    'view': "👁 Посмотреть",
    'rules': "📜 Правила",
    'my_stats': "📊 Моя статистика",
    'top_active_btn': "🏆 Топ активных",
    'interval': "⏱ Интервал",
    'limit': "📊 Лимит",
    'window': "⏱ Окно",
    'warn_count': "⚠️ Предупреждений",
    'first_punish': "🔇 Первое",
    'repeat_punish': "🔊 Повторное",
    'enable': "✅ Включить",
    'disable': "❌ Выключить",
    'duration': "Длительность",
    'minutes': "минут",
    'forever': "навсегда",
}

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
                          confirmation_type TEXT DEFAULT 'both')''')
            
            # Добавляем поле report_group_id, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN report_group_id INTEGER')
            except sqlite3.OperationalError:
                pass
            
            # Добавляем поле confirmation_type, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN confirmation_type TEXT DEFAULT "both"')
            except sqlite3.OperationalError:
                pass
            
            # Добавляем поле rules_enabled, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN rules_enabled INTEGER DEFAULT 1')
            except sqlite3.OperationalError:
                pass
            
            # Индекс для правил
            try:
                c.execute('CREATE INDEX IF NOT EXISTS idx_rules_enabled ON group_rules (rules_enabled)')
            except:
                pass
            
            # Таблица для глобальных пользователей
            c.execute('''CREATE TABLE IF NOT EXISTS global_users
                         (user_id INTEGER PRIMARY KEY,
                          global_id TEXT UNIQUE,
                          first_seen INTEGER,
                          username TEXT,
                          full_name TEXT)''')
            
            # Таблица для автоответчика
            c.execute('''CREATE TABLE IF NOT EXISTS auto_responses
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          trigger TEXT,
                          response TEXT,
                          created_at INTEGER)''')
            
            # Таблица для согласившихся с правилами
            c.execute('''CREATE TABLE IF NOT EXISTS rules_agreed
                         (chat_id INTEGER,
                          user_id INTEGER,
                          agreed_at INTEGER,
                          not_bot_confirmed INTEGER DEFAULT 0,
                          rules_confirmed INTEGER DEFAULT 0,
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
                          repeat_duration INTEGER DEFAULT 300,
                          links_enabled INTEGER DEFAULT 0,
                          links_punish TEXT DEFAULT 'mute',
                          links_duration INTEGER DEFAULT 3600,
                          max_mentions INTEGER DEFAULT 3,
                          mention_window INTEGER DEFAULT 60)''')
            
            # Таблица для логов нарушений
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
                             (chat_id, owner_id, rules_html, chat_title, chat_username, confirmation_type) 
                             VALUES (?, ?, ?, ?, ?, ?)''', 
                             (chat_id, owner_id, rules_html, chat_title, chat_username, 'both'))
            
            conn.commit()
    
    def set_rules_enabled(self, chat_id: int, enabled: bool):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET rules_enabled = ? WHERE chat_id = ?', 
                     (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_rules_enabled(self, chat_id: int) -> bool:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_enabled FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return bool(result[0]) if result else True
    
    def delete_rules(self, chat_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET rules_html = NULL WHERE chat_id = ?', (chat_id,))
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
    
    def set_rules_auto_settings(self, chat_id: int, enabled: bool, interval: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,))
            existing = c.fetchone()
            
            if existing:
                c.execute('''UPDATE group_rules 
                             SET rules_auto_enabled = ?, rules_interval = ? 
                             WHERE chat_id = ?''', (1 if enabled else 0, interval, chat_id))
            else:
                c.execute('''INSERT INTO group_rules (chat_id, rules_auto_enabled, rules_interval) 
                             VALUES (?, ?, ?)''', (chat_id, 1 if enabled else 0, interval))
            conn.commit()
    
    def get_rules_auto_settings(self, chat_id: int) -> Tuple[int, int, Optional[int], Optional[int]]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT rules_auto_enabled, rules_interval, last_rules_message_id, last_rules_time 
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
    
    def set_report_group(self, chat_id: int, report_group_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET report_group_id = ? WHERE chat_id = ?', (report_group_id, chat_id))
            conn.commit()
    
    def get_report_group(self, chat_id: int) -> Optional[int]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT report_group_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    def get_report_group_name(self, chat_id: int) -> Optional[str]:
        report_group_id = self.get_report_group(chat_id)
        if not report_group_id:
            return None
        
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (report_group_id,))
            result = c.fetchone()
            return result[0] if result else f"Группа {report_group_id}"
    
    def get_confirmation_type(self, chat_id: int) -> str:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT confirmation_type FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else 'both'
    
    def set_confirmation_type(self, chat_id: int, conf_type: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET confirmation_type = ? WHERE chat_id = ?', (conf_type, chat_id))
            conn.commit()
    
    def add_auto_response(self, chat_id: int, trigger: str, response: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO auto_responses 
                         (chat_id, trigger, response, created_at) 
                         VALUES (?, ?, ?, ?)''', 
                      (chat_id, trigger.lower(), response, int(time.time())))
            conn.commit()
    
    def get_auto_responses(self, chat_id: int) -> List[Tuple[str, str]]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT trigger, response FROM auto_responses WHERE chat_id = ?', (chat_id,))
            return c.fetchall()
    
    def remove_auto_response(self, chat_id: int, trigger: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger.lower()))
            conn.commit()
    
    def mark_user_confirmed(self, chat_id: int, user_id: int, not_bot: bool = False, rules: bool = False):
        with self.get_connection() as conn:
            c = conn.cursor()
            
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', 
                     (chat_id, user_id))
            result = c.fetchone()
            
            if result:
                not_bot_confirmed = result[0] or not_bot
                rules_confirmed = result[1] or rules
                c.execute('''UPDATE rules_agreed 
                             SET not_bot_confirmed = ?, rules_confirmed = ?, agreed_at = ? 
                             WHERE chat_id = ? AND user_id = ?''',
                         (1 if not_bot_confirmed else 0, 1 if rules_confirmed else 0, int(time.time()), chat_id, user_id))
            else:
                c.execute('''INSERT INTO rules_agreed 
                             (chat_id, user_id, agreed_at, not_bot_confirmed, rules_confirmed) 
                             VALUES (?, ?, ?, ?, ?)''',
                         (chat_id, user_id, int(time.time()), 1 if not_bot else 0, 1 if rules else 0))
            
            conn.commit()
    
    def has_user_confirmed(self, chat_id: int, user_id: int, conf_type: str = None) -> bool:
        if conf_type is None:
            conf_type = self.get_confirmation_type(chat_id)
        
        if conf_type == 'disabled':
            return True
        
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', 
                     (chat_id, user_id))
            result = c.fetchone()
            
            if not result:
                return False
            
            not_bot_confirmed, rules_confirmed = result
            
            if conf_type == 'not_bot':
                return bool(not_bot_confirmed)
            elif conf_type == 'rules':
                return bool(rules_confirmed)
            else:
                return bool(not_bot_confirmed) and bool(rules_confirmed)
    
    def get_user_confirmation_status(self, chat_id: int, user_id: int) -> Tuple[bool, bool]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', 
                     (chat_id, user_id))
            result = c.fetchone()
            
            if not result:
                return (False, False)
            
            return (bool(result[0]), bool(result[1]))
    
    def get_or_create_global_user(self, user_id: int, username: str, full_name: str) -> str:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            
            if result:
                return result[0]
            
            global_id = generate_user_id()
            c.execute('''INSERT INTO global_users 
                         (user_id, global_id, first_seen, username, full_name) 
                         VALUES (?, ?, ?, ?, ?)''',
                      (user_id, global_id, int(time.time()), username, full_name))
            conn.commit()
            return global_id
    
    def get_global_user(self, user_id: int) -> Optional[dict]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id, first_seen, username, full_name FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            if result:
                return {
                    'global_id': result[0],
                    'first_seen': result[1],
                    'username': result[2],
                    'full_name': result[3]
                }
            return None
    
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
    
    def get_antiflood_settings(self, chat_id: int) -> dict:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT enabled, msg_limit, time_window, warn_count, 
                                first_punish, first_duration, repeat_punish, repeat_duration,
                                links_enabled, links_punish, links_duration, max_mentions, mention_window
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
                    'repeat_duration': row[7] or 300,
                    'links_enabled': bool(row[8]),
                    'links_punish': row[9] or 'mute',
                    'links_duration': row[10] or 3600,
                    'max_mentions': row[11] or 3,
                    'mention_window': row[12] or 60
                }
            return {
                'enabled': False,
                'msg_limit': 5,
                'time_window': 10,
                'warn_count': 2,
                'first_punish': 'mute',
                'first_duration': 60,
                'repeat_punish': 'mute',
                'repeat_duration': 300,
                'links_enabled': False,
                'links_punish': 'mute',
                'links_duration': 3600,
                'max_mentions': 3,
                'mention_window': 60
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
                    'repeat_duration': 300,
                    'links_enabled': 0,
                    'links_punish': 'mute',
                    'links_duration': 3600,
                    'max_mentions': 3,
                    'mention_window': 60
                }
                defaults.update(kwargs)
                c.execute('''INSERT INTO antiflood_settings 
                             (chat_id, enabled, msg_limit, time_window, warn_count, 
                              first_punish, first_duration, repeat_punish, repeat_duration,
                              links_enabled, links_punish, links_duration, max_mentions, mention_window) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (chat_id, defaults['enabled'], defaults['msg_limit'], 
                           defaults['time_window'], defaults['warn_count'],
                           defaults['first_punish'], defaults['first_duration'],
                           defaults['repeat_punish'], defaults['repeat_duration'],
                           defaults['links_enabled'], defaults['links_punish'],
                           defaults['links_duration'], defaults['max_mentions'],
                           defaults['mention_window']))
            conn.commit()
    
    def log_violation(self, chat_id: int, user_id: int, user_name: str, reason: str, punishment: str, message_id: int, message_link: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO violation_logs 
                         (chat_id, user_id, user_name, reason, punishment, message_id, message_link, timestamp) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (chat_id, user_id, user_name, reason, punishment, message_id, message_link, int(time.time())))
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

def format_duration(minutes: int) -> str:
    if minutes == 0:
        return "навсегда"
    return f"{minutes} минут"

def get_message_link(chat_id: int, message_id: int) -> str:
    chat_id_str = str(chat_id)
    if chat_id_str.startswith('-100'):
        chat_id_str = chat_id_str[4:]
    return f"https://t.me/c/{chat_id_str}/{message_id}"

# ========== КЛАВИАТУРЫ С ЦВЕТАМИ ==========

def create_button(text: str, callback_data: str, color: str = None) -> InlineKeyboardButton:
    """
    Создает кнопку с указанным цветом.
    Цвета: primary (синий), secondary (серый), success (зеленый), danger (красный)
    """
    if color:
        return InlineKeyboardButton(text=text, callback_data=callback_data, color=color)
    return InlineKeyboardButton(text=text, callback_data=callback_data)

def get_back_keyboard(callback_data: str):
    """Серая кнопка назад"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", callback_data, "secondary"))
    return builder.as_markup()

def get_main_keyboard():
    """Главное меню с цветными кнопками"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📋 О боте", "about", "primary"))
    builder.add(create_button("🆘 Помощь", "help", "danger"))
    builder.add(create_button("➕ Добавить в группу", f"add_to_group_{BOT_USERNAME}", "success"))
    builder.add(create_button("⚙️ Управление группой", "group_manage_main", "primary"))
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚙️ Управление группой", "group_manage", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_keyboard():
    """Меню управления группой - основные кнопки цветные, остальные серые"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Управление правилами", "manage_rules", "primary"))
    builder.add(create_button("👋 Приветствие", "manage_welcome", "secondary"))
    builder.add(create_button("🔄 Авто-рассылка правил", "rules_auto", "secondary"))
    builder.add(create_button("🚫 Антифлуд", "antiflood_manage", "secondary"))
    builder.add(create_button("📋 Группа репортов", "set_report_group", "secondary"))
    builder.add(create_button("🤖 Автоответчик", "auto_response_manage", "secondary"))
    builder.add(create_button("🔗 Ссылки и упоминания", "links_manage", "secondary"))
    builder.add(create_button("✅ Настройки подтверждения", "confirmation_manage", "success"))
    builder.add(create_button("❌ Отвязать группу", "unlink_group_confirm", "danger"))
    builder.add(create_button("◀️ Назад", "back_to_groups", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard(has_rules: bool, rules_enabled: bool):
    """Управление правилами - опасные действия красные"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Установить правила", "set_rules", "success"))
    builder.add(create_button("📋 Готовые правила", "set_default_rules", "primary"))
    
    if has_rules:
        builder.add(create_button("👁 Посмотреть", "show_rules", "secondary"))
        builder.add(create_button("✏️ Редактировать", "edit_rules", "secondary"))
        builder.add(create_button("🗑 Удалить", "delete_rules_confirm", "danger"))
        
        status_text = "✅ Включить" if not rules_enabled else "❌ Выключить"
        status_color = "success" if not rules_enabled else "danger"
        builder.add(create_button(status_text, "toggle_rules", status_color))
    
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled: bool = False):
    builder = InlineKeyboardBuilder()
    status = "✅ Включено" if enabled else "❌ Выключено"
    toggle_color = "danger" if enabled else "success"
    builder.add(create_button(f"{'Выключить' if enabled else 'Включить'}", "toggle_welcome", toggle_color))
    builder.add(create_button("📝 Установить текст", "set_welcome_text", "primary"))
    builder.add(create_button("🖼 Установить фото", "set_welcome_photo", "primary"))
    builder.add(create_button("👁 Посмотреть", "show_welcome", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled: bool):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if enabled else "success"
    builder.add(create_button(f"{'Выключить' if enabled else 'Включить'}", "toggle_rules_auto", toggle_color))
    builder.add(create_button("⏱ Интервал", "set_interval", "primary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings: dict):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(f"{'Выключить' if settings['enabled'] else 'Включить'}", "toggle_antiflood", toggle_color))
    builder.add(create_button(f"📊 Лимит: {settings['msg_limit']}", "set_limit", "secondary"))
    builder.add(create_button(f"⏱ Окно: {settings['time_window']} сек", "set_window", "secondary"))
    builder.add(create_button(f"⚠️ Предупреждений: {settings['warn_count']}", "set_warn_count", "secondary"))
    builder.add(create_button(f"🔇 Первое наказание", "set_first_punish", "primary"))
    builder.add(create_button(f"🔊 Повторное наказание", "set_repeat_punish", "primary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(is_first: bool = True):
    prefix = "first" if is_first else "repeat"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Предупреждение", f"punish_warn_{prefix}", "secondary"))
    builder.add(create_button("🔇 Мут", f"punish_mute_{prefix}", "primary"))
    builder.add(create_button("👢 Кик", f"punish_kick_{prefix}", "danger"))
    builder.add(create_button("⛔️ Бан", f"punish_ban_{prefix}", "danger"))
    builder.add(create_button("◀️ Назад", "antiflood_manage", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_welcome_buttons(chat_id: int):
    """Публичные кнопки в приветствии - их может нажимать кто угодно"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📜 Правила", f"show_group_rules_{chat_id}", "primary"))
    builder.add(create_button("📊 Моя статистика", f"my_stats_{chat_id}", "secondary"))
    builder.add(create_button("🏆 Топ активных", f"top_active_{chat_id}", "success"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id: int, user_id: int, msg_id: int):
    """Кнопка подтверждения - зеленая"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}", "success"))
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id: int, user_id: int, msg_id: int):
    """Кнопка согласия с правилами - зеленая"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Согласен с правилами", f"agree_rules_{chat_id}_{user_id}_{msg_id}", "success"))
    return builder.as_markup()

def get_link_group_keyboard(chat_id: int):
    """Кнопки привязки группы - привязать зеленым, отмена серым"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Привязать группу", f"link_group_{chat_id}", "success"))
    builder.add(create_button("🚫 Отмена", "cancel_link", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id: int):
    """Подтверждение отвязки - опасное действие красным"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("❌ Отвязать группу", f"unlink_group_{chat_id}", "danger"))
    builder.add(create_button("🚫 Отмена", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_pm_link_keyboard():
    """Кнопка перехода в ЛС - синяя"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📱 Перейти в ЛС", "go_to_pm", "primary"))
    return builder.as_markup()

def get_report_group_keyboard(groups: List[Tuple[int, str]]):
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"set_report_group_{chat_id}", "secondary"))
    builder.add(create_button("❌ Удалить", "remove_report_group", "danger"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_keyboard(responses: List[Tuple[str, str]]):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить триггер", "add_auto_trigger", "success"))
    if responses:
        builder.add(create_button("🗑 Удалить триггер", "remove_auto_trigger", "danger"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_remove_keyboard(responses: List[Tuple[str, str]]):
    builder = InlineKeyboardBuilder()
    for trigger, _ in responses:
        builder.add(create_button(trigger, f"remove_trigger_{trigger}", "danger"))
    builder.add(create_button("◀️ Назад", "auto_response_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_manage_keyboard(settings: dict):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if settings['links_enabled'] else "success"
    builder.add(create_button(f"{'Выключить' if settings['links_enabled'] else 'Включить'}", "toggle_links", toggle_color))
    builder.add(create_button("Установить наказание", "set_links_punish", "primary"))
    builder.add(create_button("Макс упоминаний", "set_max_mentions", "secondary"))
    builder.add(create_button("Окно упоминаний", "set_mention_window", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_punish_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Предупреждение", "links_punish_warn", "secondary"))
    builder.add(create_button("🔇 Мут", "links_punish_mute", "primary"))
    builder.add(create_button("👢 Кик", "links_punish_kick", "danger"))
    builder.add(create_button("⛔️ Бан", "links_punish_ban", "danger"))
    builder.add(create_button("◀️ Назад", "links_manage", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirmation_keyboard(current_type: str, has_rules: bool):
    builder = InlineKeyboardBuilder()
    
    # Отключено - серый
    disabled_text = "🚫 Отключено"
    if current_type == 'disabled':
        disabled_text += " ✅"
    builder.add(create_button(disabled_text, "confirmation_disabled", "secondary"))
    
    # Только не бот - синий
    not_bot_text = "🤖 Только не бот"
    if current_type == 'not_bot':
        not_bot_text += " ✅"
    builder.add(create_button(not_bot_text, "confirmation_not_bot", "primary"))
    
    # Только правила - зеленый (если есть правила)
    rules_text = "📜 Только правила"
    if not has_rules:
        rules_text = "❌ " + rules_text
    elif current_type == 'rules':
        rules_text += " ✅"
    color = "success" if has_rules else "secondary"
    builder.add(create_button(rules_text, "confirmation_rules" if has_rules else "confirmation_disabled", color))
    
    # Оба шага - зеленый (если есть правила)
    both_text = "2️⃣ Оба шага"
    if not has_rules:
        both_text = "❌ " + both_text
    elif current_type == 'both':
        both_text += " ✅"
    color = "success" if has_rules else "secondary"
    builder.add(create_button(both_text, "confirmation_both" if has_rules else "confirmation_disabled", color))
    
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
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

        if await is_admin(chat_id, user.id):
            return await handler(event, data)

        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            return await handler(event, data)

        settings = db.get_antiflood_settings(chat_id)
        
        if settings['enabled']:
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
                    await self.apply_punishment(chat_id, user, punish_type, duration, "Флуд", event)
                
                flood_control[key].clear()
                return

        if settings['links_enabled'] and event.text:
            text = event.text.lower()
            has_external_link = False
            mention_count = 0
            
            if event.entities:
                for entity in event.entities:
                    if entity.type == 'url':
                        url = event.text[entity.offset:entity.offset + entity.length]
                        allowed_domains = ['t.me', 'telegram.me', 'youtube.com', 'youtu.be']
                        if not any(domain in url for domain in allowed_domains):
                            has_external_link = True
                            break
                    
                    if entity.type in ('mention', 'text_mention'):
                        mention_count += 1
            
            if has_external_link:
                await self.apply_punishment(chat_id, user, settings['links_punish'], settings['links_duration'], "Внешняя ссылка", event)
                return
            
            if mention_count > settings['max_mentions']:
                await self.apply_punishment(chat_id, user, settings['links_punish'], settings['links_duration'], f"Слишком много упоминаний ({mention_count})", event)
                return

        return await handler(event, data)
    
    async def apply_punishment(self, chat_id: int, user: types.User, punish_type: str, duration: int, reason: str, event: Message):
        now = time.time()
        message_link = get_message_link(chat_id, event.message_id)
        
        db.log_violation(chat_id, user.id, user.full_name, reason, punish_type, event.message_id, message_link)
        
        report_group_id = db.get_report_group(chat_id)
        if report_group_id:
            try:
                report_text = (
                    f"<b>🚫 Отчёт о нарушении</b>\n\n"
                    f"<b>Пользователь:</b> {user.full_name} (@{user.username})\n"
                    f"<b>ID:</b> <code>{user.id}</code>\n"
                    f"<b>Причина:</b> {reason}\n"
                    f"<b>Наказание:</b> {punish_type}\n"
                    f"<b>Время:</b> {format_datetime(int(now))}\n\n"
                    f"<a href='{message_link}'>🔗 Перейти к сообщению</a>"
                )
                await bot.send_message(report_group_id, report_text, parse_mode="HTML")
            except:
                pass
        
        try:
            if punish_type == 'warn':
                await event.reply(f"⚠️ {user.full_name}, {reason}")
            elif punish_type == 'mute':
                permissions = ChatPermissions(can_send_messages=False)
                until = int(now + duration) if duration > 0 else None
                await bot.restrict_chat_member(chat_id, user.id, permissions=permissions, until_date=until)
                await event.reply(f"🔇 {user.full_name} замьючен на {duration // 60} мин")
            elif punish_type == 'kick':
                await bot.ban_chat_member(chat_id, user.id)
                await bot.unban_chat_member(chat_id, user.id)
                await event.reply(f"👢 {user.full_name} кикнут: {reason}")
            elif punish_type == 'ban':
                until = int(now + duration) if duration > 0 else None
                await bot.ban_chat_member(chat_id, user.id, until_date=until)
                await event.reply(f"⛔️ {user.full_name} забанен на {duration // 60} мин")
        except Exception as e:
            logger.warning(f"Ошибка наказания в {chat_id}: {e}")

# Фоновые задачи
async def reset_periodic_counters():
    global stats_updating
    while True:
        now = datetime.now(SERVER_TZ)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = now - timedelta(days=now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
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
                    logger.info("Счетчики статистики сброшены")
            except Exception as e:
                logger.error(f"Ошибка сброса счетчиков: {e}")
            stats_updating = False
        await asyncio.sleep(3600)

async def rules_broadcast_task():
    while True:
        try:
            with db.get_connection() as conn:
                c = conn.cursor()
                c.execute('''SELECT chat_id, rules_auto_enabled, rules_interval, 
                                   last_rules_time, rules_html 
                            FROM group_rules 
                            WHERE rules_auto_enabled = 1 AND rules_html IS NOT NULL''')
                
                for chat_id, enabled, interval, last_time, rules_html in c.fetchall():
                    current_time = int(time.time())
                    if last_time and current_time - last_time < interval:
                        continue
                    
                    try:
                        msg = await bot.send_message(
                            chat_id,
                            f"<b>📢 Напоминание правил чата</b>\n\n{rules_html}",
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

# ========== КОМАНДЫ ==========

# Команда /start в ЛС
@dp.message(CommandStart())
async def cmd_start_pm(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        # Если команда в группе - показываем информацию о группах
        await cmd_start_group(message)
        return
    
    await state.clear()
    
    # Сохраняем владельца сообщения для проверки кнопок
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    
    text = (
        "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\n"
        "Я - умный менеджер для ваших чатов. Помогаю следить за порядком, "
        "наказываю нарушителей и автоматизирую модерацию.\n\n"
        "🔹 <b>Мои возможности:</b>\n"
        "• Установка и автоматическая рассылка правил\n"
        "• Блокировка запрещенных слов\n"
        "• Распознаю слова даже с подменой букв\n"
        "• Сохраняю всё форматирование, спойлеры и цитаты\n"
        "• Автоматические наказания (мут/бан/кик)\n\n"
        "Выберите интересующий раздел в меню ниже 👇"
    )
    await message.answer(text, reply_markup=get_main_keyboard())

# Команда /start в группе
async def cmd_start_group(message: Message):
    chat_id = message.chat.id
    text = (
        f"👋 <b>Puls Chat Manager</b>\n\n"
        f"Главное меню\n\n"
        f"• /rules - Правила\n"
        f"• /stats - Моя статистика\n"
        f"• /top - Топ активных\n"
        f"• /profile - Профиль пользователя\n"
        f"• /group - Управление группой\n"
        f"• /puls - Проверка пинга"
    )
    await message.reply(text, parse_mode="HTML")

# Команда /puls и /startpuls
@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["пульс", "понг"]))
async def cmd_ping(message: Message):
    start_time = time.time()
    msg = await message.reply("⏳ ...")
    end_time = time.time()
    ping = round((end_time - start_time) * 1000)
    response_time = round(end_time - start_time, 2)
    
    await msg.edit_text(f"Пинг: {ping} мс\nВремя ответа: {response_time} сек", parse_mode="HTML")

# Команда /stats
@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    global stats_updating
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(get_text('stats_updating'))
        return
    
    chat_id = message.chat.id
    user = message.from_user
    
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    
    if not stat:
        text = get_text('stats_empty')
    else:
        join_dt = format_datetime(stat['join_date'])
        last_dt = format_datetime(stat['last_active'])
        position = db.get_user_position(chat_id, user.id, 'all')
        
        text = (
            f"<b>Профиль {user.full_name}</b>\n\n"
            f"<b>ID пользователя:</b> <code>{global_user['global_id']}</code>\n"
            f"<b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"• За день: {stat['day_messages']} сообщ.\n"
            f"• За неделю: {stat['week_messages']} сообщ.\n"
            f"• За месяц: {stat['month_messages']} сообщ.\n"
            f"• Всего: {stat['all_messages']} сообщ.\n"
            f"• Место в топе: {position}\n"
            f"• Вошёл: {join_dt}\n"
            f"• Последняя активность: {last_dt}"
        )
    
    await message.reply(text, parse_mode="HTML")

# Команда /top
@dp.message(Command("top"))
async def cmd_top(message: Message):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    global stats_updating
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(get_text('stats_updating'))
        return
    
    chat_id = message.chat.id
    
    top = db.get_top_messages(chat_id, period='all', limit=10)
    
    if not top:
        text = get_text('stats_empty')
    else:
        text = f"<b>🏆 Топ активных (всего сообщений):</b>\n\n"
        for i, (user_id, count) in enumerate(top, 1):
            try:
                user = await bot.get_chat_member(chat_id, user_id)
                name = user.user.full_name
            except:
                name = f"ID {user_id}"
            text += f"{i}. {name} — {count} сообщ.\n"
    
    await message.reply(text, parse_mode="HTML")

# Команда /profile
@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    global stats_updating
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(get_text('stats_updating'))
        return
    
    chat_id = message.chat.id
    
    if not message.reply_to_message:
        await message.reply("Ответьте на сообщение пользователя командой /profile чтобы увидеть его статистику")
        return
    
    target_user = message.reply_to_message.from_user
    
    global_user = db.get_global_user(target_user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(target_user.id, target_user.username or "", target_user.full_name or "")
        global_user = db.get_global_user(target_user.id)
    
    stat = db.get_user_stat(chat_id, target_user.id)
    
    if not stat:
        text = get_text('stats_empty')
    else:
        join_dt = format_datetime(stat['join_date'])
        last_dt = format_datetime(stat['last_active'])
        position = db.get_user_position(chat_id, target_user.id, 'all')
        
        text = (
            f"<b>Профиль {target_user.full_name}</b>\n\n"
            f"<b>ID пользователя:</b> <code>{global_user['global_id']}</code>\n"
            f"<b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"• За день: {stat['day_messages']} сообщ.\n"
            f"• За неделю: {stat['week_messages']} сообщ.\n"
            f"• За месяц: {stat['month_messages']} сообщ.\n"
            f"• Всего: {stat['all_messages']} сообщ.\n"
            f"• Место в топе: {position}\n"
            f"• Вошёл: {join_dt}\n"
            f"• Последняя активность: {last_dt}"
        )
    
    await message.reply(text, parse_mode="HTML")

# Команда /rules
@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    chat_id = message.chat.id
    
    rules_html = db.get_rules_html(chat_id)
    if rules_html and db.get_rules_enabled(chat_id):
        await message.reply(f"<b>📢 Напоминание правил чата</b>\n\n{rules_html}", parse_mode="HTML")
    else:
        await message.answer("❌ В этом чате еще не установлены правила.")

# Команда /group
@dp.message(Command("group"))
@dp.message(Command("manage"))
async def cmd_group(message: Message):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может настраивать бота!")
        return
    
    owner_id = None
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        owner_id = result[0] if result else None
    
    if owner_id == user_id:
        await message.answer(
            "✅ Группа уже привязана к вашему аккаунту!\n\n"
            "Настраивать группу можно только в личных сообщениях с ботом.\n"
            "Перейдите в ЛС и выберите группу в меню.",
            reply_markup=get_pm_link_keyboard()
        )
    else:
        text = (
            "❌ Группа еще не привязана к вашему аккаунту.\n\n"
            "Хотите привязать эту группу?\n\n"
            "После привязки вы сможете настраивать бота в личных сообщениях."
        )
        await message.answer(
            text,
            reply_markup=get_link_group_keyboard(chat_id)
        )

# Команда /adminstats
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
        await callback.answer("❌ Вы не являетесь создателем этой группы!", show_alert=True)
        return
    
    try:
        chat = await bot.get_chat(chat_id)
        chat_title = chat.title
        chat_username = chat.username
    except:
        chat_title = "Группа"
        chat_username = None
    
    db.save_rules(
        chat_id=chat_id,
        owner_id=user_id,
        chat_title=chat_title,
        chat_username=chat_username
    )
    
    await callback.message.edit_text("✅ Группа успешно привязана! Теперь вы можете настроить её в ЛС.")
    await callback.answer("✅ Группа привязана!")
    
    try:
        await bot.send_message(
            user_id,
            f"✅ Группа <b>{chat_title}</b> успешно привязана!\n\n"
            f"Теперь вы можете настроить её, выбрав в меню групп.",
            reply_markup=get_main_keyboard()
        )
    except:
        pass

@dp.callback_query(F.data == "cancel_link")
@check_owner()
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

# ========== ОБРАБОТЧИКИ ВХОДА/ВЫХОДА ==========

@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id = update.chat.id
        user = update.new_chat_member.user
        
        db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        db.add_user_stat(chat_id, user.id, int(time.time()))
        
        owner_id = None
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            owner_id = result[0] if result else None
        
        if not owner_id:
            await send_simple_welcome(chat_id, user)
            return
        
        conf_type = db.get_confirmation_type(chat_id)
        
        if conf_type == 'disabled':
            await send_simple_welcome(chat_id, user)
            return
        
        not_bot_confirmed, rules_confirmed = db.get_user_confirmation_status(chat_id, user.id)
        
        if conf_type == 'both':
            if not_bot_confirmed and rules_confirmed:
                await send_simple_welcome(chat_id, user)
                return
        elif conf_type == 'not_bot':
            if not_bot_confirmed:
                await send_simple_welcome(chat_id, user)
                return
        elif conf_type == 'rules':
            if rules_confirmed:
                await send_simple_welcome(chat_id, user)
                return

        try:
            await bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user.id,
                permissions=types.ChatPermissions(can_send_messages=False)
            )
        except Exception as e:
            logger.warning(f"Не удалось замутить {user.id}: {e}")

        rules_html = db.get_rules_html(chat_id)
        rules_enabled = db.get_rules_enabled(chat_id)
        builder = InlineKeyboardBuilder()
        msg_text = ""

        if conf_type == 'both':
            msg_text = (
                f"👋 <b>{user.full_name}</b> зашёл в чат!\n\n"
                f"Вам нужно выполнить ДВА шага:\n1. Подтвердить, что вы не бот\n2. Прочитать и согласиться с правилами"
            )
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {update.chat.title}!\n\n"
                    "Шаг 1: Подтвердите, что вы не бот",
                    reply_markup=get_confirm_not_bot_keyboard(chat_id, user.id, 0)
                )
                
                if rules_html and rules_enabled:
                    await bot.send_message(
                        user.id,
                        f"Шаг 2: Прочитайте и согласитесь с правилами:\n\n{rules_html}",
                        reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0),
                        parse_mode="HTML"
                    )
            except Exception as e:
                logger.warning(f"Не удалось отправить подтверждение в ЛС {user.id}: {e}")
                await bot.send_message(chat_id, f"Не удалось отправить подтверждение {user.full_name} в ЛС. Пожалуйста, откройте ЛС с ботом.")
            
            builder.add(create_button("📜 Перейти в ЛС", f"go_to_pm_{chat_id}_{user.id}", "primary"))
            
        elif conf_type == 'not_bot':
            msg_text = (
                f"👋 <b>{user.full_name}</b> зашёл в чат!\n\n"
                f"Вы замьючены **навсегда**, пока не подтвердите, что вы не бот.\nНажмите кнопку ниже — мут снимется."
            )
            builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user.id}_0", "success"))
        elif conf_type == 'rules' and rules_html and rules_enabled:
            msg_text = (
                f"👋 <b>{user.full_name}</b> зашёл в чат!\n\n"
                f"Вы замьючены **навсегда**, пока не подтвердите правила.\nПерейдите в ЛС бота, прочитайте правила и подтвердите согласие — мут снимется."
            )
            builder.add(create_button("📜 Перейти в ЛС", f"go_to_pm_{chat_id}_{user.id}", "primary"))
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {update.chat.title}!\n\n"
                    "Пожалуйста, прочитайте правила ниже и подтвердите согласие.\n"
                    "Без этого вы не сможете писать в чат.\n\n"
                    f"<b>Правила:</b>\n\n{rules_html}",
                    reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить правила в ЛС {user.id}: {e}")
                await bot.send_message(chat_id, f"Не удалось отправить правила {user.full_name} в ЛС. Пожалуйста, откройте ЛС с ботом.")
        else:
            await send_simple_welcome(chat_id, user)
            return
        
        if msg_text:
            await bot.send_message(chat_id, msg_text, reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    chat_id = update.chat.id
    user = update.from_user
    db.set_left_chat(chat_id, user.id)
    
    await bot.send_message(chat_id, f"👋 Пользователь {user.full_name} вышел из чата.")

# Приветствие
async def send_simple_welcome(chat_id: int, user: types.User):
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    join_dt = format_datetime(stat['join_date']) if stat else format_datetime(time.time())
    position = db.get_user_position(chat_id, user.id, 'all')
    
    text = (
        f"Добро пожаловать, <b>{user.full_name}</b>!\n\n"
        f"<b>ID пользователя:</b> <code>{global_user['global_id']}</code>\n"
        f"<b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
        f"• Username: @{user.username or 'нет'}\n"
        f"• ID: <code>{user.id}</code>\n"
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

# ========== ОБРАБОТЧИКИ ПОДТВЕРЖДЕНИЯ ==========

@dp.callback_query(F.data.startswith("confirm_not_bot_"))
@check_public()  # Публичная кнопка - может нажимать только тот, кто зашел
async def process_confirm_not_bot(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id = int(parts[3])
    user_id = int(parts[4])
    msg_id = int(parts[5]) if len(parts) > 5 else 0

    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не ваше подтверждение!", show_alert=True)
        return

    db.mark_user_confirmed(chat_id, user_id, not_bot=True, rules=False)
    
    conf_type = db.get_confirmation_type(chat_id)
    not_bot_confirmed, rules_confirmed = db.get_user_confirmation_status(chat_id, user_id)
    
    if conf_type == 'both' and not rules_confirmed:
        await callback.message.edit_text("✅ Шаг 1 выполнен! Теперь выполните шаг 2: согласитесь с правилами.")
        await callback.answer("Шаг 1 выполнен!")
        return

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
                text=f"✅ {callback.from_user.full_name} подтвердил, что не бот и теперь может писать в чат.",
                parse_mode="HTML"
            )
        except:
            pass

    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("Спасибо за подтверждение! Теперь вы можете писать в чат.")
    await callback.answer("✅")

@dp.callback_query(F.data.startswith("agree_rules_"))
@check_public()  # Публичная кнопка - может нажимать только тот, кто зашел
async def process_agree_rules(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id = int(parts[2])
    user_id = int(parts[3])
    msg_id = int(parts[4])

    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не ваше подтверждение!", show_alert=True)
        return

    db.mark_user_confirmed(chat_id, user_id, not_bot=False, rules=True)
    
    conf_type = db.get_confirmation_type(chat_id)
    not_bot_confirmed, rules_confirmed = db.get_user_confirmation_status(chat_id, user_id)
    
    if conf_type == 'both' and not not_bot_confirmed:
        await callback.message.edit_text("✅ Шаг 2 выполнен! Теперь выполните шаг 1: подтвердите, что вы не бот.")
        await callback.answer("Шаг 2 выполнен!")
        return

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
                text=f"✅ {callback.from_user.full_name} согласился с правилами и теперь может писать в чат.",
                parse_mode="HTML"
            )
        except:
            pass

    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("Спасибо за подтверждение! Теперь вы можете писать в чат.")
    await callback.answer("✅")

# Обработчик для кнопок "Перейти в ЛС" из приветствия
@dp.callback_query(F.data.startswith("go_to_pm_"))
@check_public()
async def go_to_pm(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id = int(parts[3])
    user_id = int(parts[4])
    
    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не для вас!", show_alert=True)
        return
    
    await callback.message.answer(
        "📱 Откройте личные сообщения с ботом и завершите подтверждение.",
        reply_markup=get_pm_link_keyboard()
    )
    await callback.answer()

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    text = message.text or message.caption or ""
    
    # АВТООТВЕТЧИК
    if text:
        text_lower = text.lower()
        responses = db.get_auto_responses(chat_id)
        for trigger, response in responses:
            if trigger.lower() in text_lower:
                try:
                    await message.reply(response, parse_mode="HTML", disable_notification=True)
                    break
                except Exception as e:
                    logger.warning(f"Ошибка автоответчика в {chat_id}: {e}")
                break
    
    # СТАТИСТИКА
    conf_type = db.get_confirmation_type(chat_id)
    if db.has_user_confirmed(chat_id, user_id, conf_type):
        db.update_message_count(chat_id, user_id)

# ========== ОБРАБОТЧИКИ НАСТРОЕК В ЛС ==========

@dp.callback_query(F.data == "back_to_main")
@check_owner()
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "👋 <b>Главное меню:</b>\n\n"
        "Выберите интересующий раздел в меню ниже 👇",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "group_manage_main")
@check_owner()
async def group_manage_main(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    groups = db.get_user_groups(user_id)
    
    if not groups:
        await callback.answer("❌ У вас нет привязанных групп!", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📱 <b>Ваши группы:</b>\n\n"
        "Выберите группу для настройки:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("select_group_"))
@check_owner()
async def select_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    
    if not await is_creator(chat_id, callback.from_user.id):
        await callback.answer("❌ Вы не являетесь создателем этой группы!", show_alert=True)
        return
    
    await state.update_data(
        selected_chat_id=chat_id,
        f"msg_owner_{callback.message.message_id}": callback.from_user.id
    )
    
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

@dp.callback_query(F.data == "back_to_groups")
@check_owner()
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    groups = db.get_user_groups(user_id)
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📱 <b>Ваши группы:</b>\n\n"
        "Выберите группу для настройки:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

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
    
    status_text = "✅ Правила включены" if rules_enabled else "❌ Правила выключены"
    
    await callback.message.edit_text(
        f"<b>📝 Управление правилами</b>\n\n"
        f"{status_text}",
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
        "Вы можете использовать любое форматирование:\n"
        "• <b>Жирный</b> - &lt;b&gt;текст&lt;/b&gt;\n"
        "• <i>Курсив</i> - &lt;i&gt;текст&lt;/i&gt;\n"
        "• <tg-spoiler>Спойлер</tg-spoiler> - &lt;tg-spoiler&gt;текст&lt;/tg-spoiler&gt;\n"
        "• <blockquote>Цитата</blockquote> - &lt;blockquote&gt;текст&lt;/blockquote&gt;\n"
        "• <blockquote expandable>Свернутая цитата\nСтрока 2\nСтрока 3</blockquote> - &lt;blockquote expandable&gt;текст\nстроки&lt;/blockquote&gt;\n\n"
        "💡 <b>Важно:</b> Для свернутых цитат нужно минимум 2-3 строки внутри.",
        reply_markup=get_back_keyboard("manage_rules")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    rules_html = message.html_text.strip()
    
    if not rules_html or len(rules_html) < 10:
        await message.answer("❌ Правила слишком короткие! Отправьте более содержательный текст.")
        return
    
    db.save_rules(chat_id, rules_html=rules_html)
    db.set_rules_enabled(chat_id, True)
    
    await message.reply(
        "✅ <b>Правила успешно установлены!</b>\n\n"
        "В группе их можно посмотреть командой /rules",
        parse_mode="HTML"
    )
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
    
    await callback.answer("✅ Готовые правила успешно установлены!", show_alert=True)
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
            "❌ В этом чате еще не установлены правила.",
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    rules_html = message.html_text.strip()
    
    if not rules_html or len(rules_html) < 10:
        await message.answer("❌ Правила слишком короткие! Отправьте более содержательный текст.")
        return
    
    db.save_rules(chat_id, rules_html=rules_html)
    
    await message.reply(
        "✅ <b>Правила успешно обновлены!</b>",
        parse_mode="HTML"
    )
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
    builder.add(create_button("🚫 Отмена", "manage_rules", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        "❓ Вы уверены, что хотите удалить правила?",
        reply_markup=builder.as_markup()
    )
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
    
    await callback.answer("✅ Правила успешно удалены!", show_alert=True)
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
    
    new_status = "включено" if not current else "выключено"
    await callback.answer(f"✅ Приветствие {new_status}!", show_alert=True)
    
    await manage_welcome(callback, state)

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
        "<code>Добро пожаловать, {name}!</code>",
        reply_markup=get_back_keyboard("manage_welcome")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    welcome_text = message.html_text.strip()
    
    if not welcome_text:
        await message.answer("❌ Текст не может быть пустым!")
        return
    
    db.save_welcome(chat_id, welcome_text=welcome_text)
    
    await message.reply("✅ Текст приветствия сохранен!")
    await state.clear()

@dp.callback_query(F.data == "set_welcome_photo")
@check_owner()
async def set_welcome_photo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🖼 Отправьте фото для приветствия.\n\n"
        "Оно будет отправляться вместе с текстом.",
        reply_markup=get_back_keyboard("manage_welcome")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    photo_id = message.photo[-1].file_id
    
    db.save_welcome(chat_id, welcome_photo_id=photo_id)
    
    await message.reply("✅ Фото для приветствия сохранено!")
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
            "❌ Приветствие еще не настроено.",
            reply_markup=get_back_keyboard("manage_welcome")
        )
        await callback.answer()
        return
    
    await callback.message.delete()
    
    if photo_id:
        await callback.message.answer_photo(
            photo=photo_id,
            caption=f"👋 <b>Текущее приветствие:</b>\n\n{text}" if text else None,
            reply_markup=get_back_keyboard("manage_welcome"),
            parse_mode="HTML"
        )
    elif text:
        await callback.message.answer(
            f"👋 <b>Текущее приветствие:</b>\n\n{text}",
            reply_markup=get_back_keyboard("manage_welcome"),
            parse_mode="HTML"
        )
    
    await callback.answer()

@dp.callback_query(F.data == "rules_auto")
@check_owner()
async def rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    
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
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    new_enabled = not bool(enabled)
    
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
        "⏱ Введите интервал в минутах (от 5 до 525600):\n"
        "Примеры:\n"
        "• 60 = 1 час\n"
        "• 1440 = 1 день\n"
        "• 10080 = 1 неделя\n"
        "• 43200 = 1 месяц",
        reply_markup=get_back_keyboard("rules_auto")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    try:
        interval = int(message.text)
        if interval < 5 or interval > 525600:
            await message.answer("❌ Интервал должен быть от 5 до 525600 минут!")
            return
        
        interval_seconds = interval * 60
        enabled, _, _, _ = db.get_rules_auto_settings(chat_id)
        db.set_rules_auto_settings(chat_id, bool(enabled), interval_seconds)
        
        await message.reply(f"✅ Интервал установлен: {format_interval(interval_seconds)}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

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
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
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
        "📊 Введите лимит сообщений за интервал (от 3 до 20):",
        reply_markup=get_back_keyboard("antiflood_manage")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
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
        "⏱ Введите временное окно в секундах (от 5 до 300):",
        reply_markup=get_back_keyboard("antiflood_manage")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
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
        "⚠️ Введите количество предупреждений перед наказанием (от 1 до 5):",
        reply_markup=get_back_keyboard("antiflood_manage")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
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
@check_owner()
async def process_punish_type(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split('_')
    punish_type = parts[1]
    is_first = parts[2] == 'first'
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    if is_first:
        db.save_antiflood_settings(chat_id, first_punish=punish_type)
    else:
        db.save_antiflood_settings(chat_id, repeat_punish=punish_type)
    
    await callback.answer(f"✅ Наказание установлено: {punish_type}", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_first_duration")
@check_owner()
async def set_first_duration(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏱ Введите длительность наказания в секундах для первого нарушения (от 30 до 86400):",
        reply_markup=get_back_keyboard("antiflood_manage")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
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
        "⏱ Введите длительность наказания в секундах для повторных нарушений (от 60 до 604800):",
        reply_markup=get_back_keyboard("antiflood_manage")
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
        await message.answer("❌ Вы не являетесь создателем этой группы!")
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

@dp.callback_query(F.data == "set_report_group")
@check_owner()
async def set_report_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    groups = db.get_user_groups(user_id)
    
    if not groups:
        await callback.answer("❌ У вас нет привязанных групп!", show_alert=True)
        return
    
    current_report_group = db.get_report_group_name(chat_id)
    text = "Выберите группу, куда будут отправляться логи нарушений:"
    if current_report_group:
        text = f"📋 Текущая группа репортов: {current_report_group}\n\n" + text
    
    await callback.message.edit_text(
        text,
        reply_markup=get_report_group_keyboard(groups)
    )
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
    
    await callback.answer("✅ Группа репортов успешно установлена!", show_alert=True)
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

@dp.callback_query(F.data == "auto_response_manage")
@check_owner()
async def auto_response_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    responses = db.get_auto_responses(chat_id)
    
    if not responses:
        text = "🤖 Автоответчик пуст.\nДобавьте первое ключевое слово и ответ."
    else:
        text = "🤖 Автоответчик:\n\n"
        for trigger, resp in responses:
            text += f"• <code>{trigger}</code> → {resp[:50]}{'...' if len(resp) > 50 else ''}\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_auto_response_keyboard(responses),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "add_auto_trigger")
@check_owner()
async def add_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Введите ключевое слово (триггер):",
        reply_markup=get_back_keyboard("auto_response_manage")
    )
    await state.set_state(AutoResponseStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponseStates.waiting_for_trigger)
async def process_auto_trigger(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    trigger = message.text.strip()
    if not trigger:
        await message.answer("❌ Триггер не может быть пустым!")
        return
    
    await state.update_data(auto_trigger=trigger)
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    await message.reply(
        "Введите текст ответа:",
        reply_markup=get_back_keyboard("auto_response_manage")
    )
    await state.set_state(AutoResponseStates.waiting_for_response)

@dp.message(AutoResponseStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    response = message.html_text.strip()
    if not response:
        await message.answer("❌ Ответ не может быть пустым!")
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    trigger = data.get('auto_trigger')
    
    if not chat_id or not trigger:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    responses = db.get_auto_responses(chat_id)
    for t, _ in responses:
        if t.lower() == trigger.lower():
            await message.answer(f"❌ Триггер '{trigger}' уже существует!")
            await state.clear()
            return
    
    db.add_auto_response(chat_id, trigger, response)
    
    await message.reply(f"✅ Триггер '{trigger}' успешно добавлен!")
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
        "Выберите триггер для удаления:",
        reply_markup=get_auto_response_remove_keyboard(responses)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("remove_trigger_"))
@check_owner()
async def process_remove_trigger(callback: CallbackQuery, state: FSMContext):
    trigger = callback.data.replace("remove_trigger_", "")
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    db.remove_auto_response(chat_id, trigger)
    
    await callback.answer(f"✅ Триггер '{trigger}' успешно удалён!", show_alert=True)
    await auto_response_manage(callback, state)

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
        f"{'✅' if settings['links_enabled'] else '❌'} Фильтр ссылок\n"
        f"Наказание: {settings['links_punish']}\n"
        f"Макс упоминаний: {settings['max_mentions']} за {settings['mention_window']} сек\n\n"
        f"Выберите, что настроить:",
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
    
    status = "включён" if new_enabled else "выключен"
    await callback.answer(f"✅ Фильтр ссылок {status}!", show_alert=True)
    
    await links_manage(callback, state)

@dp.callback_query(F.data == "set_links_punish")
@check_owner()
async def set_links_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Выберите наказание:",
        reply_markup=get_links_punish_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("links_punish_"))
@check_owner()
async def process_links_punish(callback: CallbackQuery, state: FSMContext):
    punish = callback.data.split('_')[-1]
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    if punish in ['warn', 'kick']:
        db.save_antiflood_settings(chat_id, links_punish=punish)
        duration_text = "навсегда"
        await callback.message.edit_text(
            f"✅ <b>Настройки сохранены!</b>\n\nНаказание: {punish}\nДлительность: {duration_text}",
            reply_markup=get_back_keyboard("links_manage")
        )
    else:
        await state.update_data(links_punish=punish)
        await callback.message.edit_text(
            "Введите длительность в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("links_manage")
        )
        await state.set_state(LinksStates.waiting_for_duration)
    
    await callback.answer()

@dp.message(LinksStates.waiting_for_duration)
async def process_links_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    punish = data.get('links_punish')
    
    if not chat_id or not punish:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Введите положительное число или 0 (навсегда)")
            return
        
        duration_sec = minutes * 60
        db.save_antiflood_settings(chat_id, links_punish=punish, links_duration=duration_sec)
        
        duration_text = format_duration(minutes) if minutes > 0 else "навсегда"
        
        await message.reply(
            f"✅ <b>Настройки сохранены!</b>\n\nНаказание: {punish}\nДлительность: {duration_text}",
            reply_markup=get_back_keyboard("links_manage")
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "set_max_mentions")
@check_owner()
async def set_max_mentions(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Введите максимальное количество упоминаний в минуту:",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.set_state(LinksStates.waiting_for_max_mentions)
    await callback.answer()

@dp.message(LinksStates.waiting_for_max_mentions)
async def process_max_mentions(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    try:
        count = int(message.text)
        if count < 1 or count > 20:
            await message.answer("❌ Макс упоминаний должно быть от 1 до 20!")
            return
        
        db.save_antiflood_settings(chat_id, max_mentions=count)
        
        await message.reply(f"✅ Макс упоминаний установлено: {count}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "set_mention_window")
@check_owner()
async def set_mention_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Введите окно упоминаний в секундах:",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.set_state(LinksStates.waiting_for_mention_window)
    await callback.answer()

@dp.message(LinksStates.waiting_for_mention_window)
async def process_mention_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в личных сообщениях!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Вы не являетесь создателем этой группы!")
        await state.clear()
        return
    
    try:
        window = int(message.text)
        if window < 10 or window > 3600:
            await message.answer("❌ Окно упоминаний должно быть от 10 до 3600 секунд!")
            return
        
        db.save_antiflood_settings(chat_id, mention_window=window)
        
        await message.reply(f"✅ Окно упоминаний установлено: {window} сек")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

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
        'disabled': "🚫 Отключено",
        'not_bot': "🤖 Только не бот",
        'rules': "📜 Только правила",
        'both': "2️⃣ Оба шага"
    }
    
    warning_text = ""
    if not has_rules and conf_type in ['rules', 'both']:
        warning_text = "\n\n⚠️ <b>Внимание:</b> Правила не установлены. Этот вариант требует наличия правил."
    
    await callback.message.edit_text(
        f"✅ <b>Настройки подтверждения</b>\n\n"
        f"Тип подтверждения: {type_names.get(conf_type, conf_type)}"
        f"{warning_text}\n\n"
        f"Выберите тип подтверждения:",
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
    
    # Проверяем, есть ли правила в группе
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    
    # Если пытаются выбрать "Только правила" или "Оба шага", а правил нет
    if conf_type in ['rules', 'both'] and not has_rules:
        error_text = "❌ Нельзя выбрать 'Только правила', так как в группе не установлены правила!\nСначала установите правила через меню «Управление правилами»." if conf_type == 'rules' else "❌ Нельзя выбрать 'Оба шага', так как в группе не установлены правила!\nСначала установите правила через меню «Управление правилами»."
        await callback.answer(
            error_text,
            show_alert=True,
            cache_time=5
        )
        return
    
    # Если всё ок - сохраняем настройки
    db.set_confirmation_type(chat_id, conf_type)
    
    # Показываем сообщение в зависимости от выбранного типа
    type_names = {
        'disabled': "🚫 Отключено",
        'not_bot': "🤖 Только не бот",
        'rules': "📜 Только правила",
        'both': "2️⃣ Оба шага"
    }
    
    await callback.answer(f"✅ Настройки подтверждения обновлены! Текущий тип: {type_names.get(conf_type)}", show_alert=True)
    await confirmation_manage(callback, state)

@dp.callback_query(F.data == "unlink_group_confirm")
@check_owner()
async def unlink_group_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Вы уверены, что хотите отвязать группу?",
        reply_markup=get_unlink_confirm_keyboard(chat_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("unlink_group_"))
@check_owner()
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
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

@dp.callback_query(F.data == "about")
@check_public()  # Публичная кнопка
async def callback_about(callback: CallbackQuery, state: FSMContext):
    text = (
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "📌 <b>Что я умею:</b>\n"
        "• Автоматическая модерация новых участников\n"
        "• Подтверждение правил в ЛС\n"
        "• Статистика сообщений (день/неделя/месяц/всего)\n"
        "• Топ активных участников\n"
        "• Антифлуд с настраиваемыми наказаниями\n"
        "• Ссылки и упоминания фильтр\n"
        "• Автоответчик с ключевыми словами\n"
        "• Приветствие с фото/текстом\n"
        "• Авто-рассылка правил\n"
        "• Проверка пинга (/puls, /startpuls, пульс)\n\n"
        "👇 Нажмите «➕ Добавить в группу» чтобы пригласить меня в ваш чат"
    )
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "help")
@check_public()  # Публичная кнопка
async def callback_help(callback: CallbackQuery, state: FSMContext):
    text = (
        "🆘 <b>Помощь по Puls Chat Manager</b>\n\n"
        "🔹 <b>Команды в группе:</b>\n"
        "• /rules - Показать правила\n"
        "• /stats - Моя статистика\n"
        "• /top - Топ активных\n"
        "• /profile - Профиль пользователя (ответом)\n"
        "• /puls, /startpuls, пульс - Проверка пинга\n"
        "• /group - Управление группой (для создателя)\n\n"
        "🔹 <b>Как добавить бота в группу:</b>\n"
        "1. Нажмите «➕ Добавить в группу»\n"
        "2. Выберите чат\n"
        "3. Сделайте бота администратором\n"
        "4. В группе напишите /group и привяжите группу\n"
        "5. Настройте в ЛС через /start\n\n"
        "🔹 <b>Для новых участников:</b>\n"
        "• Бот автоматически мутит до подтверждения\n"
        "• Нужно подтвердить, что вы не бот\n"
        "• Мут снимается после подтверждения\n\n"
        "🔹 <b>Статистика:</b>\n"
        "• Сообщения считаются за день/неделю/месяц\n"
        "• Сброс происходит автоматически\n"
        "• Топ показывает самых активных"
    )
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

# Публичные кнопки для статистики и правил
@dp.callback_query(F.data.startswith("show_group_rules_"))
@check_public()  # Публичная кнопка
async def show_group_rules(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    rules_html = db.get_rules_html(chat_id)
    
    if rules_html:
        await callback.message.answer(
            f"📜 <b>Правила чата:</b>\n\n{rules_html}",
            parse_mode="HTML"
        )
    else:
        await callback.message.answer("❌ В этом чате еще не установлены правила.")
    
    await callback.answer()

@dp.callback_query(F.data.startswith("my_stats_"))
@check_public()  # Публичная кнопка
async def my_stats(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    global_user = db.get_global_user(user_id)
    if not global_user:
        global_id = db.get_or_create_global_user(user_id, callback.from_user.username or "", callback.from_user.full_name or "")
        global_user = db.get_global_user(user_id)
    
    stat = db.get_user_stat(chat_id, user_id)
    
    if not stat:
        text = get_text('stats_empty')
    else:
        join_dt = format_datetime(stat['join_date'])
        last_dt = format_datetime(stat['last_active'])
        position = db.get_user_position(chat_id, user_id, 'all')
        
        text = (
            f"<b>Ваш профиль</b>\n\n"
            f"<b>ID пользователя:</b> <code>{global_user['global_id']}</code>\n"
            f"<b>Впервые замечен:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"• За день: {stat['day_messages']} сообщ.\n"
            f"• За неделю: {stat['week_messages']} сообщ.\n"
            f"• За месяц: {stat['month_messages']} сообщ.\n"
            f"• Всего: {stat['all_messages']} сообщ.\n"
            f"• Место в топе: {position}\n"
            f"• Вошёл: {join_dt}\n"
            f"• Последняя активность: {last_dt}"
        )
    
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data.startswith("top_active_"))
@check_public()  # Публичная кнопка
async def top_active(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    
    top = db.get_top_messages(chat_id, period='all', limit=10)
    
    if not top:
        text = get_text('stats_empty')
    else:
        text = f"<b>🏆 Топ активных (всего сообщений):</b>\n\n"
        for i, (user_id, count) in enumerate(top, 1):
            try:
                user = await bot.get_chat_member(chat_id, user_id)
                name = user.user.full_name
            except:
                name = f"ID {user_id}"
            text += f"{i}. {name} — {count} сообщ.\n"
    
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

# Запуск бота
async def main():
    dp.message.middleware(AntiFloodMiddleware())
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
