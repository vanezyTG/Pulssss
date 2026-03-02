import asyncio
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
import sqlite3
from contextlib import contextmanager
from functools import wraps

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ChatPermissions
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
BOT_TOKEN = "8557190026:AAE1gxBApenpt8uKzhcuMz56lQAWAeMCqIk"  # Замените на ваш токен
ADMIN_IDS = [6708209142]  # Замените на ID администраторов бота
MAX_MUTE_DAYS = 36500  # 100 лет в днях
MAX_BAN_DAYS = 36500   # 100 лет в днях

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Словарь для замены похожих букв (кириллица + латиница)
SIMILAR_CHARS = {
    'а': ['a', 'а', '@'],  # a латинская, а русская, @
    'б': ['6', 'b', 'б'],
    'в': ['b', 'в', '8'],
    'г': ['r', 'г'],
    'д': ['d', 'д'],
    'е': ['e', 'е', '3'],
    'ё': ['e', 'е', 'ё'],
    'ж': ['zh', 'ж'],
    'з': ['3', 'z', 'з'],
    'и': ['u', 'и'],
    'й': ['u', 'й', 'u'],
    'к': ['k', 'к'],
    'л': ['l', 'л'],
    'м': ['m', 'м'],
    'н': ['h', 'н'],
    'о': ['o', '0', 'о', '()'],
    'п': ['n', 'п'],
    'р': ['p', 'р'],
    'с': ['c', 'с', '$'],
    'т': ['t', 'т'],
    'у': ['y', 'у'],
    'ф': ['f', 'ф'],
    'х': ['x', 'х', '%'],
    'ц': ['c', 'ц'],
    'ч': ['ch', '4'],
    'ш': ['sh', 'ш'],
    'щ': ['sch', 'щ'],
    'ъ': ['b', 'ъ'],
    'ы': ['b', 'ы'],
    'ь': ['b', 'ь'],
    'э': ['e', 'э'],
    'ю': ['yu', 'ю'],
    'я': ['ya', 'я'],
    ' ': [' ', '_', '-', '.', ',', '!', '?', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=']
}

# Создаем обратный словарь для быстрого поиска
CHAR_VARIANTS = {}
for char, variants in SIMILAR_CHARS.items():
    for variant in variants:
        CHAR_VARIANTS[variant] = char

def normalize_text(text: str) -> str:
    """
    Нормализует текст, заменяя похожие буквы и символы
    Пример: "пpивет" -> "привет" (p заменяется на р)
    """
    if not text:
        return ""
    
    text = text.lower()
    result = []
    
    for char in text:
        if char in CHAR_VARIANTS:
            result.append(CHAR_VARIANTS[char])
        else:
            # Если символ не найден в словаре, оставляем как есть
            # Но убираем специальные символы, которые могут быть использованы для обхода
            if char.isalnum() or char in [' ', '_', '-']:
                result.append(char)
    
    return ''.join(result)

def text_contains_word(text: str, word: str) -> bool:
    """
    Проверяет, содержит ли текст заданное слово с учетом:
    - Регистра букв
    - Похожих букв (латиница/кириллица)
    - Специальных символов между буквами
    - Любого положения в тексте
    """
    if not text or not word:
        return False
    
    # Нормализуем оба текста
    normalized_text = normalize_text(text.lower())
    normalized_word = normalize_text(word.lower())
    
    if not normalized_word:
        return False
    
    # Простая проверка вхождения
    if normalized_word in normalized_text:
        return True
    
    # Проверка с учетом возможных разделителей между буквами
    # Например: "п р и в е т" или "п.р.и.в.е.т"
    text_without_spaces = re.sub(r'[^а-яa-z0-9]', '', normalized_text)
    word_without_spaces = re.sub(r'[^а-яa-z0-9]', '', normalized_word)
    
    if word_without_spaces and word_without_spaces in text_without_spaces:
        return True
    
    # Проверка на границы слов (чтобы "мат" не находилось в "математика")
    pattern = r'\b' + re.escape(normalized_word) + r'\b'
    if re.search(pattern, normalized_text):
        return True
    
    return False

# Классы состояний для FSM
class RulesStates(StatesGroup):
    waiting_for_rules_text = State()
    waiting_for_interval = State()

class StopWordsStates(StatesGroup):
    waiting_for_word = State()
    waiting_for_punishment = State()
    waiting_for_time = State()
    waiting_for_unit = State()

# Декоратор для проверки владельца кнопки
def check_owner():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id
            state: FSMContext = kwargs.get('state')
            
            if state:
                data = await state.get_data()
                message_owner = None
                
                for key in data:
                    if key.startswith('msg_owner_'):
                        if str(callback.message.message_id) in key:
                            message_owner = data[key]
                            break
                
                if message_owner and message_owner != user_id:
                    await callback.answer("⚠️ Эта кнопка не для вас!", show_alert=True)
                    return
            
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

# Декоратор для проверки прав создателя группы
def check_creator():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            if callback.message.chat.type == 'private':
                await callback.answer("❌ Эта функция работает только в группах!", show_alert=True)
                return
            
            try:
                member = await bot.get_chat_member(callback.message.chat.id, callback.from_user.id)
                if member.status != 'creator':
                    await callback.answer("❌ Только создатель группы может выполнять это действие!", show_alert=True)
                    return
            except Exception as e:
                logger.error(f"Ошибка проверки прав: {e}")
                await callback.answer("❌ Ошибка проверки прав!", show_alert=True)
                return
            
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

# Декоратор для проверки прав администратора бота
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

# Работа с базой данных
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
                          rules_text TEXT,
                          rules_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300,
                          last_rules_message_id INTEGER,
                          last_rules_time INTEGER,
                          chat_title TEXT,
                          chat_username TEXT)''')
            
            # Таблица для запрещенных слов
            c.execute('''CREATE TABLE IF NOT EXISTS banned_words
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          word TEXT,
                          punishment_type TEXT,
                          punishment_time INTEGER,
                          punishment_unit TEXT,
                          UNIQUE(chat_id, word))''')
            
            # Таблица для создателей групп
            c.execute('''CREATE TABLE IF NOT EXISTS group_creators
                         (chat_id INTEGER PRIMARY KEY,
                          creator_id INTEGER)''')
            
            # Таблица для статистики нарушений
            c.execute('''CREATE TABLE IF NOT EXISTS violations
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          user_id INTEGER,
                          user_name TEXT,
                          word TEXT,
                          punishment TEXT,
                          timestamp INTEGER)''')
            
            conn.commit()
    
    # Методы для правил
    def save_rules(self, chat_id: int, rules_text: str, chat_title: str = None, chat_username: str = None):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO group_rules 
                         (chat_id, rules_text, chat_title, chat_username) 
                         VALUES (?, ?, ?, ?)''', 
                         (chat_id, rules_text, chat_title, chat_username))
            conn.commit()
    
    def get_rules(self, chat_id: int) -> Optional[str]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_text FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    def set_rules_settings(self, chat_id: int, enabled: bool, interval: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_text FROM group_rules WHERE chat_id = ?', (chat_id,))
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
    
    # Методы для запрещенных слов
    def add_banned_word(self, chat_id: int, word: str, punishment_type: str, 
                       punishment_time: int, punishment_unit: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            try:
                c.execute('''INSERT INTO banned_words 
                             (chat_id, word, punishment_type, punishment_time, punishment_unit) 
                             VALUES (?, ?, ?, ?, ?)''', 
                             (chat_id, word.lower(), punishment_type, punishment_time, punishment_unit))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
    
    def remove_banned_word(self, chat_id: int, word: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM banned_words WHERE chat_id = ? AND word = ?', 
                     (chat_id, word.lower()))
            conn.commit()
            return c.rowcount > 0
    
    def get_banned_words(self, chat_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT word, punishment_type, punishment_time, punishment_unit 
                         FROM banned_words WHERE chat_id = ?''', (chat_id,))
            return c.fetchall()
    
    def check_banned_word(self, chat_id: int, text: str):
        """Проверяет текст на наличие запрещенных слов с учетом похожих букв"""
        if not text:
            return None
        
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT word, punishment_type, punishment_time, punishment_unit 
                         FROM banned_words WHERE chat_id = ?''', (chat_id,))
            
            for word, p_type, p_time, p_unit in c.fetchall():
                if text_contains_word(text, word):
                    return (word, p_type, p_time, p_unit)
            return None
    
    # Методы для создателей групп
    def save_creator(self, chat_id: int, creator_id: int):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO group_creators (chat_id, creator_id) 
                         VALUES (?, ?)''', (chat_id, creator_id))
            conn.commit()
    
    def get_creator(self, chat_id: int) -> Optional[int]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT creator_id FROM group_creators WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    # Методы для статистики нарушений
    def add_violation(self, chat_id: int, user_id: int, user_name: str, word: str, punishment: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO violations 
                         (chat_id, user_id, user_name, word, punishment, timestamp) 
                         VALUES (?, ?, ?, ?, ?, ?)''',
                         (chat_id, user_id, user_name, word, punishment, int(time.time())))
            conn.commit()
    
    # Методы для статистики админа
    def get_all_chats(self):
        """Получает список всех чатов, где есть бот"""
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT chat_id, chat_title, chat_username, rules_enabled,
                                (SELECT COUNT(*) FROM banned_words WHERE chat_id = group_rules.chat_id) as words_count
                         FROM group_rules 
                         ORDER BY chat_id''')
            return c.fetchall()
    
    def get_total_stats(self):
        """Получает общую статистику"""
        with self.get_connection() as conn:
            c = conn.cursor()
            
            # Всего групп
            c.execute('SELECT COUNT(DISTINCT chat_id) FROM group_rules')
            total_groups = c.fetchone()[0] or 0
            
            # Всего запрещенных слов
            c.execute('SELECT COUNT(*) FROM banned_words')
            total_words = c.fetchone()[0] or 0
            
            # Всего нарушений
            c.execute('SELECT COUNT(*) FROM violations')
            total_violations = c.fetchone()[0] or 0
            
            # Уникальных нарушителей
            c.execute('SELECT COUNT(DISTINCT user_id) FROM violations')
            unique_users = c.fetchone()[0] or 0
            
            # Последние 10 нарушений
            c.execute('''SELECT chat_id, user_name, word, punishment, timestamp 
                         FROM violations ORDER BY timestamp DESC LIMIT 10''')
            recent = c.fetchall()
            
            return {
                'total_groups': total_groups,
                'total_words': total_words,
                'total_violations': total_violations,
                'unique_users': unique_users,
                'recent': recent
            }

# Создаем экземпляр базы данных
db = Database()

# Вспомогательные функции
async def is_chat_admin(chat_id: int, user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором чата"""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ['creator', 'administrator']
    except:
        return False

async def is_creator(chat_id: int, user_id: int) -> bool:
    """Проверяет, является ли пользователь создателем чата"""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status == 'creator'
    except:
        return False

def format_interval(seconds: int) -> str:
    """Форматирует интервал в читаемый вид"""
    if seconds < 60:
        return f"{seconds} сек"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} мин"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} ч"
    else:
        days = seconds // 86400
        return f"{days} дн"

def get_main_keyboard():
    """Создает главное меню"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📋 О боте", callback_data="about")
    builder.button(text="🆘 Помощь", callback_data="help")
    builder.button(text="📜 Правила", callback_data="rules")
    builder.button(text="⚙️ Управление группой", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_keyboard():
    """Создает клавиатуру управления группой"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Установить правила", callback_data="set_rules")
    builder.button(text="🔄 Авто-рассылка правил", callback_data="rules_auto")
    builder.button(text="🚫 Запрещенные слова", callback_data="banned_words")
    builder.button(text="◀️ Назад", callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled: bool):
    """Создает клавиатуру для авто-рассылки правил"""
    builder = InlineKeyboardBuilder()
    status = "✅ Включено" if enabled else "❌ Выключено"
    builder.button(text=f"Статус: {status}", callback_data="toggle_rules")
    builder.button(text="⏱ Установить интервал", callback_data="set_interval")
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def extract_message_text(message: Message) -> str:
    """
    Извлекает полный текст сообщения, включая:
    - Обычный текст
    - Текст под спойлером
    - Текст в caption
    - Цитируемый текст
    """
    text_parts = []
    
    if message.text:
        text_parts.append(message.text)
    
    if message.caption:
        text_parts.append(message.caption)
    
    if message.reply_to_message:
        reply = message.reply_to_message
        if reply.text:
            text_parts.append(reply.text)
        if reply.caption:
            text_parts.append(reply.caption)
    
    entities = []
    if message.entities:
        entities.extend(message.entities)
    if message.caption_entities:
        entities.extend(message.caption_entities)
    
    for entity in entities:
        if entity.type == 'spoiler':
            if message.text:
                spoiler_text = message.text[entity.offset:entity.offset + entity.length]
                text_parts.append(spoiler_text)
            elif message.caption:
                spoiler_text = message.caption[entity.offset:entity.offset + entity.length]
                text_parts.append(spoiler_text)
    
    return " ".join(text_parts) if text_parts else ""

# Команды для администраторов бота
@dp.message(Command("adminstats"))
@check_bot_admin()
async def cmd_admin_stats(message: Message):
    """Статистика для администратора бота"""
    stats = db.get_total_stats()
    chats = db.get_all_chats()
    
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"📱 Всего групп: {stats['total_groups']}\n"
        f"🚫 Запрещенных слов: {stats['total_words']}\n"
        f"👮‍♂️ Всего нарушений: {stats['total_violations']}\n"
        f"👥 Уникальных нарушителей: {stats['unique_users']}\n\n"
    )
    
    if chats:
        text += "<b>📋 Список групп:</b>\n"
        for chat_id, title, username, enabled, words_count in chats:
            if username:
                link = f"https://t.me/{username}"
                chat_info = f"<a href='{link}'>{title or 'Без названия'}</a>"
            else:
                chat_info = f"{title or 'Без названия'} (частная)"
            
            status = "✅" if enabled else "❌"
            text += f"{status} {chat_info} | Слов: {words_count}\n"
    
    if stats['recent']:
        text += "\n<b>🕐 Последние нарушения:</b>\n"
        for chat_id, user_name, word, punishment, timestamp in stats['recent'][:5]:
            date = datetime.fromtimestamp(timestamp).strftime('%d.%m %H:%M')
            text += f"• {date} - {user_name}: {word} ({punishment})\n"
    
    await message.answer(text)

@dp.message(Command("adminchats"))
@check_bot_admin()
async def cmd_admin_chats(message: Message):
    """Показывает все чаты, где есть бот"""
    chats = db.get_all_chats()
    
    if not chats:
        await message.answer("❌ Бот еще не добавлен ни в одну группу.")
        return
    
    text = "📱 <b>Список групп с ботом:</b>\n\n"
    
    for chat_id, title, username, enabled, words_count in chats:
        if username:
            link = f"https://t.me/{username}"
            chat_info = f"• <a href='{link}'>{title or 'Без названия'}</a>"
        else:
            # Для частных групп создаем пригласительную ссылку
            try:
                invite_link = await bot.create_chat_invite_link(chat_id, member_limit=1)
                chat_info = f"• <a href='{invite_link.invite_link}'>{title or 'Приватная группа'}</a>"
            except:
                chat_info = f"• {title or 'Приватная группа'} (нет доступа)"
        
        status = "✅" if enabled else "❌"
        text += f"{status} {chat_info} | ID: <code>{chat_id}</code> | Слов: {words_count}\n"
    
    # Разбиваем на части, если сообщение слишком длинное
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await message.answer(text[i:i+4000])
    else:
        await message.answer(text)

# Обработчики команд
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    
    text = (
        "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\n"
        "Я - умный менеджер для ваших чатов. Помогаю следить за порядком, "
        "наказываю нарушителей и автоматизирую модерацию.\n\n"
        "🔹 <b>Мои возможности:</b>\n"
        "• Установка и автоматическая рассылка правил\n"
        "• Блокировка запрещенных слов\n"
        "• Распознаю слова даже с подменой букв (p -> р, 0 -> о и т.д.)\n"
        "• Автоматические наказания (мут/бан/кик)\n\n"
        "Выберите интересующий раздел в меню ниже 👇"
    )
    await message.answer(text, reply_markup=get_main_keyboard())

@dp.message(Command("startpuls"))
async def cmd_startpuls(message: Message, state: FSMContext):
    """Альтернативная команда старта"""
    await cmd_start(message, state)

@dp.message(Command("rules"))
@dp.message(Command("rulesgroup"))
@dp.message(F.text.lower() == "правила чата")
async def cmd_rules(message: Message):
    """Показывает правила чата"""
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    rules = db.get_rules(message.chat.id)
    if rules:
        await message.reply(f"<b>📜 Правила чата:</b>\n\n{rules}")
    else:
        await message.answer("❓ В этом чате еще не установлены правила.")

# Обработчики колбэков
@dp.callback_query(F.data == "about")
@check_owner()
async def callback_about(callback: CallbackQuery, state: FSMContext):
    """Информация о боте"""
    text = (
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "📌 <b>Что я умею:</b>\n"
        "• Автоматическая модерация\n"
        "• Борьба со спамом и запрещенными словами\n"
        "• Распознаю слова с подменой букв (p -> р, 0 -> о)\n"
        "• Проверяю спойлеры и цитаты\n"
        "• Гибкие настройки правил\n"
        "• Различные виды наказаний\n\n"
        "💡 Добавьте меня в группу и сделайте администратором,\n"
        "чтобы я мог полноценно работать!"
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "help")
@check_owner()
async def callback_help(callback: CallbackQuery, state: FSMContext):
    """Подробная помощь по боту"""
    text = (
        "🆘 <b>Помощь по Puls Chat Manager</b>\n\n"
        "🔹 <b>Основные команды:</b>\n"
        "• /start - Главное меню\n"
        "• /rules - Показать правила чата\n"
        "• /addstopword - Добавить запрещенное слово\n\n"
        
        "🔹 <b>Как добавить бота в группу:</b>\n"
        "1. Добавьте бота в группу\n"
        "2. Сделайте его администратором\n"
        "3. Настройте правила через меню\n\n"
        
        "🔹 <b>Запрещенные слова:</b>\n"
        "• Добавляются командой /addstopword\n"
        "• Можно выбрать наказание (мут/бан/кик)\n"
        "• Можно установить время наказания\n"
        "• Бот распознает даже с подменой букв\n"
        "• Пример: 'мат' сработает на 'м0т', 'м@т', 'pривет' и т.д.\n\n"
        
        "🔹 <b>Авто-рассылка правил:</b>\n"
        "• Бот автоматически отправляет правила\n"
        "• Закрепляет их в чате\n"
        "• Интервал от 5 минут до 1 года\n\n"
        
        "❓ Есть вопросы? Обратитесь к администратору бота."
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "rules")
@check_owner()
async def callback_rules(callback: CallbackQuery, state: FSMContext):
    """Показывает правила использования бота"""
    text = (
        "📋 <b>Правила использования бота:</b>\n\n"
        "1️⃣ Бот должен быть администратором в группе\n"
        "2️⃣ Для настройки используйте меню управления\n"
        "3️⃣ Все наказания записываются в лог\n"
        "4️⃣ Не злоупотребляйте правами бота\n"
        "5️⃣ Бот проверяет ВЕСЬ текст, включая спойлеры и цитаты\n"
        "6️⃣ Бот распознает слова даже с подменой букв\n\n"
        "⚠️ Бот не несет ответственности за неправильные настройки"
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "group_manage")
@check_owner()
@check_creator()
async def callback_group_manage(callback: CallbackQuery, state: FSMContext):
    """Управление группой"""
    await callback.message.edit_text(
        "⚙️ <b>Управление группой</b>\n\n"
        "Выберите действие:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
@check_owner()
async def callback_back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await callback.message.edit_text(
        "👋 Главное меню:",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "set_rules")
@check_owner()
@check_creator()
async def callback_set_rules(callback: CallbackQuery, state: FSMContext):
    """Установка правил"""
    await callback.message.edit_text(
        "📝 Отправьте текст правил для этого чата.\n"
        "Вы можете использовать форматирование (жирный, курсив, спойлеры и т.д.)\n\n"
        "✏️ Просто напишите сообщение с правилами в этот чат."
    )
    await state.set_state(RulesStates.waiting_for_rules_text)
    await callback.answer()

@dp.message(RulesStates.waiting_for_rules_text)
async def process_rules_text(message: Message, state: FSMContext):
    """Обработка текста правил"""
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        await state.clear()
        return
    
    if not await is_creator(message.chat.id, message.from_user.id):
        await message.answer("❌ Только создатель может устанавливать правила!")
        await state.clear()
        return
    
    rules_text = message.html_text if message.html_text else message.text
    
    if not rules_text:
        await message.answer("❌ Не удалось извлечь текст правил!")
        await state.clear()
        return
    
    # Получаем информацию о чате
    chat_title = message.chat.title
    chat_username = message.chat.username
    
    db.save_rules(message.chat.id, rules_text, chat_title, chat_username)
    db.save_creator(message.chat.id, message.from_user.id)
    
    await message.reply(
        "✅ Правила успешно сохранены!\n\n"
        "Используйте /rules чтобы их увидеть."
    )
    await state.clear()

@dp.callback_query(F.data == "rules_auto")
@check_owner()
@check_creator()
async def callback_rules_auto(callback: CallbackQuery, state: FSMContext):
    """Настройка авто-рассылки правил"""
    enabled, interval, _, _ = db.get_rules_settings(callback.message.chat.id)
    
    text = (
        "🔄 <b>Автоматическая рассылка правил</b>\n\n"
        f"Статус: {'✅ Включено' if enabled else '❌ Выключено'}\n"
        f"Интервал: {format_interval(interval)}\n\n"
        "Бот будет автоматически отправлять и закреплять правила "
        "с заданным интервалом."
    )
    
    await callback.message.edit_text(text, reply_markup=get_rules_auto_keyboard(bool(enabled)))
    await callback.answer()

@dp.callback_query(F.data == "toggle_rules")
@check_owner()
@check_creator()
async def callback_toggle_rules(callback: CallbackQuery, state: FSMContext):
    """Включение/выключение авто-рассылки"""
    enabled, interval, _, _ = db.get_rules_settings(callback.message.chat.id)
    new_enabled = not bool(enabled)
    
    db.set_rules_settings(callback.message.chat.id, new_enabled, interval)
    
    text = (
        "🔄 <b>Автоматическая рассылка правил</b>\n\n"
        f"Статус: {'✅ Включено' if new_enabled else '❌ Выключено'}\n"
        f"Интервал: {format_interval(interval)}\n\n"
        "Настройки обновлены!"
    )
    
    await callback.message.edit_text(text, reply_markup=get_rules_auto_keyboard(new_enabled))
    await callback.answer()

@dp.callback_query(F.data == "set_interval")
@check_owner()
@check_creator()
async def callback_set_interval(callback: CallbackQuery, state: FSMContext):
    """Установка интервала"""
    await callback.message.edit_text(
        "⏱ Введите интервал в минутах (от 5 минут до 525600 минут = 1 год):\n"
        "Например:\n"
        "• 60 (1 час)\n"
        "• 1440 (1 день)\n"
        "• 10080 (1 неделя)\n"
        "• 43200 (1 месяц)\n\n"
        "Просто напишите число в чат:"
    )
    await state.set_state(RulesStates.waiting_for_interval)
    await callback.answer()

@dp.message(RulesStates.waiting_for_interval)
async def process_interval(message: Message, state: FSMContext):
    """Обработка интервала"""
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        await state.clear()
        return
    
    if not await is_creator(message.chat.id, message.from_user.id):
        await message.answer("❌ Только создатель может изменять интервал!")
        await state.clear()
        return
    
    try:
        interval = int(message.text)
        if interval < 5 or interval > 525600:
            await message.answer("❌ Интервал должен быть от 5 до 525600 минут!")
            return
        
        interval_seconds = interval * 60
        enabled, _, _, _ = db.get_rules_settings(message.chat.id)
        db.set_rules_settings(message.chat.id, bool(enabled), interval_seconds)
        
        await message.reply(f"✅ Интервал установлен: {format_interval(interval_seconds)}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "banned_words")
@check_owner()
@check_creator()
async def callback_banned_words(callback: CallbackQuery, state: FSMContext):
    """Управление запрещенными словами"""
    words = db.get_banned_words(callback.message.chat.id)
    
    if words:
        text = "🚫 <b>Запрещенные слова:</b>\n\n"
        for i, (word, p_type, p_time, p_unit) in enumerate(words, 1):
            punishment = {
                'м': 'мут',
                'б': 'бан',
                'к': 'кик'
            }.get(p_type, 'неизвестно')
            
            time_str = f"{p_time} {p_unit}" if p_type != 'к' else "мгновенно"
            text += f"{i}. <b>{word}</b> - {punishment} на {time_str}\n"
            
            # Добавляем кнопку удаления для каждого слова
            builder = InlineKeyboardBuilder()
            builder.button(text=f"❌ Удалить {word}", callback_data=f"delword_{i}")
            builder.button(text="◀️ Назад", callback_data="group_manage")
            builder.adjust(1)
    else:
        text = "📝 Список запрещенных слов пуст.\n\n"
        builder = InlineKeyboardBuilder()
        builder.button(text="◀️ Назад", callback_data="group_manage")
    
    text += "\n➕ <b>Как добавить слово:</b>\n"
    text += "Используйте команду:\n"
    text += "<code>/addstopword слово</code>\n\n"
    text += "Пример: /addstopword мат\n\n"
    text += "⚠️ <b>Важно:</b> Бот распознает слова даже если:\n"
    text += "• Заменять буквы похожими (p->р, 0->о)\n"
    text += "• Писать большими или маленькими\n"
    text += "• Добавлять пробелы/точки между буквами\n"
    text += "• Прятать в спойлер или цитату"
    
    if words:
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

# Обработчик удаления слов
@dp.callback_query(F.data.startswith("delword_"))
@check_owner()
@check_creator()
async def callback_delete_word(callback: CallbackQuery, state: FSMContext):
    """Удаление запрещенного слова"""
    try:
        index = int(callback.data.split("_")[1])
        words = db.get_banned_words(callback.message.chat.id)
        
        if 1 <= index <= len(words):
            word_to_delete = words[index-1][0]
            db.remove_banned_word(callback.message.chat.id, word_to_delete)
            await callback.answer(f"✅ Слово '{word_to_delete}' удалено!", show_alert=True)
        else:
            await callback.answer("❌ Слово не найдено!", show_alert=True)
    except Exception as e:
        await callback.answer("❌ Ошибка при удалении!", show_alert=True)
    
    # Обновляем список
    await callback_banned_words(callback, state)

@dp.message(Command("addstopword"))
async def cmd_add_stopword(message: Message, state: FSMContext):
    """Добавление запрещенного слова"""
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    if not await is_creator(message.chat.id, message.from_user.id):
        await message.answer("❌ Только создатель может добавлять запрещенные слова!")
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "❌ Использование: /addstopword слово\n\n"
            "После этого бот запросит тип наказания и время.\n\n"
            "Пример: /addstopword мат"
        )
        return
    
    word = args[1].strip()
    if len(word) < 2 or len(word) > 30:
        await message.answer("❌ Слово должно быть от 2 до 30 символов!")
        return
    
    # Проверяем, не существует ли уже такое слово
    words = db.get_banned_words(message.chat.id)
    for w, _, _, _ in words:
        if normalize_text(w) == normalize_text(word):
            await message.answer(f"❌ Похожее слово '{w}' уже существует в списке!")
            return
    
    await state.update_data(
        word=word,
        msg_owner=f"word_{message.from_user.id}",
        **{f"msg_owner_{message.message_id}": message.from_user.id}
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔇 Мут", callback_data="punish_m")
    builder.button(text="⛔️ Бан", callback_data="punish_b")
    builder.button(text="👢 Кик", callback_data="punish_k")
    builder.adjust(1)
    
    await message.reply(
        f"Слово: <b>{word}</b>\n\n"
        "Выберите тип наказания:",
        reply_markup=builder.as_markup()
    )
    await state.set_state(StopWordsStates.waiting_for_punishment)

@dp.callback_query(StopWordsStates.waiting_for_punishment)
@check_owner()
async def process_punishment_type(callback: CallbackQuery, state: FSMContext):
    """Обработка типа наказания"""
    data = await state.get_data()
    if data.get('msg_owner') != f"word_{callback.from_user.id}":
        await callback.answer("⚠️ Эта кнопка не для вас!", show_alert=True)
        return
    
    p_type = callback.data.split('_')[1]
    await state.update_data(punishment_type=p_type)
    
    if p_type == 'к':
        data = await state.get_data()
        success = db.add_banned_word(
            callback.message.chat.id,
            data['word'],
            'к',
            0,
            'м'
        )
        
        if success:
            await callback.message.edit_text(
                f"✅ Слово <b>{data['word']}</b> добавлено!\n"
                f"Наказание: кик (мгновенно)\n\n"
                f"⚠️ Бот будет распознавать это слово даже с подменой букв!"
            )
        else:
            await callback.message.edit_text(
                f"❌ Слово <b>{data['word']}</b> уже существует в списке!"
            )
        
        await state.clear()
    else:
        await callback.message.edit_text(
            "Введите время наказания (цифрой):\n"
            "Минимум: 1\n"
            "Максимум: 36500 (100 лет)"
        )
        await state.set_state(StopWordsStates.waiting_for_time)
    
    await callback.answer()

@dp.message(StopWordsStates.waiting_for_time)
async def process_punishment_time(message: Message, state: FSMContext):
    """Обработка времени наказания"""
    data = await state.get_data()
    owner_id = int(data.get('msg_owner', '0').replace('word_', ''))
    if owner_id != message.from_user.id:
        await message.answer("❌ Эта команда не для вас!")
        return
    
    try:
        p_time = int(message.text)
        if p_time < 1:
            await message.answer("❌ Время должно быть не меньше 1!")
            return
        
        p_type = data.get('punishment_type')
        max_time = 36500 if p_type in ['м', 'б'] else 1
        
        if p_time > max_time:
            await message.answer(f"❌ Максимальное время: {max_time}!")
            return
        
        await state.update_data(punishment_time=p_time)
        
        builder = InlineKeyboardBuilder()
        builder.button(text="Минуты", callback_data="unit_m")
        builder.button(text="Часы", callback_data="unit_h")
        builder.button(text="Дни", callback_data="unit_d")
        builder.adjust(1)
        
        await message.reply(
            "Выберите единицу времени:",
            reply_markup=builder.as_markup()
        )
        await state.set_state(StopWordsStates.waiting_for_unit)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(StopWordsStates.waiting_for_unit)
@check_owner()
async def process_punishment_unit(callback: CallbackQuery, state: FSMContext):
    """Обработка единицы времени"""
    data = await state.get_data()
    if data.get('msg_owner') != f"word_{callback.from_user.id}":
        await callback.answer("⚠️ Эта кнопка не для вас!", show_alert=True)
        return
    
    unit = callback.data.split('_')[1]
    data = await state.get_data()
    word = data['word']
    p_type = data['punishment_type']
    p_time = data['punishment_time']
    
    if unit == 'д' and p_time > MAX_MUTE_DAYS:
        p_time = MAX_MUTE_DAYS
    elif unit == 'ч':
        max_hours = MAX_MUTE_DAYS * 24
        if p_time > max_hours:
            p_time = max_hours
    elif unit == 'м':
        max_minutes = MAX_MUTE_DAYS * 24 * 60
        if p_time > max_minutes:
            p_time = max_minutes
    
    success = db.add_banned_word(
        callback.message.chat.id,
        word,
        p_type,
        p_time,
        unit
    )
    
    if success:
        punishment_name = {
            'м': 'мут',
            'б': 'бан',
            'к': 'кик'
        }.get(p_type)
        
        time_str = f"{p_time} {unit}"
        
        await callback.message.edit_text(
            f"✅ Слово <b>{word}</b> добавлено!\n"
            f"Наказание: {punishment_name} на {time_str}\n\n"
            f"⚠️ Бот будет распознавать это слово:\n"
            f"• В любом регистре\n"
            f"• С подменой букв (p->р, 0->о)\n"
            f"• В спойлерах и цитатах\n"
            f"• Даже с пробелами между буквами"
        )
    else:
        await callback.message.edit_text(
            f"❌ Слово <b>{word}</b> уже существует в списке!"
        )
    
    await state.clear()
    await callback.answer()

# Обработчик сообщений для проверки запрещенных слов
@dp.message(F.chat.type.in_({'group', 'supergroup'}))
async def check_message(message: Message):
    """Проверяет сообщения на наличие запрещенных слов"""
    if await is_creator(message.chat.id, message.from_user.id):
        return
    
    if await is_chat_admin(message.chat.id, message.from_user.id):
        return
    
    full_text = extract_message_text(message)
    
    if not full_text:
        return
    
    result = db.check_banned_word(message.chat.id, full_text)
    if result:
        word, p_type, p_time, p_unit = result
        
        try:
            # Сохраняем нарушение в статистику
            punishment_name = {
                'м': 'мут',
                'б': 'бан',
                'к': 'кик'
            }.get(p_type, 'неизвестно')
            
            db.add_violation(
                message.chat.id,
                message.from_user.id,
                message.from_user.full_name or message.from_user.username or "Неизвестно",
                word,
                punishment_name
            )
            
            if p_type == 'к':
                await bot.ban_chat_member(message.chat.id, message.from_user.id)
                await bot.unban_chat_member(message.chat.id, message.from_user.id)
                await message.reply(
                    f"👢 Пользователь {message.from_user.full_name} был кикнут\n"
                    f"Причина: использование слова «{word}»\n"
                    f"📍 Обнаружено даже с подменой букв!"
                )
                
            elif p_type == 'м':
                until_date = None
                if p_time > 0:
                    if p_unit == 'м':
                        until_date = datetime.now() + timedelta(minutes=p_time)
                    elif p_unit == 'ч':
                        until_date = datetime.now() + timedelta(hours=p_time)
                    elif p_unit == 'д':
                        days = min(p_time, MAX_MUTE_DAYS)
                        until_date = datetime.now() + timedelta(days=days)
                
                permissions = ChatPermissions(can_send_messages=False)
                await bot.restrict_chat_member(
                    message.chat.id, 
                    message.from_user.id,
                    permissions=permissions,
                    until_date=until_date
                )
                
                time_str = f"{p_time} {p_unit}"
                await message.reply(
                    f"🔇 Пользователь {message.from_user.full_name} получил мут на {time_str}\n"
                    f"Причина: использование слова «{word}»\n"
                    f"📍 Обнаружено даже с подменой букв!"
                )
                
            elif p_type == 'б':
                until_date = None
                if p_time > 0:
                    if p_unit == 'м':
                        until_date = datetime.now() + timedelta(minutes=p_time)
                    elif p_unit == 'ч':
                        until_date = datetime.now() + timedelta(hours=p_time)
                    elif p_unit == 'д':
                        days = min(p_time, MAX_BAN_DAYS)
                        until_date = datetime.now() + timedelta(days=days)
                
                await bot.ban_chat_member(
                    message.chat.id, 
                    message.from_user.id,
                    until_date=until_date
                )
                
                time_str = f"{p_time} {p_unit}"
                await message.reply(
                    f"⛔️ Пользователь {message.from_user.full_name} забанен на {time_str}\n"
                    f"Причина: использование слова «{word}»\n"
                    f"📍 Обнаружено даже с подменой букв!"
                )
                
        except Exception as e:
            logger.error(f"Ошибка при наказании: {e}")
            await message.reply(
                "❌ Не удалось применить наказание. Проверьте права бота."
            )

# Фоновая задача для автоматической рассылки правил
async def rules_broadcast_task():
    while True:
        try:
            with db.get_connection() as conn:
                c = conn.cursor()
                c.execute('''SELECT chat_id, rules_enabled, rules_interval, 
                                   last_rules_time, rules_text 
                            FROM group_rules 
                            WHERE rules_enabled = 1 AND rules_text IS NOT NULL''')
                
                for chat_id, enabled, interval, last_time, rules_text in c.fetchall():
                    current_time = int(time.time())
                    
                    if last_time and current_time - last_time < interval:
                        continue
                    
                    try:
                        msg = await bot.send_message(
                            chat_id,
                            f"<b>📋 Напоминание правил чата:</b>\n\n{rules_text}"
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

# Запуск бота
async def on_startup():
    logger.info("🚀 Бот Puls Chat Manager запущен!")
    logger.info(f"Администраторы: {ADMIN_IDS}")
    asyncio.create_task(rules_broadcast_task())

async def on_shutdown():
    logger.info("👋 Бот остановлен!")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
