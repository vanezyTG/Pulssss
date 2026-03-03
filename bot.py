import asyncio
import logging
import re
import time
import json
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any
import sqlite3
from contextlib import contextmanager
from functools import wraps

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ChatPermissions, MessageEntity
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
BOT_TOKEN = "8557190026:AAFME58NZn6kdEexCUyjv5DoIhXs2-mwNpk"  # Замените на ваш токен
BOT_USERNAME = "PulsOfficialManager_bot"  # Замените на username вашего бота (без @)
ADMIN_IDS = [6708209142]  # Замените на ID администраторов бота
MAX_MUTE_DAYS = 36500  # 100 лет в днях
MAX_BAN_DAYS = 36500   # 100 лет в днях

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Словарь для замены похожих букв
SIMILAR_CHARS = {
    'а': ['a', 'а', '@', '4'],
    'б': ['6', 'b', 'б'],
    'в': ['b', 'в', '8'],
    'г': ['r', 'г'],
    'д': ['d', 'д'],
    'е': ['e', 'е', '3'],
    'ё': ['e', 'е', 'ё'],
    'ж': ['zh', 'ж'],
    'з': ['3', 'z', 'з'],
    'и': ['u', 'и', '1'],
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

# Создаем обратный словарь
CHAR_VARIANTS = {}
for char, variants in SIMILAR_CHARS.items():
    for variant in variants:
        CHAR_VARIANTS[variant] = char

def normalize_text(text: str) -> str:
    """Нормализует текст, заменяя похожие буквы"""
    if not text:
        return ""
    
    text = text.lower()
    result = []
    
    for char in text:
        if char in CHAR_VARIANTS:
            result.append(CHAR_VARIANTS[char])
        else:
            if char.isalnum() or char in [' ', '_', '-']:
                result.append(char)
    
    return ''.join(result)

def text_contains_word(text: str, word: str) -> bool:
    """Проверяет наличие слова с учетом подмены букв"""
    if not text or not word:
        return False
    
    # Удаляем HTML теги из текста для проверки
    text = re.sub(r'<[^>]+>', '', text)
    
    normalized_text = normalize_text(text)
    normalized_word = normalize_text(word)
    
    if not normalized_word:
        return False
    
    # Проверяем вхождение слова
    if normalized_word in normalized_text:
        return True
    
    # Проверяем с удалением всех разделителей
    text_without_spaces = re.sub(r'[^а-яa-z0-9]', '', normalized_text)
    word_without_spaces = re.sub(r'[^а-яa-z0-9]', '', normalized_word)
    
    if word_without_spaces and word_without_spaces in text_without_spaces:
        return True
    
    return False

def entities_to_json(entities: Optional[List[MessageEntity]]) -> Optional[str]:
    """Конвертирует список entities в JSON для сохранения в БД"""
    if not entities:
        return None
    
    entities_list = []
    for entity in entities:
        entities_list.append({
            'type': entity.type,
            'offset': entity.offset,
            'length': entity.length,
            'url': entity.url,
            'language': entity.language,
            'custom_emoji_id': entity.custom_emoji_id
        })
    return json.dumps(entities_list, ensure_ascii=False)

def json_to_entities(json_str: Optional[str]) -> Optional[List[MessageEntity]]:
    """Конвертирует JSON обратно в список entities"""
    if not json_str:
        return None
    
    try:
        entities_list = json.loads(json_str)
        result = []
        for e in entities_list:
            entity = MessageEntity(
                type=e['type'],
                offset=e['offset'],
                length=e['length'],
                url=e.get('url'),
                language=e.get('language'),
                custom_emoji_id=e.get('custom_emoji_id')
            )
            result.append(entity)
        return result
    except:
        return None

# Классы состояний
class RulesStates(StatesGroup):
    waiting_for_rules_text = State()
    waiting_for_interval = State()

class StopWordsStates(StatesGroup):
    waiting_for_word = State()
    waiting_for_punishment = State()
    waiting_for_time = State()
    waiting_for_unit = State()

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
            
            # Таблица для правил групп - теперь храним текст и entities отдельно
            c.execute('''CREATE TABLE IF NOT EXISTS group_rules
                         (chat_id INTEGER PRIMARY KEY,
                          rules_text TEXT,
                          rules_entities TEXT,
                          rules_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300,
                          last_rules_message_id INTEGER,
                          last_rules_time INTEGER,
                          chat_title TEXT,
                          chat_username TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS banned_words
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          word TEXT,
                          punishment_type TEXT,
                          punishment_time INTEGER,
                          punishment_unit TEXT,
                          UNIQUE(chat_id, word))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS group_creators
                         (chat_id INTEGER PRIMARY KEY,
                          creator_id INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS violations
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          user_id INTEGER,
                          user_name TEXT,
                          word TEXT,
                          punishment TEXT,
                          timestamp INTEGER)''')
            
            conn.commit()
    
    def save_rules(self, chat_id: int, rules_text: str, rules_entities: Optional[str], 
                   chat_title: str = None, chat_username: str = None):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO group_rules 
                         (chat_id, rules_text, rules_entities, chat_title, chat_username) 
                         VALUES (?, ?, ?, ?, ?)''', 
                         (chat_id, rules_text, rules_entities, chat_title, chat_username))
            conn.commit()
    
    def get_rules(self, chat_id: int) -> Tuple[Optional[str], Optional[str]]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_text, rules_entities FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return (result[0], result[1]) if result else (None, None)
    
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
    
    def add_violation(self, chat_id: int, user_id: int, user_name: str, word: str, punishment: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO violations 
                         (chat_id, user_id, user_name, word, punishment, timestamp) 
                         VALUES (?, ?, ?, ?, ?, ?)''',
                         (chat_id, user_id, user_name, word, punishment, int(time.time())))
            conn.commit()
    
    def get_all_chats(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT chat_id, chat_title, chat_username, rules_enabled,
                                (SELECT COUNT(*) FROM banned_words WHERE chat_id = group_rules.chat_id) as words_count
                         FROM group_rules 
                         ORDER BY chat_id''')
            return c.fetchall()
    
    def get_total_stats(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            
            c.execute('SELECT COUNT(DISTINCT chat_id) FROM group_rules')
            total_groups = c.fetchone()[0] or 0
            
            c.execute('SELECT COUNT(*) FROM banned_words')
            total_words = c.fetchone()[0] or 0
            
            c.execute('SELECT COUNT(*) FROM violations')
            total_violations = c.fetchone()[0] or 0
            
            c.execute('SELECT COUNT(DISTINCT user_id) FROM violations')
            unique_users = c.fetchone()[0] or 0
            
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

db = Database()

# Вспомогательные функции
async def is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ['creator', 'administrator']
    except:
        return False

async def is_creator(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status == 'creator'
    except:
        return False

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
def get_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📋 О боте", callback_data="about")
    builder.button(text="🆘 Помощь", callback_data="help")
    builder.button(text="📜 Правила", callback_data="rules")
    builder.button(text="➕ Добавить в группу", url=f"https://t.me/{BOT_USERNAME}?startgroup=true")
    builder.button(text="⚙️ Управление группой", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Установить правила", callback_data="set_rules")
    builder.button(text="🔄 Авто-рассылка правил", callback_data="rules_auto")
    builder.button(text="🚫 Запрещенные слова", callback_data="banned_words")
    builder.button(text="◀️ Назад", callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled: bool):
    builder = InlineKeyboardBuilder()
    status = "✅ Включено" if enabled else "❌ Выключено"
    builder.button(text=f"Статус: {status}", callback_data="toggle_rules")
    builder.button(text="⏱ Установить интервал", callback_data="set_interval")
    builder.button(text="◀️ Назад", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

# Команды админа
@dp.message(Command("adminstats"))
@check_bot_admin()
async def cmd_admin_stats(message: Message):
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
    
    await message.answer(text)

@dp.message(Command("adminchats"))
@check_bot_admin()
async def cmd_admin_chats(message: Message):
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
            try:
                invite_link = await bot.create_chat_invite_link(chat_id, member_limit=1)
                chat_info = f"• <a href='{invite_link.invite_link}'>{title or 'Приватная группа'}</a>"
            except:
                chat_info = f"• {title or 'Приватная группа'} (нет доступа)"
        
        status = "✅" if enabled else "❌"
        text += f"{status} {chat_info} | ID: <code>{chat_id}</code> | Слов: {words_count}\n"
    
    await message.answer(text)

# Основные команды
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
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
        "👇 Нажмите «➕ Добавить в группу» чтобы пригласить меня в ваш чат"
    )
    await message.answer(text, reply_markup=get_main_keyboard())

@dp.message(Command("startpuls"))
async def cmd_startpuls(message: Message, state: FSMContext):
    await cmd_start(message, state)

@dp.message(Command("rules"))
@dp.message(Command("rulesgroup"))
@dp.message(F.text.lower() == "правила чата")
async def cmd_rules(message: Message):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        return
    
    rules_text, rules_entities_json = db.get_rules(message.chat.id)
    if rules_text and rules_entities_json:
        rules_entities = json_to_entities(rules_entities_json)
        await message.reply(
            text=rules_text,
            entities=rules_entities
        )
    elif rules_text:
        # Если есть только текст без entities (старые записи)
        await message.reply(f"<b>📜 Правила чата:</b>\n\n{rules_text}", parse_mode="HTML")
    else:
        await message.answer("❓ В этом чате еще не установлены правила.")

# Callback handlers
@dp.callback_query(F.data == "about")
@check_owner()
async def callback_about(callback: CallbackQuery, state: FSMContext):
    text = (
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "📌 <b>Что я умею:</b>\n"
        "• Автоматическая модерация\n"
        "• Борьба со спамом и запрещенными словами\n"
        "• Распознаю слова с подменой букв\n"
        "• Сохраняю форматирование, спойлеры и цитаты\n"
        "• Гибкие настройки правил\n"
        "• Различные виды наказаний\n\n"
        "👇 Нажмите «➕ Добавить в группу» чтобы пригласить меня в ваш чат"
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "help")
@check_owner()
async def callback_help(callback: CallbackQuery, state: FSMContext):
    text = (
        "🆘 <b>Помощь по Puls Chat Manager</b>\n\n"
        "🔹 <b>Основные команды:</b>\n"
        "• /start - Главное меню\n"
        "• /rules - Показать правила чата\n"
        "• /addstopword - Добавить запрещенное слово\n\n"
        
        "🔹 <b>Как добавить бота в группу:</b>\n"
        "1. Нажмите кнопку «➕ Добавить в группу» в меню\n"
        "2. Выберите чат из списка\n"
        "3. Сделайте бота администратором\n"
        "4. Настройте правила через меню\n\n"
        
        "🔹 <b>Запрещенные слова:</b>\n"
        "• Добавляются командой /addstopword\n"
        "• Можно выбрать наказание (мут/бан/кик)\n"
        "• Бот распознает даже с подменой букв\n"
        "• Пример: 'мат' сработает на 'м0т', 'pривет'\n\n"
        
        "🔹 <b>Авто-рассылка правил:</b>\n"
        "• Бот автоматически отправляет правила\n"
        "• Закрепляет их в чате\n"
        "• Интервал от 5 минут до 1 года\n\n"
        
        "🔹 <b>Форматирование правил:</b>\n"
        "• <b>Жирный</b> - <code>&lt;b&gt;текст&lt;/b&gt;</code>\n"
        "• <i>Курсив</i> - <code>&lt;i&gt;текст&lt;/i&gt;</code>\n"
        "• <tg-spoiler>Спойлер</tg-spoiler> - <code>&lt;tg-spoiler&gt;текст&lt;/tg-spoiler&gt;</code>\n"
        "• <blockquote>Цитата</blockquote> - <code>&lt;blockquote&gt;текст&lt;/blockquote&gt;</code>\n"
        "• <blockquote expandable>Свернутая цитата\nНовая строка</blockquote> - <code>&lt;blockquote expandable&gt;текст\nстроки&lt;/blockquote&gt;</code>"
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "rules")
@check_owner()
async def callback_rules(callback: CallbackQuery, state: FSMContext):
    text = (
        "📋 <b>Правила использования бота:</b>\n\n"
        "1️⃣ Бот должен быть администратором в группе\n"
        "2️⃣ Для настройки используйте меню управления\n"
        "3️⃣ Все наказания записываются в лог\n"
        "4️⃣ Не злоупотребляйте правами бота\n"
        "5️⃣ Бот сохраняет всё форматирование и цитаты\n\n"
        "⚠️ Бот не несет ответственности за неправильные настройки"
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "group_manage")
@check_owner()
@check_creator()
async def callback_group_manage(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⚙️ <b>Управление группой</b>\n\n"
        "Выберите действие:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
@check_owner()
async def callback_back_to_main(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "👋 Главное меню:",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "set_rules")
@check_owner()
@check_creator()
async def callback_set_rules(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📝 Отправьте текст правил для этого чата.\n"
        "Вы можете использовать любое форматирование:\n"
        "• Жирный, курсив, подчеркнутый\n"
        "• Спойлеры (скрытый текст)\n"
        "• Цитаты (в том числе свернутые)\n"
        "• Списки и ссылки\n\n"
        "✏️ Просто напишите сообщение с правилами в этот чат.\n"
        "Бот сохранит всё форматирование!\n\n"
        "💡 <b>Важно для свернутых цитат:</b>\n"
        "Внутри <code>&lt;blockquote expandable&gt;</code> должно быть\n"
        "<b>несколько строк</b>, чтобы цитата сворачивалась!"
    )
    await state.set_state(RulesStates.waiting_for_rules_text)
    await callback.answer()

@dp.message(RulesStates.waiting_for_rules_text)
async def process_rules_text(message: Message, state: FSMContext):
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах!")
        await state.clear()
        return
    
    if not await is_creator(message.chat.id, message.from_user.id):
        await message.answer("❌ Только создатель может устанавливать правила!")
        await state.clear()
        return
    
    # Сохраняем текст и entities отдельно
    rules_text = message.text or message.caption
    rules_entities = message.entities or message.caption_entities
    
    if not rules_text:
        await message.answer("❌ Не удалось извлечь текст правил!")
        await state.clear()
        return
    
    # Проверяем, что текст достаточно длинный
    if len(rules_text.strip()) < 10:
        await message.answer("❌ Правила слишком короткие!")
        await state.clear()
        return
    
    # Конвертируем entities в JSON для сохранения
    rules_entities_json = entities_to_json(rules_entities)
    
    chat_title = message.chat.title
    chat_username = message.chat.username
    
    db.save_rules(message.chat.id, rules_text, rules_entities_json, chat_title, chat_username)
    db.save_creator(message.chat.id, message.from_user.id)
    
    await message.reply(
        "✅ <b>Правила успешно сохранены!</b>\n\n"
        "Всё форматирование сохранено:\n"
        "• <b>жирный текст</b>\n"
        "• <i>курсив</i>\n"
        "• <tg-spoiler>спойлеры</tg-spoiler>\n"
        "• <blockquote>цитаты</blockquote>\n"
        "• <blockquote expandable>свернутые цитаты\nс несколькими строками</blockquote>\n\n"
        "Используйте /rules чтобы проверить.",
        parse_mode="HTML"
    )
    await state.clear()

@dp.callback_query(F.data == "rules_auto")
@check_owner()
@check_creator()
async def callback_rules_auto(callback: CallbackQuery, state: FSMContext):
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
    words = db.get_banned_words(callback.message.chat.id)
    
    if words:
        text = "🚫 <b>Запрещенные слова в этом чате:</b>\n\n"
        for i, (word, p_type, p_time, p_unit) in enumerate(words, 1):
            punishment = {
                'м': 'мут',
                'б': 'бан',
                'к': 'кик'
            }.get(p_type, 'неизвестно')
            
            time_str = f"{p_time} {p_unit}" if p_type != 'к' else "мгновенно"
            text += f"{i}. <b>{word}</b> — {punishment} на {time_str}\n"
        
        text += "\n"
    else:
        text = "📝 В этом чате пока нет запрещенных слов.\n\n"
    
    text += "➕ <b>Добавить слово:</b>\n"
    text += "Используйте команду /addstopword слово\n"
    text += "Пример: /addstopword мат\n\n"
    text += "⚠️ Бот распознает слова даже с подменой букв!"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data="group_manage")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

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
    if message.from_user.is_bot:
        return
    
    if await is_creator(message.chat.id, message.from_user.id):
        return
    
    if await is_chat_admin(message.chat.id, message.from_user.id):
        return
    
    # Получаем текст сообщения (включая HTML форматирование)
    text_to_check = message.html_text or message.caption or ""
    
    if not text_to_check:
        return
    
    result = db.check_banned_word(message.chat.id, text_to_check)
    if result:
        word, p_type, p_time, p_unit = result
        
        try:
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
                    f"👢 Пользователь {message.from_user.full_name} кикнут\n"
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
                    f"🔇 {message.from_user.full_name} получил мут на {time_str}\n"
                    f"Причина: слово «{word}»\n"
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
                    f"⛔️ {message.from_user.full_name} забанен на {time_str}\n"
                    f"Причина: слово «{word}»\n"
                    f"📍 Обнаружено даже с подменой букв!"
                )
                
        except Exception as e:
            logger.error(f"Ошибка при наказании: {e}")
            await message.reply("❌ Не удалось применить наказание. Проверьте права бота.")

# Фоновая задача для автоматической рассылки правил
async def rules_broadcast_task():
    while True:
        try:
            with db.get_connection() as conn:
                c = conn.cursor()
                c.execute('''SELECT chat_id, rules_enabled, rules_interval, 
                                   last_rules_time, rules_text, rules_entities 
                            FROM group_rules 
                            WHERE rules_enabled = 1 AND rules_text IS NOT NULL''')
                
                for chat_id, enabled, interval, last_time, rules_text, rules_entities_json in c.fetchall():
                    current_time = int(time.time())
                    
                    if last_time and current_time - last_time < interval:
                        continue
                    
                    try:
                        if rules_entities_json:
                            rules_entities = json_to_entities(rules_entities_json)
                            msg = await bot.send_message(
                                chat_id,
                                text=f"<b>📋 Напоминание правил чата:</b>\n\n{rules_text}",
                                entities=rules_entities
                            )
                        else:
                            msg = await bot.send_message(
                                chat_id,
                                text=f"<b>📋 Напоминание правил чата:</b>\n\n{rules_text}",
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

# Запуск бота
async def on_startup():
    logger.info("🚀 Бот Puls Chat Manager запущен!")
    logger.info(f"Администраторы: {ADMIN_IDS}")
    logger.info(f"Username: @{BOT_USERNAME}")
    asyncio.create_task(rules_broadcast_task())

async def on_shutdown():
    logger.info("👋 Бот остановлен!")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
