import asyncio
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
import sqlite3
from contextlib import contextmanager
from functools import wraps

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ChatPermissions, InlineKeyboardMarkup
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
MAX_MUTE_DAYS = 36500  # 100 лет в днях
MAX_BAN_DAYS = 36500   # 100 лет в днях

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

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
            # Получаем данные о пользователе, который нажал кнопку
            user_id = callback.from_user.id
            
            # Проверяем, есть ли сохраненный владелец для этого сообщения
            # В aiogram нет прямого доступа к владельцу сообщения, поэтому будем хранить в FSM
            state = kwargs.get('state')
            if state:
                data = await state.get_data()
                message_owner = data.get(f"msg_owner_{callback.message.message_id}")
                
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
                          last_rules_time INTEGER)''')
            
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
            
            conn.commit()
    
    # Методы для правил
    def save_rules(self, chat_id: int, rules_text: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO group_rules (chat_id, rules_text) 
                         VALUES (?, ?)''', (chat_id, rules_text))
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
            # Сначала проверяем существование записи
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
        if not text:
            return None
        text_lower = text.lower()
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT word, punishment_type, punishment_time, punishment_unit 
                         FROM banned_words WHERE chat_id = ?''', (chat_id,))
            for word, p_type, p_time, p_unit in c.fetchall():
                # Проверяем вхождение слова как целое слово или часть текста
                pattern = r'\b' + re.escape(word) + r'\b'
                if re.search(pattern, text_lower):
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
    builder.button(text="📜 Правила", callback_data="rules")
    builder.button(text="⚙️ Управление группой", callback_data="group_manage")
    builder.button(text="📊 Статистика", callback_data="stats")
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

# Обработчики команд
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    # Сохраняем владельца сообщения
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    
    text = (
        "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\n"
        "Я - умный менеджер для ваших чатов. Помогаю следить за порядком, "
        "наказываю нарушителей и автоматизирую модерацию.\n\n"
        "🔹 <b>Мои возможности:</b>\n"
        "• Установка и автоматическая рассылка правил\n"
        "• Блокировка запрещенных слов\n"
        "• Автоматические наказания (мут/бан/кик)\n"
        "• Гибкая система настроек\n\n"
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
        # Отправляем с цитированием исходного сообщения
        await message.reply(f"<b>📜 Правила чата:</b>\n\n{rules}")
    else:
        await message.answer("❓ В этом чате еще не установлены правила.")

# Обработчики колбэков с защитой
@dp.callback_query(F.data == "about")
@check_owner()
async def callback_about(callback: CallbackQuery, state: FSMContext):
    """Информация о боте"""
    text = (
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "Версия: 2.0 (aiogram)\n\n"
        "📌 <b>Что я умею:</b>\n"
        "• Автоматическая модерация\n"
        "• Борьба со спамом и запрещенными словами\n"
        "• Гибкие настройки правил\n"
        "• Различные виды наказаний\n\n"
        "💡 Добавьте меня в группу и сделайте администратором,\n"
        "чтобы я мог полноценно работать!"
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
        "4️⃣ Не злоупотребляйте правами бота\n\n"
        "⚠️ Бот не несет ответственности за неправильные настройки"
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "stats")
@check_owner()
async def callback_stats(callback: CallbackQuery, state: FSMContext):
    """Статистика бота"""
    # Получаем статистику из базы данных
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(DISTINCT chat_id) FROM group_rules')
        groups_count = c.fetchone()[0] or 0
        
        c.execute('SELECT COUNT(*) FROM banned_words')
        words_count = c.fetchone()[0] or 0
    
    text = (
        "📊 <b>Статистика бота:</b>\n\n"
        f"📱 Групп с правилами: {groups_count}\n"
        f"🚫 Запрещенных слов: {words_count}\n"
        f"⏱ Активен: круглосуточно\n"
        f"🔄 Версия: 2.0"
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
        "Вы можете использовать форматирование (жирный, курсив и т.д.)\n\n"
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
    
    # Проверяем, что пользователь - создатель
    if not await is_creator(message.chat.id, message.from_user.id):
        await message.answer("❌ Только создатель может устанавливать правила!")
        await state.clear()
        return
    
    # Сохраняем правила с полным форматированием
    db.save_rules(message.chat.id, message.html_text)
    
    # Сохраняем создателя группы
    db.save_creator(message.chat.id, message.from_user.id)
    
    await message.reply("✅ Правила успешно сохранены!\n\nИспользуйте /rules чтобы их увидеть.")
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
    
    # Проверяем, что пользователь - создатель
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
            
            time_str = f"{p_time} {p_unit}"
            text += f"{i}. <b>{word}</b> - {punishment} на {time_str}\n"
            
            # Добавляем кнопку удаления для каждого слова
            # (можно реализовать позже)
    else:
        text = "📝 Список запрещенных слов пуст.\n\n"
    
    text += "\n➕ <b>Как добавить слово:</b>\n"
    text += "Используйте команду:\n"
    text += "<code>/addstopword слово</code>\n\n"
    text += "Пример: /addstopword мат"
    
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
            "После этого бот запросит тип наказания и время."
        )
        return
    
    word = args[1].strip()
    if len(word) < 5 or len(word) > 30:
        await message.answer("❌ Слово должно быть от 5 до 30 символов!")
        return
    
    # Сохраняем слово и владельца сообщения в состоянии
    await state.update_data(
        word=word,
        msg_owner=f"word_{message.from_user.id}"
    )
    
    # Запрашиваем тип наказания
    builder = InlineKeyboardBuilder()
    builder.button(text="🔇 Мут (временный)", callback_data="punish_m")
    builder.button(text="⛔️ Бан (навсегда или временный)", callback_data="punish_b")
    builder.button(text="👢 Кик (сразу)", callback_data="punish_k")
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
    # Дополнительная проверка, что это тот же пользователь
    data = await state.get_data()
    if data.get('msg_owner') != f"word_{callback.from_user.id}":
        await callback.answer("⚠️ Эта кнопка не для вас!", show_alert=True)
        return
    
    p_type = callback.data.split('_')[1]  # punish_m -> m
    
    await state.update_data(punishment_type=p_type)
    
    if p_type == 'к':  # Для кика время не нужно
        # Сразу добавляем слово
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
                f"Наказание: кик"
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
            "Для мута максимум: 100 лет\n"
            "Для бана максимум: 100 лет"
        )
        await state.set_state(StopWordsStates.waiting_for_time)
    
    await callback.answer()

@dp.message(StopWordsStates.waiting_for_time)
async def process_punishment_time(message: Message, state: FSMContext):
    """Обработка времени наказания"""
    # Проверяем, что это тот же пользователь
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
        
        # Проверяем максимальное время для типа наказания
        p_type = data.get('punishment_type')
        max_time = 36500 if p_type in ['м', 'б'] else 1  # 100 лет в днях
        
        if p_time > max_time:
            await message.answer(f"❌ Максимальное время: {max_time}!")
            return
        
        await state.update_data(punishment_time=p_time)
        
        # Запрашиваем единицу времени
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
    # Проверяем, что это тот же пользователь
    data = await state.get_data()
    if data.get('msg_owner') != f"word_{callback.from_user.id}":
        await callback.answer("⚠️ Эта кнопка не для вас!", show_alert=True)
        return
    
    unit = callback.data.split('_')[1]  # unit_m -> m
    
    # Получаем все данные
    data = await state.get_data()
    word = data['word']
    p_type = data['punishment_type']
    p_time = data['punishment_time']
    
    # Проверяем максимальное время в зависимости от единицы
    if unit == 'д':
        if p_time > MAX_MUTE_DAYS:
            p_time = MAX_MUTE_DAYS
    elif unit == 'ч':
        if p_time > MAX_MUTE_DAYS * 24:
            p_time = MAX_MUTE_DAYS * 24
    elif unit == 'м':
        if p_time > MAX_MUTE_DAYS * 24 * 60:
            p_time = MAX_MUTE_DAYS * 24 * 60
    
    # Добавляем слово в базу
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
        
        time_str = f"{p_time} {unit}" if p_type != 'к' else "мгновенно"
        
        await callback.message.edit_text(
            f"✅ Слово <b>{word}</b> добавлено!\n"
            f"Наказание: {punishment_name} на {time_str}"
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
    if not message.text or message.from_user.is_bot:
        return
    
    # Проверяем, является ли пользователь создателем или админом
    if await is_creator(message.chat.id, message.from_user.id):
        return  # Создателя не наказываем
    
    if await is_chat_admin(message.chat.id, message.from_user.id):
        return  # Админов тоже не наказываем
    
    # Проверяем наличие запрещенных слов
    result = db.check_banned_word(message.chat.id, message.text)
    if result:
        word, p_type, p_time, p_unit = result
        
        try:
            if p_type == 'к':
                # Кик
                await bot.ban_chat_member(message.chat.id, message.from_user.id)
                await bot.unban_chat_member(message.chat.id, message.from_user.id)
                await message.reply(
                    f"👢 Пользователь {message.from_user.full_name} был кикнут\n"
                    f"Причина: использование запрещенного слова «{word}»"
                )
                
            elif p_type == 'м':
                # Мут
                until_date = None
                if p_time > 0:
                    if p_unit == 'м':
                        until_date = timedelta(minutes=p_time)
                    elif p_unit == 'ч':
                        until_date = timedelta(hours=p_time)
                    elif p_unit == 'д':
                        until_date = timedelta(days=min(p_time, MAX_MUTE_DAYS))
                    
                    until_date = datetime.now() + until_date
                
                permissions = ChatPermissions(can_send_messages=False)
                await bot.restrict_chat_member(
                    message.chat.id, 
                    message.from_user.id,
                    permissions=permissions,
                    until_date=until_date
                )
                
                time_str = f"{p_time} {p_unit}" if p_time > 0 else "навсегда"
                await message.reply(
                    f"🔇 Пользователь {message.from_user.full_name} получил мут на {time_str}\n"
                    f"Причина: использование запрещенного слова «{word}»"
                )
                
            elif p_type == 'б':
                # Бан
                until_date = None
                if p_time > 0:
                    if p_unit == 'м':
                        until_date = timedelta(minutes=p_time)
                    elif p_unit == 'ч':
                        until_date = timedelta(hours=p_time)
                    elif p_unit == 'д':
                        until_date = timedelta(days=min(p_time, MAX_BAN_DAYS))
                    
                    until_date = datetime.now() + until_date
                
                await bot.ban_chat_member(
                    message.chat.id, 
                    message.from_user.id,
                    until_date=until_date
                )
                
                time_str = f"{p_time} {p_unit}" if p_time > 0 else "навсегда"
                await message.reply(
                    f"⛔️ Пользователь {message.from_user.full_name} забанен на {time_str}\n"
                    f"Причина: использование запрещенного слова «{word}»"
                )
                
        except Exception as e:
            logger.error(f"Ошибка при наказании: {e}")
            await message.reply("❌ Не удалось применить наказание. Проверьте права бота.")

# Фоновая задача для автоматической рассылки правил
async def rules_broadcast_task():
    """Фоновая задача для отправки правил"""
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
                    
                    # Проверяем, пора ли отправлять
                    if last_time and current_time - last_time < interval:
                        continue
                    
                    try:
                        # Отправляем правила
                        msg = await bot.send_message(
                            chat_id,
                            f"<b>📋 Напоминание правил чата:</b>\n\n{rules_text}"
                        )
                        
                        # Пытаемся закрепить сообщение
                        try:
                            await bot.pin_chat_message(chat_id, msg.message_id)
                        except:
                            pass  # Не получилось закрепить - не страшно
                        
                        # Обновляем время последней отправки
                        db.update_last_rules(chat_id, msg.message_id)
                        
                    except Exception as e:
                        logger.error(f"Ошибка отправки правил в чат {chat_id}: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче: {e}")
        
        # Проверяем каждую минуту
        await asyncio.sleep(60)

# Запуск бота
async def on_startup():
    """Действия при запуске"""
    logger.info("Бот запущен!")
    # Запускаем фоновую задачу
    asyncio.create_task(rules_broadcast_task())

async def on_shutdown():
    """Действия при остановке"""
    logger.info("Бот остановлен!")

async def main():
    """Главная функция"""
    # Регистрируем обработчики запуска/остановки
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
