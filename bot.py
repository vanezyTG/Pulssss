
import asyncio
import logging
import time
import random
import string
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict, Any, Set
import sqlite3
from contextlib import contextmanager
from functools import wraps, lru_cache
from collections import defaultdict, deque
import threading
import os
import shutil
import re
import html
import json
import sys
import traceback

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, ChatMemberUpdated, ChatPermissions, 
    InlineKeyboardButton, FSInputFile, InlineKeyboardMarkup,
    ReactionTypeEmoji, ChatJoinRequest
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def excepthook(exctype, value, tb):
    logger.error(''.join(traceback.format_exception(exctype, value, tb)))

sys.excepthook = excepthook

BOT_TOKEN = "8557190026:AAEWJo-DqwgAeLyz94xbH7lXe9snUZQk30Y"
BOT_USERNAME = "PulsOfficialManager_bot"
ADMIN_IDS = [6708209142]

MAX_TRIGGERS = 100
MAX_TRIGGER_LENGTH = 20
MAX_TRIGGER_WORDS = 1
MAX_RESPONSE_LENGTH = 4000
MAX_RULES_LENGTH = 10000
MAX_WELCOME_LENGTH = 4000
MAX_AUTO_COMMENT_LENGTH = 4000

SPAM_MESSAGE_LIMIT = 30
SPAM_CHECK_TIME = 30
SPAM_WARN_LIMIT = 3

SUPPORT_LINK = "https://t.me/support_puls"

MAX_BUTTON_PRESSES = 4
BUTTON_CHECK_TIME = 3
BUTTON_BLOCK_TIME = 8

MAX_COMMANDS = 3
COMMAND_CHECK_TIME = 2
COMMAND_BLOCK_TIME = 5

CLEANUP_INTERVAL = 300
DATA_TTL = 900

SERVER_TZ = datetime.now().astimezone().tzinfo

user_messages = defaultdict(list)
user_button_presses = defaultdict(list)
user_commands = defaultdict(list)
button_blocked_users = defaultdict(float)
command_blocked_users = defaultdict(float)
raid_detector = defaultdict(list)
global_spammers = {}
spam_lock = threading.Lock()
stats_lock = threading.Lock()
stats_updating = False
technical_maintenance = False
maintenance_message = "🛠 Бот временно остановлен на технические работы."

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Оптимизированный регекс для мата
PROFANITY_PATTERN = re.compile(
    r'(ху[йеюяи]|пизд[ауыео]|бля[дть]|еб[ауыео]|заеб|нахуй|похуй|охуе|долбоеб|мудак|гандон|пидор|педик|лох|шлюх|сук[ау]|твар[ьи]|ублюд|дебил|даун|идиот|кретин|придур|чмо|fuck|shit|bitch|asshole|dick|cunt|pussy|whore|slut)',
    re.IGNORECASE
)

def check_profanity(text: str) -> bool:
    if not text:
        return False
    return bool(PROFANITY_PATTERN.search(text))

def extract_links(text: str) -> List[str]:
    if not text:
        return []
    url_pattern = r'https?://[^\s]+'
    tg_pattern = r'(?:t\.me|telegram\.me)/[^\s]+'
    links = re.findall(url_pattern, text, re.IGNORECASE)
    links.extend(re.findall(tg_pattern, text, re.IGNORECASE))
    return links

def check_links(text: str) -> bool:
    return len(extract_links(text)) > 0

class MessageTemplate:
    def __init__(self, key: str, default_text: str, default_photo: str = None):
        self.key = key
        self.default_text = default_text
        self.default_photo = default_photo
        self.custom_text = None
        self.custom_photo = None
    
    def get_text(self) -> str:
        return self.custom_text if self.custom_text else self.default_text
    
    def get_photo(self) -> Optional[str]:
        return self.custom_photo if self.custom_photo else self.default_photo
    
    def set_custom(self, text: str = None, photo: str = None):
        if text is not None:
            self.custom_text = text
        if photo is not None:
            self.custom_photo = photo
    
    def reset(self):
        self.custom_text = None
        self.custom_photo = None

class MessageCustomization:
    def __init__(self):
        self.templates = {}
        self.init_defaults()
    
    def init_defaults(self):
        self.templates['welcome_pm'] = MessageTemplate('welcome_pm', "👋 Добро пожаловать в Puls Chat Manager!\n\nВыберите раздел в меню ниже 👇")
        self.templates['welcome_group'] = MessageTemplate('welcome_group', "👋 Puls Chat Manager\n\n/rules - Правила\n/stats - Моя статистика\n/top - Топ активных\n/profile - Профиль пользователя\n/group - Управление группой\n/puls - Проверка пинга\n/mute [время] [причина] - замутить\n/unmute - размутить\n/ban [время] [причина] - забанить\n/unban - разбанить\n/kick [причина] - кикнуть\n/warn [причина] - предупредить\n/mods - список модераторов")
        self.templates['profile_header'] = MessageTemplate('profile_header', "Профиль {premium_emoji} {name}")
        self.templates['profile_id'] = MessageTemplate('profile_id', "🆔 ID: <code>{global_id}</code>")
        self.templates['profile_first_seen'] = MessageTemplate('profile_first_seen', "📅 Впервые замечен: {first_seen}")
        self.templates['profile_premium'] = MessageTemplate('profile_premium', "⭐ Премиум пользователь")
        self.templates['profile_antispam'] = MessageTemplate('profile_antispam', "🛡️ Антиспам база Puls: {warnings}/{limit} предупреждений")
        self.templates['profile_stats_header'] = MessageTemplate('profile_stats_header', "📊 Статистика в этом чате:")
        self.templates['profile_day'] = MessageTemplate('profile_day', "• За день: {count} 💬")
        self.templates['profile_week'] = MessageTemplate('profile_week', "• За неделю: {count} 💬")
        self.templates['profile_month'] = MessageTemplate('profile_month', "• За месяц: {count} 💬")
        self.templates['profile_total'] = MessageTemplate('profile_total', "• Всего: {count} 💬")
        self.templates['profile_position'] = MessageTemplate('profile_position', "• Место в топе: {position}")
        self.templates['profile_no_stats'] = MessageTemplate('profile_no_stats', "📊 У пользователя пока нет сообщений в этом чате")
        self.templates['top_header'] = MessageTemplate('top_header', "🏆 Топ активных (всего сообщений):")
        self.templates['top_entry'] = MessageTemplate('top_entry', "{medal} {premium_emoji} {name} — {count} 💬{warnings}")
        self.templates['welcome_simple'] = MessageTemplate('welcome_simple', "Добро пожаловать, {premium_emoji} {name}!\n\n🆔 ID: <code>{global_id}</code>\n📅 Впервые замечен: {first_seen}\n{premium_line}🛡️ Антиспам база Puls: {warnings}/{limit} предупреждений\n\n• Username: @{username}\n• Telegram ID: <code>{user_id}</code>\n• Вошёл: {join_dt}\n• Место в топе: {position}")
        self.templates['group_linked'] = MessageTemplate('group_linked', "✅ Группа успешно привязана!\n\nНазвание: {title}\nID: <code>{chat_id}</code>\n\nТеперь вы можете настроить её в личных сообщениях с ботом.\nНажмите /start в ЛС, чтобы открыть главное меню.")
        self.templates['group_linked_pm'] = MessageTemplate('group_linked_pm', "✅ Группа {title} успешно привязана!\n\nТеперь она доступна в разделе «Настройки групп» в главном меню.")
        self.templates['group_unlinked'] = MessageTemplate('group_unlinked', "✅ Группа отвязана от вашего аккаунта.")
        self.templates['trigger_added'] = MessageTemplate('trigger_added', "✅ Триггер '{trigger}' добавлен ({count}/{max})")
        self.templates['trigger_exists'] = MessageTemplate('trigger_exists', "❌ Триггер '{trigger}' уже существует")
        self.templates['trigger_limit'] = MessageTemplate('trigger_limit', "❌ Достигнут лимит триггеров ({max})")
        self.templates['trigger_empty'] = MessageTemplate('trigger_empty', "❌ Триггер не может быть пустым")
        self.templates['trigger_too_long'] = MessageTemplate('trigger_too_long', "❌ Триггер слишком длинный! Максимум {max_len} символов")
        self.templates['trigger_too_many_words'] = MessageTemplate('trigger_too_many_words', "❌ Триггер должен содержать максимум {max_words} слово")
        self.templates['trigger_removed'] = MessageTemplate('trigger_removed', "✅ Триггер '{trigger}' удалён!")
        self.templates['spammer_detected'] = MessageTemplate('spammer_detected', "🚫 Обнаружен спамер в базе Пульса!\n\nПользователь: {user_link}\nПричина: {reason}\nПредупреждений: {warnings}/{limit}\n\nАдмины могут разблокировать в этом чате командой:\n<code>/unban {user_id}</code>")
        self.templates['spammer_pm'] = MessageTemplate('spammer_pm', "🚫 Вы были забанены в группе {chat_title}\n\nПричина: вы находитесь в антиспам базе Пульса.\nПредупреждений: {warnings}/{limit}\n\nДля выхода из антиспам базы обратитесь к разработчикам:\n{support_link}")
        self.templates['raid_detected'] = MessageTemplate('raid_detected', "⚠️ Обнаружена рейд-атака!\n\nЗа {window} сек вступило {count} новых участников.\nАвтоматически забанены новые участники на {duration}.")
    
    def get_template(self, key: str) -> MessageTemplate:
        return self.templates.get(key)
    
    def format_message(self, key: str, **kwargs) -> str:
        template = self.get_template(key)
        if template:
            try:
                return template.get_text().format(**kwargs)
            except:
                return template.get_text()
        return ""
    
    def get_photo(self, key: str) -> Optional[str]:
        template = self.get_template(key)
        if template:
            return template.get_photo()
        return None

customization = MessageCustomization()

def safe_html(text: str, preserve_tags: bool = True) -> str:
    if not text:
        return ""
    if not preserve_tags:
        return html.escape(text)
    allowed_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'tg-spoiler', 'a', 'strong', 'em', 'ins', 'strike', 'del']
    placeholders = {}
    text = re.sub(r'<blockquote expandable>', '!!BLOCKQUOTE_EXPANDABLE_OPEN!!', text, flags=re.IGNORECASE)
    text = re.sub(r'</blockquote>', '!!BLOCKQUOTE_CLOSE!!', text, flags=re.IGNORECASE)
    for i, tag in enumerate(allowed_tags):
        pattern_open = f'<{tag}>'
        pattern_close = f'</{tag}>'
        placeholder_open = f'!!TAG_{i}_OPEN!!'
        placeholder_close = f'!!TAG_{i}_CLOSE!!'
        text = re.sub(pattern_open, lambda m: (placeholders.setdefault(placeholder_open, f'<{tag}>') or placeholder_open), text, flags=re.IGNORECASE)
        text = re.sub(pattern_close, lambda m: (placeholders.setdefault(placeholder_close, f'</{tag}>') or placeholder_close), text, flags=re.IGNORECASE)
    text = html.escape(text)
    for ph, tag_html in placeholders.items():
        text = text.replace(ph, tag_html)
    text = text.replace('!!BLOCKQUOTE_EXPANDABLE_OPEN!!', '<blockquote expandable>')
    text = text.replace('!!BLOCKQUOTE_CLOSE!!', '</blockquote>')
    return text

def check_owner():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id
            state = kwargs.get('state')
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
            public = ['confirm_not_bot', 'agree_rules', 'go_to_pm', 'show_group_rules', 'my_stats', 'top_active']
            if any(callback.data.startswith(p) for p in public):
                return await func(callback, *args, **kwargs)
            return await check_owner()(func)(callback, *args, **kwargs)
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

def edit_only():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

def action_with_flood():
    def decorator(func):
        @wraps(func)
        async def wrapper(callback: CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id
            now = time.time()
            if now < button_blocked_users.get(user_id, 0):
                await callback.answer("⚠️ Подождите немного!", show_alert=True)
                return
            key = f"{user_id}_{callback.data}"
            user_button_presses[key] = [t for t in user_button_presses[key] if now - t < BUTTON_CHECK_TIME]
            if len(user_button_presses[key]) >= MAX_BUTTON_PRESSES:
                button_blocked_users[user_id] = now + BUTTON_BLOCK_TIME
                await callback.answer(f"⚠️ Слишком частые нажатия! Блокировка на {BUTTON_BLOCK_TIME} сек.", show_alert=True)
                return
            user_button_presses[key].append(now)
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

class CommandFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if not isinstance(event, Message) or not event.text or not event.text.startswith('/'):
            return await handler(event, data)
        user_id = event.from_user.id
        now = time.time()
        if now < command_blocked_users.get(user_id, 0):
            try:
                await event.delete()
            except:
                pass
            return
        user_commands[user_id] = [t for t in user_commands[user_id] if now - t < COMMAND_CHECK_TIME]
        if len(user_commands[user_id]) >= MAX_COMMANDS:
            command_blocked_users[user_id] = now + COMMAND_BLOCK_TIME
            try:
                await event.delete()
            except:
                pass
            return
        user_commands[user_id].append(now)
        return await handler(event, data)

class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if not isinstance(event, Message) or event.chat.type not in {'group', 'supergroup'}:
            return await handler(event, data)
        chat_id = event.chat.id
        user = event.from_user
        if user.is_bot:
            return await handler(event, data)
        db.update_message_count(chat_id, user.id)
        if db.get_puls_antispam_enabled(chat_id):
            is_spammer, reason, _ = check_spammer(user.id, chat_id)
            if is_spammer:
                await event.delete()
                await event.answer(customization.format_message('spammer_detected', user_link=f"<a href='tg://user?id={user.id}'>{user.full_name}</a>", reason=reason, warnings=0, limit=SPAM_WARN_LIMIT, user_id=user.id))
                try:
                    await bot.ban_chat_member(chat_id, user.id)
                except:
                    pass
                await send_to_log_group(chat_id, 'violation', f"🚫 Спамер {user.full_name}: {reason}")
                return
        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            return await handler(event, data)
        settings = db.get_antiflood_settings(chat_id)
        if not settings['enabled']:
            return await handler(event, data)
        now = time.time()
        msg_type = 'text' if event.text else 'media'
        key = f"{chat_id}_{user.id}"
        if key not in user_messages:
            user_messages[key] = deque(maxlen=50)
        while user_messages[key] and now - user_messages[key][0][0] > settings['time_window']:
            user_messages[key].popleft()
        text_count = sum(1 for t, mt in user_messages[key] if mt == 'text')
        media_count = sum(1 for t, mt in user_messages[key] if mt == 'media')
        if msg_type == 'media' and media_count >= settings['media_limit']:
            await self.handle_violation(event, chat_id, user, settings, "Медиа-флуд")
            return
        elif msg_type == 'text' and text_count >= settings['msg_limit']:
            await self.handle_violation(event, chat_id, user, settings, "Текстовый флуд")
            return
        text = event.text or event.caption or ""
        if db.get_links_filter_settings(chat_id)['enabled'] and check_links(text):
            await self.handle_violation(event, chat_id, user, settings, "Отправка ссылок")
            return
        if db.get_profanity_filter_settings(chat_id)['enabled'] and check_profanity(text):
            await self.handle_violation(event, chat_id, user, settings, "Использование нецензурной лексики")
            return
        user_messages[key].append((now, msg_type))
        return await handler(event, data)
    
    async def handle_violation(self, event, chat_id, user, settings, reason):
        warns = db.get_user_warns(chat_id, user.id)
        if warns['count'] < settings['warn_count']:
            new_count = db.add_user_warn(chat_id, user.id)
            await event.reply(f"⚠️ {user.full_name}, не флудите! Предупреждение {new_count}/{settings['warn_count']}")
            await send_to_log_group(chat_id, 'violation', f"⚠️ {user.full_name} получил предупреждение ({new_count}/{settings['warn_count']}): {reason}")
            return
        punish = settings['punish_after_warn'] if warns['count'] >= settings['warn_count'] else (settings['first_punish'] if warns['count'] == 0 else settings['repeat_punish'])
        duration = settings['punish_after_warn_duration'] if warns['count'] >= settings['warn_count'] else (settings['first_duration'] if warns['count'] == 0 else settings['repeat_duration'])
        db.reset_user_warns(chat_id, user.id)
        await self.apply_punishment(event, chat_id, user, punish, duration, reason)
    
    async def apply_punishment(self, event, chat_id, user, punish, duration, reason):
        try:
            message_link = get_message_link(chat_id, event.message_id)
            user_link = f"<a href='tg://user?id={user.id}'>{user.full_name}</a>"
            mod_link = f"<a href='tg://user?id={event.from_user.id}'>{event.from_user.full_name}</a>"
            duration_text = format_time(duration) if duration > 0 else "навсегда"
            
            if punish == 'mute':
                until = int(time.time() + duration) if duration > 0 else None
                await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
                
                mute_text = f"🔇 Мут\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n⏱ Длительность: {duration_text}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
                await event.reply(mute_text, reply_markup=get_lift_restriction_keyboard('mute', user.id, event.message_id))
                await send_to_log_group(chat_id, 'mod_action', mute_text)
                
            elif punish == 'ban':
                until = int(time.time() + duration) if duration > 0 else None
                await bot.ban_chat_member(chat_id, user.id, until_date=until)
                
                ban_text = f"⛔ Бан\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n⏱ Длительность: {duration_text}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
                await event.reply(ban_text, reply_markup=get_lift_restriction_keyboard('ban', user.id, event.message_id))
                await send_to_log_group(chat_id, 'mod_action', ban_text)
                
            elif punish == 'kick':
                await bot.ban_chat_member(chat_id, user.id)
                await bot.unban_chat_member(chat_id, user.id)
                
                kick_text = f"👢 Кик\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
                await event.reply(kick_text)
                await send_to_log_group(chat_id, 'mod_action', kick_text)
                
        except Exception as e:
            logger.error(f"Ошибка наказания: {e}")

class MaintenanceMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        global technical_maintenance
        if isinstance(event, (Message, CallbackQuery)):
            if event.from_user.id in ADMIN_IDS:
                return await handler(event, data)
        if technical_maintenance:
            if isinstance(event, Message):
                await event.reply(maintenance_message)
                return
            if isinstance(event, CallbackQuery):
                await event.answer("🛠 Бот временно остановлен на технические работы", show_alert=True)
                return
        return await handler(event, data)

def parse_time(time_str: str) -> int:
    if not time_str or time_str == '0' or time_str.lower() == 'навсегда':
        return 0
    time_str = time_str.lower()
    if time_str.isdigit():
        return int(time_str) * 60
    patterns = [(r'(\d+)\s*м', 60), (r'(\d+)\s*ч', 3600), (r'(\d+)\s*д', 86400)]
    for pattern, mult in patterns:
        match = re.search(pattern, time_str)
        if match:
            return int(match.group(1)) * mult
    return 0

def format_time(seconds: int) -> str:
    if seconds <= 0:
        return "навсегда"
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    mins = (seconds % 3600) // 60
    parts = []
    if days: parts.append(f"{days} дн")
    if hours: parts.append(f"{hours} ч")
    if mins: parts.append(f"{mins} мин")
    return " ".join(parts) if parts else "0 мин"

def generate_user_id() -> str:
    return ''.join(random.choices(string.digits, k=9))

def get_message_type(message: Message) -> str:
    if message.text: return 'text'
    if message.photo: return 'photo'
    if message.video: return 'video'
    if message.animation: return 'animation'
    if message.sticker: return 'sticker'
    return 'other'

def get_message_link(chat_id: int, message_id: int) -> str:
    cid = str(chat_id).replace('-100', '')
    return f"https://t.me/c/{cid}/{message_id}"

def get_premium_status_emoji(is_premium: bool) -> str:
    return "⭐" if is_premium else ""

async def add_premium_reaction(message: Message, emoji: str = "⭐"):
    try:
        await message.react([ReactionTypeEmoji(emoji=emoji)])
    except:
        pass

async def send_to_log_group(source_chat_id: int, event_type: str, data: str):
    try:
        log_group_info = db.get_source_chat_log_group(source_chat_id)
        if not log_group_info:
            return False
        settings = {
            'send_violations': log_group_info['send_violations'],
            'send_mod_actions': log_group_info['send_mod_actions'],
            'send_joins': log_group_info['send_joins'],
            'send_leaves': log_group_info['send_leaves'],
            'send_messages': log_group_info['send_messages']
        }
        type_map = {'violation': 'send_violations', 'mod_action': 'send_mod_actions', 'join': 'send_joins', 'leave': 'send_leaves', 'message': 'send_messages'}
        if not settings.get(type_map.get(event_type, ''), 1):
            return False
        await bot.send_message(log_group_info['log_group_id'], data, parse_mode="HTML")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки в лог-группу: {e}")
        return False

async def cleanup_old_data():
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        now = time.time()
        for uid in list(user_messages.keys()):
            user_messages[uid] = [(t, mt) for t, mt in user_messages[uid] if now - t < DATA_TTL]
            if not user_messages[uid]:
                del user_messages[uid]
        for key in list(user_button_presses.keys()):
            user_button_presses[key] = [t for t in user_button_presses[key] if now - t < DATA_TTL]
            if not user_button_presses[key]:
                del user_button_presses[key]
        for uid in list(user_commands.keys()):
            user_commands[uid] = [t for t in user_commands[uid] if now - t < DATA_TTL]
            if not user_commands[uid]:
                del user_commands[uid]
        for uid in list(command_blocked_users.keys()):
            if now > command_blocked_users[uid]:
                del command_blocked_users[uid]
        for uid in list(button_blocked_users.keys()):
            if now > button_blocked_users[uid]:
                del button_blocked_users[uid]
        for cid in list(raid_detector.keys()):
            raid_detector[cid] = [t for t in raid_detector[cid] if now - t < DATA_TTL]
            if not raid_detector[cid]:
                del raid_detector[cid]

async def clean_old_messages():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        for user_id in list(user_messages.keys()):
            user_messages[user_id] = [t for t in user_messages[user_id] if now - t[0] < SPAM_CHECK_TIME * 2]
            if not user_messages[user_id]:
                del user_messages[user_id]
        for key in list(user_button_presses.keys()):
            user_button_presses[key] = [t for t in user_button_presses[key] if now - t < BUTTON_CHECK_TIME * 10]
            if not user_button_presses[key]:
                del user_button_presses[key]

async def clean_old_logs():
    while True:
        await asyncio.sleep(86400)
        old_time = int(time.time()) - 30 * 86400
        with db.get_connection() as conn:
            conn.execute('DELETE FROM violation_logs WHERE timestamp < ?', (old_time,))
            conn.execute('DELETE FROM moderator_logs WHERE timestamp < ?', (old_time,))
            conn.commit()

def check_spammer(user_id: int, chat_id: int = None) -> Tuple[bool, Optional[str], int]:
    with spam_lock:
        if user_id in global_spammers:
            info = global_spammers[user_id]
            if chat_id and chat_id in info.get("разбанен_в", set()):
                return False, None, info.get("предупреждения", 0)
            return True, info.get("причина", "спам"), info.get("предупреждения", 0)
    return False, None, 0

def add_spammer_warning(user_id: int, reason: str = "спам") -> Tuple[bool, int, bool]:
    with spam_lock:
        if user_id not in global_spammers:
            global_spammers[user_id] = {"причина": reason, "когда_добавлен": int(time.time()), "разбанен_в": set(), "предупреждения": 1}
            return True, 1, False
        info = global_spammers[user_id]
        info["предупреждения"] = info.get("предупреждения", 0) + 1
        info["причина"] = reason
        return True, info["предупреждения"], info["предупреждения"] >= SPAM_WARN_LIMIT

def get_spammer_warnings(user_id: int) -> int:
    with spam_lock:
        if user_id in global_spammers:
            return global_spammers[user_id].get("предупреждения", 0)
    return 0

def unban_spammer_in_chat(user_id: int, chat_id: int) -> bool:
    with spam_lock:
        if user_id in global_spammers:
            if "разбанен_в" not in global_spammers[user_id]:
                global_spammers[user_id]["разбанен_в"] = set()
            global_spammers[user_id]["разбанен_в"].add(chat_id)
            return True
    return False

class Database:
    def __init__(self, db_path="puls_manager.db"):
        self.db_path = db_path
        self.init_db()
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS group_rules
                         (chat_id INTEGER PRIMARY KEY, owner_id INTEGER, rules_html TEXT,
                          rules_enabled INTEGER DEFAULT 1, welcome_enabled INTEGER DEFAULT 0,
                          welcome_text TEXT, welcome_photo_id TEXT, rules_auto_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300, chat_title TEXT, chat_username TEXT,
                          confirmation_type TEXT DEFAULT 'not_bot', puls_antispam_enabled INTEGER DEFAULT 1,
                          confirm_ban INTEGER DEFAULT 0, confirm_kick INTEGER DEFAULT 0, confirm_mute INTEGER DEFAULT 0,
                          max_warns INTEGER DEFAULT 3, punish_after_max_warns TEXT DEFAULT 'ban',
                          punish_after_max_warns_duration INTEGER DEFAULT 86400,
                          profanity_filter_enabled INTEGER DEFAULT 0, profanity_punishment TEXT DEFAULT 'warn',
                          profanity_duration INTEGER DEFAULT 0, links_filter_enabled INTEGER DEFAULT 0,
                          links_punishment TEXT DEFAULT 'warn', links_duration INTEGER DEFAULT 0,
                          raid_protection_enabled INTEGER DEFAULT 0, raid_join_limit INTEGER DEFAULT 5,
                          raid_time_window INTEGER DEFAULT 30, raid_punishment TEXT DEFAULT 'ban',
                          raid_duration INTEGER DEFAULT 3600, auto_comment_enabled INTEGER DEFAULT 0,
                          auto_comment_text TEXT, auto_comment_media_id TEXT, auto_comment_media_type TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_users
                         (user_id INTEGER PRIMARY KEY, global_id TEXT UNIQUE, first_seen INTEGER,
                          username TEXT, full_name TEXT, is_premium INTEGER DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS auto_responses
                         (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, trigger TEXT,
                          response TEXT, response_type TEXT DEFAULT 'text', media_id TEXT, created_at INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS rules_agreed
                         (chat_id INTEGER, user_id INTEGER, agreed_at INTEGER,
                          not_bot_confirmed INTEGER DEFAULT 0, rules_confirmed INTEGER DEFAULT 0,
                          PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS user_stats
                         (chat_id INTEGER, user_id INTEGER, join_date INTEGER,
                          all_messages INTEGER DEFAULT 0, month_messages INTEGER DEFAULT 0,
                          week_messages INTEGER DEFAULT 0, day_messages INTEGER DEFAULT 0,
                          last_active INTEGER, left_chat INTEGER DEFAULT 0,
                          PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS antiflood_settings
                         (chat_id INTEGER PRIMARY KEY, enabled INTEGER DEFAULT 0,
                          msg_limit INTEGER DEFAULT 5, media_limit INTEGER DEFAULT 3,
                          time_window INTEGER DEFAULT 10, warn_count INTEGER DEFAULT 3,
                          first_punish TEXT DEFAULT 'mute', first_duration INTEGER DEFAULT 60,
                          repeat_punish TEXT DEFAULT 'ban', repeat_duration INTEGER DEFAULT 3600,
                          punish_after_warn TEXT DEFAULT 'mute', punish_after_warn_duration INTEGER DEFAULT 3600)''')
            c.execute('''CREATE TABLE IF NOT EXISTS violation_logs
                         (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, user_id INTEGER,
                          user_name TEXT, reason TEXT, punishment TEXT, message_id INTEGER,
                          message_link TEXT, timestamp INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS user_warns
                         (chat_id INTEGER, user_id INTEGER, warn_count INTEGER DEFAULT 0,
                          last_warn_time INTEGER, PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS moderators
                         (chat_id INTEGER, user_id INTEGER, can_mute INTEGER DEFAULT 0,
                          can_kick INTEGER DEFAULT 0, can_ban INTEGER DEFAULT 0, can_warn INTEGER DEFAULT 0,
                          given_by INTEGER, given_at INTEGER, PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS moderator_logs
                         (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, moderator_id INTEGER,
                          moderator_name TEXT, action TEXT, target_id INTEGER, target_name TEXT,
                          duration INTEGER, reason TEXT, timestamp INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS log_groups
                         (log_group_id INTEGER PRIMARY KEY, owner_id INTEGER, group_title TEXT,
                          created_at INTEGER, is_active INTEGER DEFAULT 1)''')
            c.execute('''CREATE TABLE IF NOT EXISTS log_group_settings
                         (id INTEGER PRIMARY KEY AUTOINCREMENT, source_chat_id INTEGER, log_group_id INTEGER,
                          send_violations INTEGER DEFAULT 1, send_mod_actions INTEGER DEFAULT 1,
                          send_joins INTEGER DEFAULT 0, send_leaves INTEGER DEFAULT 0,
                          send_messages INTEGER DEFAULT 0, UNIQUE(source_chat_id, log_group_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_spammers
                         (user_id INTEGER PRIMARY KEY, reason TEXT, added_at INTEGER,
                          unbanned_in TEXT DEFAULT '[]', warnings INTEGER DEFAULT 1)''')
            c.execute('''CREATE TABLE IF NOT EXISTS custom_messages
                         (msg_key TEXT PRIMARY KEY, custom_text TEXT, custom_photo TEXT)''')
            c.execute('SELECT user_id, reason, added_at, unbanned_in, warnings FROM global_spammers')
            for row in c.fetchall():
                global_spammers[row[0]] = {"причина": row[1], "когда_добавлен": row[2],
                                           "разбанен_в": set(json.loads(row[3])) if row[3] else set(),
                                           "предупреждения": row[4] or 1}
            c.execute('SELECT msg_key, custom_text, custom_photo FROM custom_messages')
            for row in c.fetchall():
                tpl = customization.get_template(row[0])
                if tpl:
                    tpl.set_custom(row[1], row[2])
            conn.commit()
    
    def get_puls_antispam_enabled(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT puls_antispam_enabled FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return bool(r[0]) if r else True
    
    def set_puls_antispam_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET puls_antispam_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_confirmation_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT confirm_ban, confirm_kick, confirm_mute FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return {'ban': bool(r[0]), 'kick': bool(r[1]), 'mute': bool(r[2])} if r else {'ban': False, 'kick': False, 'mute': False}
    
    def set_confirmation_setting(self, chat_id, action, enabled):
        with self.get_connection() as conn:
            conn.execute(f'UPDATE group_rules SET confirm_{action} = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_profanity_filter_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT profanity_filter_enabled, profanity_punishment, profanity_duration FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return {'enabled': bool(r[0]), 'punishment': r[1] or 'warn', 'duration': r[2] or 0} if r else {'enabled': False, 'punishment': 'warn', 'duration': 0}
    
    def set_profanity_filter_settings(self, chat_id, enabled, punishment, duration):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET profanity_filter_enabled = ?, profanity_punishment = ?, profanity_duration = ? WHERE chat_id = ?',
                        (1 if enabled else 0, punishment, duration, chat_id))
            conn.commit()
    
    def get_links_filter_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT links_filter_enabled, links_punishment, links_duration FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return {'enabled': bool(r[0]), 'punishment': r[1] or 'warn', 'duration': r[2] or 0} if r else {'enabled': False, 'punishment': 'warn', 'duration': 0}
    
    def set_links_filter_settings(self, chat_id, enabled, punishment, duration):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET links_filter_enabled = ?, links_punishment = ?, links_duration = ? WHERE chat_id = ?',
                        (1 if enabled else 0, punishment, duration, chat_id))
            conn.commit()
    
    def get_raid_protection_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT raid_protection_enabled, raid_join_limit, raid_time_window, raid_punishment, raid_duration FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return {'enabled': bool(r[0]), 'limit': r[1] or 5, 'window': r[2] or 30, 'punishment': r[3] or 'ban', 'duration': r[4] or 3600} if r else {'enabled': False, 'limit': 5, 'window': 30, 'punishment': 'ban', 'duration': 3600}
    
    def set_raid_protection_settings(self, chat_id, enabled, limit, window, punishment, duration):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET raid_protection_enabled = ?, raid_join_limit = ?, raid_time_window = ?, raid_punishment = ?, raid_duration = ? WHERE chat_id = ?',
                        (1 if enabled else 0, limit, window, punishment, duration, chat_id))
            conn.commit()
    
    def get_auto_comment_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT auto_comment_enabled, auto_comment_text, auto_comment_media_id, auto_comment_media_type FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return {'enabled': bool(r[0]), 'text': r[1], 'media_id': r[2], 'media_type': r[3]} if r else {'enabled': False, 'text': None, 'media_id': None, 'media_type': None}
    
    def set_auto_comment_settings(self, chat_id, enabled, text, media_id, media_type):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET auto_comment_enabled = ?, auto_comment_text = ?, auto_comment_media_id = ?, auto_comment_media_type = ? WHERE chat_id = ?',
                        (1 if enabled else 0, text, media_id, media_type, chat_id))
            conn.commit()
    
    def get_max_warns_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT max_warns, punish_after_max_warns, punish_after_max_warns_duration FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return {'max_warns': r[0] or 3, 'punish': r[1] or 'ban', 'duration': r[2] or 86400} if r else {'max_warns': 3, 'punish': 'ban', 'duration': 86400}
    
    def set_max_warns_settings(self, chat_id, max_warns, punish, duration):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET max_warns = ?, punish_after_max_warns = ?, punish_after_max_warns_duration = ? WHERE chat_id = ?',
                        (max_warns, punish, duration, chat_id))
            conn.commit()
    
    def save_rules(self, chat_id, rules_html=None, owner_id=None, chat_title=None, chat_username=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            existing = c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
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
                    c.execute(f"UPDATE group_rules SET {', '.join(updates)} WHERE chat_id = ?", params + [chat_id])
            else:
                c.execute('''INSERT INTO group_rules (chat_id, owner_id, rules_html, chat_title, chat_username, confirmation_type, puls_antispam_enabled) 
                             VALUES (?, ?, ?, ?, ?, ?, 1)''', (chat_id, owner_id, rules_html, chat_title, chat_username, 'not_bot'))
            conn.commit()
    
    def set_rules_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET rules_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_rules_enabled(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT rules_enabled FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return bool(r[0]) if r else True
    
    def delete_rules(self, chat_id):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET rules_html = NULL WHERE chat_id = ?', (chat_id,))
            conn.commit()
    
    def save_welcome(self, chat_id, welcome_text=None, welcome_photo_id=None):
        with self.get_connection() as conn:
            if welcome_text is not None:
                conn.execute('UPDATE group_rules SET welcome_text = ? WHERE chat_id = ?', (welcome_text, chat_id))
            if welcome_photo_id is not None:
                conn.execute('UPDATE group_rules SET welcome_photo_id = ? WHERE chat_id = ?', (welcome_photo_id, chat_id))
            conn.commit()
    
    def get_welcome(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT welcome_text, welcome_photo_id FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return (r[0], r[1]) if r else (None, None)
    
    def set_welcome_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET welcome_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_welcome_enabled(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT welcome_enabled FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return bool(r[0]) if r else False
    
    def get_rules_html(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT rules_html FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return r[0] if r else None
    
    def set_rules_auto_settings(self, chat_id, enabled, interval):
        with self.get_connection() as conn:
            existing = conn.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            if existing:
                conn.execute('UPDATE group_rules SET rules_auto_enabled = ?, rules_interval = ? WHERE chat_id = ?', (1 if enabled else 0, interval, chat_id))
            else:
                conn.execute('INSERT INTO group_rules (chat_id, rules_auto_enabled, rules_interval) VALUES (?, ?, ?)', (chat_id, 1 if enabled else 0, interval))
            conn.commit()
    
    def get_rules_auto_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT rules_auto_enabled, rules_interval FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return (r[0], r[1]) if r else (0, 300)
    
    def update_last_rules(self, chat_id, message_id):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET last_rules_message_id = ?, last_rules_time = ? WHERE chat_id = ?', (message_id, int(time.time()), chat_id))
            conn.commit()
    
    def get_user_groups(self, user_id):
        with self.get_connection() as conn:
            return conn.execute('SELECT chat_id, chat_title FROM group_rules WHERE owner_id = ?', (user_id,)).fetchall()
    
    def get_all_chats(self):
        with self.get_connection() as conn:
            return conn.execute('SELECT chat_id, chat_title, chat_username, rules_enabled, welcome_enabled FROM group_rules ORDER BY chat_id').fetchall()
    
    def get_confirmation_type(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT confirmation_type FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
            return r[0] if r else 'not_bot'
    
    def set_confirmation_type(self, chat_id, conf_type):
        with self.get_connection() as conn:
            conn.execute('UPDATE group_rules SET confirmation_type = ? WHERE chat_id = ?', (conf_type, chat_id))
            conn.commit()
    
    def add_auto_response(self, chat_id, trigger, response, response_type='text', media_id=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            count = c.execute('SELECT COUNT(*) FROM auto_responses WHERE chat_id = ?', (chat_id,)).fetchone()[0]
            if count >= MAX_TRIGGERS:
                return False, customization.format_message('trigger_limit', max=MAX_TRIGGERS)
            if c.execute('SELECT 1 FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger)).fetchone():
                return False, customization.format_message('trigger_exists', trigger=trigger)
            c.execute('INSERT INTO auto_responses (chat_id, trigger, response, response_type, media_id, created_at) VALUES (?, ?, ?, ?, ?, ?)',
                     (chat_id, trigger, response, response_type, media_id, int(time.time())))
            conn.commit()
            return True, customization.format_message('trigger_added', trigger=trigger, count=count+1, max=MAX_TRIGGERS)
    
    def get_auto_responses(self, chat_id):
        with self.get_connection() as conn:
            return conn.execute('SELECT trigger, response, response_type, media_id FROM auto_responses WHERE chat_id = ? ORDER BY created_at', (chat_id,)).fetchall()
    
    def remove_auto_response(self, chat_id, trigger):
        with self.get_connection() as conn:
            conn.execute('DELETE FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger))
            conn.commit()
            return conn.rowcount > 0
    
    def mark_user_confirmed(self, chat_id, user_id, not_bot=False, rules=False):
        with self.get_connection() as conn:
            c = conn.cursor()
            existing = c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            if existing:
                nb = existing[0] or not_bot
                rl = existing[1] or rules
                c.execute('UPDATE rules_agreed SET not_bot_confirmed = ?, rules_confirmed = ?, agreed_at = ? WHERE chat_id = ? AND user_id = ?',
                         (1 if nb else 0, 1 if rl else 0, int(time.time()), chat_id, user_id))
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
            r = conn.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            if not r:
                return False
            nb, rl = r
            if conf_type == 'not_bot':
                return bool(nb)
            elif conf_type == 'rules':
                return bool(rl) and self.get_rules_html(chat_id) is not None
            return bool(nb) and bool(rl)
    
    def get_user_confirmation_status(self, chat_id, user_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            return (bool(r[0]), bool(r[1])) if r else (False, False)
    
    def get_or_create_global_user(self, user_id, username, full_name, is_premium=False):
        with self.get_connection() as conn:
            r = conn.execute('SELECT global_id, is_premium FROM global_users WHERE user_id = ?', (user_id,)).fetchone()
            if r:
                if r[1] != is_premium:
                    conn.execute('UPDATE global_users SET is_premium = ? WHERE user_id = ?', (1 if is_premium else 0, user_id))
                    conn.commit()
                return r[0]
            gid = generate_user_id()
            conn.execute('INSERT INTO global_users (user_id, global_id, first_seen, username, full_name, is_premium) VALUES (?, ?, ?, ?, ?, ?)',
                        (user_id, gid, int(time.time()), username, full_name, 1 if is_premium else 0))
            conn.commit()
            return gid
    
    def get_global_user(self, user_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT global_id, first_seen, username, full_name, is_premium FROM global_users WHERE user_id = ?', (user_id,)).fetchone()
            if r:
                return {'global_id': r[0], 'first_seen': r[1], 'username': r[2], 'full_name': r[3], 'is_premium': bool(r[4])}
            return None
    
    def add_user_stat(self, chat_id, user_id, join_date):
        with self.get_connection() as conn:
            conn.execute('INSERT OR REPLACE INTO user_stats (chat_id, user_id, join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat) VALUES (?, ?, ?, 0, 0, 0, 0, ?, 0)',
                        (chat_id, user_id, join_date, join_date))
            conn.commit()
    
    def update_message_count(self, chat_id, user_id):
        with self.get_connection() as conn:
            conn.execute('UPDATE user_stats SET all_messages = all_messages + 1, month_messages = month_messages + 1, week_messages = week_messages + 1, day_messages = day_messages + 1, last_active = ? WHERE chat_id = ? AND user_id = ?',
                        (int(time.time()), chat_id, user_id))
            if conn.rowcount == 0:
                conn.execute('INSERT INTO user_stats (chat_id, user_id, join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat) VALUES (?, ?, ?, 1, 1, 1, 1, ?, 0)',
                            (chat_id, user_id, int(time.time()), int(time.time())))
            conn.commit()
    
    def set_left_chat(self, chat_id, user_id):
        with self.get_connection() as conn:
            conn.execute('UPDATE user_stats SET left_chat = 1 WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            conn.commit()
    
    def get_user_stat(self, chat_id, user_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat FROM user_stats WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            if r:
                return {'join_date': r[0], 'all_messages': r[1], 'month_messages': r[2], 'week_messages': r[3], 'day_messages': r[4], 'last_active': r[5], 'left_chat': bool(r[6])}
            return None
    
    def get_top_messages(self, chat_id, period='all', limit=10):
        field = {'day': 'day_messages', 'week': 'week_messages', 'month': 'month_messages', 'all': 'all_messages'}.get(period, 'all_messages')
        with self.get_connection() as conn:
            return conn.execute(f'SELECT user_id, {field} FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY {field} DESC LIMIT ?', (chat_id, limit)).fetchall()
    
    def get_user_position(self, chat_id, user_id, period='all'):
        field = {'day': 'day_messages', 'week': 'week_messages', 'month': 'month_messages', 'all': 'all_messages'}.get(period, 'all_messages')
        with self.get_connection() as conn:
            users = conn.execute(f'SELECT user_id FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY {field} DESC', (chat_id,)).fetchall()
            for i, (uid,) in enumerate(users, 1):
                if uid == user_id:
                    return i
            return 0
    
    def get_antiflood_settings(self, chat_id):
        with self.get_connection() as conn:
            r = conn.execute('''SELECT enabled, msg_limit, media_limit, time_window, warn_count, first_punish, first_duration, repeat_punish, repeat_duration, punish_after_warn, punish_after_warn_duration
                                 FROM antiflood_settings WHERE chat_id = ?''', (chat_id,)).fetchone()
            if r:
                return {'enabled': bool(r[0]), 'msg_limit': r[1] or 5, 'media_limit': r[2] or 3, 'time_window': r[3] or 10, 'warn_count': r[4] or 3,
                        'first_punish': r[5] or 'mute', 'first_duration': r[6] or 60, 'repeat_punish': r[7] or 'ban', 'repeat_duration': r[8] or 3600,
                        'punish_after_warn': r[9] or 'mute', 'punish_after_warn_duration': r[10] or 3600}
            return {'enabled': False, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3,
                    'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600,
                    'punish_after_warn': 'mute', 'punish_after_warn_duration': 3600}
    
    def set_antiflood_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            conn.execute('INSERT OR REPLACE INTO antiflood_settings (chat_id, enabled) VALUES (?, ?)', (chat_id, 1 if enabled else 0))
            conn.commit()
    
    def save_antiflood_settings(self, chat_id, **kwargs):
        with self.get_connection() as conn:
            exists = conn.execute('SELECT 1 FROM antiflood_settings WHERE chat_id = ?', (chat_id,)).fetchone()
            if exists:
                if kwargs:
                    fields = ', '.join(f"{k}=?" for k in kwargs)
                    conn.execute(f'UPDATE antiflood_settings SET {fields} WHERE chat_id = ?', list(kwargs.values()) + [chat_id])
            else:
                defaults = {'enabled': 0, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3,
                            'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600,
                            'punish_after_warn': 'mute', 'punish_after_warn_duration': 3600}
                defaults.update(kwargs)
                conn.execute('''INSERT INTO antiflood_settings (chat_id, enabled, msg_limit, media_limit, time_window, warn_count, first_punish, first_duration, repeat_punish, repeat_duration, punish_after_warn, punish_after_warn_duration)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                             (chat_id, defaults['enabled'], defaults['msg_limit'], defaults['media_limit'], defaults['time_window'], defaults['warn_count'],
                              defaults['first_punish'], defaults['first_duration'], defaults['repeat_punish'], defaults['repeat_duration'],
                              defaults['punish_after_warn'], defaults['punish_after_warn_duration']))
            conn.commit()
    
    def get_user_warns(self, chat_id, user_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT warn_count, last_warn_time FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            return {'count': r[0], 'last_time': r[1]} if r else {'count': 0, 'last_time': 0}
    
    def add_user_warn(self, chat_id, user_id):
        with self.get_connection() as conn:
            existing = conn.execute('SELECT warn_count FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            if existing:
                new_count = existing[0] + 1
                conn.execute('UPDATE user_warns SET warn_count = ?, last_warn_time = ? WHERE chat_id = ? AND user_id = ?',
                            (new_count, int(time.time()), chat_id, user_id))
            else:
                new_count = 1
                conn.execute('INSERT INTO user_warns (chat_id, user_id, warn_count, last_warn_time) VALUES (?, ?, ?, ?)',
                            (chat_id, user_id, 1, int(time.time())))
            conn.commit()
            return new_count
    
    def reset_user_warns(self, chat_id, user_id):
        with self.get_connection() as conn:
            conn.execute('DELETE FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            conn.commit()
    
    def log_violation(self, chat_id, user_id, user_name, reason, punishment, message_id, message_link):
        with self.get_connection() as conn:
            conn.execute('INSERT INTO violation_logs (chat_id, user_id, user_name, reason, punishment, message_id, message_link, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                        (chat_id, user_id, user_name, reason, punishment, message_id, message_link, int(time.time())))
            conn.commit()
    
    def get_moderator_permissions(self, chat_id, user_id):
        with self.get_connection() as conn:
            r = conn.execute('SELECT can_mute, can_kick, can_ban, can_warn FROM moderators WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            return {'can_mute': bool(r[0]), 'can_kick': bool(r[1]), 'can_ban': bool(r[2]), 'can_warn': bool(r[3])} if r else {'can_mute': False, 'can_kick': False, 'can_ban': False, 'can_warn': False}
    
    def set_moderator_permission(self, chat_id, user_id, permission, value, given_by):
        with self.get_connection() as conn:
            c = conn.cursor()
            exists = c.execute('SELECT 1 FROM moderators WHERE chat_id = ? AND user_id = ?', (chat_id, user_id)).fetchone()
            if exists:
                c.execute(f'UPDATE moderators SET {permission} = ?, given_by = ?, given_at = ? WHERE chat_id = ? AND user_id = ?',
                         (1 if value else 0, given_by, int(time.time()), chat_id, user_id))
            else:
                defaults = {'can_mute': 0, 'can_kick': 0, 'can_ban': 0, 'can_warn': 0}
                defaults[permission] = 1 if value else 0
                c.execute('''INSERT INTO moderators (chat_id, user_id, can_mute, can_kick, can_ban, can_warn, given_by, given_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                         (chat_id, user_id, defaults['can_mute'], defaults['can_kick'], defaults['can_ban'], defaults['can_warn'], given_by, int(time.time())))
            conn.commit()
    
    def get_all_moderators(self, chat_id):
        with self.get_connection() as conn:
            return conn.execute('SELECT user_id, can_mute, can_kick, can_ban, can_warn FROM moderators WHERE chat_id = ?', (chat_id,)).fetchall()
    
    def log_moderator_action(self, chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason):
        with self.get_connection() as conn:
            conn.execute('''INSERT INTO moderator_logs (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, timestamp)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, int(time.time())))
            conn.commit()
    
    def create_log_group(self, log_group_id, owner_id, group_title):
        with self.get_connection() as conn:
            conn.execute('''INSERT OR REPLACE INTO log_groups (log_group_id, owner_id, group_title, created_at, is_active)
                             VALUES (?, ?, ?, ?, 1)''', (log_group_id, owner_id, group_title, int(time.time())))
            conn.commit()
            return True
    
    def get_log_group(self, log_group_id):
        with self.get_connection() as conn:
            return conn.execute('SELECT * FROM log_groups WHERE log_group_id = ?', (log_group_id,)).fetchone()
    
    def get_user_log_groups(self, user_id):
        with self.get_connection() as conn:
            return conn.execute('SELECT log_group_id, group_title FROM log_groups WHERE owner_id = ?', (user_id,)).fetchall()
    
    def set_source_chat_log_group(self, source_chat_id, log_group_id, settings=None):
        with self.get_connection() as conn:
            if settings:
                conn.execute('''INSERT OR REPLACE INTO log_group_settings (source_chat_id, log_group_id, send_violations, send_mod_actions, send_joins, send_leaves, send_messages)
                                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
                             (source_chat_id, log_group_id, settings.get('send_violations', 1), settings.get('send_mod_actions', 1),
                              settings.get('send_joins', 0), settings.get('send_leaves', 0), settings.get('send_messages', 0)))
            else:
                conn.execute('''INSERT OR REPLACE INTO log_group_settings (source_chat_id, log_group_id, send_violations, send_mod_actions)
                                 VALUES (?, ?, 1, 1)''', (source_chat_id, log_group_id))
            conn.commit()
    
    def get_source_chat_log_group(self, source_chat_id):
        with self.get_connection() as conn:
            return conn.execute('''SELECT lgs.*, lg.group_title FROM log_group_settings lgs JOIN log_groups lg ON lgs.log_group_id = lg.log_group_id
                                    WHERE lgs.source_chat_id = ?''', (source_chat_id,)).fetchone()
    
    def update_log_group_settings(self, source_chat_id, log_group_id, **kwargs):
        with self.get_connection() as conn:
            if kwargs:
                fields = ', '.join(f"{k}=?" for k in kwargs)
                conn.execute(f'UPDATE log_group_settings SET {fields} WHERE source_chat_id = ? AND log_group_id = ?',
                             list(kwargs.values()) + [source_chat_id, log_group_id])
                conn.commit()
                return True
            return False
    
    def remove_source_chat_log_group(self, source_chat_id):
        with self.get_connection() as conn:
            conn.execute('DELETE FROM log_group_settings WHERE source_chat_id = ?', (source_chat_id,))
            conn.commit()
    
    def save_custom_message(self, key, text=None, photo=None):
        with self.get_connection() as conn:
            if text is not None or photo is not None:
                existing = conn.execute('SELECT 1 FROM custom_messages WHERE msg_key = ?', (key,)).fetchone()
                if existing:
                    updates = []
                    params = []
                    if text is not None:
                        updates.append("custom_text = ?")
                        params.append(text)
                    if photo is not None:
                        updates.append("custom_photo = ?")
                        params.append(photo)
                    params.append(key)
                    conn.execute(f'UPDATE custom_messages SET {", ".join(updates)} WHERE msg_key = ?', params)
                else:
                    conn.execute('INSERT INTO custom_messages (msg_key, custom_text, custom_photo) VALUES (?, ?, ?)', (key, text, photo))
                conn.commit()
                return True
            return False
    
    def reset_custom_message(self, key):
        with self.get_connection() as conn:
            conn.execute('DELETE FROM custom_messages WHERE msg_key = ?', (key,))
            conn.commit()
            return True

db = Database()

# ==================== STATES ====================
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
    waiting_for_punish_after_warn_duration = State()

class AutoResponseStates(StatesGroup):
    waiting_for_trigger = State()
    waiting_for_response = State()

class LinksStates(StatesGroup):
    waiting_for_duration = State()

class ProfanityStates(StatesGroup):
    waiting_for_punishment = State()
    waiting_for_duration = State()

class RaidStates(StatesGroup):
    waiting_for_limit = State()
    waiting_for_window = State()
    waiting_for_punishment = State()
    waiting_for_duration = State()

class AutoCommentStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_media = State()

class MaintenanceStates(StatesGroup):
    waiting_for_message = State()

class AdminBroadcastStates(StatesGroup):
    waiting_for_target = State()
    waiting_for_text = State()
    waiting_for_media = State()

class PunishDurationStates(StatesGroup):
    waiting_for_duration = State()

class ModerationStates(StatesGroup):
    waiting_for_mute_duration = State()
    waiting_for_mute_reason = State()
    waiting_for_ban_duration = State()
    waiting_for_ban_reason = State()
    waiting_for_kick_reason = State()
    waiting_for_warn_reason = State()
    waiting_for_confirm_action = State()
    waiting_for_give_mute_user = State()

class LogGroupStates(StatesGroup):
    waiting_for_confirmation = State()

class CustomMessageStates(StatesGroup):
    waiting_for_message_key = State()
    waiting_for_new_text = State()
    waiting_for_new_photo = State()

# ==================== HELPER FUNCTIONS ====================
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

async def check_moderator_permission(chat_id, user_id, permission):
    if await is_creator(chat_id, user_id):
        return True
    perms = db.get_moderator_permissions(chat_id, user_id)
    return perms.get(permission, False)

def format_datetime(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def format_interval(seconds):
    if seconds < 60:
        return f"{seconds} сек"
    elif seconds < 3600:
        return f"{seconds // 60} мин"
    elif seconds < 86400:
        return f"{seconds // 3600} ч"
    else:
        return f"{seconds // 86400} дн"

async def rules_broadcast_task():
    while True:
        try:
            with db.get_connection() as conn:
                rows = conn.execute('SELECT chat_id, rules_auto_enabled, rules_interval, rules_html FROM group_rules WHERE rules_auto_enabled = 1 AND rules_html IS NOT NULL').fetchall()
                for chat_id, enabled, interval, rules_html in rows:
                    last = conn.execute('SELECT last_rules_time FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
                    if last and last[0] and int(time.time()) - last[0] < interval:
                        continue
                    try:
                        msg = await bot.send_message(chat_id, f"📢 Напоминание правил\n\n{rules_html}", parse_mode="HTML")
                        try:
                            await bot.pin_chat_message(chat_id, msg.message_id)
                        except:
                            pass
                        conn.execute('UPDATE group_rules SET last_rules_message_id = ?, last_rules_time = ? WHERE chat_id = ?', (msg.message_id, int(time.time()), chat_id))
                        conn.commit()
                    except Exception as e:
                        logger.error(f"Ошибка отправки правил: {e}")
        except Exception as e:
            logger.error(f"Ошибка в rules_broadcast_task: {e}")
        await asyncio.sleep(60)

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
                    conn.execute('UPDATE user_stats SET day_messages = 0 WHERE last_active < ?', (day_start.timestamp(),))
                    conn.execute('UPDATE user_stats SET week_messages = 0 WHERE last_active < ?', (week_start.timestamp(),))
                    conn.execute('UPDATE user_stats SET month_messages = 0 WHERE last_active < ?', (month_start.timestamp(),))
                    conn.commit()
            except Exception as e:
                logger.error(f"Ошибка сброса счетчиков: {e}")
            stats_updating = False
        await asyncio.sleep(3600)

async def notify_all_groups(text):
    with db.get_connection() as conn:
        for chat_id, in conn.execute('SELECT chat_id FROM group_rules').fetchall():
            try:
                await bot.send_message(chat_id, text)
                await asyncio.sleep(0.05)
            except:
                pass

# ==================== КЛАВИАТУРЫ ====================
def create_button(text: str, callback_data: str, color: str = None):
    return InlineKeyboardButton(text=text, callback_data=callback_data)

def get_back_keyboard(callback_data):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀ Назад", callback_data))
    return builder.as_markup()

def get_main_keyboard(is_group: bool = False, is_admin: bool = False):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("ℹ️ О боте", "about"))
    builder.add(create_button("🆘 Помощь", "help"))
    builder.add(create_button("⚙️ Настройки групп", "group_manage_main"))
    if is_group:
        builder.add(create_button("📜 Правила", "show_rules_group"))
        builder.add(create_button("📊 Статистика", "my_stats_group"))
        builder.add(create_button("🏆 Топ", "top_active_group"))
    if is_admin and not is_group:
        builder.add(create_button("👑 Админ панель", "admin_panel"))
    builder.adjust(2)
    builder.row(create_button("➕ Добавить в группу", f"add_to_group_{BOT_USERNAME}"))
    return builder.as_markup()

def get_group_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Правила", "manage_rules"))
    builder.add(create_button("👋 Приветствие", "manage_welcome"))
    builder.add(create_button("🔄 Авто-рассылка", "rules_auto"))
    builder.add(create_button("🚫 Антифлуд", "antiflood_manage"))
    builder.add(create_button("🛡️ Антиспам Пульса", "puls_antispam_manage"))
    builder.add(create_button("✅ Подтверждение действий", "confirmation_actions_manage"))
    builder.add(create_button("📋 Группа логов", "log_group_manage"))
    builder.add(create_button("🤖 Автоответчик", "auto_response_manage"))
    builder.add(create_button("🔗 Анти-ссылки", "links_manage"))
    builder.add(create_button("🚫 Антимат", "profanity_manage"))
    builder.add(create_button("🛡️ Рейд-защита", "raid_manage"))
    builder.add(create_button("💬 Авто-комментарий", "auto_comment_manage"))
    builder.add(create_button("✅ Подтверждение входа", "confirmation_manage"))
    builder.add(create_button("🛡️ Модераторы", "moderators_manage"))
    builder.add(create_button("❌ Отвязать", "unlink_group_confirm"))
    builder.add(create_button("◀ Назад", "back_to_groups"))
    builder.adjust(2)
    return builder.as_markup()

def get_links_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    builder.add(create_button(status, "toggle_links_filter"))
    builder.add(create_button(f"Наказание: {settings['punishment']}", "set_links_punishment"))
    if settings['punishment'] in ['mute', 'ban']:
        builder.add(create_button(f"Длительность: {format_time(settings['duration'])}", "set_links_duration"))
    builder.add(create_button("ℹ️ Что это?", "links_filter_info"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_profanity_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    builder.add(create_button(status, "toggle_profanity_filter"))
    builder.add(create_button(f"Наказание: {settings['punishment']}", "set_profanity_punishment"))
    if settings['punishment'] in ['mute', 'ban']:
        builder.add(create_button(f"Длительность: {format_time(settings['duration'])}", "set_profanity_duration"))
    builder.add(create_button("ℹ️ Что это?", "profanity_filter_info"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_raid_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    builder.add(create_button(status, "toggle_raid_protection"))
    builder.add(create_button(f"Лимит: {settings['limit']}", "set_raid_limit"))
    builder.add(create_button(f"Период: {settings['window']} сек", "set_raid_window"))
    builder.add(create_button(f"Наказание: {settings['punishment']}", "set_raid_punishment"))
    if settings['punishment'] in ['mute', 'ban']:
        builder.add(create_button(f"Длительность: {format_time(settings['duration'])}", "set_raid_duration"))
    builder.add(create_button("ℹ️ Что это?", "raid_protection_info"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_auto_comment_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    builder.add(create_button(status, "toggle_auto_comment"))
    builder.add(create_button("📝 Текст", "set_auto_comment_text"))
    builder.add(create_button("🖼 Медиа", "set_auto_comment_media"))
    if settings['text'] or settings['media_id']:
        builder.add(create_button("👁 Просмотр", "view_auto_comment"))
    builder.add(create_button("ℹ️ Что это?", "auto_comment_info"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_auto_response_keyboard(responses):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "add_auto_trigger"))
    if responses:
        builder.add(create_button("🗑 Удалить", "remove_auto_trigger"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_auto_response_remove_keyboard(responses):
    builder = InlineKeyboardBuilder()
    for i, (trigger, _, _, _) in enumerate(responses):
        short = trigger[:20] + "..." if len(trigger) > 20 else trigger
        builder.add(create_button(short, f"rem_trig_{i}"))
    builder.add(create_button("◀ Назад", "auto_response_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard(has_rules, rules_enabled):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Установить", "set_rules"))
    builder.add(create_button("📋 Готовые", "set_default_rules"))
    if has_rules:
        builder.add(create_button("👁 Посмотреть", "show_rules"))
        builder.add(create_button("✏️ Изменить", "edit_rules"))
        builder.add(create_button("🗑 Удалить", "delete_rules_confirm"))
        status = "✅ Включить" if not rules_enabled else "❌ Выключить"
        builder.add(create_button(status, "toggle_rules"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if enabled else "✅ Включить"
    builder.add(create_button(status, "toggle_welcome"))
    builder.add(create_button("📝 Текст", "set_welcome_text"))
    builder.add(create_button("🖼 Фото", "set_welcome_photo"))
    builder.add(create_button("👁 Посмотреть", "show_welcome"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if enabled else "✅ Включить"
    builder.add(create_button(status, "toggle_rules_auto"))
    builder.add(create_button("⏱ Интервал", "set_interval"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    builder.add(create_button(status, "toggle_antiflood"))
    builder.add(create_button(f"📝 Текст: {settings['msg_limit']}", "set_msg_limit"))
    builder.add(create_button(f"🎬 Медиа: {settings['media_limit']}", "set_media_limit"))
    builder.add(create_button(f"⏱ Период: {settings['time_window']} сек", "set_window"))
    builder.add(create_button(f"⚠️ Предупреждений: {settings['warn_count']}", "set_warn_count"))
    builder.add(create_button("🔇 Первое", "set_first_punish"))
    builder.add(create_button("🔊 Повторное", "set_repeat_punish"))
    builder.add(create_button("⚠️ После варнов", "set_punish_after_warn"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_punish_type_keyboard(punish_type):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"punish_warn_{punish_type}"))
    builder.add(create_button("🔇 Мут", f"punish_mute_{punish_type}"))
    builder.add(create_button("👢 Кик", f"punish_kick_{punish_type}"))
    builder.add(create_button("⛔ Бан", f"punish_ban_{punish_type}"))
    builder.add(create_button("◀ Назад", "antiflood_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_punishment_keyboard(action):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"{action}_warn"))
    builder.add(create_button("🔇 Mute", f"{action}_mute"))
    builder.add(create_button("👢 Kick", f"{action}_kick"))
    builder.add(create_button("⛔ Ban", f"{action}_ban"))
    builder.add(create_button("◀ Назад", f"{action}_back"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirmation_actions_keyboard(settings):
    builder = InlineKeyboardBuilder()
    ban = "✅" if settings['ban'] else "❌"
    kick = "✅" if settings['kick'] else "❌"
    mute = "✅" if settings['mute'] else "❌"
    builder.add(create_button(f"{ban} Подтверждение бана", "toggle_confirm_ban"))
    builder.add(create_button(f"{kick} Подтверждение кика", "toggle_confirm_kick"))
    builder.add(create_button(f"{mute} Подтверждение мута", "toggle_confirm_mute"))
    builder.add(create_button("ℹ️ Что это?", "confirmation_actions_info"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_puls_antispam_keyboard(enabled):
    builder = InlineKeyboardBuilder()
    status = "❌ Выключить" if enabled else "✅ Включить"
    builder.add(create_button(status, "toggle_puls_antispam"))
    builder.add(create_button("ℹ️ Что это?", "puls_antispam_info"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_log_group_manage_keyboard(has_log_group, log_group_info=None):
    builder = InlineKeyboardBuilder()
    if has_log_group:
        builder.add(create_button("📊 Настройки логов", "log_group_settings"))
        builder.add(create_button("🔄 Отвязать", "unlink_log_group"))
        builder.add(create_button("👁 Инфо", "log_group_info"))
    else:
        builder.add(create_button("➕ Привязать группу логов", "link_log_group"))
        builder.add(create_button("ℹ️ Как создать", "log_group_help"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_log_settings_keyboard(settings):
    builder = InlineKeyboardBuilder()
    v = "✅" if settings.get('send_violations', 1) else "❌"
    m = "✅" if settings.get('send_mod_actions', 1) else "❌"
    j = "✅" if settings.get('send_joins', 0) else "❌"
    l = "✅" if settings.get('send_leaves', 0) else "❌"
    msg = "✅" if settings.get('send_messages', 0) else "❌"
    builder.add(create_button(f"{v} Нарушения", "toggle_log_violations"))
    builder.add(create_button(f"{m} Действия модов", "toggle_log_mod"))
    builder.add(create_button(f"{j} Входы", "toggle_log_joins"))
    builder.add(create_button(f"{l} Выходы", "toggle_log_leaves"))
    builder.add(create_button(f"{msg} Сообщения", "toggle_log_messages"))
    builder.add(create_button("◀ Назад", "log_group_manage"))
    builder.adjust(2)
    return builder.as-markup()

def get_moderators_manage_keyboard(moderators):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Дать права", "give_mod_rights"))
    if moderators:
        builder.add(create_button("❌ Забрать права", "remove_mod_rights"))
    builder.add(create_button("👁 Список", "list_moderators"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_mod_rights_keyboard(user_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔇 Мут", f"give_mute_{user_id}"))
    builder.add(create_button("👢 Кик", f"give_kick_{user_id}"))
    builder.add(create_button("⛔ Бан", f"give_ban_{user_id}"))
    builder.add(create_button("⚠️ Варн", f"give_warn_{user_id}"))
    builder.add(create_button("◀ Назад", "moderators_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirmation_keyboard(current_type, has_rules):
    builder = InlineKeyboardBuilder()
    disabled = "🚫 Отключено" + (" ✅" if current_type == 'disabled' else "")
    builder.add(create_button(disabled, "confirmation_disabled"))
    not_bot = "🤖 Только не бот" + (" ✅" if current_type == 'not_bot' else "")
    builder.add(create_button(not_bot, "confirmation_not_bot"))
    rules = "📜 Только правила" + (" ✅" if current_type == 'rules' else "")
    if not has_rules:
        rules = "❌ " + rules
    builder.add(create_button(rules, "confirmation_rules" if has_rules else "confirmation_disabled"))
    both = "2️⃣ Оба шага" + (" ✅" if current_type == 'both' else "")
    if not has_rules:
        both = "❌ " + both
    builder.add(create_button(both, "confirmation_both" if has_rules else "confirmation_disabled"))
    builder.add(create_button("◀ Назад", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_link_group_keyboard(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Привязать", f"link_group_{chat_id}"))
    builder.add(create_button("🚫 Отмена", "cancel_link"))
    builder.adjust(2)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("❌ Отвязать", f"unlink_group_{chat_id}"))
    builder.add(create_button("🚫 Отмена", "group_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_action_keyboard(action, user_id, duration=None, reason=None):
    builder = InlineKeyboardBuilder()
    data = f"confirm_{action}_{user_id}"
    if duration:
        data += f"_{duration}"
    if reason:
        data += f"_{reason[:20]}"
    builder.add(create_button("✅ Подтверждаю", f"{data}_yes"))
    builder.add(create_button("❌ Отмена", f"{data}_no"))
    builder.adjust(2)
    return builder.as_markup()

def get_lift_restriction_keyboard(action, user_id, message_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔓 Снять ограничение", f"lift_{action}_{user_id}_{message_id}"))
    return builder.as_markup()

def get_pm_link_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("💬 Перейти в ЛС", "go_to_pm"))
    return builder.as_markup()

def get_welcome_buttons(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📜 Правила", f"show_group_rules_{chat_id}"))
    builder.add(create_button("📊 Моя статистика", f"my_stats_{chat_id}"))
    builder.add(create_button("🏆 Топ", f"top_active_{chat_id}"))
    builder.adjust(3)
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id, user_id, msg_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}"))
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id, user_id, msg_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Согласен", f"agree_rules_{chat_id}_{user_id}_{msg_id}"))
    return builder.as_markup()

def get_admin_custom_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Тексты", "admin_custom_texts"))
    builder.add(create_button("🖼 Фото", "admin_custom_photos"))
    builder.add(create_button("🔄 Сбросить всё", "admin_custom_reset_all"))
    builder.add(create_button("◀ Назад", "admin_panel"))
    builder.adjust(2)
    return builder.as_markup()

def get_texts_list_keyboard(page=0):
    templates = [(k, v.get_text()[:40] + "..." if len(v.get_text()) > 40 else v.get_text(), v.get_photo() is not None) for k, v in customization.templates.items()]
    per_page = 10
    start = page * per_page
    end = start + per_page
    current = templates[start:end]
    builder = InlineKeyboardBuilder()
    for key, preview, has_photo in current:
        icon = "🖼" if has_photo else "📝"
        builder.add(create_button(f"{icon} {key}", f"edit_text_{key}"))
    nav = []
    if page > 0:
        nav.append(create_button("←", f"texts_page_{page-1}"))
    if end < len(templates):
        nav.append(create_button("→", f"texts_page_{page+1}"))
    if nav:
        builder.row(*nav)
    builder.add(create_button("◀ Назад", "admin_custom"))
    builder.adjust(1)
    return builder.as_markup()

def get_photos_list_keyboard(page=0):
    templates = [(k, v.get_text()[:30]) for k, v in customization.templates.items() if v.get_photo()]
    per_page = 10
    start = page * per_page
    end = start + per_page
    current = templates[start:end]
    builder = InlineKeyboardBuilder()
    for key, preview in current:
        builder.add(create_button(f"🖼 {key}", f"edit_photo_{key}"))
    nav = []
    if page > 0:
        nav.append(create_button("←", f"photos_page_{page-1}"))
    if end < len(templates):
        nav.append(create_button("→", f"photos_page_{page+1}"))
    if nav:
        builder.row(*nav)
    builder.add(create_button("◀ Назад", "admin_custom"))
    builder.adjust(1)
    return builder.as_markup()

# ==================== ОСНОВНЫЕ ХЕНДЛЕРЫ ====================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    is_premium = getattr(message.from_user, 'is_premium', False)
    db.get_or_create_global_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name or "", is_premium)
    is_admin = message.from_user.id in ADMIN_IDS
    is_group = message.chat.type != 'private'
    if message.chat.type == 'private':
        welcome = customization.get_template('welcome_pm').get_text()
    else:
        welcome = customization.get_template('welcome_group').get_text()
    photo = customization.get_photo('welcome_pm' if message.chat.type == 'private' else 'welcome_group')
    if photo:
        await bot.send_photo(message.chat.id, photo, caption=welcome, reply_markup=get_main_keyboard(is_group, is_admin), parse_mode="HTML")
    else:
        await message.answer(welcome, reply_markup=get_main_keyboard(is_group, is_admin), parse_mode="HTML")
    await add_premium_reaction(message, "⭐")

@dp.message(Command("groupsettings"))
@pm_only()
async def cmd_group_settings(message: Message):
    groups = db.get_user_groups(message.from_user.id)
    if not groups:
        await message.answer("❌ У вас нет привязанных групп.\nДобавьте бота в группу и напишите /group в той группе.")
        return
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}"))
    builder.add(create_button("◀ Назад", "back_to_main"))
    builder.adjust(1)
    await message.answer("📱 Ваши группы:", reply_markup=builder.as_markup())
    await add_premium_reaction(message, "📱")

@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["пульс", "понг"]))
async def cmd_ping(message: Message):
    start = time.time()
    msg = await message.reply("⏳ Измеряем пинг...")
    ping = round((time.time() - start) * 1000)
    await msg.edit_text(f"📡 Пинг: {ping} мс\n⏱ Время ответа: {ping/1000:.2f} сек", parse_mode="HTML")
    await add_premium_reaction(message, "📡")

@dp.message(Command("stats"))
@group_only()
async def cmd_stats(message: Message):
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    chat_id, user = message.chat.id, message.from_user
    is_premium = getattr(user, 'is_premium', False)
    gid = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
    gu = db.get_global_user(user.id)
    stat = db.get_user_stat(chat_id, user.id)
    pos = db.get_user_position(chat_id, user.id, 'all')
    warns = get_spammer_warnings(user.id)
    emoji = get_premium_status_emoji(gu['is_premium'])
    if not stat:
        text = (f"Профиль {emoji} {user.full_name}\n\n" +
                f"🆔 ID: <code>{gu['global_id']}</code>\n" +
                f"📅 Впервые замечен: {format_datetime(gu['first_seen'])}\n" +
                ("⭐ Премиум пользователь\n" if gu['is_premium'] else "") +
                f"🛡️ Антиспам база Puls: {warns}/{SPAM_WARN_LIMIT} предупреждений\n\n" +
                "📊 У пользователя пока нет сообщений в этом чате")
    else:
        text = (f"Профиль {emoji} {user.full_name}\n\n" +
                f"🆔 ID: <code>{gu['global_id']}</code>\n" +
                f"📅 Впервые замечен: {format_datetime(gu['first_seen'])}\n" +
                ("⭐ Премиум пользователь\n" if gu['is_premium'] else "") +
                f"🛡️ Антиспам база Puls: {warns}/{SPAM_WARN_LIMIT} предупреждений\n\n" +
                "📊 Статистика в этом чате:\n" +
                f"• За день: {stat['day_messages']} 💬\n" +
                f"• За неделю: {stat['week_messages']} 💬\n" +
                f"• За месяц: {stat['month_messages']} 💬\n" +
                f"• Всего: {stat['all_messages']} 💬\n" +
                f"• Место в топе: {pos}")
    await message.reply(text, parse_mode="HTML")
    await add_premium_reaction(message, "📊")

@dp.message(Command("top"))
@group_only()
async def cmd_top(message: Message):
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    top = db.get_top_messages(message.chat.id, limit=10)
    if not top:
        await message.reply("📊 В этом чате пока нет сообщений")
        return
    text = "🏆 Топ активных (всего сообщений):\n\n"
    for i, (uid, count) in enumerate(top, 1):
        try:
            member = await bot.get_chat_member(message.chat.id, uid)
            name = member.user.full_name
            emoji = get_premium_status_emoji(getattr(member.user, 'is_premium', False))
            warns = get_spammer_warnings(uid)
        except:
            name = f"ID {uid}"
            emoji = ""
            warns = 0
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        warning = f" ⚠️{warns}" if warns > 0 else ""
        text += f"{medal} {emoji} {name} — {count} 💬{warning}\n"
    await message.reply(text, parse_mode="HTML")
    await add_premium_reaction(message, "🏆")

@dp.message(Command("profile"))
@group_only()
async def cmd_profile(message: Message):
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя, чтобы увидеть его профиль")
        return
    target = message.reply_to_message.from_user
    chat_id = message.chat.id
    is_premium = getattr(target, 'is_premium', False)
    gid = db.get_or_create_global_user(target.id, target.username or "", target.full_name or "", is_premium)
    gu = db.get_global_user(target.id)
    stat = db.get_user_stat(chat_id, target.id)
    pos = db.get_user_position(chat_id, target.id, 'all')
    warns = get_spammer_warnings(target.id)
    emoji = get_premium_status_emoji(gu['is_premium'])
    if not stat:
        text = (f"Профиль {emoji} {target.full_name}\n\n" +
                f"🆔 ID: <code>{gu['global_id']}</code>\n" +
                f"📅 Впервые замечен: {format_datetime(gu['first_seen'])}\n" +
                ("⭐ Премиум пользователь\n" if gu['is_premium'] else "") +
                f"🛡️ Антиспам база Puls: {warns}/{SPAM_WARN_LIMIT} предупреждений\n\n" +
                "📊 У пользователя пока нет сообщений в этом чате")
    else:
        text = (f"Профиль {emoji} {target.full_name}\n\n" +
                f"🆔 ID: <code>{gu['global_id']}</code>\n" +
                f"📅 Впервые замечен: {format_datetime(gu['first_seen'])}\n" +
                ("⭐ Премиум пользователь\n" if gu['is_premium'] else "") +
                f"🛡️ Антиспам база Puls: {warns}/{SPAM_WARN_LIMIT} предупреждений\n\n" +
                "📊 Статистика в этом чате:\n" +
                f"• За день: {stat['day_messages']} 💬\n" +
                f"• За неделю: {stat['week_messages']} 💬\n" +
                f"• За месяц: {stat['month_messages']} 💬\n" +
                f"• Всего: {stat['all_messages']} 💬\n" +
                f"• Место в топе: {pos}")
    await message.reply(text, parse_mode="HTML")
    await add_premium_reaction(message, "👤")

@dp.message(Command("rules"))
@group_only()
async def cmd_rules(message: Message):
    rules = db.get_rules_html(message.chat.id)
    if rules and db.get_rules_enabled(message.chat.id):
        await message.reply(rules, parse_mode="HTML")
        await add_premium_reaction(message, "📜")
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
        r = conn.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
        owner = r[0] if r else None
    if owner == user_id:
        await message.answer("✅ Группа уже привязана к вашему аккаунту.\n\nВсе настройки доступны в личных сообщениях с ботом.\nНажмите кнопку ниже, чтобы перейти в ЛС.", reply_markup=get_pm_link_keyboard())
    else:
        await message.answer("❌ Группа ещё не привязана к вашему аккаунту.\n\nНажмите кнопку ниже, чтобы привязать группу.\nПосле привязки вы сможете настраивать бота в ЛС.", reply_markup=get_link_group_keyboard(chat_id))
    await add_premium_reaction(message, "⚙️")

@dp.callback_query(F.data.startswith("link_group_"))
@edit_only()
@check_owner()
async def link_group(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    if not await is_creator(chat_id, user_id):
        await callback.answer("❌ Вы не создатель этой группы!", show_alert=True)
        return
    try:
        chat = await bot.get_chat(chat_id)
        title = chat.title or "Группа"
        username = chat.username
    except:
        title, username = "Группа", None
    db.save_rules(chat_id, owner_id=user_id, chat_title=title, chat_username=username)
    await callback.message.edit_text(customization.format_message('group_linked', title=title, chat_id=chat_id), parse_mode="HTML")
    await callback.answer("✅ Группа привязана!")
    try:
        await bot.send_message(user_id, customization.format_message('group_linked_pm', title=title), parse_mode="HTML")
    except:
        pass

@dp.callback_query(F.data == "cancel_link")
@edit_only()
@check_owner()
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.message(Command("mute"))
@group_only()
async def cmd_mute(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await check_moderator_permission(chat_id, user_id, 'can_mute'):
        await message.answer("❌ У вас нет права мутить пользователей!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которого хотите замутить")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя мутить бота!")
        return
    args = message.text.split(maxsplit=2)
    duration = parse_time(args[1]) if len(args) > 1 else 0
    reason = args[2] if len(args) > 2 else "Не указана"
    if db.get_confirmation_settings(chat_id).get('mute', False):
        await state.update_data(action='mute', target_id=target.id, target_name=target.full_name, duration=duration, reason=reason)
        duration_line = f"⏱ Длительность: {format_time(duration)}\n" if duration else ""
        await message.answer(f"⚠️ Подтвердите действие\n\nВы хотите замутить {target.full_name}\n{duration_line}Причина: {reason}\n\nПодтвердите действие:", reply_markup=get_confirm_action_keyboard('mute', target.id, duration, reason), parse_mode="HTML")
        await state.set_state(ModerationStates.waiting_for_confirm_action)
        return
    await execute_mute(chat_id, target.id, target.full_name, duration, reason, message.from_user, message.message_id)

async def execute_mute(chat_id, target_id, target_name, duration, reason, moderator, message_id):
    try:
        until = int(time.time() + duration) if duration > 0 else None
        await bot.restrict_chat_member(chat_id, target_id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
        message_link = get_message_link(chat_id, message_id)
        user_link = f"<a href='tg://user?id={target_id}'>{target_name}</a>"
        mod_link = f"<a href='tg://user?id={moderator.id}'>{moderator.full_name}</a>"
        duration_text = format_time(duration) if duration > 0 else "навсегда"
        mute_text = f"🔇 Мут\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n⏱ Длительность: {duration_text}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
        await bot.send_message(chat_id, mute_text, reply_markup=get_lift_restriction_keyboard('mute', target_id, message_id))
        db.log_moderator_action(chat_id, moderator.id, moderator.full_name, 'mute', target_id, target_name, duration, reason)
        await send_to_log_group(chat_id, 'mod_action', mute_text)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка при муте: {e}")

@dp.message(Command("unmute"))
@group_only()
async def cmd_unmute(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await check_moderator_permission(chat_id, user_id, 'can_mute'):
        await message.answer("❌ У вас нет права размучивать пользователей!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которого хотите размутить")
        return
    target = message.reply_to_message.from_user
    try:
        await bot.restrict_chat_member(chat_id, target.id, permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
        user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
        mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
        unmute_text = f"🔊 Размут\n\n👤 Пользователь: {user_link}\n👮 Модератор: {mod_link}"
        await message.answer(unmute_text, parse_mode="HTML")
        await send_to_log_group(chat_id, 'mod_action', unmute_text)
    except Exception as e:
        await message.answer(f"❌ Ошибка при размуте: {e}")

@dp.message(Command("ban"))
@group_only()
async def cmd_ban(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await check_moderator_permission(chat_id, user_id, 'can_ban'):
        await message.answer("❌ У вас нет права банить пользователей!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которого хотите забанить")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя банить бота!")
        return
    args = message.text.split(maxsplit=2)
    duration = parse_time(args[1]) if len(args) > 1 else 0
    reason = args[2] if len(args) > 2 else "Не указана"
    if db.get_confirmation_settings(chat_id).get('ban', False):
        await state.update_data(action='ban', target_id=target.id, target_name=target.full_name, duration=duration, reason=reason)
        duration_line = f"⏱ Длительность: {format_time(duration)}\n" if duration else ""
        await message.answer(f"⚠️ Подтвердите действие\n\nВы хотите забанить {target.full_name}\n{duration_line}Причина: {reason}\n\nПодтвердите действие:", reply_markup=get_confirm_action_keyboard('ban', target.id, duration, reason), parse_mode="HTML")
        await state.set_state(ModerationStates.waiting_for_confirm_action)
        return
    await execute_ban(chat_id, target.id, target.full_name, duration, reason, message.from_user, message.message_id)

async def execute_ban(chat_id, target_id, target_name, duration, reason, moderator, message_id):
    try:
        until = int(time.time() + duration) if duration > 0 else None
        await bot.ban_chat_member(chat_id, target_id, until_date=until)
        message_link = get_message_link(chat_id, message_id)
        user_link = f"<a href='tg://user?id={target_id}'>{target_name}</a>"
        mod_link = f"<a href='tg://user?id={moderator.id}'>{moderator.full_name}</a>"
        duration_text = format_time(duration) if duration > 0 else "навсегда"
        ban_text = f"⛔ Бан\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n⏱ Длительность: {duration_text}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
        await bot.send_message(chat_id, ban_text, reply_markup=get_lift_restriction_keyboard('ban', target_id, message_id))
        db.log_moderator_action(chat_id, moderator.id, moderator.full_name, 'ban', target_id, target_name, duration, reason)
        await send_to_log_group(chat_id, 'mod_action', ban_text)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка при бане: {e}")

@dp.message(Command("kick"))
@group_only()
async def cmd_kick(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await check_moderator_permission(chat_id, user_id, 'can_kick'):
        await message.answer("❌ У вас нет права кикать пользователей!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которого хотите кикнуть")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя кикать бота!")
        return
    args = message.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else "Не указана"
    if db.get_confirmation_settings(chat_id).get('kick', False):
        await state.update_data(action='kick', target_id=target.id, target_name=target.full_name, reason=reason)
        await message.answer(f"⚠️ Подтвердите действие\n\nВы хотите кикнуть {target.full_name}\nПричина: {reason}\n\nПодтвердите действие:", reply_markup=get_confirm_action_keyboard('kick', target.id, reason=reason), parse_mode="HTML")
        await state.set_state(ModerationStates.waiting_for_confirm_action)
        return
    await execute_kick(chat_id, target.id, target.full_name, reason, message.from_user, message.message_id)

async def execute_kick(chat_id, target_id, target_name, reason, moderator, message_id):
    try:
        await bot.ban_chat_member(chat_id, target_id)
        await bot.unban_chat_member(chat_id, target_id)
        message_link = get_message_link(chat_id, message_id)
        user_link = f"<a href='tg://user?id={target_id}'>{target_name}</a>"
        mod_link = f"<a href='tg://user?id={moderator.id}'>{moderator.full_name}</a>"
        kick_text = f"👢 Кик\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
        await bot.send_message(chat_id, kick_text)
        db.log_moderator_action(chat_id, moderator.id, moderator.full_name, 'kick', target_id, target_name, 0, reason)
        await send_to_log_group(chat_id, 'mod_action', kick_text)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка при кике: {e}")

@dp.message(Command("warn"))
@group_only()
async def cmd_warn(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await check_moderator_permission(chat_id, user_id, 'can_warn'):
        await message.answer("❌ У вас нет права выдавать предупреждения!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которого хотите предупредить")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя предупреждать бота!")
        return
    args = message.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else "Не указана"
    count = db.add_user_warn(chat_id, target.id)
    max_warns = db.get_max_warns_settings(chat_id)['max_warns']
    message_link = get_message_link(chat_id, message.message_id)
    user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
    mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
    warn_text = f"⚠️ Варн | {count}/{max_warns}\n\n👤 Нарушитель: {user_link}\n📝 Причина: {reason}\n👮 Модератор: {mod_link}\n\n🔗 <a href='{message_link}'>Перейти к сообщению</a>"
    await message.answer(warn_text, parse_mode="HTML")
    db.log_moderator_action(chat_id, message.from_user.id, message.from_user.full_name, 'warn', target.id, target.full_name, 0, reason)
    await send_to_log_group(chat_id, 'mod_action', warn_text)
    if count >= max_warns:
        ws = db.get_max_warns_settings(chat_id)
        await execute_ban(chat_id, target.id, target.full_name, ws['duration'], f"Превышение лимита варнов ({count}/{max_warns})", message.from_user, message.message_id)
        db.reset_user_warns(chat_id, target.id)

@dp.message(Command("unban"))
@group_only()
async def cmd_unban(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_admin(chat_id, user_id) and not await check_moderator_permission(chat_id, user_id, 'can_ban'):
        await message.answer("❌ У вас нет права разбанивать пользователей!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя или укажите ID")
        return
    target = message.reply_to_message.from_user
    try:
        await bot.unban_chat_member(chat_id, target.id)
        unban_spammer_in_chat(target.id, chat_id)
        user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
        mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
        unban_text = f"✅ Разбан\n\n👤 Пользователь: {user_link}\n👮 Модератор: {mod_link}"
        await message.answer(unban_text, parse_mode="HTML")
        await send_to_log_group(chat_id, 'mod_action', unban_text)
    except Exception as e:
        await message.answer(f"❌ Ошибка при разбане: {e}")

@dp.message(Command("mods"))
@group_only()
async def cmd_mods(message: Message):
    chat_id = message.chat.id
    mods = db.get_all_moderators(chat_id)
    text = "🛡️ Модераторы группы:\n\n"
    try:
        creator = await bot.get_chat(chat_id)
        cm = await bot.get_chat_member(chat_id, creator.id)
        text += f"👑 Владелец: {cm.user.full_name}\n\n"
    except:
        pass
    if mods:
        text += "👮 Назначенные модераторы:\n"
        for m in mods:
            try:
                u = await bot.get_chat_member(chat_id, m[0])
                rights = []
                if m[1]: rights.append("🔇 мут")
                if m[2]: rights.append("👢 кик")
                if m[3]: rights.append("⛔ бан")
                if m[4]: rights.append("⚠️ варн")
                text += f"• {u.user.full_name} - {', '.join(rights) if rights else 'нет прав'}\n"
            except:
                continue
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("give_mute"))
@group_only()
async def cmd_give_mute(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target.id, 'can_mute', True, message.from_user.id)
    user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
    mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
    await message.answer(f"✅ Пользователю {user_link} выдано право мутить", parse_mode="HTML")
    await send_to_log_group(chat_id, 'mod_action', f"🔇 {user_link} получил право мутить от {mod_link}")

@dp.message(Command("give_kick"))
@group_only()
async def cmd_give_kick(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target.id, 'can_kick', True, message.from_user.id)
    user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
    mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
    await message.answer(f"✅ Пользователю {user_link} выдано право кикать", parse_mode="HTML")
    await send_to_log_group(chat_id, 'mod_action', f"👢 {user_link} получил право кикать от {mod_link}")

@dp.message(Command("give_ban"))
@group_only()
async def cmd_give_ban(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target.id, 'can_ban', True, message.from_user.id)
    user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
    mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
    await message.answer(f"✅ Пользователю {user_link} выдано право банить", parse_mode="HTML")
    await send_to_log_group(chat_id, 'mod_action', f"⛔ {user_link} получил право банить от {mod_link}")

@dp.message(Command("give_warn"))
@group_only()
async def cmd_give_warn(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target = message.reply_to_message.from_user
    if target.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target.id, 'can_warn', True, message.from_user.id)
    user_link = f"<a href='tg://user?id={target.id}'>{target.full_name}</a>"
    mod_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
    await message.answer(f"✅ Пользователю {user_link} выдано право выдавать предупреждения", parse_mode="HTML")
    await send_to_log_group(chat_id, 'mod_action', f"⚠️ {user_link} получил право варнить от {mod_link}")

@dp.message(Command("ungive_mute"))
@group_only()
async def cmd_ungive_mute(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target.id, 'can_mute', False, message.from_user.id)
    await message.answer(f"✅ У пользователя {target.full_name} забрано право мутить")

@dp.message(Command("ungive_kick"))
@group_only()
async def cmd_ungive_kick(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target.id, 'can_kick', False, message.from_user.id)
    await message.answer(f"✅ У пользователя {target.full_name} забрано право кикать")

@dp.message(Command("ungive_ban"))
@group_only()
async def cmd_ungive_ban(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target.id, 'can_ban', False, message.from_user.id)
    await message.answer(f"✅ У пользователя {target.full_name} забрано право банить")

@dp.message(Command("ungive_warn"))
@group_only()
async def cmd_ungive_warn(message: Message):
    chat_id = message.chat.id
    if not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target.id, 'can_warn', False, message.from_user.id)
    await message.answer(f"✅ У пользователя {target.full_name} забрано право выдавать предупреждения")

@dp.message(Command("loggroup"))
@group_only()
async def cmd_loggroup(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может сделать эту группу группой логов!")
        return
    try:
        member = await bot.get_chat_member(chat_id, bot.id)
        if member.status not in ['administrator', 'creator']:
            await message.answer("❌ Бот не является администратором в этой группе!\n\nСначала выдайте боту права администратора.")
            return
    except:
        await message.answer("❌ Ошибка проверки прав бота!")
        return
    try:
        chat = await bot.get_chat(chat_id)
        if chat.linked_chat_id:
            await message.answer("❌ Эта группа привязана к каналу как обсуждение!\n\nГруппа логов должна быть обычной группой, не привязанной к каналу.")
            return
    except:
        pass
    await state.update_data(log_group_id=chat_id, log_group_title=message.chat.title)
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Запомнить", f"save_log_group_{chat_id}", "success"))
    builder.add(create_button("❌ Отмена", "cancel_save_log_group", "danger"))
    await message.answer(f"📋 Группа логов\n\nНазвание: {message.chat.title}\nID: <code>{chat_id}</code>\n\nЗапомнить эту группу как группу для логов?", reply_markup=builder.as_markup(), parse_mode="HTML")
    await state.set_state(LogGroupStates.waiting_for_confirmation)

@dp.callback_query(F.data.startswith("save_log_group_"))
@edit_only()
async def save_log_group(callback: CallbackQuery, state: FSMContext):
    log_group_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    data = await state.get_data()
    group_title = data.get('log_group_title', 'Группа логов')
    db.create_log_group(log_group_id, user_id, group_title)
    await callback.message.edit_text(f"✅ Группа логов сохранена!\n\nНазвание: {group_title}\nID: <code>{log_group_id}</code>\n\nТеперь вы можете привязать эту группу к вашим чатам в настройках.", parse_mode="HTML")
    await callback.answer("✅ Группа сохранена!")
    await state.clear()

@dp.callback_query(F.data == "cancel_save_log_group")
@edit_only()
async def cancel_save_log_group(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Отменено")
    await callback.answer()
    await state.clear()

@dp.message(F.new_chat_members)
async def on_bot_added(message: Message):
    bot_info = await bot.get_me()
    if any(m.id == bot_info.id for m in message.new_chat_members):
        await message.answer("👋 Спасибо, что добавили Puls Chat Manager!\n\nПока я работаю в ограниченном режиме.\nЧтобы я мог полностью защищать чат (правила, антифлуд, модерация):\n1️⃣ Сделайте меня администратором\n2️⃣ Напишите команду /group в этой группе\n\nВсе настройки будут доступны в ЛС с ботом.", parse_mode="HTML")

@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id, user = update.chat.id, update.new_chat_member.user
        db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", getattr(user, 'is_premium', False))
        raid = db.get_raid_protection_settings(chat_id)
        if raid['enabled']:
            now = time.time()
            raid_detector[chat_id] = [t for t in raid_detector[chat_id] if now - t < raid['window']]
            raid_detector[chat_id].append(now)
            if len(raid_detector[chat_id]) >= raid['limit']:
                raid_detector[chat_id].clear()
                await bot.send_message(chat_id, customization.format_message('raid_detected', window=raid['window'], count=raid['limit'], duration=format_time(raid['duration'])), parse_mode="HTML")
                await execute_ban(chat_id, user.id, user.full_name, raid['duration'], "Рейд-атака (массовое вступление)", bot, 0)
                return
        db.add_user_stat(chat_id, user.id, int(time.time()))
        user_link = f"<a href='tg://user?id={user.id}'>{user.full_name}</a>"
        await send_to_log_group(chat_id, 'join', f"👋 Вход\n\n👤 Пользователь: {user_link}\n🆔 ID: <code>{user.id}</code>")
        if check_spammer(user.id, chat_id)[0] and db.get_puls_antispam_enabled(chat_id):
            try:
                await bot.ban_chat_member(chat_id, user.id)
                await bot.send_message(chat_id, customization.format_message('spammer_detected', user_link=user_link, reason="спам-база", warnings=0, limit=SPAM_WARN_LIMIT, user_id=user.id), parse_mode="HTML")
                return
            except:
                pass
        await send_simple_welcome(chat_id, user)

@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    db.set_left_chat(update.chat.id, update.from_user.id)
    user_link = f"<a href='tg://user?id={update.from_user.id}'>{update.from_user.full_name}</a>"
    await send_to_log_group(update.chat.id, 'leave', f"👋 Выход\n\n👤 Пользователь: {user_link}\n🆔 ID: <code>{update.from_user.id}</code>")

async def send_simple_welcome(chat_id, user):
    is_premium = getattr(user, 'is_premium', False)
    gu = db.get_global_user(user.id) or {'global_id': generate_user_id(), 'first_seen': int(time.time()), 'is_premium': is_premium}
    stat = db.get_user_stat(chat_id, user.id)
    pos = db.get_user_position(chat_id, user.id, 'all')
    warns = get_spammer_warnings(user.id)
    welcome_text = customization.format_message('welcome_simple', premium_emoji=get_premium_status_emoji(gu['is_premium']), name=user.full_name,
            global_id=gu['global_id'], first_seen=format_datetime(gu['first_seen']),
            premium_line=("⭐ Премиум пользователь\n" if gu['is_premium'] else ""), warnings=warns, limit=SPAM_WARN_LIMIT,
            username=user.username or 'нет', user_id=user.id, join_dt=format_datetime(stat['join_date'] if stat else time.time()), position=pos)
    wtext, wphoto = db.get_welcome(chat_id)
    if wphoto:
        await bot.send_photo(chat_id, wphoto, caption=welcome_text + (f"\n\n{wtext}" if wtext else ""), reply_markup=get_welcome_buttons(chat_id), parse_mode="HTML")
    else:
        await bot.send_message(chat_id, welcome_text + (f"\n\n{wtext}" if wtext else ""), reply_markup=get_welcome_buttons(chat_id), parse_mode="HTML")

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    if message.text and message.text.startswith('/'):
        return
    text = (message.text or message.caption or "").lower().strip()
    responses = db.get_auto_responses(message.chat.id)
    for trigger, resp, rtype, media in responses:
        if trigger.lower() in text:
            try:
                if rtype == 'text':
                    await message.reply(resp or "", parse_mode="HTML", disable_notification=True)
                elif rtype == 'photo' and media:
                    await message.reply_photo(media, caption=resp or None, parse_mode="HTML")
                elif rtype == 'video' and media:
                    await message.reply_video(media, caption=resp or None, parse_mode="HTML")
                elif rtype == 'animation' and media:
                    await message.reply_animation(media, caption=resp or None, parse_mode="HTML")
                elif rtype == 'sticker' and media:
                    await message.reply_sticker(media)
                elif rtype == 'voice' and media:
                    await message.reply_voice(media)
                elif rtype == 'video_note' and media:
                    await message.reply_video_note(media)
                elif rtype == 'document' and media:
                    await message.reply_document(media, caption=resp or None, parse_mode="HTML")
                return
            except Exception as e:
                logger.error(f"Ошибка автоответа: {e}")

@dp.message(F.chat.type == "supergroup", F.forward_from_chat)
async def handle_channel_post(message: Message):
    chat_id = message.chat.id
    settings = db.get_auto_comment_settings(chat_id)
    if not settings['enabled']:
        return
    if not settings['text'] and not settings['media_id']:
        return
    channel = message.forward_from_chat
    if channel.username:
        post_link = f"https://t.me/{channel.username}/{message.message_id}"
    else:
        post_link = f"https://t.me/c/{str(channel.id).replace('-100', '')}/{message.message_id}"
    text = (settings['text'] or "").replace("{post_link}", post_link).replace("{channel_title}", channel.title or "Канал")
    try:
        if settings['media_id']:
            if settings['media_type'] == 'photo':
                await bot.send_photo(chat_id, settings['media_id'], caption=text, parse_mode="HTML")
            elif settings['media_type'] == 'video':
                await bot.send_video(chat_id, settings['media_id'], caption=text, parse_mode="HTML")
            elif settings['media_type'] == 'animation':
                await bot.send_animation(chat_id, settings['media_id'], caption=text, parse_mode="HTML")
            elif settings['media_type'] == 'sticker':
                await bot.send_sticker(chat_id, settings['media_id'])
        else:
            await bot.send_message(chat_id, text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка авто-комментария в чате {chat_id}: {e}")

@dp.callback_query(F.data == "back_to_main")
@edit_only()
@check_owner()
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    await callback.message.edit_text("👋 Главное меню\n\nВыберите раздел:", reply_markup=get_main_keyboard(is_group, is_admin))
    await callback.answer()

@dp.callback_query(F.data == "group_manage_main")
@edit_only()
@check_owner()
async def group_manage_main(callback: CallbackQuery):
    groups = db.get_user_groups(callback.from_user.id)
    if not groups:
        await callback.answer("❌ У вас нет привязанных групп!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}"))
    builder.add(create_button("◀ Назад", "back_to_main"))
    builder.adjust(1)
    await callback.message.edit_text("📱 Ваши группы\n\nВыберите группу для настройки:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("select_group_"))
@edit_only()
@check_owner()
async def select_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    if not await is_creator(chat_id, callback.from_user.id):
        await callback.answer("❌ Вы больше не являетесь создателем этой группы!", show_alert=True)
        return
    await state.update_data(selected_chat_id=chat_id, **{f"msg_owner_{callback.message.message_id}": callback.from_user.id})
    with db.get_connection() as conn:
        r = conn.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
        title = r[0] if r else "Группа"
    await callback.message.edit_text(f"⚙️ Настройка группы: {title}\n\nВыберите действие:", reply_markup=get_group_manage_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_groups")
@edit_only()
@check_owner()
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    groups = db.get_user_groups(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}"))
    builder.add(create_button("◀ Назад", "back_to_main"))
    builder.adjust(1)
    await callback.message.edit_text("📱 Ваши группы\n\nВыберите группу:", reply_markup=builder.as_markup())
    await callback.answer()

# ==================== НАСТРОЙКИ ГРУППЫ ====================
@dp.callback_query(F.data == "links_manage")
@edit_only()
@check_owner()
async def links_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_links_filter_settings(chat_id)
    status = "✅ Включен" if settings['enabled'] else "❌ Выключен"
    await callback.message.edit_text(f"🔗 Анти-ссылки\n\nСтатус: {status}\nНаказание: {settings['punishment']}\nДлительность: {format_time(settings['duration'])}\n\nБлокирует любые ссылки (http, https, t.me, telegram.me)", reply_markup=get_links_manage_keyboard(settings))
    await callback.answer()

@dp.callback_query(F.data == "toggle_links_filter")
@edit_only()
@check_owner()
async def toggle_links_filter(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_links_filter_settings(chat_id)
    db.set_links_filter_settings(chat_id, not s['enabled'], s['punishment'], s['duration'])
    await callback.answer(f"✅ Анти-ссылки {'включены' if not s['enabled'] else 'выключены'}!")
    await links_manage(callback, state)

@dp.callback_query(F.data == "set_links_punishment")
@edit_only()
@check_owner()
async def set_links_punishment(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Выберите наказание для ссылок:", reply_markup=get_punishment_keyboard("links"))
    await callback.answer()

@dp.callback_query(F.data == "set_links_duration")
@edit_only()
@check_owner()
async def set_links_duration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("links_manage"))
    await state.set_state(LinksStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "links_filter_info")
@edit_only()
@check_owner()
async def links_filter_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("ℹ️ Что такое анти-ссылки?\n\nБот автоматически обнаруживает и блокирует сообщения, содержащие ссылки:\n• http://, https://\n• t.me, telegram.me\n\nПри обнаружении ссылки применяется наказание:\n• Warn - предупреждение\n• Mute - ограничение на отправку сообщений\n• Kick - удаление из чата\n• Ban - блокировка в чате", reply_markup=get_back_keyboard("links_manage"))
    await callback.answer()

@dp.callback_query(F.data == "profanity_manage")
@edit_only()
@check_owner()
async def profanity_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_profanity_filter_settings(chat_id)
    status = "✅ Включен" if s['enabled'] else "❌ Выключен"
    await callback.message.edit_text(f"🚫 Антимат\n\nСтатус: {status}\nНаказание: {s['punishment']}\nДлительность: {format_time(s['duration'])}\n\nБлокирует нецензурную лексику", reply_markup=get_profanity_manage_keyboard(s))
    await callback.answer()

@dp.callback_query(F.data == "toggle_profanity_filter")
@edit_only()
@check_owner()
async def toggle_profanity_filter(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_profanity_filter_settings(chat_id)
    db.set_profanity_filter_settings(chat_id, not s['enabled'], s['punishment'], s['duration'])
    await callback.answer(f"✅ Антимат {'включен' if not s['enabled'] else 'выключен'}!")
    await profanity_manage(callback, state)

@dp.callback_query(F.data == "set_profanity_punishment")
@edit_only()
@check_owner()
async def set_profanity_punishment(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Выберите наказание за мат:", reply_markup=get_punishment_keyboard("profanity"))
    await callback.answer()

@dp.callback_query(F.data == "set_profanity_duration")
@edit_only()
@check_owner()
async def set_profanity_duration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("profanity_manage"))
    await state.set_state(ProfanityStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "profanity_filter_info")
@edit_only()
@check_owner()
async def profanity_filter_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("ℹ️ Что такое антимат?\n\nБот автоматически обнаруживает и блокирует сообщения, содержащие нецензурную лексику.\n\nПри обнаружении мата применяется наказание:\n• Warn - предупреждение\n• Mute - ограничение на отправку сообщений\n• Kick - удаление из чата\n• Ban - блокировка в чате", reply_markup=get_back_keyboard("profanity_manage"))
    await callback.answer()

@dp.callback_query(F.data == "raid_manage")
@edit_only()
@check_owner()
async def raid_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_raid_protection_settings(chat_id)
    status = "✅ Включена" if s['enabled'] else "❌ Выключена"
    await callback.message.edit_text(f"🛡️ Рейд-защита\n\nСтатус: {status}\nЛимит вступлений: {s['limit']}\nПериод: {s['window']} сек\nНаказание: {s['punishment']}\nДлительность: {format_time(s['duration'])}\n\nАвтоматически блокирует массовые вступления в чат", reply_markup=get_raid_manage_keyboard(s))
    await callback.answer()

@dp.callback_query(F.data == "toggle_raid_protection")
@edit_only()
@check_owner()
async def toggle_raid_protection(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_raid_protection_settings(chat_id)
    db.set_raid_protection_settings(chat_id, not s['enabled'], s['limit'], s['window'], s['punishment'], s['duration'])
    await callback.answer(f"✅ Рейд-защита {'включена' if not s['enabled'] else 'выключена'}!")
    await raid_manage(callback, state)

@dp.callback_query(F.data == "set_raid_limit")
@edit_only()
@check_owner()
async def set_raid_limit(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📊 Введите лимит вступлений (3-50):", reply_markup=get_back_keyboard("raid_manage"))
    await state.set_state(RaidStates.waiting_for_limit)
    await callback.answer()

@dp.callback_query(F.data == "set_raid_window")
@edit_only()
@check_owner()
async def set_raid_window(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏱ Введите период в секундах (10-300):", reply_markup=get_back_keyboard("raid_manage"))
    await state.set_state(RaidStates.waiting_for_window)
    await callback.answer()

@dp.callback_query(F.data == "set_raid_punishment")
@edit_only()
@check_owner()
async def set_raid_punishment(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Выберите наказание для рейдеров:", reply_markup=get_punishment_keyboard("raid"))
    await callback.answer()

@dp.callback_query(F.data == "set_raid_duration")
@edit_only()
@check_owner()
async def set_raid_duration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("raid_manage"))
    await state.set_state(RaidStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "raid_protection_info")
@edit_only()
@check_owner()
async def raid_protection_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("ℹ️ Что такое рейд-защита?\n\nФункция автоматически обнаруживает массовые вступления пользователей в чат.\n\nЕсли за указанный период вступает больше пользователей, чем лимит:\n• Все новые участники получают наказание\n• В чат отправляется предупреждение\n\nЭто защищает от рейд-атак и спам-ботов.", reply_markup=get_back_keyboard("raid_manage"))
    await callback.answer()

@dp.callback_query(F.data == "auto_comment_manage")
@edit_only()
@check_owner()
async def auto_comment_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_auto_comment_settings(chat_id)
    text_preview = s['text'][:50] + "..." if s['text'] and len(s['text']) > 50 else s['text'] or "не установлен"
    status = "✅ Включен" if s['enabled'] else "❌ Выключен"
    await callback.message.edit_text(f"💬 Авто-комментарий к постам\n\nСтатус: {status}\nТекст: {text_preview}\nМедиа: {'✅ есть' if s['media_id'] else '❌ нет'}\n\nБот будет автоматически писать этот комментарий под каждым новым постом канала в обсуждении", reply_markup=get_auto_comment_keyboard(s))
    await callback.answer()

@dp.callback_query(F.data == "toggle_auto_comment")
@edit_only()
@check_owner()
async def toggle_auto_comment(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_auto_comment_settings(chat_id)
    db.set_auto_comment_settings(chat_id, not s['enabled'], s['text'], s['media_id'], s['media_type'])
    await callback.answer(f"✅ Авто-комментарий {'включен' if not s['enabled'] else 'выключен'}!")
    await auto_comment_manage(callback, state)

@dp.callback_query(F.data == "set_auto_comment_text")
@edit_only()
@check_owner()
async def set_auto_comment_text(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📝 Отправьте текст для авто-комментария:\n\nМожно использовать:\n• {post_link} - ссылка на пост\n• {channel_title} - название канала", reply_markup=get_back_keyboard("auto_comment_manage"))
    await state.set_state(AutoCommentStates.waiting_for_text)
    await callback.answer()

@dp.callback_query(F.data == "set_auto_comment_media")
@edit_only()
@check_owner()
async def set_auto_comment_media(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🖼 Отправьте медиа для авто-комментария:\n\nПоддерживается: фото, видео, GIF, стикер", reply_markup=get_back_keyboard("auto_comment_manage"))
    await state.set_state(AutoCommentStates.waiting_for_media)
    await callback.answer()

@dp.callback_query(F.data == "view_auto_comment")
@edit_only()
@check_owner()
async def view_auto_comment(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_auto_comment_settings(chat_id)
    if s['media_id']:
        if s['media_type'] == 'photo':
            await callback.message.answer_photo(s['media_id'], caption=s['text'] or "Авто-комментарий", parse_mode="HTML")
        elif s['media_type'] == 'video':
            await callback.message.answer_video(s['media_id'], caption=s['text'] or "Авто-комментарий", parse_mode="HTML")
        elif s['media_type'] == 'animation':
            await callback.message.answer_animation(s['media_id'], caption=s['text'] or "Авто-комментарий", parse_mode="HTML")
        elif s['media_type'] == 'sticker':
            await callback.message.answer_sticker(s['media_id'])
    else:
        await callback.message.answer(s['text'] or "Авто-комментарий не настроен", parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "auto_comment_info")
@edit_only()
@check_owner()
async def auto_comment_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("ℹ️ Что такое авто-комментарий?\n\nФункция автоматически публикует комментарий под каждым новым постом канала.\n\nДля работы необходимо:\n1️⃣ Канал должен быть привязан к супергруппе как обсуждение\n2️⃣ Бот должен быть администратором в супергруппе\n\nПоддерживаются переменные:\n• {post_link} - ссылка на пост\n• {channel_title} - название канала", reply_markup=get_back_keyboard("auto_comment_manage"))
    await callback.answer()

@dp.callback_query(F.data == "auto_response_manage")
@edit_only()
@check_owner()
async def auto_response_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    resp = db.get_auto_responses(chat_id)
    if not resp:
        text = "🤖 Автоответчик\n\nСписок триггеров пуст.\nМаксимум: 100 триггеров"
    else:
        text = f"🤖 Автоответчик ({len(resp)}/100)\n\n"
        for t, r, rt, _ in resp:
            short_r = r[:30] + "..." if len(r) > 30 else r
            type_emoji = "📝" if rt == 'text' else "🖼" if rt == 'photo' else "🎬" if rt in ['video', 'animation'] else "🎯" if rt == 'sticker' else "🎤" if rt == 'voice' else "📄" if rt == 'document' else "❓"
            text += f"• {type_emoji} <code>{t}</code> → {short_r}\n"
    await callback.message.edit_text(text, reply_markup=get_auto_response_keyboard(resp), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "add_auto_trigger")
@edit_only()
@check_owner()
async def add_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    resp = db.get_auto_responses(chat_id)
    if len(resp) >= MAX_TRIGGERS:
        await callback.answer(f"❌ Достигнут лимит триггеров ({MAX_TRIGGERS})!", show_alert=True)
        return
    await callback.message.edit_text(f"📝 Введите ключевое слово (триггер).\nМакс. длина: {MAX_TRIGGER_LENGTH} символов\nМакс. слов: {MAX_TRIGGER_WORDS}\n\nТриггер будет проверяться на точное совпадение и вхождение в текст.", reply_markup=get_back_keyboard("auto_response_manage"))
    await state.set_state(AutoResponseStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponseStates.waiting_for_trigger)
async def process_auto_trigger(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    trigger = message.text.strip()
    if not trigger:
        await message.answer("❌ Триггер не может быть пустым!")
        return
    if len(trigger) > MAX_TRIGGER_LENGTH:
        await message.answer(f"❌ Триггер слишком длинный! Максимум {MAX_TRIGGER_LENGTH} символов")
        return
    if len(trigger.split()) > MAX_TRIGGER_WORDS:
        await message.answer(f"❌ Триггер должен содержать максимум {MAX_TRIGGER_WORDS} слово")
        return
    await state.update_data(auto_trigger=trigger)
    await message.answer(f"📝 Введите ответ для триггера '{trigger}'.\nМакс. длина: {MAX_RESPONSE_LENGTH} символов\n\nВы можете отправить:\n• Текст с форматированием\n• Фото с подписью\n• Видео с подписью\n• GIF с подписью\n• Стикер\n• Голосовое сообщение\n• Видео-кружок\n• Документ с подписью", reply_markup=get_back_keyboard("auto_response_manage"))
    await state.set_state(AutoResponseStates.waiting_for_response)

@dp.message(AutoResponseStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    trigger = data.get('auto_trigger')
    if not chat_id or not trigger:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    rtype = 'text'
    resp = ""
    media = None
    if message.text:
        rtype = 'text'
        resp = message.html_text.strip()
    elif message.photo:
        rtype = 'photo'
        media = message.photo[-1].file_id
        resp = message.caption or ""
    elif message.video:
        rtype = 'video'
        media = message.video.file_id
        resp = message.caption or ""
    elif message.animation:
        rtype = 'animation'
        media = message.animation.file_id
        resp = message.caption or ""
    elif message.sticker:
        rtype = 'sticker'
        media = message.sticker.file_id
        resp = ""
    elif message.voice:
        rtype = 'voice'
        media = message.voice.file_id
        resp = ""
    elif message.video_note:
        rtype = 'video_note'
        media = message.video_note.file_id
        resp = ""
    elif message.document:
        rtype = 'document'
        media = message.document.file_id
        resp = message.caption or ""
    if rtype == 'text' and not resp:
        await message.answer("❌ Ответ не может быть пустым!")
        return
    if len(resp) > MAX_RESPONSE_LENGTH:
        await message.answer(f"❌ Ответ слишком длинный! Максимум {MAX_RESPONSE_LENGTH} символов")
        return
    success, msg = db.add_auto_response(chat_id, trigger, resp, rtype, media)
    await message.answer(msg)
    await add_premium_reaction(message, "✅" if success else "❌")
    await state.clear()

@dp.callback_query(F.data == "remove_auto_trigger")
@edit_only()
@check_owner()
async def remove_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    resp = db.get_auto_responses(chat_id)
    if not resp:
        await callback.answer("❌ Нет триггеров для удаления!", show_alert=True)
        return
    await callback.message.edit_text("🗑 Выберите триггер для удаления:", reply_markup=get_auto_response_remove_keyboard(resp))
    await callback.answer()

@dp.callback_query(F.data.startswith("rem_trig_"))
@edit_only()
@check_owner()
async def process_remove_trigger(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    resp = db.get_auto_responses(chat_id)
    if idx < 0 or idx >= len(resp):
        await callback.answer("❌ Триггер не найден!", show_alert=True)
        return
    trigger = resp[idx][0]
    db.remove_auto_response(chat_id, trigger)
    await callback.answer(f"✅ Триггер '{trigger}' удалён!")
    await auto_response_manage(callback, state)

@dp.callback_query(F.data == "log_group_manage")
@edit_only()
@check_owner()
async def log_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    info = db.get_source_chat_log_group(chat_id)
    has = info is not None
    await callback.message.edit_text("📋 Группа логов\n\nСюда будут отправляться логи нарушений, действий модераторов и другие события.", reply_markup=get_log_group_manage_keyboard(has, info))
    await callback.answer()

@dp.callback_query(F.data == "log_group_help")
@edit_only()
@check_owner()
async def log_group_help(callback: CallbackQuery):
    await callback.message.edit_text("📋 Как создать группу логов:\n\n1️⃣ Создайте новую группу в Telegram (обычную, не привязанную к каналу)\n2️⃣ Добавьте бота в эту группу\n3️⃣ Выдайте боту права администратора\n4️⃣ В этой группе напишите команду /loggroup\n5️⃣ Нажмите «Запомнить»\n\nПосле этого группа будет сохранена и появится в списке групп логов в настройках.", reply_markup=get_back_keyboard("log_group_manage"))
    await callback.answer()

@dp.callback_query(F.data == "link_log_group")
@edit_only()
@check_owner()
async def link_log_group(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    groups = db.get_user_log_groups(user_id)
    if not groups:
        await callback.message.edit_text("❌ У вас ещё нет созданных групп логов!\n\nСоздайте группу логов, следуя инструкции.", reply_markup=get_back_keyboard("log_group_manage"))
        await callback.answer()
        return
    builder = InlineKeyboardBuilder()
    for gid, title in groups:
        builder.add(create_button(title or f"Группа {gid}", f"select_log_group_{gid}"))
    builder.add(create_button("◀ Назад", "log_group_manage"))
    builder.adjust(1)
    await callback.message.edit_text("📋 Выберите группу логов\n\nВ эту группу будут отправляться события из текущего чата:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("select_log_group_"))
@edit_only()
@check_owner()
async def select_log_group(callback: CallbackQuery, state: FSMContext):
    gid = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.set_source_chat_log_group(chat_id, gid)
    await callback.answer("✅ Группа логов привязана!")
    await log_group_manage(callback, state)

@dp.callback_query(F.data == "log_group_settings")
@edit_only()
@check_owner()
async def log_group_settings(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if not info:
        await callback.answer("❌ Группа логов не привязана!", show_alert=True)
        return
    settings = {
        'send_violations': info['send_violations'],
        'send_mod_actions': info['send_mod_actions'],
        'send_joins': info['send_joins'],
        'send_leaves': info['send_leaves'],
        'send_messages': info['send_messages']
    }
    await callback.message.edit_text(f"📋 Настройки отправки в лог-группу\n\nГруппа: {info['group_title']}\nID: <code>{info['log_group_id']}</code>\n\nВыберите, какие события отправлять:", reply_markup=get_log_settings_keyboard(settings), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "toggle_log_violations")
@edit_only()
@check_owner()
async def toggle_log_violations(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if info:
        new = 0 if info['send_violations'] else 1
        db.update_log_group_settings(chat_id, info['log_group_id'], send_violations=new)
        await callback.answer("✅ Настройки обновлены!")
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_mod")
@edit_only()
@check_owner()
async def toggle_log_mod(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if info:
        new = 0 if info['send_mod_actions'] else 1
        db.update_log_group_settings(chat_id, info['log_group_id'], send_mod_actions=new)
        await callback.answer("✅ Настройки обновлены!")
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_joins")
@edit_only()
@check_owner()
async def toggle_log_joins(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if info:
        new = 0 if info['send_joins'] else 1
        db.update_log_group_settings(chat_id, info['log_group_id'], send_joins=new)
        await callback.answer("✅ Настройки обновлены!")
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_leaves")
@edit_only()
@check_owner()
async def toggle_log_leaves(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if info:
        new = 0 if info['send_leaves'] else 1
        db.update_log_group_settings(chat_id, info['log_group_id'], send_leaves=new)
        await callback.answer("✅ Настройки обновлены!")
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_messages")
@edit_only()
@check_owner()
async def toggle_log_messages(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if info:
        new = 0 if info['send_messages'] else 1
        db.update_log_group_settings(chat_id, info['log_group_id'], send_messages=new)
        await callback.answer("✅ Настройки обновлены!")
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "log_group_info")
@edit_only()
@check_owner()
async def log_group_info(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    info = db.get_source_chat_log_group(chat_id)
    if not info:
        await callback.answer("❌ Группа логов не привязана!", show_alert=True)
        return
    await callback.message.edit_text(f"📋 Информация о группе логов\n\nГруппа: {info['group_title']}\nID: <code>{info['log_group_id']}</code>", reply_markup=get_back_keyboard("log_group_manage"), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "unlink_log_group")
@edit_only()
@check_owner()
async def unlink_log_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.remove_source_chat_log_group(chat_id)
    await callback.answer("✅ Группа логов отвязана!")
    await log_group_manage(callback, state)

@dp.callback_query(F.data == "moderators_manage")
@edit_only()
@check_owner()
async def moderators_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    mods = db.get_all_moderators(chat_id)
    await callback.message.edit_text("🛡️ Управление модераторами\n\nЗдесь вы можете назначать и забирать права модераторов.", reply_markup=get_moderators_manage_keyboard(mods))
    await callback.answer()

@dp.callback_query(F.data == "give_mod_rights")
@edit_only()
@check_owner()
async def give_mod_rights(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("👤 Ответьте на сообщение пользователя, которому хотите дать права модератора,\nили отправьте его ID / username.")
    await state.set_state(ModerationStates.waiting_for_give_mute_user)
    await callback.answer()

@dp.message(ModerationStates.waiting_for_give_mute_user)
async def process_give_mod_user(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    elif message.text.isdigit():
        target_id = int(message.text)
    elif message.text.startswith('@'):
        try:
            m = await bot.get_chat_member(chat_id, message.text)
            target_id = m.user.id
        except:
            await message.answer("❌ Пользователь не найден в этом чате!")
            return
    if not target_id:
        await message.answer("❌ Не удалось определить пользователя!")
        return
    await state.update_data(target_mod_id=target_id)
    await message.answer("Выберите права для пользователя:", reply_markup=get_mod_rights_keyboard(target_id))

@dp.callback_query(F.data.startswith("give_mute_"))
@edit_only()
@check_owner()
async def give_mute_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_mute', True, callback.from_user.id)
    await callback.answer("✅ Право мутить выдано!")
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data.startswith("give_kick_"))
@edit_only()
@check_owner()
async def give_kick_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_kick', True, callback.from_user.id)
    await callback.answer("✅ Право кикать выдано!")
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data.startswith("give_ban_"))
@edit_only()
@check_owner()
async def give_ban_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_ban', True, callback.from_user.id)
    await callback.answer("✅ Право банить выдано!")
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data.startswith("give_warn_"))
@edit_only()
@check_owner()
async def give_warn_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_warn', True, callback.from_user.id)
    await callback.answer("✅ Право варнить выдано!")
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data == "list_moderators")
@edit_only()
@check_owner()
async def list_moderators(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    mods = db.get_all_moderators(chat_id)
    if not mods:
        await callback.message.edit_text("📋 Нет назначенных модераторов", reply_markup=get_back_keyboard("moderators_manage"))
        await callback.answer()
        return
    text = "🛡️ Список модераторов:\n\n"
    for m in mods:
        try:
            u = await bot.get_chat_member(chat_id, m[0])
            rights = []
            if m[1]: rights.append("🔇 мут")
            if m[2]: rights.append("👢 кик")
            if m[3]: rights.append("⛔ бан")
            if m[4]: rights.append("⚠️ варн")
            text += f"• <b>{u.user.full_name}</b>\n  Права: {', '.join(rights) if rights else 'нет прав'}\n\n"
        except:
            continue
    await callback.message.edit_text(text, reply_markup=get_back_keyboard("moderators_manage"), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "manage_rules")
@edit_only()
@check_owner()
async def manage_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    has = db.get_rules_html(chat_id) is not None
    enabled = db.get_rules_enabled(chat_id)
    status = "✅ Включены" if enabled else "❌ Выключены"
    await callback.message.edit_text(f"📝 Управление правилами\n\nСтатус: {status}", reply_markup=get_rules_manage_keyboard(has, enabled))
    await callback.answer()

@dp.callback_query(F.data == "set_rules")
@edit_only()
@check_owner()
async def set_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте текст правил для этой группы.\n\nВы можете использовать форматирование:\n• <b>Жирный</b> - &lt;b&gt;текст&lt;/b&gt;\n• <i>Курсив</i> - &lt;i&gt;текст&lt;/i&gt;\n• <u>Подчеркнутый</u> - &lt;u&gt;текст&lt;/u&gt;\n• <s>Зачеркнутый</s> - &lt;s&gt;текст&lt;/s&gt;\n• <tg-spoiler>Спойлер</tg-spoiler> - &lt;tg-spoiler&gt;текст&lt;/tg-spoiler&gt;\n• <blockquote>Цитата</blockquote> - &lt;blockquote&gt;текст&lt;/blockquote&gt;\n• <blockquote expandable>Свернутая цитата</blockquote> - &lt;blockquote expandable&gt;текст&lt;/blockquote&gt;\n• <code>Код</code> - &lt;code&gt;текст&lt;/code&gt;\n• <pre>Блок кода</pre> - &lt;pre&gt;текст&lt;/pre&gt;", reply_markup=get_back_keyboard("manage_rules"))
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
    if len(rules_html) > 10000:
        await message.answer("❌ Правила слишком длинные! Максимум 10000 символов")
        return
    db.save_rules(chat_id, rules_html=rules_html)
    db.set_rules_enabled(chat_id, True)
    await message.reply("✅ Правила сохранены!")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "set_default_rules")
@edit_only()
@check_owner()
async def set_default_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    default_rules = """<b>📜 Правила чата</b>

<b>1. Уважение</b>
• Запрещены оскорбления участников и администрации
• Запрещены любые формы дискриминации
• Наказание: предупреждение или мут 1-24 часа

<b>2. Контент</b>
• Запрещена реклама без разрешения администрации
• Запрещены ссылки на сторонние ресурсы
• Запрещен спам и флуд (более 5 сообщений подряд)
• Наказание: предупреждение или мут 1-24 часа

<b>3. Безопасность</b>
• Запрещены угрозы и призывы к насилию
• Запрещены мошеннические действия
• Наказание: бан 7-30 дней

<b>4. Администрация</b>
• Решения администрации окончательны
• Вопросы по наказаниям решаются в ЛС
• Обжалование: @support_puls

Незнание правил не освобождает от ответственности!"""
    db.save_rules(chat_id, rules_html=default_rules)
    db.set_rules_enabled(chat_id, True)
    await callback.answer("✅ Готовые правила установлены!")
    await manage_rules(callback, state)

@dp.callback_query(F.data == "show_rules")
@edit_only()
@check_owner()
async def show_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    rules = db.get_rules_html(chat_id)
    if rules:
        await callback.message.edit_text(f"📜 Текущие правила:\n\n{rules}", reply_markup=get_back_keyboard("manage_rules"), parse_mode="HTML")
    else:
        await callback.message.edit_text("❌ Правила ещё не установлены", reply_markup=get_back_keyboard("manage_rules"))
    await callback.answer()

@dp.callback_query(F.data == "edit_rules")
@edit_only()
@check_owner()
async def edit_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте новый текст правил:", reply_markup=get_back_keyboard("manage_rules"))
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
    if len(rules_html) > 10000:
        await message.answer("❌ Правила слишком длинные! Максимум 10000 символов")
        return
    db.save_rules(chat_id, rules_html=rules_html)
    await message.reply("✅ Правила обновлены!")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "delete_rules_confirm")
@edit_only()
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
    builder.adjust(2)
    await callback.message.edit_text("❓ Вы уверены, что хотите удалить правила?", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "delete_rules")
@edit_only()
@check_owner()
async def delete_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.delete_rules(chat_id)
    await callback.answer("✅ Правила удалены!")
    await manage_rules(callback, state)

@dp.callback_query(F.data == "toggle_rules")
@edit_only()
@check_owner()
async def toggle_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    current = db.get_rules_enabled(chat_id)
    db.set_rules_enabled(chat_id, not current)
    await callback.answer(f"✅ Правила {'включены' if not current else 'выключены'}!")
    await manage_rules(callback, state)

@dp.callback_query(F.data == "manage_welcome")
@edit_only()
@check_owner()
async def manage_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled = db.get_welcome_enabled(chat_id)
    await callback.message.edit_text("👋 Управление приветствием\n\nНастройте приветствие для новых участников.", reply_markup=get_welcome_manage_keyboard(enabled))
    await callback.answer()

@dp.callback_query(F.data == "toggle_welcome")
@edit_only()
@check_owner()
async def toggle_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    current = db.get_welcome_enabled(chat_id)
    db.set_welcome_enabled(chat_id, not current)
    await callback.answer(f"✅ Приветствие {'включено' if not current else 'выключено'}!")
    await manage_welcome(callback, state)

@dp.callback_query(F.data == "set_welcome_text")
@edit_only()
@check_owner()
async def set_welcome_text(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте текст приветствия.\n\nМожно использовать:\n• {name} - имя\n• {username} - юзернейм\n• {chat} - название группы\n\nПример: Добро пожаловать, {name}!", reply_markup=get_back_keyboard("manage_welcome"))
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
    if len(welcome_text) > 4000:
        await message.answer("❌ Текст слишком длинный! Максимум 4000 символов")
        return
    db.save_welcome(chat_id, welcome_text=welcome_text)
    await message.reply("✅ Текст приветствия сохранён!")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "set_welcome_photo")
@edit_only()
@check_owner()
async def set_welcome_photo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("🖼 Отправьте фото для приветствия.\n\nОно будет отправляться вместе с текстом.", reply_markup=get_back_keyboard("manage_welcome"))
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
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.message(WelcomeStates.waiting_for_welcome_photo)
async def process_welcome_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Пожалуйста, отправьте фото!")

@dp.callback_query(F.data == "show_welcome")
@edit_only()
@check_owner()
async def show_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    text, photo = db.get_welcome(chat_id)
    if not text and not photo:
        await callback.message.edit_text("❌ Приветствие ещё не настроено", reply_markup=get_back_keyboard("manage_welcome"))
        await callback.answer()
        return
    await callback.message.delete()
    if photo:
        await callback.message.answer_photo(photo, caption=f"👋 Текущее приветствие:\n\n{text}" if text else None, reply_markup=get_back_keyboard("manage_welcome"), parse_mode="HTML")
    else:
        await callback.message.answer(f"👋 Текущее приветствие:\n\n{text}", reply_markup=get_back_keyboard("manage_welcome"), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "rules_auto")
@edit_only()
@check_owner()
async def rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled, interval = db.get_rules_auto_settings(chat_id)
    status = "✅ Включена" if enabled else "❌ Выключена"
    await callback.message.edit_text(f"🔄 Авто-рассылка правил\n\nСтатус: {status}\nИнтервал: {format_interval(interval)}", reply_markup=get_rules_auto_keyboard(enabled))
    await callback.answer()

@dp.callback_query(F.data == "toggle_rules_auto")
@edit_only()
@check_owner()
async def toggle_rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    enabled, interval = db.get_rules_auto_settings(chat_id)
    db.set_rules_auto_settings(chat_id, not enabled, interval)
    await callback.answer(f"✅ Авто-рассылка {'включена' if not enabled else 'выключена'}!")
    await rules_auto(callback, state)

@dp.callback_query(F.data == "set_interval")
@edit_only()
@check_owner()
async def set_interval(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("⏱ Введите интервал в минутах (от 5 до 525600):", reply_markup=get_back_keyboard("rules_auto"))
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
        enabled, _ = db.get_rules_auto_settings(chat_id)
        db.set_rules_auto_settings(chat_id, enabled, interval * 60)
        await message.reply(f"✅ Интервал установлен: {format_interval(interval * 60)}")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число!")

@dp.callback_query(F.data == "antiflood_manage")
@edit_only()
@check_owner()
async def antiflood_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    s = db.get_antiflood_settings(chat_id)
    status = "✅ Включён" if s['enabled'] else "❌ Выключен"
    await callback.message.edit_text(f"🚫 Антифлуд\n\nСтатус: {status}\n• Текст: {s['msg_limit']} сообщ.\n• Медиа: {s['media_limit']} сообщ.\n• Период: {s['time_window']} сек\n• Предупреждений: {s['warn_count']}\n• Первое: {s['first_punish']} ({format_time(s['first_duration'])})\n• Повторное: {s['repeat_punish']} ({format_time(s['repeat_duration'])})\n• После варнов: {s['punish_after_warn']} ({format_time(s['punish_after_warn_duration'])})\n\nУмный антифлуд отличает текстовые и медиа-сообщения.", reply_markup=get_antiflood_manage_keyboard(s))
    await callback.answer()

@dp.callback_query(F.data == "toggle_antiflood")
@edit_only()
@check_owner()
async def toggle_antiflood(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    s = db.get_antiflood_settings(chat_id)
    db.set_antiflood_enabled(chat_id, not s['enabled'])
    await callback.answer(f"✅ Антифлуд {'включён' if not s['enabled'] else 'выключен'}!")
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_msg_limit")
@edit_only()
@check_owner()
async def set_msg_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("📊 Введите лимит текстовых сообщений (3-50):", reply_markup=get_back_keyboard("antiflood_manage"))
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
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_media_limit")
@edit_only()
@check_owner()
async def set_media_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("🎬 Введите лимит медиа-сообщений (2-20):", reply_markup=get_back_keyboard("antiflood_manage"))
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
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_window")
@edit_only()
@check_owner()
async def set_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("⏱ Введите период в секундах (5-300):", reply_markup=get_back_keyboard("antiflood_manage"))
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
            await message.answer("❌ Период должен быть от 5 до 300 секунд!")
            return
        db.save_antiflood_settings(chat_id, time_window=window)
        await message.reply(f"✅ Период установлен: {window} сек")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_warn_count")
@edit_only()
@check_owner()
async def set_warn_count(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("⚠️ Введите количество предупреждений перед наказанием (1-10):", reply_markup=get_back_keyboard("antiflood_manage"))
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
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_first_punish")
@edit_only()
@check_owner()
async def set_first_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("🔇 Выберите наказание для первого нарушения:", reply_markup=get_punish_type_keyboard("first"))
    await callback.answer()

@dp.callback_query(F.data == "set_repeat_punish")
@edit_only()
@check_owner()
async def set_repeat_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("🔊 Выберите наказание для повторных нарушений:", reply_markup=get_punish_type_keyboard("repeat"))
    await callback.answer()

@dp.callback_query(F.data == "set_punish_after_warn")
@edit_only()
@check_owner()
async def set_punish_after_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("⚠️ Выберите наказание после достижения лимита предупреждений:", reply_markup=get_punish_type_keyboard("after"))
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_warn_"))
@edit_only()
@check_owner()
async def punish_warn(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    if punish_type == "first":
        db.save_antiflood_settings(chat_id, first_punish='warn')
        await callback.answer("✅ Первое наказание: предупреждение", show_alert=True)
    elif punish_type == "repeat":
        db.save_antiflood_settings(chat_id, repeat_punish='warn')
        await callback.answer("✅ Повторное наказание: предупреждение", show_alert=True)
    elif punish_type == "after":
        db.save_antiflood_settings(chat_id, punish_after_warn='warn')
        await callback.answer("✅ Наказание после варнов: предупреждение", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_mute_"))
@edit_only()
@check_owner()
async def punish_mute(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("⏱ Введите длительность мута в минутах (0 = навсегда):", reply_markup=get_back_keyboard("antiflood_manage"))
    await state.update_data(punish_setting=punish_type, punish_action='mute')
    await state.set_state(PunishDurationStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_kick_"))
@edit_only()
@check_owner()
async def punish_kick(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    if punish_type == "first":
        db.save_antiflood_settings(chat_id, first_punish='kick')
        await callback.answer("✅ Наказание: кик", show_alert=True)
    elif punish_type == "repeat":
        db.save_antiflood_settings(chat_id, repeat_punish='kick')
        await callback.answer("✅ Повторное наказание: кик", show_alert=True)
    elif punish_type == "after":
        db.save_antiflood_settings(chat_id, punish_after_warn='kick')
        await callback.answer("✅ Наказание после варнов: кик", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_ban_"))
@edit_only()
@check_owner()
async def punish_ban(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("⏱ Введите длительность бана в минутах (0 = навсегда):", reply_markup=get_back_keyboard("antiflood_manage"))
    await state.update_data(punish_setting=punish_type, punish_action='ban')
    await state.set_state(PunishDurationStates.waiting_for_duration)
    await callback.answer()

@dp.message(PunishDurationStates.waiting_for_duration)
async def process_punish_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    punish_setting = data.get('punish_setting')
    punish_action = data.get('punish_action')
    if not chat_id or not punish_setting or not punish_action:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Введите положительное число или 0!")
            return
        duration = minutes * 60
        if punish_setting == "first":
            db.save_antiflood_settings(chat_id, first_punish=punish_action, first_duration=duration)
            await message.reply(f"✅ Первое наказание: {punish_action} на {format_time(duration)}")
        elif punish_setting == "repeat":
            db.save_antiflood_settings(chat_id, repeat_punish=punish_action, repeat_duration=duration)
            await message.reply(f"✅ Повторное наказание: {punish_action} на {format_time(duration)}")
        elif punish_setting == "after":
            db.save_antiflood_settings(chat_id, punish_after_warn=punish_action, punish_after_warn_duration=duration)
            await message.reply(f"✅ Наказание после варнов: {punish_action} на {format_time(duration)}")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "puls_antispam_manage")
@edit_only()
@check_owner()
async def puls_antispam_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled = db.get_puls_antispam_enabled(chat_id)
    status = "✅ Включен" if enabled else "❌ Выключен"
    await callback.message.edit_text(f"🛡️ Антиспам Пульса\n\nСтатус: {status}\n\nКогда функция включена, бот автоматически проверяет всех новых участников по глобальной базе спамеров Пульса. Если обнаружен спамер, он сразу банится.\n\nТакже бот отслеживает явный спам (50+ сообщений в минуту) и добавляет нарушителей в базу.", reply_markup=get_puls_antispam_keyboard(enabled))
    await callback.answer()

@dp.callback_query(F.data == "toggle_puls_antispam")
@edit_only()
@check_owner()
async def toggle_puls_antispam(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    current = db.get_puls_antispam_enabled(chat_id)
    db.set_puls_antispam_enabled(chat_id, not current)
    await callback.answer(f"✅ Антиспам Пульса {'включен' if not current else 'выключен'}!")
    await puls_antispam_manage(callback, state)

@dp.callback_query(F.data == "puls_antispam_info")
@edit_only()
@check_owner()
async def puls_antispam_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("ℹ️ Что такое Антиспам Пульса?\n\nЭто глобальная система защиты от спамеров:\n\n1️⃣ Если пользователь отправляет 50+ сообщений за 1 минуту, он получает предупреждение.\n2️⃣ При 3 предупреждениях он навсегда добавляется в базу спамеров Пульса.\n3️⃣ При входе в любую группу с ботом он автоматически банится.\n4️⃣ Админы могут разбанить спамера в своей группе командой /unban.\n5️⃣ Только разработчики бота могут удалить из базы командой /remove_spammer.\n\nСсылка на поддержку: @support_puls", reply_markup=get_back_keyboard("puls_antispam_manage"))
    await callback.answer()

@dp.callback_query(F.data == "confirmation_actions_manage")
@edit_only()
@check_owner()
async def confirmation_actions_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    await callback.message.edit_text("✅ Подтверждение опасных действий\n\nВы можете включить подтверждение для каждого действия отдельно.\nЕсли включено, перед выполнением действия бот спросит подтверждение.", reply_markup=get_confirmation_actions_keyboard(settings))
    await callback.answer()

@dp.callback_query(F.data == "toggle_confirm_ban")
@edit_only()
@check_owner()
async def toggle_confirm_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    new_value = not settings.get('ban', False)
    db.set_confirmation_setting(chat_id, 'ban', new_value)
    await callback.answer(f"✅ Подтверждение бана {'включено' if new_value else 'выключено'}!")
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "toggle_confirm_kick")
@edit_only()
@check_owner()
async def toggle_confirm_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    new_value = not settings.get('kick', False)
    db.set_confirmation_setting(chat_id, 'kick', new_value)
    await callback.answer(f"✅ Подтверждение кика {'включено' if new_value else 'выключено'}!")
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "toggle_confirm_mute")
@edit_only()
@check_owner()
async def toggle_confirm_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    new_value = not settings.get('mute', False)
    db.set_confirmation_setting(chat_id, 'mute', new_value)
    await callback.answer(f"✅ Подтверждение мута {'включено' if new_value else 'выключено'}!")
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "confirmation_actions_info")
@edit_only()
@check_owner()
async def confirmation_actions_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("ℹ️ Что такое подтверждение действий?\n\nЕсли функция включена для конкретного действия, то перед его выполнением бот попросит подтверждение. Это защищает от случайных нажатий.\n\nНапример, если включено подтверждение бана, то после команды /ban бот сначала покажет информацию и спросит 'Подтверждаете?'. Только после подтверждения пользователь будет забанен.\n\nПо умолчанию всё выключено.", reply_markup=get_back_keyboard("confirmation_actions_manage"))
    await callback.answer()

@dp.callback_query(F.data == "confirmation_manage")
@edit_only()
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
        warning = "\n\n⚠️ Внимание: Правила не установлены. Эта настройка не будет работать."
    await callback.message.edit_text(f"✅ Настройки подтверждения\n\nТип: {type_names.get(conf_type, conf_type)}{warning}", reply_markup=get_confirmation_keyboard(conf_type, has_rules))
    await callback.answer()

@dp.callback_query(F.data.startswith("confirmation_"))
@edit_only()
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
        error = "❌ Нельзя включить 'Только правила' или 'Оба шага' - сначала установите правила в группе!"
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

@dp.callback_query(F.data == "unlink_group_confirm")
@edit_only()
@check_owner()
async def unlink_group_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text("❓ Вы уверены, что хотите отвязать группу?\n\nВсе настройки будут сохранены, но вы больше не сможете управлять ей.", reply_markup=get_unlink_confirm_keyboard(chat_id))
    await callback.answer()

@dp.callback_query(F.data.startswith("unlink_group_"))
@edit_only()
@check_owner()
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    with db.get_connection() as conn:
        conn.execute('UPDATE group_rules SET owner_id = NULL WHERE chat_id = ?', (chat_id,))
        conn.commit()
    await callback.message.edit_text("✅ Группа отвязана от вашего аккаунта.")
    await callback.answer("✅ Группа отвязана!")
    await state.clear()
    await cmd_start(callback.message, state)

@dp.callback_query(F.data == "group_manage")
@edit_only()
@check_owner()
async def back_to_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.message.edit_text("❌ Ошибка! Начните заново.")
        return
    with db.get_connection() as conn:
        r = conn.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,)).fetchone()
        title = r[0] if r else "Группа"
    await callback.message.edit_text(f"⚙️ Настройка группы: {title}\n\nВыберите действие:", reply_markup=get_group_manage_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("show_group_rules_"))
@edit_only()
@check_public()
async def show_group_rules(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    rules = db.get_rules_html(chat_id)
    if rules and db.get_rules_enabled(chat_id):
        await callback.message.answer(rules, parse_mode="HTML")
    else:
        await callback.message.answer("❌ В этом чате ещё не установлены правила.")
    await callback.answer()

@dp.callback_query(F.data.startswith("my_stats_"))
@edit_only()
@check_public()
async def my_stats(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user = callback.from_user
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    gu = db.get_global_user(user.id) or {'global_id': generate_user_id(), 'first_seen': int(time.time()), 'is_premium': getattr(user, 'is_premium', False)}
    stat = db.get_user_stat(chat_id, user.id)
    pos = db.get_user_position(chat_id, user.id, 'all')
    warns = get_spammer_warnings(user.id)
    emoji = get_premium_status_emoji(gu['is_premium'])
    if not stat:
        text = (f"Профиль {emoji} {user.full_name}\n\n" +
                f"🆔 ID: <code>{gu['global_id']}</code>\n" +
                f"📅 Впервые замечен: {format_datetime(gu['first_seen'])}\n" +
                ("⭐ Премиум пользователь\n" if gu['is_premium'] else "") +
                f"🛡️ Антиспам база Puls: {warns}/{SPAM_WARN_LIMIT} предупреждений\n\n" +
                "📊 У пользователя пока нет сообщений в этом чате")
    else:
        text = (f"Профиль {emoji} {user.full_name}\n\n" +
                f"🆔 ID: <code>{gu['global_id']}</code>\n" +
                f"📅 Впервые замечен: {format_datetime(gu['first_seen'])}\n" +
                ("⭐ Премиум пользователь\n" if gu['is_premium'] else "") +
                f"🛡️ Антиспам база Puls: {warns}/{SPAM_WARN_LIMIT} предупреждений\n\n" +
                "📊 Статистика в этом чате:\n" +
                f"• За день: {stat['day_messages']} 💬\n" +
                f"• За неделю: {stat['week_messages']} 💬\n" +
                f"• За месяц: {stat['month_messages']} 💬\n" +
                f"• Всего: {stat['all_messages']} 💬\n" +
                f"• Место в топе: {pos}")
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data.startswith("top_active_"))
@edit_only()
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
    text = "🏆 Топ активных (всего сообщений):\n\n"
    for i, (uid, cnt) in enumerate(top, 1):
        try:
            m = await bot.get_chat_member(chat_id, uid)
            name = m.user.full_name
            emoji = get_premium_status_emoji(getattr(m.user, 'is_premium', False))
            warns = get_spammer_warnings(uid)
        except:
            name = f"ID {uid}"
            emoji = ""
            warns = 0
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        warning = f" ⚠️{warns}" if warns > 0 else ""
        text += f"{medal} {emoji} {name} — {cnt} 💬{warning}\n"
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "about")
@edit_only()
@check_public()
async def about(callback: CallbackQuery):
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    text = """🤖 Puls Chat Manager

Версия: 8.0.0

📌 Возможности:
• Управление правилами
• Авто-рассылка
• Умный антифлуд (текст/медиа)
• Антиспам Пульса (глобальная база спамеров)
• Анти-ссылки (блокировка любых ссылок)
• Антимат (фильтр нецензурной лексики)
• Рейд-защита (блокировка массовых вступлений)
• Авто-комментарий к постам канала
• Автоответчик (до 100 триггеров)
• Статистика сообщений
• Приветствия
• Система модерации (мут/бан/кик/варн)
• Кнопка снятия ограничения
• Группы логов
• Подтверждение входа
• Подтверждение опасных действий
• Полная кастомизация всех сообщений и фото
• Поддержка премиум эмодзи ⭐

➕ Нажмите «Добавить в группу» чтобы пригласить меня"""
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(is_group, is_admin))
    await callback.answer()

@dp.callback_query(F.data == "help")
@edit_only()
@check_public()
async def help(callback: CallbackQuery):
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    text = """🆘 Помощь

🔹 Команды в группе:
• /rules - показать правила
• /stats - моя статистика
• /top - топ активных
• /profile - профиль пользователя
• /group - управление группой
• /puls - проверка пинга
• /mute [время] [причина] - замутить
• /unmute - размутить
• /ban [время] [причина] - забанить
• /unban - разбанить
• /kick [причина] - кикнуть
• /warn [причина] - предупредить
• /mods - список модераторов

🔹 Команды для владельца:
• /give_mute - дать право мутить
• /ungive_mute - забрать право мутить
• /give_kick - дать право кикать
• /ungive_kick - забрать право кикать
• /give_ban - дать право банить
• /ungive_ban - забрать право банить
• /give_warn - дать право варнить
• /ungive_warn - забрать право варнить

🔹 В ЛС:
• /start - главное меню
• /groupsettings - управление группами
• /loggroup - управление группами логов

🔹 Антиспам Пульса:
• Бот автоматически отслеживает 50+ сообщений в минуту
• 3 предупреждения = добавление в базу спамеров
• В профиле отображается количество предупреждений
• Поддержка: @support_puls"""
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(is_group, is_admin))
    await callback.answer()

@dp.callback_query(F.data == "admin_panel")
@edit_only()
@check_bot_admin()
async def admin_panel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await state.clear()
    status = "🟢 РАБОТАЕТ" if not technical_maintenance else "🔴 ТЕХРАБОТЫ"
    spammer_count = len(global_spammers)
    text = f"""👑 Панель администратора

Статус бота: {status}
Сообщение: {maintenance_message}
Спамеров в базе: {spammer_count}

Выберите действие:"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📊 Статистика", "admin_stats"))
    builder.add(create_button("📱 Группы", "admin_groups"))
    builder.add(create_button("👥 Пользователи", "admin_users"))
    builder.add(create_button("📋 Логи", "admin_logs"))
    builder.add(create_button("🛠 Техработы", "admin_maintenance"))
    builder.add(create_button("🚫 Спамеры", "admin_spammers"))
    builder.add(create_button("📢 Рассылка", "admin_broadcast"))
    builder.add(create_button("📦 Бэкап", "admin_backup"))
    builder.add(create_button("🎨 Кастомизация", "admin_custom"))
    builder.add(create_button("❌ Выключить", "admin_shutdown"))
    builder.add(create_button("◀ Назад", "back_to_main"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_custom")
@edit_only()
@check_bot_admin()
async def admin_custom(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("🎨 Кастомизация бота\n\nЗдесь вы можете изменить тексты и фото всех сообщений бота.\n\nВыберите раздел:", reply_markup=get_admin_custom_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_custom_texts")
@edit_only()
@check_bot_admin()
async def admin_custom_texts(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Редактирование текстов\n\nВыберите сообщение для редактирования:", reply_markup=get_texts_list_keyboard(0))
    await callback.answer()

@dp.callback_query(F.data.startswith("texts_page_"))
@edit_only()
@check_bot_admin()
async def texts_page(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    page = int(callback.data.split('_')[-1])
    await callback.message.edit_text("📝 Редактирование текстов\n\nВыберите сообщение для редактирования:", reply_markup=get_texts_list_keyboard(page))
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_text_"))
@edit_only()
@check_bot_admin()
async def edit_text(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    msg_key = callback.data.replace("edit_text_", "")
    template = customization.get_template(msg_key)
    if not template:
        await callback.answer("❌ Шаблон не найден!", show_alert=True)
        return
    current_text = template.get_text()
    has_photo = template.get_photo() is not None
    text = f"📝 Редактирование сообщения: {msg_key}\n\nТекущий текст:\n{current_text}\n\n{'🖼 У сообщения есть фото' if has_photo else ''}\n\nОтправьте новый текст для этого сообщения.\nИли отправьте /cancel для отмены."
    await state.update_data(edit_msg_key=msg_key)
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.set_state(CustomMessageStates.waiting_for_new_text)
    await callback.answer()

@dp.message(CustomMessageStates.waiting_for_new_text)
async def process_new_text(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    msg_key = data.get('edit_msg_key')
    if not msg_key:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    new_text = message.html_text.strip() if message.text else ""
    if not new_text:
        await message.answer("❌ Отправьте текст!")
        return
    template = customization.get_template(msg_key)
    if template:
        template.set_custom(new_text, None)
        db.save_custom_message(msg_key, new_text, None)
        await message.answer(f"✅ Сообщение {msg_key} обновлено!")
        await add_premium_reaction(message, "✅")
    else:
        await message.answer("❌ Шаблон не найден!")
    await state.clear()

@dp.callback_query(F.data == "admin_custom_photos")
@edit_only()
@check_bot_admin()
async def admin_custom_photos(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("🖼 Редактирование фото\n\nВыберите сообщение для изменения фото:", reply_markup=get_photos_list_keyboard(0))
    await callback.answer()

@dp.callback_query(F.data.startswith("photos_page_"))
@edit_only()
@check_bot_admin()
async def photos_page(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    page = int(callback.data.split('_')[-1])
    await callback.message.edit_text("🖼 Редактирование фото\n\nВыберите сообщение для изменения фото:", reply_markup=get_photos_list_keyboard(page))
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_photo_"))
@edit_only()
@check_bot_admin()
async def edit_photo(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    msg_key = callback.data.replace("edit_photo_", "")
    template = customization.get_template(msg_key)
    if not template:
        await callback.answer("❌ Шаблон не найден!", show_alert=True)
        return
    current_photo = template.get_photo()
    text = f"🖼 Редактирование фото для: {msg_key}\n\n{'✅ Текущее фото есть' if current_photo else '❌ Текущего фото нет'}\n\nОтправьте новое фото для этого сообщения.\nИли отправьте /reset чтобы убрать фото.\nИли отправьте /cancel для отмены."
    await state.update_data(edit_photo_key=msg_key)
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.set_state(CustomMessageStates.waiting_for_new_photo)
    await callback.answer()

@dp.message(CustomMessageStates.waiting_for_new_photo, F.photo)
async def process_new_photo(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    msg_key = data.get('edit_photo_key')
    if not msg_key:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    photo_id = message.photo[-1].file_id
    template = customization.get_template(msg_key)
    if template:
        template.set_custom(photo=photo_id)
        db.save_custom_message(msg_key, photo=photo_id)
        await message.answer(f"✅ Фото для {msg_key} обновлено!")
        await add_premium_reaction(message, "✅")
    else:
        await message.answer("❌ Шаблон не найден!")
    await state.clear()

@dp.message(CustomMessageStates.waiting_for_new_photo, F.text == "/reset")
async def reset_photo(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    msg_key = data.get('edit_photo_key')
    if not msg_key:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    template = customization.get_template(msg_key)
    if template:
        template.reset()
        db.reset_custom_message(msg_key)
        await message.answer(f"✅ Фото для {msg_key} сброшено к стандартному!")
        await add_premium_reaction(message, "✅")
    else:
        await message.answer("❌ Шаблон не найден!")
    await state.clear()

@dp.message(CustomMessageStates.waiting_for_new_photo)
async def process_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Отправьте фото или /reset!")

@dp.callback_query(F.data == "admin_custom_reset_all")
@edit_only()
@check_bot_admin()
async def admin_custom_reset_all(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, сбросить всё", "admin_custom_reset_confirm", "danger"))
    builder.add(create_button("❌ Нет", "admin_custom", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text("⚠️ Вы уверены, что хотите сбросить все кастомные настройки?\n\nВсе тексты и фото вернутся к стандартным.", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_custom_reset_confirm")
@edit_only()
@check_bot_admin()
async def admin_custom_reset_confirm(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    for key, template in customization.templates.items():
        template.reset()
        db.reset_custom_message(key)
    await callback.message.edit_text("✅ Все настройки сброшены к стандартным!", reply_markup=get_back_keyboard("admin_custom"))
    await callback.answer()

@dp.callback_query(F.data == "admin_spammers")
@edit_only()
@check_bot_admin()
async def admin_spammers(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    if not global_spammers:
        await callback.message.edit_text("✅ База спамеров пуста. Пока никто не спамил.", reply_markup=get_back_keyboard("admin_panel"))
        await callback.answer()
        return
    text = "🚫 Глобальная база спамеров:\n\n"
    for user_id, info in list(global_spammers.items())[:20]:
        reason = info.get("причина", "неизвестно")
        date = format_datetime(info.get("когда_добавлен", 0))
        unbanned_in = len(info.get("разбанен_в", set()))
        warnings = info.get("предупреждения", 1)
        text += f"• ID: {user_id}\n  Причина: {reason}\n  Предупреждений: {warnings}/{SPAM_WARN_LIMIT}\n  Добавлен: {date}\n  Разбанен в {unbanned_in} чатах\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔄 Обновить", "admin_spammers", "primary"))
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_maintenance")
@edit_only()
@check_bot_admin()
async def admin_maintenance(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    global technical_maintenance, maintenance_message
    status = "🔴 ВКЛ" if technical_maintenance else "🟢 ВЫКЛ"
    text = f"🛠 Режим технических работ\n\nСтатус: {status}\nСообщение: {maintenance_message}"
    builder = InlineKeyboardBuilder()
    if technical_maintenance:
        builder.add(create_button("🟢 Выключить", "maintenance_off", "success"))
    else:
        builder.add(create_button("🔴 Включить", "maintenance_on", "danger"))
    builder.add(create_button("✏️ Изменить сообщение", "maintenance_message", "primary"))
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "maintenance_on")
@edit_only()
@check_bot_admin()
async def maintenance_on(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    global technical_maintenance
    technical_maintenance = True
    await notify_all_groups(maintenance_message)
    await callback.answer("🛠 Техработы ВКЛЮЧЕНЫ!", show_alert=True)
    await admin_maintenance(callback)

@dp.callback_query(F.data == "maintenance_off")
@edit_only()
@check_bot_admin()
async def maintenance_off(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    global technical_maintenance
    technical_maintenance = False
    await notify_all_groups("✅ Бот снова в работе!")
    await callback.answer("🟢 Техработы ВЫКЛЮЧЕНЫ!", show_alert=True)
    await admin_maintenance(callback)

@dp.callback_query(F.data == "maintenance_message")
@edit_only()
@check_bot_admin()
async def maintenance_message(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте новое сообщение для режима техработ:")
    await state.set_state(MaintenanceStates.waiting_for_message)
    await callback.answer()

@dp.message(Command("cancel"))
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено")

@dp.message(MaintenanceStates.waiting_for_message)
async def process_maintenance_message(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    global maintenance_message
    maintenance_message = message.text
    await state.clear()
    await message.reply(f"✅ Сообщение сохранено: {maintenance_message}")
    await add_premium_reaction(message, "✅")

@dp.callback_query(F.data == "admin_stats")
@edit_only()
@check_bot_admin()
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        groups = conn.execute('SELECT COUNT(*) FROM group_rules').fetchone()[0]
        users = conn.execute('SELECT COUNT(*) FROM global_users').fetchone()[0]
        violations = conn.execute('SELECT COUNT(*) FROM violation_logs').fetchone()[0]
        triggers = conn.execute('SELECT COUNT(*) FROM auto_responses').fetchone()[0]
        mod_actions = conn.execute('SELECT COUNT(*) FROM moderator_logs').fetchone()[0]
    spammer_count = len(global_spammers)
    text = f"""📊 Статистика бота

📱 Групп: {groups}
👥 Пользователей: {users}
🚫 Нарушений: {violations}
🛡️ Действий модераторов: {mod_actions}
🚫 Спамеров в базе: {spammer_count}
🤖 Триггеров: {triggers}/{MAX_TRIGGERS}

🕐 Время сервера: {datetime.now(SERVER_TZ).strftime('%Y-%m-%d %H:%M:%S')}"""
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔄 Обновить", "admin_stats", "primary"))
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_groups")
@edit_only()
@check_bot_admin()
async def admin_groups(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        groups = conn.execute('SELECT chat_id, chat_title, rules_enabled, welcome_enabled, puls_antispam_enabled FROM group_rules LIMIT 20').fetchall()
    text = "📱 Группы (первые 20):\n\n"
    for chat_id, title, rules_enabled, welcome_enabled, puls_enabled in groups:
        status = []
        if rules_enabled:
            status.append("📜✅")
        if welcome_enabled:
            status.append("👋✅")
        if puls_enabled:
            status.append("🛡️✅")
        status_text = f" [{''.join(status)}]" if status else ""
        text += f"• {title or 'Без названия'}{status_text} | ID: {chat_id}\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
@edit_only()
@check_bot_admin()
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        users = conn.execute('SELECT full_name, global_id, first_seen, is_premium FROM global_users ORDER BY first_seen DESC LIMIT 20').fetchall()
    text = "👥 Последние пользователи:\n\n"
    for name, gid, ts, is_premium in users:
        date = format_datetime(ts)
        premium_emoji = "⭐" if is_premium else ""
        text += f"• {premium_emoji} {name}\n  ID: {gid} | {date}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_logs")
@edit_only()
@check_bot_admin()
async def admin_logs(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        logs = conn.execute('SELECT user_name, reason, punishment, timestamp FROM violation_logs ORDER BY timestamp DESC LIMIT 20').fetchall()
    text = "📋 Последние нарушения:\n\n"
    if logs:
        for name, reason, punishment, ts in logs:
            date = format_datetime(ts)
            text += f"• {name}\n  {reason} → {punishment} | {date}\n\n"
    else:
        text += "Нарушений пока нет."
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🗑 Очистить", "admin_logs_clear", "danger"))
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_logs_clear")
@edit_only()
@check_bot_admin()
async def admin_logs_clear(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, очистить", "admin_logs_clear_confirm", "danger"))
    builder.add(create_button("🚫 Нет", "admin_logs", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text("⚠️ Вы уверены, что хотите очистить все логи?\n\nЭто действие нельзя отменить!", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_logs_clear_confirm")
@edit_only()
@check_bot_admin()
async def admin_logs_clear_confirm(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        conn.execute('DELETE FROM violation_logs')
        conn.commit()
    await callback.answer("✅ Все логи очищены!", show_alert=True)
    await admin_logs(callback)

@dp.callback_query(F.data == "admin_broadcast")
@edit_only()
@check_bot_admin()
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📱 Только в группы", "broadcast_groups", "primary"))
    builder.add(create_button("💬 Только в ЛС", "broadcast_pm", "primary"))
    builder.add(create_button("🌍 В группы и ЛС", "broadcast_all", "success"))
    builder.add(create_button("◀ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text("📢 Рассылка сообщений\n\nВыберите, куда отправлять рассылку:", reply_markup=builder.as_markup())
    await state.set_state(AdminBroadcastStates.waiting_for_target)
    await callback.answer()

@dp.callback_query(F.data.startswith("broadcast_"))
@edit_only()
@check_bot_admin()
async def broadcast_target(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    target = callback.data.replace("broadcast_", "")
    await state.update_data(broadcast_target=target)
    await callback.message.edit_text("📝 Отправьте текст или медиа для рассылки.\n\nПоддерживается: текст, фото, видео, GIF, стикер, документ, аудио.\n\nИли отправьте /cancel для отмены.", reply_markup=get_back_keyboard("admin_panel"))
    await state.set_state(AdminBroadcastStates.waiting_for_text)
    await callback.answer()

@dp.message(AdminBroadcastStates.waiting_for_text)
async def process_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    target = data.get('broadcast_target')
    if not target:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    media_id = None
    media_type = None
    caption = message.caption or ""
    text = message.text or caption
    if message.photo:
        media_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        media_id = message.video.file_id
        media_type = 'video'
    elif message.animation:
        media_id = message.animation.file_id
        media_type = 'animation'
    elif message.sticker:
        media_id = message.sticker.file_id
        media_type = 'sticker'
    elif message.document:
        media_id = message.document.file_id
        media_type = 'document'
    elif message.audio:
        media_id = message.audio.file_id
        media_type = 'audio'
    chats = []
    if target in ['groups', 'all']:
        with db.get_connection() as conn:
            chats.extend([r[0] for r in conn.execute('SELECT chat_id FROM group_rules').fetchall()])
    if target in ['pm', 'all']:
        with db.get_connection() as conn:
            chats.extend([r[0] for r in conn.execute('SELECT user_id FROM global_users').fetchall()])
    if not chats:
        await message.answer("❌ Нет получателей для рассылки!")
        await state.clear()
        return
    sent, failed = 0, 0
    errors = []
    status_msg = await message.answer(f"📤 Начинаю рассылку...\nВсего получателей: {len(chats)}")
    for chat_id in set(chats):
        try:
            if media_id:
                if media_type == 'photo':
                    await bot.send_photo(chat_id, media_id, caption=text or None, parse_mode="HTML")
                elif media_type == 'video':
                    await bot.send_video(chat_id, media_id, caption=text or None, parse_mode="HTML")
                elif media_type == 'animation':
                    await bot.send_animation(chat_id, media_id, caption=text or None, parse_mode="HTML")
                elif media_type == 'sticker':
                    await bot.send_sticker(chat_id, media_id)
                elif media_type == 'document':
                    await bot.send_document(chat_id, media_id, caption=text or None, parse_mode="HTML")
                elif media_type == 'audio':
                    await bot.send_audio(chat_id, media_id, caption=text or None, parse_mode="HTML")
            else:
                await bot.send_message(chat_id, text, parse_mode="HTML")
            sent += 1
        except TelegramForbiddenError:
            failed += 1
            errors.append(f"❌ {chat_id}: Бот заблокирован или чат не найден")
        except Exception as e:
            failed += 1
            errors.append(f"❌ {chat_id}: {str(e)[:50]}")
        if (sent + failed) % 10 == 0:
            await status_msg.edit_text(f"📤 Прогресс: {sent + failed}/{len(chats)}\n✅ {sent}\n❌ {failed}")
        await asyncio.sleep(0.05)
    await status_msg.edit_text(f"✅ Рассылка завершена!\n✅ Успешно: {sent}\n❌ Ошибок: {failed}")
    if errors:
        await message.answer("\n".join(errors[:10]), parse_mode="HTML")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "admin_backup")
@edit_only()
@check_bot_admin()
async def admin_backup(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2("puls_manager.db", backup_name)
        await callback.message.answer_document(FSInputFile(backup_name), caption=f"✅ Бэкап создан: {backup_name}")
        os.remove(backup_name)
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

@dp.callback_query(F.data == "admin_shutdown")
@edit_only()
@check_bot_admin()
async def admin_shutdown(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, выключить", "admin_shutdown_confirm", "danger"))
    builder.add(create_button("🚫 Нет", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text("⚠️ Вы уверены, что хотите выключить бота?\n\nАдминистраторы всё ещё будут иметь доступ.", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_shutdown_confirm")
@edit_only()
@check_bot_admin()
async def admin_shutdown_confirm(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    global technical_maintenance, maintenance_message
    technical_maintenance = True
    maintenance_message = "🛑 Бот остановлен администратором"
    await callback.message.edit_text("🛑 Бот остановлен\n\nАдминистраторы всё ещё имеют доступ.")
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_"))
@edit_only()
@check_owner()
async def process_confirm_action(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    action = data.get('action')
    target_id = data.get('target_id')
    target_name = data.get('target_name')
    duration = data.get('duration')
    reason = data.get('reason')
    if callback.data.endswith('_yes'):
        if action == 'mute':
            await execute_mute(callback.message.chat.id, target_id, target_name, duration, reason, callback.from_user, data.get('message_id', 0))
        elif action == 'ban':
            await execute_ban(callback.message.chat.id, target_id, target_name, duration, reason, callback.from_user, data.get('message_id', 0))
        elif action == 'kick':
            await execute_kick(callback.message.chat.id, target_id, target_name, reason, callback.from_user, data.get('message_id', 0))
        await callback.message.edit_text("✅ Действие выполнено!")
    else:
        await callback.message.edit_text("❌ Действие отменено")
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("lift_"))
@edit_only()
@check_public()
async def lift_restriction(callback: CallbackQuery):
    parts = callback.data.split('_')
    action = parts[1]
    target_id = int(parts[2])
    original_message_id = int(parts[3]) if len(parts) > 3 else 0
    moderator = callback.from_user
    chat_id = callback.message.chat.id
    try:
        if action == 'mute':
            await bot.restrict_chat_member(chat_id, target_id, permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True))
            mod_link = f"<a href='tg://user?id={moderator.id}'>{moderator.full_name}</a>"
            await callback.message.edit_text(f"🔓 Ограничение снято\n\n👮 Модератор: {mod_link}", parse_mode="HTML")
            if original_message_id:
                await bot.send_message(chat_id, f"✅ Нарушения пользователя сняты модератором {mod_link}", reply_to_message_id=original_message_id, parse_mode="HTML")
        elif action == 'ban':
            await bot.unban_chat_member(chat_id, target_id)
            unban_spammer_in_chat(target_id, chat_id)
            mod_link = f"<a href='tg://user?id={moderator.id}'>{moderator.full_name}</a>"
            user_link = f"<a href='tg://user?id={target_id}'>пользователь</a>"
            await callback.message.edit_text(f"✅ Разбанен\n\n👤 Пользователь: {user_link}\n👮 Модератор: {mod_link}", parse_mode="HTML")
        await callback.answer("✅ Ограничение снято!")
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

@dp.callback_query(F.data.startswith("confirm_not_bot_"))
@edit_only()
@check_public()
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
    not_bot, rules = db.get_user_confirmation_status(chat_id, user_id)
    if conf_type == 'both' and not rules:
        await callback.message.edit_text("✅ Шаг 1 выполнен! Теперь выполните шаг 2.")
        await callback.answer()
        return
    try:
        await bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=True))
    except:
        pass
    if msg_id > 0:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=f"✅ {callback.from_user.full_name} подтвердил, что не бот")
        except:
            pass
    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("✅ Спасибо за подтверждение! Теперь вы можете писать в чат.")
    await callback.answer()
    await add_premium_reaction(callback.message, "✅")

@dp.callback_query(F.data.startswith("agree_rules_"))
@edit_only()
@check_public()
async def process_agree_rules(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id = int(parts[2])
    user_id = int(parts[3])
    msg_id = int(parts[4]) if len(parts) > 4 else 0
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
    try:
        await bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=True))
    except:
        pass
    if msg_id > 0:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=f"✅ {callback.from_user.full_name} согласился с правилами")
        except:
            pass
    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("✅ Спасибо! Теперь вы можете писать в чат.")
    await callback.answer()
    await add_premium_reaction(callback.message, "✅")

@dp.callback_query(F.data.startswith("go_to_pm_"))
@edit_only()
@check_public()
async def go_to_pm(callback: CallbackQuery):
    await callback.message.answer("💬 Откройте личные сообщения с ботом и завершите подтверждение.", reply_markup=get_pm_link_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("add_to_group_"))
async def add_to_group(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(f"➕ Добавьте бота в группу: https://t.me/{BOT_USERNAME}?startgroup=start")

@dp.callback_query(F.data.startswith("links_"))
@edit_only()
@check_owner()
async def links_punishment_callbacks(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    action = callback.data.split('_')[1]
    if action in ['warn', 'kick']:
        s = db.get_links_filter_settings(chat_id)
        db.set_links_filter_settings(chat_id, s['enabled'], action, 0)
        await callback.answer(f"✅ Наказание: {action}", show_alert=True)
        await links_manage(callback, state)
    elif action == 'mute':
        await state.update_data(links_action='mute')
        await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("links_manage"))
        await state.set_state(LinksStates.waiting_for_duration)
    elif action == 'ban':
        await state.update_data(links_action='ban')
        await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("links_manage"))
        await state.set_state(LinksStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("profanity_"))
@edit_only()
@check_owner()
async def profanity_punishment_callbacks(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    action = callback.data.split('_')[1]
    if action in ['warn', 'kick']:
        s = db.get_profanity_filter_settings(chat_id)
        db.set_profanity_filter_settings(chat_id, s['enabled'], action, 0)
        await callback.answer(f"✅ Наказание: {action}", show_alert=True)
        await profanity_manage(callback, state)
    elif action == 'mute':
        await state.update_data(profanity_action='mute')
        await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("profanity_manage"))
        await state.set_state(ProfanityStates.waiting_for_duration)
    elif action == 'ban':
        await state.update_data(profanity_action='ban')
        await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("profanity_manage"))
        await state.set_state(ProfanityStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("raid_"))
@edit_only()
@check_owner()
async def raid_punishment_callbacks(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    action = callback.data.split('_')[1]
    if action in ['warn', 'kick']:
        s = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, s['enabled'], s['limit'], s['window'], action, s['duration'])
        await callback.answer(f"✅ Наказание: {action}", show_alert=True)
        await raid_manage(callback, state)
    elif action == 'mute':
        await state.update_data(raid_action='mute')
        await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("raid_manage"))
        await state.set_state(RaidStates.waiting_for_duration)
    elif action == 'ban':
        await state.update_data(raid_action='ban')
        await callback.message.edit_text("⏱ Введите длительность в минутах (0 = навсегда):", reply_markup=get_back_keyboard("raid_manage"))
        await state.set_state(RaidStates.waiting_for_duration)
    await callback.answer()

@dp.message(LinksStates.waiting_for_duration)
async def process_links_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    action = data.get('links_action')
    if not chat_id or not action:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Введите положительное число или 0!")
            return
        duration = minutes * 60
        s = db.get_links_filter_settings(chat_id)
        db.set_links_filter_settings(chat_id, s['enabled'], action, duration)
        await message.reply(f"✅ Наказание: {action} на {format_time(duration)}")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.message(ProfanityStates.waiting_for_duration)
async def process_profanity_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    action = data.get('profanity_action')
    if not chat_id or not action:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Введите положительное число или 0!")
            return
        duration = minutes * 60
        s = db.get_profanity_filter_settings(chat_id)
        db.set_profanity_filter_settings(chat_id, s['enabled'], action, duration)
        await message.reply(f"✅ Наказание: {action} на {format_time(duration)}")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.message(RaidStates.waiting_for_limit)
async def process_raid_limit(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    try:
        limit = int(message.text)
        if limit < 3 or limit > 50:
            await message.answer("❌ Лимит должен быть от 3 до 50!")
            return
        s = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, s['enabled'], limit, s['window'], s['punishment'], s['duration'])
        await message.reply(f"✅ Лимит вступлений установлен: {limit}")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.message(RaidStates.waiting_for_window)
async def process_raid_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    try:
        window = int(message.text)
        if window < 10 or window > 300:
            await message.answer("❌ Период должен быть от 10 до 300 секунд!")
            return
        s = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, s['enabled'], s['limit'], window, s['punishment'], s['duration'])
        await message.reply(f"✅ Период установлен: {window} сек")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.message(RaidStates.waiting_for_duration)
async def process_raid_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Настройки только в ЛС!")
        await state.clear()
        return
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    action = data.get('raid_action')
    if not chat_id or not action:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Введите положительное число или 0!")
            return
        duration = minutes * 60
        s = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, s['enabled'], s['limit'], s['window'], action, duration)
        await message.reply(f"✅ Наказание: {action} на {format_time(duration)}")
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.message(AutoCommentStates.waiting_for_text)
async def process_auto_comment_text(message: Message, state: FSMContext):
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
    text = message.html_text.strip()
    if len(text) > MAX_AUTO_COMMENT_LENGTH:
        await message.answer(f"❌ Текст слишком длинный! Максимум {MAX_AUTO_COMMENT_LENGTH} символов")
        return
    s = db.get_auto_comment_settings(chat_id)
    db.set_auto_comment_settings(chat_id, s['enabled'], text, s['media_id'], s['media_type'])
    await message.reply("✅ Текст авто-комментария сохранён!")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.message(AutoCommentStates.waiting_for_media, F.photo | F.video | F.animation | F.sticker)
async def process_auto_comment_media(message: Message, state: FSMContext):
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
    media_id = None
    media_type = None
    if message.photo:
        media_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        media_id = message.video.file_id
        media_type = 'video'
    elif message.animation:
        media_id = message.animation.file_id
        media_type = 'animation'
    elif message.sticker:
        media_id = message.sticker.file_id
        media_type = 'sticker'
    caption = message.caption or ""
    s = db.get_auto_comment_settings(chat_id)
    db.set_auto_comment_settings(chat_id, s['enabled'], caption, media_id, media_type)
    await message.reply("✅ Медиа авто-комментария сохранено!")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.message(AutoCommentStates.waiting_for_media)
async def process_auto_comment_media_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Отправьте фото, видео, GIF или стикер!")

async def main():
    dp.message.middleware(CommandFloodMiddleware())
    dp.message.middleware(AntiFloodMiddleware())
    dp.message.middleware(MaintenanceMiddleware())
    dp.callback_query.middleware(MaintenanceMiddleware())
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    asyncio.create_task(cleanup_old_data())
    asyncio.create_task(clean_old_messages())
    asyncio.create_task(clean_old_logs())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
