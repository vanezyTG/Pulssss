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
from aiogram.exceptions import TelegramBadRequest

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

RAID_JOIN_LIMIT = 5
RAID_TIME_WINDOW = 30
RAID_BAN_DURATION = 3600

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

# Расширенный список матных слов
PROFANITY_WORDS = {
    'хуй', 'хуя', 'хую', 'хуем', 'хуе', 'хуи', 'хуйня', 'хуйней', 'хуйню', 'хуйне',
    'пизда', 'пизду', 'пизде', 'пиздой', 'пизды', 'пизд', 'пиздюк', 'пиздюка',
    'бля', 'блять', 'блядь', 'бляди', 'блядью', 'блядский',
    'ебать', 'ебаться', 'ебал', 'ебало', 'ебана', 'ебануть', 'ебанулся', 'ебанутая',
    'ебанный', 'ебанутый', 'ебаный', 'ебет', 'ебу', 'ебёшь',
    'заебал', 'заебали', 'заебало', 'заебись', 'заебок',
    'нахуй', 'нахуя', 'похуй', 'похуям', 'похуист', 'похую',
    'охуел', 'охуели', 'охуенно', 'охуенный', 'охуеть',
    'поебень', 'выебон', 'выебываться', 'доебался', 'приебался',
    'ебануться', 'ебанутый', 'ебашить', 'ебашит',
    'жопа', 'жопой', 'жопе', 'жопы', 'жопный',
    'мудак', 'мудака', 'мудаку', 'мудаком', 'мудаки',
    'гандон', 'гандона', 'гандону', 'гандоном', 'гандоны',
    'пидор', 'пидора', 'пидору', 'пидором', 'пидоры',
    'педик', 'педика', 'педику', 'педиком', 'педики',
    'лох', 'лоха', 'лоху', 'лохом', 'лохи',
    'лошара', 'лошары', 'лошаре', 'лошарой',
    'шлюха', 'шлюхи', 'шлюхе', 'шлюхой', 'шлюх',
    'курва', 'курвы', 'курве', 'курвой',
    'проститутка', 'проститутки',
    'сука', 'суки', 'суке', 'сукой', 'сук',
    'сучка', 'сучки', 'сучке', 'сучкой',
    'тварь', 'твари', 'тварью',
    'ублюдок', 'ублюдка', 'ублюдку', 'ублюдком',
    'дебил', 'дебила', 'дебилу', 'дебилом', 'дебилы',
    'даун', 'дауна', 'дауну', 'дауном', 'дауны',
    'идиот', 'идиота', 'идиоту', 'идиотом', 'идиоты',
    'кретин', 'кретина', 'кретину', 'кретином', 'кретины',
    'придурок', 'придурка', 'придурку', 'придурком', 'придурки',
    'долбоеб', 'долбоеба', 'долбоебу', 'долбоебом', 'долбоебы',
    'долбаеб', 'долбаеба', 'долбаебу', 'долбаебом',
    'чмо', 'чмом', 'чма', 'чму',
    'редиска', 'редиской',
    'гнида', 'гниды', 'гниде', 'гнидой',
    'мразь', 'мрази', 'мразью',
    'скотина', 'скотины', 'скотине', 'скотиной',
    'сволочь', 'сволочи', 'сволочью',
    'падла', 'падлы', 'падле', 'падлой',
    'паршивец', 'паршивца',
    'выродок', 'выродка', 'выродку', 'выродком',
    'убогий', 'убогого', 'убогому',
    'уебок', 'уебка', 'уебку', 'уебком'
}

def check_profanity(text: str) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    text_cleaned = re.sub(r'[^а-яa-z\s]', '', text_lower)
    words = set(text_cleaned.split())
    for profane in PROFANITY_WORDS:
        if profane in text_lower or profane in words:
            return True
    return False

def extract_links(text: str) -> List[str]:
    if not text:
        return []
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?::\d+)?(?:/[-\w$.+!*\'(),;:@&=?/~#%]*)?'
    tg_pattern = r'(?:t\.me|telegram\.me)/(?:[a-zA-Z0-9_]+)'
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
        self.templates['welcome_pm'] = MessageTemplate(
            'welcome_pm',
            "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\n"
            "Я помогу вам управлять чатами, следить за порядком и автоматизировать модерацию.\n\n"
            "Выберите раздел в меню ниже 👇"
        )
        
        self.templates['welcome_group'] = MessageTemplate(
            'welcome_group',
            "👋 <b>Puls Chat Manager</b>\n\n"
            "• /rules - Правила\n"
            "• /stats - Моя статистика\n"
            "• /top - Топ активных\n"
            "• /profile - Профиль пользователя\n"
            "• /group - Управление группой\n"
            "• /puls - Проверка пинга\n"
            "• /mute [время] [причина] - замутить\n"
            "• /unmute - размутить\n"
            "• /ban [время] [причина] - забанить\n"
            "• /unban - разбанить\n"
            "• /kick [причина] - кикнуть\n"
            "• /warn [причина] - предупредить\n"
            "• /mods - список модераторов"
        )
        
        self.templates['profile_header'] = MessageTemplate(
            'profile_header',
            "<b>Профиль {premium_emoji} {name}</b>"
        )
        
        self.templates['profile_id'] = MessageTemplate(
            'profile_id',
            "🆔 <b>ID:</b> <code>{global_id}</code>"
        )
        
        self.templates['profile_first_seen'] = MessageTemplate(
            'profile_first_seen',
            "📅 <b>Впервые замечен:</b> {first_seen}"
        )
        
        self.templates['profile_premium'] = MessageTemplate(
            'profile_premium',
            "⭐ <b>Премиум пользователь</b>"
        )
        
        self.templates['profile_antispam'] = MessageTemplate(
            'profile_antispam',
            "🛡️ <b>Антиспам база Puls:</b> {warnings}/{limit} предупреждений"
        )
        
        self.templates['profile_stats_header'] = MessageTemplate(
            'profile_stats_header',
            "📊 <b>Статистика в этом чате:</b>"
        )
        
        self.templates['profile_day'] = MessageTemplate(
            'profile_day',
            "• За день: {count} 💬"
        )
        
        self.templates['profile_week'] = MessageTemplate(
            'profile_week',
            "• За неделю: {count} 💬"
        )
        
        self.templates['profile_month'] = MessageTemplate(
            'profile_month',
            "• За месяц: {count} 💬"
        )
        
        self.templates['profile_total'] = MessageTemplate(
            'profile_total',
            "• Всего: {count} 💬"
        )
        
        self.templates['profile_position'] = MessageTemplate(
            'profile_position',
            "• Место в топе: {position}"
        )
        
        self.templates['profile_no_stats'] = MessageTemplate(
            'profile_no_stats',
            "📊 У пользователя пока нет сообщений в этом чате"
        )
        
        self.templates['top_header'] = MessageTemplate(
            'top_header',
            "<b>🏆 Топ активных (всего сообщений):</b>"
        )
        
        self.templates['top_entry'] = MessageTemplate(
            'top_entry',
            "{medal} {premium_emoji} {name} — {count} 💬{warnings}"
        )
        
        self.templates['welcome_simple'] = MessageTemplate(
            'welcome_simple',
            "Добро пожаловать, {premium_emoji} <b>{name}</b>!\n\n"
            "🆔 <b>ID:</b> <code>{global_id}</code>\n"
            "📅 <b>Впервые замечен:</b> {first_seen}\n"
            "{premium_line}"
            "🛡️ <b>Антиспам база Puls:</b> {warnings}/{limit} предупреждений\n\n"
            "• Username: @{username}\n"
            "• Telegram ID: <code>{user_id}</code>\n"
            "• Вошёл: {join_dt}\n"
            "• Место в топе: {position}"
        )
        
        self.templates['mute_message'] = MessageTemplate(
            'mute_message',
            "🔇 <b>Пользователь {name} замьючен</b>\n\n"
            "👮 Модератор: {moderator}\n"
            "⏱ Длительность: {duration}\n"
            "📝 Причина: {reason}"
        )
        
        self.templates['ban_message'] = MessageTemplate(
            'ban_message',
            "⛔️ <b>Пользователь {name} забанен</b>\n\n"
            "👮 Модератор: {moderator}\n"
            "⏱ Длительность: {duration}\n"
            "📝 Причина: {reason}"
        )
        
        self.templates['kick_message'] = MessageTemplate(
            'kick_message',
            "👢 <b>Пользователь {name} кикнут</b>\n\n"
            "👮 Модератор: {moderator}\n"
            "📝 Причина: {reason}"
        )
        
        self.templates['warn_message'] = MessageTemplate(
            'warn_message',
            "⚠️ <b>Предупреждение пользователю {name}</b>\n\n"
            "👮 Модератор: {moderator}\n"
            "📊 Всего предупреждений: {warn_count}/{max_warns}\n"
            "📝 Причина: {reason}"
        )
        
        self.templates['unmute_message'] = MessageTemplate(
            'unmute_message',
            "🔊 <b>Пользователь {name} размучен</b>\n\n"
            "👮 Модератор: {moderator}"
        )
        
        self.templates['lift_restriction_message'] = MessageTemplate(
            'lift_restriction_message',
            "🔓 <b>Ограничение снято</b>\n\n"
            "👮 Модератор: {moderator}"
        )
        
        self.templates['spammer_detected'] = MessageTemplate(
            'spammer_detected',
            "🚫 Обнаружен спамер в базе Пульса!\n"
            "Пользователь: {user_link}\n"
            "Причина: {reason}\n"
            "Предупреждений: {warnings}/{limit}\n\n"
            "Админы могут разблокировать в этом чате командой:\n"
            "<code>/unban {user_id}</code>"
        )
        
        self.templates['group_linked'] = MessageTemplate(
            'group_linked',
            "✅ <b>Группа успешно привязана!</b>\n\n"
            "Название: {title}\n"
            "ID: <code>{chat_id}</code>\n\n"
            "Теперь вы можете настроить её в личных сообщениях с ботом.\n"
            "Нажмите /start в ЛС, чтобы открыть главное меню."
        )
        
        self.templates['trigger_added'] = MessageTemplate(
            'trigger_added',
            "✅ Триггер '{trigger}' добавлен ({count}/{max})"
        )
        
        self.templates['trigger_exists'] = MessageTemplate(
            'trigger_exists',
            "❌ Триггер '{trigger}' уже существует"
        )
        
        self.templates['trigger_limit'] = MessageTemplate(
            'trigger_limit',
            "❌ Достигнут лимит триггеров ({max})"
        )
        
        self.templates['profanity_detected'] = MessageTemplate(
            'profanity_detected',
            "🚫 <b>Обнаружен мат</b>\n\n"
            "Пользователь: {user_link}\n"
            "Наказание: {punishment}\n"
            "Причина: использование нецензурной лексики"
        )
        
        self.templates['raid_detected'] = MessageTemplate(
            'raid_detected',
            "⚠️ <b>Обнаружена рейд-атака!</b>\n\n"
            "За {window} сек вступило {count} новых участников.\n"
            "Автоматически забанены новые участники на {duration}."
        )
    
    def get_template(self, key: str) -> MessageTemplate:
        return self.templates.get(key)
    
    def format_message(self, key: str, **kwargs) -> str:
        template = self.get_template(key)
        if template:
            try:
                return template.get_text().format(**kwargs)
            except Exception as e:
                logger.error(f"Ошибка форматирования {key}: {e}")
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
    if preserve_tags:
        allowed_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'tg-spoiler', 'a', 'strong', 'em', 'ins', 'strike', 'del', 'blockquote']
        placeholders = {}
        
        for i, tag in enumerate(allowed_tags):
            pattern_open = f'<{tag}(\\s+expandable)?>'
            pattern_close = f'</{tag}>'
            placeholder_open = f'!!TAG_{i}_OPEN!!'
            placeholder_close = f'!!TAG_{i}_CLOSE!!'
            
            def make_replace_open(tag_name):
                def replace_open(match):
                    attr_str = match.group(1) or ''
                    placeholders[placeholder_open] = f'<{tag_name}{attr_str}>'
                    return placeholder_open
                return replace_open
            
            def make_replace_close(tag_name):
                def replace_close(match):
                    placeholders[placeholder_close] = f'</{tag_name}>'
                    return placeholder_close
                return replace_close
            
            text = re.sub(pattern_open, make_replace_open(tag), text, flags=re.IGNORECASE)
            text = re.sub(pattern_close, make_replace_close(tag), text, flags=re.IGNORECASE)
        
        text = html.escape(text)
        
        for placeholder, tag_html in placeholders.items():
            text = text.replace(placeholder, tag_html)
        
        return text
    else:
        return html.escape(text)

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
            public_buttons = ['confirm_not_bot', 'agree_rules', 'go_to_pm']
            if any(callback.data.startswith(btn) for btn in public_buttons):
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
                wait_time = int(button_blocked_users[user_id] - now)
                await callback.answer(f"⚠️ Вы заблокированы на {wait_time} сек!", show_alert=True)
                return
            
            key = f"{user_id}_{callback.data}"
            user_button_presses[key] = [t for t in user_button_presses[key] if now - t < BUTTON_CHECK_TIME]
            
            if len(user_button_presses[key]) >= MAX_BUTTON_PRESSES:
                button_blocked_users[user_id] = now + BUTTON_BLOCK_TIME
                await callback.answer(f"⚠️ Слишком часто! Блокировка {BUTTON_BLOCK_TIME} сек.", show_alert=True)
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
            wait_time = int(command_blocked_users[user_id] - now)
            try:
                await event.delete()
            except:
                pass
            await event.answer(f"⚠️ Подождите {wait_time} сек!", show_alert=True)
            return

        user_commands[user_id] = [t for t in user_commands[user_id] if now - t < COMMAND_CHECK_TIME]

        if len(user_commands[user_id]) >= MAX_COMMANDS:
            command_blocked_users[user_id] = now + COMMAND_BLOCK_TIME
            try:
                await event.delete()
            except:
                pass
            await event.answer(f"⚠️ Слишком часто! Блокировка {COMMAND_BLOCK_TIME} сек.", show_alert=True)
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
        
        # ВАЖНО: Статистика обновляется для ВСЕХ пользователей, включая админов и модераторов
        db.update_message_count(chat_id, user.id)
        
        # Проверка спамера в глобальной базе
        if db.get_puls_antispam_enabled(chat_id):
            is_spammer, reason, warnings = check_spammer(user.id, chat_id)
            if is_spammer:
                await event.delete()
                user_link = f"<a href='tg://user?id={user.id}'>{html.escape(user.full_name)}</a>"
                await event.answer(
                    customization.format_message('spammer_detected',
                        user_link=user_link, reason=reason, warnings=warnings,
                        limit=SPAM_WARN_LIMIT, user_id=user.id),
                    parse_mode="HTML"
                )
                try:
                    await bot.ban_chat_member(chat_id, user.id)
                except:
                    pass
                return
        
        # Проверка подтверждения входа
        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            return await handler(event, data)
        
        # Получение настроек антифлуда
        settings = db.get_antiflood_settings(chat_id)
        
        if not settings['enabled']:
            return await handler(event, data)
        
        # Умный антифлуд
        now = time.time()
        msg_type = get_message_type(event)
        is_media = msg_type != 'text'
        
        key = f"{chat_id}_{user.id}"
        
        if key not in user_messages:
            user_messages[key] = deque(maxlen=50)
        
        # Очистка старых сообщений
        while user_messages[key] and now - user_messages[key][0][0] > settings['time_window']:
            user_messages[key].popleft()
        
        # Подсчёт сообщений по типу
        text_count = sum(1 for t, mt in user_messages[key] if mt == 'text')
        media_count = sum(1 for t, mt in user_messages[key] if mt != 'text')
        
        # Проверка флуда
        if is_media:
            if media_count >= settings['media_limit']:
                await self.handle_violation(event, chat_id, user, settings, "Медиа-флуд")
                return
        else:
            if text_count >= settings['msg_limit']:
                await self.handle_violation(event, chat_id, user, settings, "Текстовый флуд")
                return
        
        # Проверка ссылок
        text = event.text or event.caption or ""
        links_filter = db.get_links_filter_settings(chat_id)
        if links_filter['enabled'] and check_links(text):
            await self.handle_links_violation(event, chat_id, user, links_filter, text)
            return
        
        # Проверка мата
        profanity_filter = db.get_profanity_filter_settings(chat_id)
        if profanity_filter['enabled'] and check_profanity(text):
            await self.handle_profanity_violation(event, chat_id, user, profanity_filter, text)
            return
        
        user_messages[key].append((now, msg_type))
        return await handler(event, data)
    
    async def handle_violation(self, event: Message, chat_id: int, user: types.User, settings: dict, reason: str):
        warns = db.get_user_warns(chat_id, user.id)
        warn_count = warns['count']
        
        if warn_count < settings['warn_count']:
            new_warn_count = db.add_user_warn(chat_id, user.id)
            await event.reply(f"⚠️ {user.full_name}, не флуди! Предупреждение {new_warn_count}/{settings['warn_count']}")
            await add_premium_reaction(event, "⚠️")
            return
        else:
            if warn_count >= settings['warn_count']:
                punish_type = settings['punish_after_warn']
                duration = settings['punish_after_warn_duration']
                db.reset_user_warns(chat_id, user.id)
            else:
                if warn_count == 0:
                    punish_type = settings['first_punish']
                    duration = settings['first_duration']
                else:
                    punish_type = settings['repeat_punish']
                    duration = settings['repeat_duration']
        
        await self.apply_punishment(event, chat_id, user, punish_type, duration, reason)
    
    async def handle_links_violation(self, event: Message, chat_id: int, user: types.User, settings: dict, text: str):
        links = extract_links(text)
        reason = f"Отправка ссылок: {', '.join(links[:3])}"
        await self.apply_punishment(event, chat_id, user, settings['punishment'], settings['duration'], reason)
    
    async def handle_profanity_violation(self, event: Message, chat_id: int, user: types.User, settings: dict, text: str):
        reason = "Использование нецензурной лексики"
        await self.apply_punishment(event, chat_id, user, settings['punishment'], settings['duration'], reason)
    
    async def apply_punishment(self, event: Message, chat_id: int, user: types.User, punish_type: str, duration: int, reason: str):
        message_link = get_message_link(chat_id, event.message_id)
        db.log_violation(chat_id, user.id, user.full_name, reason, punish_type, event.message_id, message_link)
        
        try:
            if punish_type == 'mute':
                until = int(time.time() + duration) if duration > 0 else None
                await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
                await event.reply(
                    customization.format_message('mute_message',
                        name=user.full_name, moderator=event.from_user.full_name,
                        duration=format_time(duration), reason=reason),
                    parse_mode="HTML"
                )
            elif punish_type == 'ban':
                until = int(time.time() + duration) if duration > 0 else None
                await bot.ban_chat_member(chat_id, user.id, until_date=until)
                await event.reply(
                    customization.format_message('ban_message',
                        name=user.full_name, moderator=event.from_user.full_name,
                        duration=format_time(duration), reason=reason),
                    parse_mode="HTML"
                )
            elif punish_type == 'kick':
                await bot.ban_chat_member(chat_id, user.id)
                await bot.unban_chat_member(chat_id, user.id)
                await event.reply(
                    customization.format_message('kick_message',
                        name=user.full_name, moderator=event.from_user.full_name, reason=reason),
                    parse_mode="HTML"
                )
            elif punish_type == 'warn':
                new_warn_count = db.add_user_warn(chat_id, user.id)
                max_warns = db.get_max_warns_settings(chat_id)['max_warns']
                await event.reply(
                    customization.format_message('warn_message',
                        name=user.full_name, moderator=event.from_user.full_name,
                        warn_count=new_warn_count, max_warns=max_warns, reason=reason),
                    parse_mode="HTML"
                )
                
                # Проверка на превышение лимита варнов
                if new_warn_count >= max_warns:
                    warn_settings = db.get_max_warns_settings(chat_id)
                    await self.apply_punishment(event, chat_id, user, warn_settings['punish'], warn_settings['duration'], f"Превышение лимита варнов ({new_warn_count}/{max_warns})")
                    db.reset_user_warns(chat_id, user.id)
            
            await add_premium_reaction(event, "⚠️")
        except Exception as e:
            logger.error(f"Ошибка наказания: {e}")

def parse_time(time_str: str) -> int:
    if not time_str or time_str == '0' or time_str.lower() == 'навсегда':
        return 0
    time_str = time_str.lower().strip()
    if time_str.isdigit():
        return int(time_str) * 60
    patterns = [
        (r'(\d+)\s*с', 1), (r'(\d+)\s*сек', 1), (r'(\d+)\s*м', 60),
        (r'(\d+)\s*мин', 60), (r'(\d+)\s*ч', 3600), (r'(\d+)\s*час', 3600),
        (r'(\d+)\s*д', 86400), (r'(\d+)\s*дн', 86400),
    ]
    for pattern, multiplier in patterns:
        match = re.search(pattern, time_str)
        if match:
            return int(match.group(1)) * multiplier
    return 0

def format_time(seconds: int) -> str:
    if seconds <= 0:
        return "навсегда"
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    parts = []
    if days > 0:
        parts.append(f"{days} дн")
    if hours > 0:
        parts.append(f"{hours} ч")
    if minutes > 0:
        parts.append(f"{minutes} мин")
    if secs > 0 and days == 0:
        parts.append(f"{secs} сек")
    return " ".join(parts)

def generate_user_id() -> str:
    return ''.join(random.choices(string.digits, k=9))

def get_message_type(message: Message) -> str:
    if message.text:
        return 'text'
    elif message.photo:
        return 'photo'
    elif message.video:
        return 'video'
    elif message.animation:
        return 'animation'
    elif message.sticker:
        return 'sticker'
    elif message.voice:
        return 'voice'
    elif message.video_note:
        return 'video_note'
    elif message.document:
        return 'document'
    elif message.audio:
        return 'audio'
    return 'other'

def get_message_link(chat_id: int, message_id: int) -> str:
    chat_id_str = str(chat_id)
    if chat_id_str.startswith('-100'):
        chat_id_str = chat_id_str[4:]
    return f"https://t.me/c/{chat_id_str}/{message_id}"

def get_premium_status_emoji(is_premium: bool) -> str:
    return "⭐" if is_premium else ""

async def add_premium_reaction(message: Message, emoji: str = "⭐"):
    try:
        await message.react([ReactionTypeEmoji(emoji=emoji)])
    except:
        pass

def check_spammer(user_id: int, chat_id: int = None) -> Tuple[bool, Optional[str], int]:
    with spam_lock:
        if user_id in global_spammers:
            spammer_info = global_spammers[user_id]
            if chat_id and chat_id in spammer_info.get("разбанен_в", set()):
                return False, None, spammer_info.get("предупреждения", 0)
            return True, spammer_info.get("причина", "спам"), spammer_info.get("предупреждения", 0)
    return False, None, 0

def add_spammer_warning(user_id: int, reason: str = "подозрение на спам") -> Tuple[bool, int, bool]:
    with spam_lock:
        if user_id not in global_spammers:
            global_spammers[user_id] = {
                "причина": reason,
                "когда_добавлен": int(time.time()),
                "разбанен_в": set(),
                "предупреждения": 1
            }
            return True, 1, False
        spammer_info = global_spammers[user_id]
        current_warns = spammer_info.get("предупреждения", 0) + 1
        spammer_info["предупреждения"] = current_warns
        spammer_info["причина"] = reason
        if current_warns >= SPAM_WARN_LIMIT:
            spammer_info["причина"] = "подтвержденный спамер"
            return True, current_warns, True
        return True, current_warns, False

def remove_spammer_from_db(user_id: int) -> bool:
    with spam_lock:
        if user_id in global_spammers:
            del global_spammers[user_id]
            return True
    return False

def get_spammer_warnings(user_id: int) -> int:
    with spam_lock:
        if user_id in global_spammers:
            return global_spammers[user_id].get("предупреждения", 0)
    return 0

def add_spammer_to_db(user_id: int, reason: str, warnings: int = 1):
    with spam_lock:
        if user_id not in global_spammers:
            global_spammers[user_id] = {
                "причина": reason,
                "когда_добавлен": int(time.time()),
                "разбанен_в": set(),
                "предупреждения": warnings
            }

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
                         (chat_id INTEGER PRIMARY KEY,
                          owner_id INTEGER,
                          rules_html TEXT,
                          rules_enabled INTEGER DEFAULT 1,
                          welcome_enabled INTEGER DEFAULT 0,
                          welcome_text TEXT,
                          welcome_photo_id TEXT,
                          welcome_media_type TEXT DEFAULT 'text',
                          rules_auto_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300,
                          last_rules_message_id INTEGER,
                          last_rules_time INTEGER,
                          chat_title TEXT,
                          chat_username TEXT,
                          confirmation_type TEXT DEFAULT 'not_bot',
                          puls_antispam_enabled INTEGER DEFAULT 1,
                          confirm_ban INTEGER DEFAULT 0,
                          confirm_kick INTEGER DEFAULT 0,
                          confirm_mute INTEGER DEFAULT 0,
                          max_warns INTEGER DEFAULT 3,
                          punish_after_max_warns TEXT DEFAULT 'ban',
                          punish_after_max_warns_duration INTEGER DEFAULT 86400,
                          profanity_filter_enabled INTEGER DEFAULT 0,
                          profanity_punishment TEXT DEFAULT 'warn',
                          profanity_duration INTEGER DEFAULT 0,
                          links_filter_enabled INTEGER DEFAULT 0,
                          links_punishment TEXT DEFAULT 'warn',
                          links_duration INTEGER DEFAULT 0,
                          raid_protection_enabled INTEGER DEFAULT 0,
                          raid_join_limit INTEGER DEFAULT 5,
                          raid_time_window INTEGER DEFAULT 30,
                          raid_punishment TEXT DEFAULT 'ban',
                          raid_duration INTEGER DEFAULT 3600,
                          auto_comment_enabled INTEGER DEFAULT 0,
                          auto_comment_text TEXT,
                          auto_comment_media_id TEXT,
                          auto_comment_media_type TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS global_users
                         (user_id INTEGER PRIMARY KEY,
                          global_id TEXT UNIQUE,
                          first_seen INTEGER,
                          username TEXT,
                          full_name TEXT,
                          is_premium INTEGER DEFAULT 0)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS auto_responses
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          trigger TEXT,
                          response TEXT,
                          response_type TEXT DEFAULT 'text',
                          media_id TEXT,
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
                          punish_after_warn_duration INTEGER DEFAULT 3600)''')
            
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
            
            c.execute('''CREATE TABLE IF NOT EXISTS user_warns
                         (chat_id INTEGER,
                          user_id INTEGER,
                          warn_count INTEGER DEFAULT 0,
                          last_warn_time INTEGER,
                          PRIMARY KEY (chat_id, user_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS moderators
                         (chat_id INTEGER,
                          user_id INTEGER,
                          can_mute INTEGER DEFAULT 0,
                          can_kick INTEGER DEFAULT 0,
                          can_ban INTEGER DEFAULT 0,
                          can_warn INTEGER DEFAULT 0,
                          given_by INTEGER,
                          given_at INTEGER,
                          PRIMARY KEY (chat_id, user_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS moderator_logs
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER,
                          moderator_id INTEGER,
                          moderator_name TEXT,
                          action TEXT,
                          target_id INTEGER,
                          target_name TEXT,
                          duration INTEGER,
                          reason TEXT,
                          timestamp INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS log_groups
                         (log_group_id INTEGER PRIMARY KEY,
                          owner_id INTEGER,
                          group_title TEXT,
                          created_at INTEGER,
                          is_active INTEGER DEFAULT 1)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS log_group_settings
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          source_chat_id INTEGER,
                          log_group_id INTEGER,
                          send_violations INTEGER DEFAULT 1,
                          send_mod_actions INTEGER DEFAULT 1,
                          send_joins INTEGER DEFAULT 0,
                          send_leaves INTEGER DEFAULT 0,
                          send_messages INTEGER DEFAULT 0,
                          UNIQUE(source_chat_id, log_group_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS global_spammers
                         (user_id INTEGER PRIMARY KEY,
                          reason TEXT,
                          added_at INTEGER,
                          unbanned_in TEXT DEFAULT '[]',
                          warnings INTEGER DEFAULT 1)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS custom_messages
                         (msg_key TEXT PRIMARY KEY,
                          custom_text TEXT,
                          custom_photo TEXT)''')
            
            c.execute('CREATE INDEX IF NOT EXISTS idx_user_stats_chat ON user_stats(chat_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_user_stats_user ON user_stats(user_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_violations_time ON violation_logs(timestamp)')
            
            c.execute('SELECT user_id, reason, added_at, unbanned_in, warnings FROM global_spammers')
            for row in c.fetchall():
                user_id = row[0]
                reason = row[1]
                added_at = row[2]
                unbanned_in = set(json.loads(row[3])) if row[3] else set()
                warnings = row[4] or 1
                global_spammers[user_id] = {
                    "причина": reason,
                    "когда_добавлен": added_at,
                    "разбанен_в": unbanned_in,
                    "предупреждения": warnings
                }
            
            c.execute('SELECT msg_key, custom_text, custom_photo FROM custom_messages')
            for row in c.fetchall():
                key = row[0]
                custom_text = row[1]
                custom_photo = row[2]
                template = customization.get_template(key)
                if template:
                    template.set_custom(custom_text, custom_photo)
            
            conn.commit()
    
    def get_puls_antispam_enabled(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT puls_antispam_enabled FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return bool(result[0]) if result else True
    
    def set_puls_antispam_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET puls_antispam_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_confirmation_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT confirm_ban, confirm_kick, confirm_mute FROM group_rules WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'ban': bool(row[0]), 'kick': bool(row[1]), 'mute': bool(row[2])}
            return {'ban': False, 'kick': False, 'mute': False}
    
    def set_confirmation_setting(self, chat_id, action, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            field = f"confirm_{action}"
            c.execute(f'UPDATE group_rules SET {field} = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def get_profanity_filter_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT profanity_filter_enabled, profanity_punishment, profanity_duration FROM group_rules WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'enabled': bool(row[0]), 'punishment': row[1] or 'warn', 'duration': row[2] or 0}
            return {'enabled': False, 'punishment': 'warn', 'duration': 0}
    
    def set_profanity_filter_settings(self, chat_id, enabled, punishment, duration):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET profanity_filter_enabled = ?, profanity_punishment = ?, profanity_duration = ? WHERE chat_id = ?',
                     (1 if enabled else 0, punishment, duration, chat_id))
            conn.commit()
    
    def get_links_filter_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT links_filter_enabled, links_punishment, links_duration FROM group_rules WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'enabled': bool(row[0]), 'punishment': row[1] or 'warn', 'duration': row[2] or 0}
            return {'enabled': False, 'punishment': 'warn', 'duration': 0}
    
    def set_links_filter_settings(self, chat_id, enabled, punishment, duration):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET links_filter_enabled = ?, links_punishment = ?, links_duration = ? WHERE chat_id = ?',
                     (1 if enabled else 0, punishment, duration, chat_id))
            conn.commit()
    
    def get_raid_protection_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT raid_protection_enabled, raid_join_limit, raid_time_window, raid_punishment, raid_duration FROM group_rules WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'enabled': bool(row[0]), 'limit': row[1] or 5, 'window': row[2] or 30, 'punishment': row[3] or 'ban', 'duration': row[4] or 3600}
            return {'enabled': False, 'limit': 5, 'window': 30, 'punishment': 'ban', 'duration': 3600}
    
    def set_raid_protection_settings(self, chat_id, enabled, limit, window, punishment, duration):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET raid_protection_enabled = ?, raid_join_limit = ?, raid_time_window = ?, raid_punishment = ?, raid_duration = ? WHERE chat_id = ?',
                     (1 if enabled else 0, limit, window, punishment, duration, chat_id))
            conn.commit()
    
    def get_auto_comment_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT auto_comment_enabled, auto_comment_text, auto_comment_media_id, auto_comment_media_type FROM group_rules WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'enabled': bool(row[0]), 'text': row[1], 'media_id': row[2], 'media_type': row[3]}
            return {'enabled': False, 'text': None, 'media_id': None, 'media_type': None}
    
    def set_auto_comment_settings(self, chat_id, enabled, text, media_id, media_type):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET auto_comment_enabled = ?, auto_comment_text = ?, auto_comment_media_id = ?, auto_comment_media_type = ? WHERE chat_id = ?',
                     (1 if enabled else 0, text, media_id, media_type, chat_id))
            conn.commit()
    
    def get_max_warns_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT max_warns, punish_after_max_warns, punish_after_max_warns_duration FROM group_rules WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'max_warns': row[0] or 3, 'punish': row[1] or 'ban', 'duration': row[2] or 86400}
            return {'max_warns': 3, 'punish': 'ban', 'duration': 86400}
    
    def set_max_warns_settings(self, chat_id, max_warns, punish, duration):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET max_warns = ?, punish_after_max_warns = ?, punish_after_max_warns_duration = ? WHERE chat_id = ?',
                     (max_warns, punish, duration, chat_id))
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
                c.execute('''INSERT INTO group_rules (chat_id, owner_id, rules_html, chat_title, chat_username, confirmation_type, puls_antispam_enabled) 
                             VALUES (?, ?, ?, ?, ?, ?, 1)''', (chat_id, owner_id, rules_html, chat_title, chat_username, 'not_bot'))
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
            return [(row[0], row[1]) for row in c.fetchall()]
    
    def get_all_chats(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id, chat_title, chat_username, rules_enabled, welcome_enabled FROM group_rules ORDER BY chat_id')
            return c.fetchall()
    
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
    
    def add_auto_response(self, chat_id, trigger, response, response_type='text', media_id=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM auto_responses WHERE chat_id = ?', (chat_id,))
            count = c.fetchone()[0]
            if count >= MAX_TRIGGERS:
                return False, customization.format_message('trigger_limit', max=MAX_TRIGGERS)
            
            c.execute('SELECT 1 FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger))
            if c.fetchone():
                return False, customization.format_message('trigger_exists', trigger=trigger)
            
            c.execute('INSERT INTO auto_responses (chat_id, trigger, response, response_type, media_id, created_at) VALUES (?, ?, ?, ?, ?, ?)', 
                     (chat_id, trigger, response, response_type, media_id, int(time.time())))
            conn.commit()
            return True, customization.format_message('trigger_added', trigger=trigger, count=count+1, max=MAX_TRIGGERS)
    
    def get_auto_responses(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT trigger, response, response_type, media_id FROM auto_responses WHERE chat_id = ? ORDER BY created_at', (chat_id,))
            return [(row[0], row[1], row[2], row[3]) for row in c.fetchall()]
    
    def remove_auto_response(self, chat_id, trigger):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger))
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
    
    def get_or_create_global_user(self, user_id, username, full_name, is_premium=False):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id, is_premium FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            if result:
                if result[1] != is_premium:
                    c.execute('UPDATE global_users SET is_premium = ? WHERE user_id = ?', (1 if is_premium else 0, user_id))
                    conn.commit()
                return result[0]
            global_id = generate_user_id()
            c.execute('INSERT INTO global_users (user_id, global_id, first_seen, username, full_name, is_premium) VALUES (?, ?, ?, ?, ?, ?)', 
                     (user_id, global_id, int(time.time()), username, full_name, 1 if is_premium else 0))
            conn.commit()
            return global_id
    
    def get_global_user(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id, first_seen, username, full_name, is_premium FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            if result:
                return {
                    'global_id': result[0], 
                    'first_seen': result[1], 
                    'username': result[2], 
                    'full_name': result[3],
                    'is_premium': bool(result[4])
                }
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
            if c.rowcount == 0:
                c.execute('INSERT INTO user_stats (chat_id, user_id, join_date, all_messages, month_messages, week_messages, day_messages, last_active, left_chat) VALUES (?, ?, ?, 1, 1, 1, 1, ?, 0)', 
                         (chat_id, user_id, int(time.time()), int(time.time())))
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
                                first_punish, first_duration, repeat_punish, repeat_duration, 
                                punish_after_warn, punish_after_warn_duration
                         FROM antiflood_settings WHERE chat_id = ?''', (chat_id,))
            row = c.fetchone()
            if row:
                return {
                    'enabled': bool(row[0]), 
                    'msg_limit': row[1] or 5, 
                    'media_limit': row[2] or 3, 
                    'time_window': row[3] or 10, 
                    'warn_count': row[4] or 3,
                    'first_punish': row[5] or 'mute', 
                    'first_duration': row[6] or 60,
                    'repeat_punish': row[7] or 'ban', 
                    'repeat_duration': row[8] or 3600,
                    'punish_after_warn': row[9] or 'mute',
                    'punish_after_warn_duration': row[10] or 3600
                }
            return {
                'enabled': False, 
                'msg_limit': 5, 
                'media_limit': 3, 
                'time_window': 10, 
                'warn_count': 3,
                'first_punish': 'mute', 
                'first_duration': 60, 
                'repeat_punish': 'ban', 
                'repeat_duration': 3600,
                'punish_after_warn': 'mute',
                'punish_after_warn_duration': 3600
            }
    
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
                defaults = {
                    'enabled': 0, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3,
                    'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600,
                    'punish_after_warn': 'mute', 'punish_after_warn_duration': 3600
                }
                defaults.update(kwargs)
                c.execute('''INSERT INTO antiflood_settings 
                             (chat_id, enabled, msg_limit, media_limit, time_window, warn_count, 
                              first_punish, first_duration, repeat_punish, repeat_duration,
                              punish_after_warn, punish_after_warn_duration) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (chat_id, defaults['enabled'], defaults['msg_limit'], defaults['media_limit'],
                           defaults['time_window'], defaults['warn_count'],
                           defaults['first_punish'], defaults['first_duration'],
                           defaults['repeat_punish'], defaults['repeat_duration'],
                           defaults['punish_after_warn'], defaults['punish_after_warn_duration']))
            conn.commit()
    
    def get_user_warns(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT warn_count, last_warn_time FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            row = c.fetchone()
            if row:
                return {'count': row[0], 'last_time': row[1]}
            return {'count': 0, 'last_time': 0}
    
    def add_user_warn(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT warn_count FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            row = c.fetchone()
            if row:
                new_count = row[0] + 1
                c.execute('UPDATE user_warns SET warn_count = ?, last_warn_time = ? WHERE chat_id = ? AND user_id = ?', 
                         (new_count, int(time.time()), chat_id, user_id))
            else:
                new_count = 1
                c.execute('INSERT INTO user_warns (chat_id, user_id, warn_count, last_warn_time) VALUES (?, ?, ?, ?)',
                         (chat_id, user_id, 1, int(time.time())))
            conn.commit()
            return new_count
    
    def reset_user_warns(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            conn.commit()
    
    def log_violation(self, chat_id, user_id, user_name, reason, punishment, message_id, message_link):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO violation_logs (chat_id, user_id, user_name, reason, punishment, message_id, message_link, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', 
                     (chat_id, user_id, user_name, reason, punishment, message_id, message_link, int(time.time())))
            conn.commit()
    
    def get_moderator_permissions(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT can_mute, can_kick, can_ban, can_warn FROM moderators WHERE chat_id = ? AND user_id = ?''', (chat_id, user_id))
            row = c.fetchone()
            if row:
                return {'can_mute': bool(row[0]), 'can_kick': bool(row[1]), 'can_ban': bool(row[2]), 'can_warn': bool(row[3])}
            return {'can_mute': False, 'can_kick': False, 'can_ban': False, 'can_warn': False}
    
    def set_moderator_permission(self, chat_id, user_id, permission, value, given_by):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM moderators WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            exists = c.fetchone()
            if exists:
                c.execute(f'UPDATE moderators SET {permission} = ?, given_by = ?, given_at = ? WHERE chat_id = ? AND user_id = ?',
                         (1 if value else 0, given_by, int(time.time()), chat_id, user_id))
            else:
                defaults = {'can_mute': 0, 'can_kick': 0, 'can_ban': 0, 'can_warn': 0}
                defaults[permission] = 1 if value else 0
                c.execute('''INSERT INTO moderators 
                             (chat_id, user_id, can_mute, can_kick, can_ban, can_warn, given_by, given_at) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                         (chat_id, user_id, defaults['can_mute'], defaults['can_kick'], 
                          defaults['can_ban'], defaults['can_warn'], given_by, int(time.time())))
            conn.commit()
    
    def get_all_moderators(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT user_id, can_mute, can_kick, can_ban, can_warn FROM moderators WHERE chat_id = ?''', (chat_id,))
            return c.fetchall()
    
    def log_moderator_action(self, chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO moderator_logs 
                         (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, timestamp)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, int(time.time())))
            conn.commit()
    
    def create_log_group(self, log_group_id, owner_id, group_title):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO log_groups (log_group_id, owner_id, group_title, created_at, is_active)
                         VALUES (?, ?, ?, ?, 1)''',
                     (log_group_id, owner_id, group_title, int(time.time())))
            conn.commit()
            return True
    
    def get_log_group(self, log_group_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM log_groups WHERE log_group_id = ?', (log_group_id,))
            return c.fetchone()
    
    def get_user_log_groups(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT log_group_id, group_title FROM log_groups WHERE owner_id = ?', (user_id,))
            return c.fetchall()
    
    def set_source_chat_log_group(self, source_chat_id, log_group_id, settings=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            if settings:
                c.execute('''INSERT OR REPLACE INTO log_group_settings 
                             (source_chat_id, log_group_id, send_violations, send_mod_actions, 
                              send_joins, send_leaves, send_messages)
                             VALUES (?, ?, ?, ?, ?, ?, ?)''',
                         (source_chat_id, log_group_id,
                          settings.get('send_violations', 1), settings.get('send_mod_actions', 1),
                          settings.get('send_joins', 0), settings.get('send_leaves', 0), settings.get('send_messages', 0)))
            else:
                c.execute('''INSERT OR REPLACE INTO log_group_settings 
                             (source_chat_id, log_group_id, send_violations, send_mod_actions)
                             VALUES (?, ?, 1, 1)''',
                         (source_chat_id, log_group_id))
            conn.commit()
    
    def get_source_chat_log_group(self, source_chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT lgs.*, lg.group_title 
                         FROM log_group_settings lgs
                         JOIN log_groups lg ON lgs.log_group_id = lg.log_group_id
                         WHERE lgs.source_chat_id = ?''', (source_chat_id,))
            return c.fetchone()
    
    def update_log_group_settings(self, source_chat_id, log_group_id, **kwargs):
        with self.get_connection() as conn:
            c = conn.cursor()
            if kwargs:
                fields = ', '.join(f"{k}=?" for k in kwargs)
                values = list(kwargs.values()) + [source_chat_id, log_group_id]
                c.execute(f'UPDATE log_group_settings SET {fields} WHERE source_chat_id = ? AND log_group_id = ?', values)
                conn.commit()
                return True
            return False
    
    def remove_source_chat_log_group(self, source_chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM log_group_settings WHERE source_chat_id = ?', (source_chat_id,))
            conn.commit()
    
    def save_custom_message(self, key: str, text: str = None, photo: str = None):
        with self.get_connection() as conn:
            c = conn.cursor()
            if text is not None or photo is not None:
                existing = c.execute('SELECT 1 FROM custom_messages WHERE msg_key = ?', (key,)).fetchone()
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
                    c.execute(f'UPDATE custom_messages SET {", ".join(updates)} WHERE msg_key = ?', params)
                else:
                    c.execute('INSERT INTO custom_messages (msg_key, custom_text, custom_photo) VALUES (?, ?, ?)',
                             (key, text, photo))
                conn.commit()
                return True
            return False
    
    def reset_custom_message(self, key: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM custom_messages WHERE msg_key = ?', (key,))
            conn.commit()
            return True

db = Database()

# States
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
    waiting_for_remove_trigger = State()

class LinksStates(StatesGroup):
    waiting_for_duration = State()
    waiting_for_max_mentions = State()
    waiting_for_mention_window = State()

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
    waiting_for_unban_user = State()
    waiting_for_unmute_user = State()
    waiting_for_confirm_action = State()
    waiting_for_give_mute_user = State()
    waiting_for_give_kick_user = State()
    waiting_for_give_ban_user = State()
    waiting_for_give_warn_user = State()

class LogGroupStates(StatesGroup):
    waiting_for_log_group_id = State()
    waiting_for_log_settings = State()

class CustomMessageStates(StatesGroup):
    waiting_for_message_key = State()
    waiting_for_new_text = State()
    waiting_for_new_photo = State()

# Utility functions
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

async def send_to_log_group(source_chat_id, event_type, data):
    log_group_info = db.get_source_chat_log_group(source_chat_id)
    if not log_group_info:
        return False
    log_group_id = log_group_info['log_group_id']
    settings = {
        'send_violations': log_group_info['send_violations'],
        'send_mod_actions': log_group_info['send_mod_actions'],
        'send_joins': log_group_info['send_joins'],
        'send_leaves': log_group_info['send_leaves'],
        'send_messages': log_group_info['send_messages']
    }
    if event_type == 'violation' and not settings['send_violations']:
        return False
    if event_type == 'mod_action' and not settings['send_mod_actions']:
        return False
    if event_type == 'join' and not settings['send_joins']:
        return False
    if event_type == 'leave' and not settings['send_leaves']:
        return False
    if event_type == 'message' and not settings['send_messages']:
        return False
    try:
        await bot.send_message(log_group_id, data, parse_mode="HTML")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки в лог-группу {log_group_id}: {e}")
        return False

async def check_and_handle_spam(message: Message) -> bool:
    user_id = message.from_user.id
    chat_id = message.chat.id
    now = time.time()
    
    is_spammer, spam_reason, warnings = check_spammer(user_id, chat_id)
    if is_spammer:
        await message.delete()
        user_link = f"<a href='tg://user?id={user_id}'>{html.escape(message.from_user.full_name)}</a>"
        spammer_text = customization.format_message(
            'spammer_detected',
            user_link=user_link,
            reason=spam_reason,
            warnings=warnings,
            limit=SPAM_WARN_LIMIT,
            user_id=user_id
        )
        await message.answer(spammer_text, parse_mode="HTML")
        try:
            await bot.ban_chat_member(chat_id, user_id)
        except:
            pass
        return True
    
    user_messages[user_id] = [t for t in user_messages[user_id] if now - t < SPAM_CHECK_TIME]
    user_messages[user_id].append(now)
    
    if len(user_messages[user_id]) >= SPAM_MESSAGE_LIMIT:
        added, current_warns, limit_reached = add_spammer_warning(user_id, f"отправил {len(user_messages[user_id])} сообщений за минуту")
        add_spammer_to_db(user_id, f"отправил {len(user_messages[user_id])} сообщений за минуту", current_warns)
        try:
            if current_warns == 1:
                warn_template = 'spam_warning_1'
            elif current_warns == 2:
                warn_template = 'spam_warning_2'
            else:
                warn_template = 'spam_warning_3'
            
            warn_message = customization.format_message(
                warn_template,
                count=len(user_messages[user_id]),
                current=current_warns,
                limit=SPAM_WARN_LIMIT,
                support_link=SUPPORT_LINK
            )
            await bot.send_message(user_id, warn_message, parse_mode="HTML")
        except:
            pass
        await message.delete()
        if limit_reached:
            await message.answer(f"🚫 Пользователь {message.from_user.full_name} добавлен в антиспам базу Пульса!")
            try:
                await bot.ban_chat_member(chat_id, user_id)
            except:
                pass
        return True
    return False

async def clean_old_messages():
    while True:
        now = time.time()
        for user_id in list(user_messages.keys()):
            user_messages[user_id] = [t for t in user_messages[user_id] if now - t < SPAM_CHECK_TIME]
            if not user_messages[user_id]:
                del user_messages[user_id]
        for key in list(user_button_presses.keys()):
            user_button_presses[key] = [t for t in user_button_presses[key] if now - t < BUTTON_CHECK_TIME]
            if not user_button_presses[key]:
                del user_button_presses[key]
        await asyncio.sleep(300)

async def clean_old_logs():
    while True:
        old_time = int(time.time()) - 30 * 86400
        with db.get_connection() as conn:
            conn.execute('DELETE FROM violation_logs WHERE timestamp < ?', (old_time,))
            conn.execute('DELETE FROM moderator_logs WHERE timestamp < ?', (old_time,))
            conn.commit()
        await asyncio.sleep(86400)

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
                    logger.info("⭐ Счетчики сброшены")
            except Exception as e:
                logger.error(f"❌ Ошибка сброса: {e}")
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
                        msg = await bot.send_message(chat_id, f"<b>📢 Напоминание правил</b>\n\n{safe_html(rules_html, True)}", parse_mode="HTML")
                        try:
                            await bot.pin_chat_message(chat_id, msg.message_id)
                        except:
                            pass
                        db.update_last_rules(chat_id, msg.message_id)
                    except Exception as e:
                        logger.error(f"❌ Ошибка отправки правил в {chat_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка в фоновой задаче: {e}")
        await asyncio.sleep(60)

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

# Keyboard builders
def create_button(text: str, callback_data: str, color: str = None):
    return InlineKeyboardButton(text=text, callback_data=callback_data)

def get_back_keyboard(callback_data):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", callback_data, "secondary"))
    return builder.as_markup()

def get_main_keyboard(is_group: bool = False, is_admin: bool = False):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("ℹ️ О боте", "about", "primary"))
    builder.add(create_button("🆘 Помощь", "help", "danger"))
    builder.add(create_button("➕ Добавить в группу", f"add_to_group_{BOT_USERNAME}", "success"))
    builder.add(create_button("⚙️ Настройки групп", "group_manage_main", "primary"))
    if is_group:
        builder.add(create_button("📜 Правила", "show_rules_group", "secondary"))
        builder.add(create_button("📊 Статистика", "my_stats_group", "secondary"))
        builder.add(create_button("🏆 Топ", "top_active_group", "success"))
    if is_admin and not is_group:
        builder.add(create_button("👑 Админ панель", "admin_panel", "danger"))
    builder.adjust(2)
    return builder.as_markup()

def get_group_manage_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Правила", "manage_rules", "primary"))
    builder.add(create_button("👋 Приветствие", "manage_welcome", "secondary"))
    builder.add(create_button("🔄 Авто-рассылка", "rules_auto", "secondary"))
    builder.add(create_button("🚫 Антифлуд", "antiflood_manage", "primary"))
    builder.add(create_button("🛡️ Антиспам Пульса", "puls_antispam_manage", "danger"))
    builder.add(create_button("✅ Подтверждение действий", "confirmation_actions_manage", "primary"))
    builder.add(create_button("📋 Группа логов", "log_group_manage", "secondary"))
    builder.add(create_button("🤖 Автоответчик", "auto_response_manage", "success"))
    builder.add(create_button("🔗 Анти-ссылки", "links_manage", "secondary"))
    builder.add(create_button("🚫 Антимат", "profanity_manage", "secondary"))
    builder.add(create_button("🛡️ Рейд-защита", "raid_manage", "danger"))
    builder.add(create_button("💬 Авто-комментарий", "auto_comment_manage", "primary"))
    builder.add(create_button("✅ Подтверждение входа", "confirmation_manage", "primary"))
    builder.add(create_button("🛡️ Модераторы", "moderators_manage", "primary"))
    builder.add(create_button("❌ Отвязать", "unlink_group_confirm", "danger"))
    builder.add(create_button("◀️ Назад", "back_to_groups", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_actions_keyboard(settings):
    builder = InlineKeyboardBuilder()
    ban_status = "✅" if settings.get('ban', False) else "❌"
    kick_status = "✅" if settings.get('kick', False) else "❌"
    mute_status = "✅" if settings.get('mute', False) else "❌"
    builder.add(create_button(f"{ban_status} Подтверждение бана", "toggle_confirm_ban", "secondary"))
    builder.add(create_button(f"{kick_status} Подтверждение кика", "toggle_confirm_kick", "secondary"))
    builder.add(create_button(f"{mute_status} Подтверждение мута", "toggle_confirm_mute", "secondary"))
    builder.add(create_button("ℹ️ Что это?", "confirmation_actions_info", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_puls_antispam_keyboard(enabled):
    builder = InlineKeyboardBuilder()
    status_text = "❌ Выключить" if enabled else "✅ Включить"
    status_color = "danger" if enabled else "success"
    builder.add(create_button(status_text, "toggle_puls_antispam", status_color))
    builder.add(create_button("ℹ️ Что это?", "puls_antispam_info", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status_text = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    status_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(status_text, "toggle_links_filter", status_color))
    builder.add(create_button(f"Наказание: {settings['punishment']}", "set_links_punishment", "primary"))
    if settings['punishment'] in ['mute', 'ban']:
        builder.add(create_button(f"Длительность: {format_time(settings['duration'])}", "set_links_duration", "secondary"))
    builder.add(create_button("ℹ️ Что это?", "links_filter_info", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_profanity_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status_text = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    status_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(status_text, "toggle_profanity_filter", status_color))
    builder.add(create_button(f"Наказание: {settings['punishment']}", "set_profanity_punishment", "primary"))
    if settings['punishment'] in ['mute', 'ban']:
        builder.add(create_button(f"Длительность: {format_time(settings['duration'])}", "set_profanity_duration", "secondary"))
    builder.add(create_button("ℹ️ Что это?", "profanity_filter_info", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_raid_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status_text = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    status_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(status_text, "toggle_raid_protection", status_color))
    builder.add(create_button(f"Лимит вступлений: {settings['limit']}", "set_raid_limit", "secondary"))
    builder.add(create_button(f"Период: {settings['window']} сек", "set_raid_window", "secondary"))
    builder.add(create_button(f"Наказание: {settings['punishment']}", "set_raid_punishment", "primary"))
    if settings['punishment'] in ['mute', 'ban']:
        builder.add(create_button(f"Длительность: {format_time(settings['duration'])}", "set_raid_duration", "secondary"))
    builder.add(create_button("ℹ️ Что это?", "raid_protection_info", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_comment_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status_text = "❌ Выключить" if settings['enabled'] else "✅ Включить"
    status_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(status_text, "toggle_auto_comment", status_color))
    builder.add(create_button("📝 Установить текст", "set_auto_comment_text", "primary"))
    builder.add(create_button("🖼 Установить медиа", "set_auto_comment_media", "primary"))
    if settings['text'] or settings['media_id']:
        builder.add(create_button("👁 Просмотреть", "view_auto_comment", "secondary"))
    builder.add(create_button("ℹ️ Что это?", "auto_comment_info", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_punishment_keyboard(action_type):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"{action_type}_warn", "secondary"))
    builder.add(create_button("🔇 Mute", f"{action_type}_mute", "primary"))
    builder.add(create_button("👢 Kick", f"{action_type}_kick", "danger"))
    builder.add(create_button("⛔️ Ban", f"{action_type}_ban", "danger"))
    builder.add(create_button("◀️ Назад", f"back_to_{action_type.replace('_', '_')}", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_action_keyboard(action, user_id, duration=None, reason=None):
    builder = InlineKeyboardBuilder()
    data_prefix = f"confirm_{action}_{user_id}"
    if duration:
        data_prefix += f"_{duration}"
    if reason:
        short_reason = reason[:20] if reason else "none"
        data_prefix += f"_{short_reason}"
    builder.add(create_button("✅ Подтверждаю", f"{data_prefix}_yes", "danger"))
    builder.add(create_button("❌ Отмена", f"{data_prefix}_no", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_lift_restriction_keyboard(action, user_id, message_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔓 Снять ограничение", f"lift_{action}_{user_id}_{message_id}", "success"))
    return builder.as_markup()

def get_moderators_manage_keyboard(moderators):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Дать права", "give_mod_rights", "success"))
    if moderators:
        builder.add(create_button("❌ Забрать права", "remove_mod_rights", "danger"))
    builder.add(create_button("👁 Список", "list_moderators", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_mod_rights_keyboard(user_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔇 Право мутить", f"give_mute_{user_id}", "primary"))
    builder.add(create_button("👢 Право кикать", f"give_kick_{user_id}", "danger"))
    builder.add(create_button("⛔ Право банить", f"give_ban_{user_id}", "danger"))
    builder.add(create_button("⚠️ Право варнить", f"give_warn_{user_id}", "secondary"))
    builder.add(create_button("◀️ Назад", "moderators_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_log_group_manage_keyboard(has_log_group, log_group_info=None):
    builder = InlineKeyboardBuilder()
    if has_log_group and log_group_info:
        builder.add(create_button("📊 Настройки логов", "log_group_settings", "primary"))
        builder.add(create_button("🔄 Отвязать", "unlink_log_group", "danger"))
        builder.add(create_button("👁 Инфо", "log_group_info", "secondary"))
    else:
        builder.add(create_button("➕ Привязать группу логов", "link_log_group", "success"))
        builder.add(create_button("ℹ️ Как создать", "log_group_help", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_log_settings_keyboard(settings):
    builder = InlineKeyboardBuilder()
    status_violations = "✅" if settings.get('send_violations', 1) else "❌"
    status_mod = "✅" if settings.get('send_mod_actions', 1) else "❌"
    status_joins = "✅" if settings.get('send_joins', 0) else "❌"
    status_leaves = "✅" if settings.get('send_leaves', 0) else "❌"
    status_messages = "✅" if settings.get('send_messages', 0) else "❌"
    builder.add(create_button(f"{status_violations} Нарушения", "toggle_log_violations", "secondary"))
    builder.add(create_button(f"{status_mod} Действия модераторов", "toggle_log_mod", "secondary"))
    builder.add(create_button(f"{status_joins} Входы", "toggle_log_joins", "secondary"))
    builder.add(create_button(f"{status_leaves} Выходы", "toggle_log_leaves", "secondary"))
    builder.add(create_button(f"{status_messages} Сообщения", "toggle_log_messages", "secondary"))
    builder.add(create_button("◀️ Назад", "log_group_manage", "secondary"))
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
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if enabled else "success"
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_welcome", toggle_color))
    builder.add(create_button("📝 Текст", "set_welcome_text", "primary"))
    builder.add(create_button("🖼 Фото", "set_welcome_photo", "primary"))
    builder.add(create_button("👁 Посмотреть", "show_welcome", "secondary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if enabled else "success"
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_rules_auto", toggle_color))
    builder.add(create_button("⏱ Интервал", "set_interval", "primary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if settings['enabled'] else "success"
    builder.add(create_button(f"{'❌ Выключить' if settings['enabled'] else '✅ Включить'}", "toggle_antiflood", toggle_color))
    builder.add(create_button(f"📝 Текст: {settings['msg_limit']}", "set_msg_limit", "secondary"))
    builder.add(create_button(f"🎬 Медиа: {settings['media_limit']}", "set_media_limit", "secondary"))
    builder.add(create_button(f"⏱ Период: {settings['time_window']} сек", "set_window", "secondary"))
    builder.add(create_button(f"⚠️ Предупреждений: {settings['warn_count']}", "set_warn_count", "secondary"))
    builder.add(create_button("🔇 Первое наказание", "set_first_punish", "primary"))
    builder.add(create_button("🔊 Повторное", "set_repeat_punish", "primary"))
    builder.add(create_button("⚠️ После варнов", "set_punish_after_warn", "primary"))
    builder.add(create_button("◀️ Назад", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(punish_type="first"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"punish_warn_{punish_type}", "secondary"))
    builder.add(create_button("🔇 Мут", f"punish_mute_{punish_type}", "primary"))
    builder.add(create_button("👢 Кик", f"punish_kick_{punish_type}", "danger"))
    builder.add(create_button("⛔️ Бан", f"punish_ban_{punish_type}", "danger"))
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
    builder.add(create_button("🚫 Отмена", "cancel_link", "danger"))
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
    builder.add(create_button("💬 Перейти в ЛС", "go_to_pm", "primary"))
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
    for i, (trigger, _, _, _) in enumerate(responses):
        short = trigger[:15] + "..." if len(trigger) > 15 else trigger
        builder.add(create_button(short, f"rem_trig_{i}", "danger"))
    builder.add(create_button("◀️ Назад", "auto_response_manage", "secondary"))
    builder.adjust(1)
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

def get_admin_custom_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Тексты сообщений", "admin_custom_texts", "primary"))
    builder.add(create_button("🖼 Фото сообщений", "admin_custom_photos", "primary"))
    builder.add(create_button("🔄 Сбросить всё", "admin_custom_reset_all", "danger"))
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_texts_list_keyboard(page=0):
    templates = []
    for key, template in customization.templates.items():
        templates.append((
            key,
            template.get_text()[:50] + "..." if len(template.get_text()) > 50 else template.get_text(),
            template.get_photo() is not None
        ))
    items_per_page = 10
    start = page * items_per_page
    end = start + items_per_page
    current_templates = templates[start:end]
    
    builder = InlineKeyboardBuilder()
    for key, preview, has_photo in current_templates:
        photo_emoji = "🖼" if has_photo else ""
        builder.add(create_button(f"{photo_emoji} {key}", f"edit_text_{key}", "secondary"))
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(create_button("◀️", f"texts_page_{page-1}", "secondary"))
    if end < len(templates):
        nav_buttons.append(create_button("▶️", f"texts_page_{page+1}", "secondary"))
    
    if nav_buttons:
        builder.row(*nav_buttons)
    
    builder.add(create_button("◀️ Назад", "admin_custom", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_photos_list_keyboard(page=0):
    templates = [(k, v.get_text()[:30]) for k, v in customization.templates.items() if v.get_photo()]
    items_per_page = 10
    start = page * items_per_page
    end = start + items_per_page
    current_templates = templates[start:end]
    
    builder = InlineKeyboardBuilder()
    for key, preview in current_templates:
        builder.add(create_button(f"🖼 {key}", f"edit_photo_{key}", "secondary"))
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(create_button("◀️", f"photos_page_{page-1}", "secondary"))
    if end < len(templates):
        nav_buttons.append(create_button("▶️", f"photos_page_{page+1}", "secondary"))
    
    if nav_buttons:
        builder.row(*nav_buttons)
    
    builder.add(create_button("◀️ Назад", "admin_custom", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

# Message handlers
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    is_premium = getattr(message.from_user, 'is_premium', False)
    is_admin = message.from_user.id in ADMIN_IDS
    is_group = message.chat.type != 'private'
    
    if message.chat.type == 'private':
        welcome_text = customization.get_template('welcome_pm').get_text()
    else:
        welcome_text = customization.get_template('welcome_group').get_text()
    
    photo = customization.get_photo('welcome_pm' if message.chat.type == 'private' else 'welcome_group')
    
    if photo:
        await bot.send_photo(
            message.chat.id,
            photo=photo,
            caption=welcome_text,
            reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            welcome_text,
            reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin),
            parse_mode="HTML"
        )
    await add_premium_reaction(message, "⭐")

@dp.message(Command("groupsettings"))
@pm_only()
async def cmd_group_settings(message: Message, state: FSMContext):
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
    await add_premium_reaction(message, "📱")

@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["пульс", "понг"]))
async def cmd_ping(message: Message):
    start = time.time()
    msg = await message.reply("⏳ ...")
    ping = round((time.time() - start) * 1000)
    await msg.edit_text(f"📡 <b>Пинг:</b> {ping} мс\n⏱ <b>Время:</b> {ping/1000:.2f} сек", parse_mode="HTML")
    await add_premium_reaction(message, "📡")

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
    is_premium = getattr(user, 'is_premium', False)
    global_user = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
    global_user_data = db.get_global_user(user.id)
    stat = db.get_user_stat(chat_id, user.id)
    position = db.get_user_position(chat_id, user.id, 'all')
    warnings = get_spammer_warnings(user.id)
    
    premium_emoji = get_premium_status_emoji(global_user_data['is_premium'])
    
    if not stat:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        no_stats = customization.get_template('profile_no_stats').get_text()
        
        text = f"{header}\n\n{id_line}\n{first_seen}\n{premium_line}\n{antispam}\n\n{no_stats}"
    else:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        stats_header = customization.get_template('profile_stats_header').get_text()
        day = customization.format_message('profile_day', count=stat['day_messages'])
        week = customization.format_message('profile_week', count=stat['week_messages'])
        month = customization.format_message('profile_month', count=stat['month_messages'])
        total = customization.format_message('profile_total', count=stat['all_messages'])
        position_line = customization.format_message('profile_position', position=position)
        
        text = (
            f"{header}\n\n"
            f"{id_line}\n"
            f"{first_seen}\n"
            f"{premium_line}\n"
            f"{antispam}\n\n"
            f"{stats_header}\n"
            f"{day}\n"
            f"{week}\n"
            f"{month}\n"
            f"{total}\n"
            f"{position_line}"
        )
    
    await message.reply(safe_html(text, True), parse_mode="HTML")
    await add_premium_reaction(message, "📊")

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
    
    header = customization.get_template('top_header').get_text()
    text = f"{header}\n\n"
    
    for i, (user_id, count) in enumerate(top, 1):
        try:
            member = await bot.get_chat_member(message.chat.id, user_id)
            name = member.user.full_name
            is_premium = getattr(member.user, 'is_premium', False)
            premium_emoji = get_premium_status_emoji(is_premium)
            warnings = get_spammer_warnings(user_id)
        except:
            name = f"ID {user_id}"
            premium_emoji = ""
            warnings = 0
        
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        warning_text = f" ⚠️{warnings}" if warnings > 0 else ""
        
        entry = customization.format_message(
            'top_entry',
            medal=medal,
            premium_emoji=premium_emoji,
            name=name,
            count=count,
            warnings=warning_text
        )
        text += f"{entry}\n"
    
    await message.reply(safe_html(text, True), parse_mode="HTML")
    await add_premium_reaction(message, "🏆")

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
    is_premium = getattr(target_user, 'is_premium', False)
    global_user = db.get_or_create_global_user(target_user.id, target_user.username or "", target_user.full_name or "", is_premium)
    global_user_data = db.get_global_user(target_user.id)
    stat = db.get_user_stat(chat_id, target_user.id)
    position = db.get_user_position(chat_id, target_user.id, 'all')
    warnings = get_spammer_warnings(target_user.id)
    
    premium_emoji = get_premium_status_emoji(global_user_data['is_premium'])
    
    if not stat:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=target_user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        no_stats = customization.get_template('profile_no_stats').get_text()
        
        text = f"{header}\n\n{id_line}\n{first_seen}\n{premium_line}\n{antispam}\n\n{no_stats}"
    else:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=target_user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        stats_header = customization.get_template('profile_stats_header').get_text()
        day = customization.format_message('profile_day', count=stat['day_messages'])
        week = customization.format_message('profile_week', count=stat['week_messages'])
        month = customization.format_message('profile_month', count=stat['month_messages'])
        total = customization.format_message('profile_total', count=stat['all_messages'])
        position_line = customization.format_message('profile_position', position=position)
        
        text = (
            f"{header}\n\n"
            f"{id_line}\n"
            f"{first_seen}\n"
            f"{premium_line}\n"
            f"{antispam}\n\n"
            f"{stats_header}\n"
            f"{day}\n"
            f"{week}\n"
            f"{month}\n"
            f"{total}\n"
            f"{position_line}"
        )
    
    await message.reply(safe_html(text, True), parse_mode="HTML")
    await add_premium_reaction(message, "👤")

@dp.message(Command("rules"))
@group_only()
async def cmd_rules(message: Message):
    rules = db.get_rules_html(message.chat.id)
    if rules and db.get_rules_enabled(message.chat.id):
        await message.reply(safe_html(rules, True), parse_mode="HTML")
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
        chat_title = chat.title or "Группа"
        chat_username = chat.username
    except Exception as e:
        logger.error(f"Ошибка при получении информации о группе: {e}")
        chat_title = "Группа"
        chat_username = None
    
    db.save_rules(chat_id, owner_id=user_id, chat_title=chat_title, chat_username=chat_username)
    
    group_linked_text = customization.format_message(
        'group_linked',
        title=chat_title,
        chat_id=chat_id
    )
    
    await callback.message.edit_text(
        group_linked_text,
        parse_mode="HTML"
    )
    await callback.answer("✅ Группа привязана!")
    
    try:
        await bot.send_message(
            user_id,
            f"✅ Группа <b>{chat_title}</b> успешно привязана!\n\n"
            "Теперь она доступна в разделе «Настройки групп» в главном меню.",
            parse_mode="HTML"
        )
    except:
        pass

@dp.callback_query(F.data == "cancel_link")
@edit_only()
@check_owner()
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.message(Command("unban"))
@group_only()
async def cmd_unban(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_admin(chat_id, user_id) and not await check_moderator_permission(chat_id, user_id, 'can_ban'):
        await message.answer("❌ У вас нет права разбанивать пользователей!")
        return
    args = message.text.split()
    target_id = None
    target_name = "пользователь"
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
    elif len(args) >= 2:
        try:
            target_id = int(args[1])
        except:
            username = args[1].replace('@', '')
            try:
                member = await bot.get_chat_member(chat_id, f"@{username}")
                target_id = member.user.id
                target_name = member.user.full_name
            except:
                await message.answer("❌ Пользователь не найден в этом чате!")
                return
    else:
        await message.answer(
            "❌ Укажите пользователя!\n\n"
            "Пример: /unban 123456789\n"
            "Или ответьте на сообщение пользователя"
        )
        return
    
    try:
        await bot.unban_chat_member(chat_id, target_id)
        unban_spammer_in_chat(target_id, chat_id)
        await message.answer(f"✅ Пользователь {target_name} разбанен в этом чате!")
        await send_to_log_group(chat_id, 'mod_action', f"<b>✅ Разбан</b>\n\n👮 Админ: {message.from_user.full_name}\n👤 Пользователь: {target_name}")
    except Exception as e:
        await message.answer(f"❌ Ошибка при разбане: {e}")

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
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя мутить бота!")
        return
    
    args = message.text.split(maxsplit=2)
    duration_str = args[1] if len(args) > 1 else "0"
    reason = args[2] if len(args) > 2 else "Не указана"
    duration = parse_time(duration_str)
    
    confirm_settings = db.get_confirmation_settings(chat_id)
    if confirm_settings.get('mute', False):
        await state.update_data(
            action='mute',
            target_id=target_user.id,
            target_name=target_user.full_name,
            duration=duration,
            duration_str=duration_str,
            reason=reason,
            message_id=message.message_id
        )
        
        confirm_text = customization.format_message(
            'confirm_action',
            action="замутить",
            name=target_user.full_name,
            duration_line=f"⏱ Длительность: {format_time(duration) if duration > 0 else 'навсегда'}\n",
            reason=reason
        )
        
        await message.answer(
            confirm_text,
            reply_markup=get_confirm_action_keyboard('mute', target_user.id, duration, reason),
            parse_mode="HTML"
        )
        await state.set_state(ModerationStates.waiting_for_confirm_action)
        return
    
    await execute_mute(message.chat.id, target_user.id, target_user.full_name, duration, reason, message.from_user, message.message_id)

async def execute_mute(chat_id: int, target_id: int, target_name: str, duration: int, reason: str, moderator: types.User, message_id: int):
    try:
        until = int(time.time() + duration) if duration > 0 else None
        await bot.restrict_chat_member(
            chat_id, 
            target_id, 
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until
        )
        duration_text = format_time(duration) if duration > 0 else "навсегда"
        
        mute_text = customization.format_message(
            'mute_message',
            name=target_name,
            moderator=moderator.full_name,
            duration=duration_text,
            reason=reason
        )
        
        await bot.send_message(
            chat_id,
            mute_text,
            reply_markup=get_lift_restriction_keyboard('mute', target_id, message_id),
            parse_mode="HTML"
        )
        db.log_moderator_action(
            chat_id, moderator.id, moderator.full_name,
            'mute', target_id, target_name, duration, reason
        )
        await send_to_log_group(chat_id, 'mod_action', f"<b>🔇 Мут</b>\n\n👮 Модератор: {moderator.full_name}\n👤 Пользователь: {target_name}\n⏱ Длительность: {duration_text}\n📝 Причина: {reason}")
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
    target_user = message.reply_to_message.from_user
    try:
        await bot.restrict_chat_member(
            chat_id, 
            target_user.id, 
            permissions=ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True
            )
        )
        
        unmute_text = customization.format_message(
            'unmute_message',
            name=target_user.full_name,
            moderator=message.from_user.full_name
        )
        
        await message.answer(unmute_text, parse_mode="HTML")
        await send_to_log_group(chat_id, 'mod_action', f"<b>🔊 Размут</b>\n\n👮 Модератор: {message.from_user.full_name}\n👤 Пользователь: {target_user.full_name}")
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
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя банить бота!")
        return
    
    args = message.text.split(maxsplit=2)
    duration_str = args[1] if len(args) > 1 else "0"
    reason = args[2] if len(args) > 2 else "Не указана"
    duration = parse_time(duration_str)
    
    confirm_settings = db.get_confirmation_settings(chat_id)
    if confirm_settings.get('ban', False):
        await state.update_data(
            action='ban',
            target_id=target_user.id,
            target_name=target_user.full_name,
            duration=duration,
            duration_str=duration_str,
            reason=reason,
            message_id=message.message_id
        )
        
        confirm_text = customization.format_message(
            'confirm_action',
            action="забанить",
            name=target_user.full_name,
            duration_line=f"⏱ Длительность: {format_time(duration) if duration > 0 else 'навсегда'}\n",
            reason=reason
        )
        
        await message.answer(
            confirm_text,
            reply_markup=get_confirm_action_keyboard('ban', target_user.id, duration, reason),
            parse_mode="HTML"
        )
        await state.set_state(ModerationStates.waiting_for_confirm_action)
        return
    
    await execute_ban(message.chat.id, target_user.id, target_user.full_name, duration, reason, message.from_user, message.message_id)

async def execute_ban(chat_id: int, target_id: int, target_name: str, duration: int, reason: str, moderator: types.User, message_id: int):
    try:
        until = int(time.time() + duration) if duration > 0 else None
        await bot.ban_chat_member(chat_id, target_id, until_date=until)
        duration_text = format_time(duration) if duration > 0 else "навсегда"
        
        ban_text = customization.format_message(
            'ban_message',
            name=target_name,
            moderator=moderator.full_name,
            duration=duration_text,
            reason=reason
        )
        
        await bot.send_message(
            chat_id,
            ban_text,
            reply_markup=get_lift_restriction_keyboard('ban', target_id, message_id),
            parse_mode="HTML"
        )
        db.log_moderator_action(
            chat_id, moderator.id, moderator.full_name,
            'ban', target_id, target_name, duration, reason
        )
        await send_to_log_group(chat_id, 'mod_action', f"<b>⛔ Бан</b>\n\n👮 Модератор: {moderator.full_name}\n👤 Пользователь: {target_name}\n⏱ Длительность: {duration_text}\n📝 Причина: {reason}")
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
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя кикать бота!")
        return
    
    args = message.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else "Не указана"
    
    confirm_settings = db.get_confirmation_settings(chat_id)
    if confirm_settings.get('kick', False):
        await state.update_data(
            action='kick',
            target_id=target_user.id,
            target_name=target_user.full_name,
            reason=reason
        )
        
        confirm_text = customization.format_message(
            'confirm_action',
            action="кикнуть",
            name=target_user.full_name,
            duration_line="",
            reason=reason
        )
        
        await message.answer(
            confirm_text,
            reply_markup=get_confirm_action_keyboard('kick', target_user.id, reason=reason),
            parse_mode="HTML"
        )
        await state.set_state(ModerationStates.waiting_for_confirm_action)
        return
    
    await execute_kick(message.chat.id, target_user.id, target_user.full_name, reason, message.from_user)

async def execute_kick(chat_id: int, target_id: int, target_name: str, reason: str, moderator: types.User):
    try:
        await bot.ban_chat_member(chat_id, target_id)
        await bot.unban_chat_member(chat_id, target_id)
        
        kick_text = customization.format_message(
            'kick_message',
            name=target_name,
            moderator=moderator.full_name,
            reason=reason
        )
        
        await bot.send_message(chat_id, kick_text, parse_mode="HTML")
        db.log_moderator_action(
            chat_id, moderator.id, moderator.full_name,
            'kick', target_id, target_name, 0, reason
        )
        await send_to_log_group(chat_id, 'mod_action', f"<b>👢 Кик</b>\n\n👮 Модератор: {moderator.full_name}\n👤 Пользователь: {target_name}\n📝 Причина: {reason}")
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
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя предупреждать бота!")
        return
    
    args = message.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else "Не указана"
    
    try:
        warn_count = db.add_user_warn(chat_id, target_user.id)
        max_warns = db.get_max_warns_settings(chat_id)['max_warns']
        
        warn_text = customization.format_message(
            'warn_message',
            name=target_user.full_name,
            moderator=message.from_user.full_name,
            warn_count=warn_count,
            max_warns=max_warns,
            reason=reason
        )
        
        await message.answer(warn_text, parse_mode="HTML")
        db.log_moderator_action(
            chat_id, message.from_user.id, message.from_user.full_name,
            'warn', target_user.id, target_user.full_name, 0, reason
        )
        await send_to_log_group(chat_id, 'mod_action', f"<b>⚠️ Предупреждение</b>\n\n👮 Модератор: {message.from_user.full_name}\n👤 Пользователь: {target_user.full_name}\n📊 Предупреждение №{warn_count}\n📝 Причина: {reason}")
        
        # Check if max warns reached
        if warn_count >= max_warns:
            warn_settings = db.get_max_warns_settings(chat_id)
            await execute_punishment(chat_id, target_user.id, target_user.full_name, warn_settings['punish'], warn_settings['duration'], f"Превышение лимита варнов ({warn_count}/{max_warns})", message.from_user)
            db.reset_user_warns(chat_id, target_user.id)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

async def execute_punishment(chat_id: int, target_id: int, target_name: str, punish_type: str, duration: int, reason: str, moderator: types.User):
    try:
        if punish_type == 'mute':
            until = int(time.time() + duration) if duration > 0 else None
            await bot.restrict_chat_member(chat_id, target_id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
            await bot.send_message(
                chat_id,
                customization.format_message('mute_message', name=target_name, moderator=moderator.full_name, duration=format_time(duration), reason=reason),
                parse_mode="HTML"
            )
        elif punish_type == 'ban':
            until = int(time.time() + duration) if duration > 0 else None
            await bot.ban_chat_member(chat_id, target_id, until_date=until)
            await bot.send_message(
                chat_id,
                customization.format_message('ban_message', name=target_name, moderator=moderator.full_name, duration=format_time(duration), reason=reason),
                parse_mode="HTML"
            )
        elif punish_type == 'kick':
            await bot.ban_chat_member(chat_id, target_id)
            await bot.unban_chat_member(chat_id, target_id)
            await bot.send_message(
                chat_id,
                customization.format_message('kick_message', name=target_name, moderator=moderator.full_name, reason=reason),
                parse_mode="HTML"
            )
        elif punish_type == 'warn':
            new_warn_count = db.add_user_warn(chat_id, target_id)
            max_warns = db.get_max_warns_settings(chat_id)['max_warns']
            await bot.send_message(
                chat_id,
                customization.format_message('warn_message', name=target_name, moderator=moderator.full_name, warn_count=new_warn_count, max_warns=max_warns, reason=reason),
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Ошибка наказания: {e}")

@dp.message(Command("mods"))
@group_only()
async def cmd_mods(message: Message):
    chat_id = message.chat.id
    moderators = db.get_all_moderators(chat_id)
    text = "🛡️ <b>Модераторы группы:</b>\n\n"
    try:
        creator = await bot.get_chat(chat_id)
        creator_member = await bot.get_chat_member(chat_id, creator.id)
        text += f"👑 <b>Владелец:</b> {creator_member.user.full_name}\n\n"
    except:
        pass
    if moderators:
        text += "👮 <b>Назначенные модераторы:</b>\n"
        for mod in moderators:
            try:
                user = await bot.get_chat_member(chat_id, mod[0])
                name = user.user.full_name
                rights = []
                if mod[1]: rights.append("🔇")
                if mod[2]: rights.append("👢")
                if mod[3]: rights.append("⛔")
                if mod[4]: rights.append("⚠️")
                rights_text = " ".join(rights) if rights else "нет прав"
                text += f"• {name} - {rights_text}\n"
            except:
                continue
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("give_mute"))
@group_only()
async def cmd_give_mute(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target_user.id, 'can_mute', True, user_id)
    await message.answer(f"✅ Пользователю {target_user.full_name} выдано право мутить")
    await send_to_log_group(chat_id, 'mod_action', f"<b>🔇 Выдача права на мут</b>\n\n👮 Админ: {message.from_user.full_name}\n👤 Пользователь: {target_user.full_name}")

@dp.message(Command("ungive_mute"))
@group_only()
async def cmd_ungive_mute(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target_user = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target_user.id, 'can_mute', False, user_id)
    await message.answer(f"✅ У пользователя {target_user.full_name} забрано право мутить")

@dp.message(Command("give_kick"))
@group_only()
async def cmd_give_kick(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target_user.id, 'can_kick', True, user_id)
    await message.answer(f"✅ Пользователю {target_user.full_name} выдано право кикать")

@dp.message(Command("ungive_kick"))
@group_only()
async def cmd_ungive_kick(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target_user = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target_user.id, 'can_kick', False, user_id)
    await message.answer(f"✅ У пользователя {target_user.full_name} забрано право кикать")

@dp.message(Command("give_ban"))
@group_only()
async def cmd_give_ban(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target_user.id, 'can_ban', True, user_id)
    await message.answer(f"✅ Пользователю {target_user.full_name} выдано право банить")

@dp.message(Command("ungive_ban"))
@group_only()
async def cmd_ungive_ban(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target_user = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target_user.id, 'can_ban', False, user_id)
    await message.answer(f"✅ У пользователя {target_user.full_name} забрано право банить")

@dp.message(Command("give_warn"))
@group_only()
async def cmd_give_warn(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может выдавать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, которому хотите дать права")
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    db.set_moderator_permission(chat_id, target_user.id, 'can_warn', True, user_id)
    await message.answer(f"✅ Пользователю {target_user.full_name} выдано право выдавать предупреждения")

@dp.message(Command("ungive_warn"))
@group_only()
async def cmd_ungive_warn(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target_user = message.reply_to_message.from_user
    db.set_moderator_permission(chat_id, target_user.id, 'can_warn', False, user_id)
    await message.answer(f"✅ У пользователя {target_user.full_name} забрано право выдавать предупреждения")

@dp.message(F.new_chat_members)
async def on_bot_added(message: Message):
    bot_info = await bot.get_me()
    if any(member.id == bot_info.id for member in message.new_chat_members):
        await message.answer(
            "👋 <b>Спасибо, что добавили Puls Chat Manager!</b>\n\n"
            "Пока я работаю в ограниченном режиме.\n"
            "Чтобы я мог полностью защищать чат (правила, антифлуд, модерация):\n"
            "1️⃣ Сделайте меня <b>администратором</b>\n"
            "2️⃣ Напишите команду <code>/group</code> в этой группе\n\n"
            "Все настройки будут доступны в ЛС с ботом.",
            parse_mode="HTML"
        )

@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id, user = update.chat.id, update.new_chat_member.user
        
        # Check raid protection
        raid_settings = db.get_raid_protection_settings(chat_id)
        if raid_settings['enabled']:
            now = time.time()
            raid_detector[chat_id] = [t for t in raid_detector[chat_id] if now - t < raid_settings['window']]
            raid_detector[chat_id].append(now)
            
            if len(raid_detector[chat_id]) >= raid_settings['limit']:
                raid_detector[chat_id].clear()
                await bot.send_message(
                    chat_id,
                    customization.format_message('raid_detected',
                        window=raid_settings['window'],
                        count=raid_settings['limit'],
                        duration=format_time(raid_settings['duration'])),
                    parse_mode="HTML"
                )
                # Ban all recent joiners
                for t in raid_detector[chat_id]:
                    await execute_punishment(chat_id, user.id, user.full_name, raid_settings['punishment'], raid_settings['duration'], "Рейд-атака", bot)
                return
        
        is_premium = getattr(user, 'is_premium', False)
        db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
        db.add_user_stat(chat_id, user.id, int(time.time()))
        await send_to_log_group(chat_id, 'join', f"<b>👋 Вход</b>\n\n👤 {user.full_name}\n🆔 <code>{user.id}</code>")
        
        # Check spammer
        is_spammer, spam_reason, warnings = check_spammer(user.id, chat_id)
        if is_spammer and db.get_puls_antispam_enabled(chat_id):
            try:
                await bot.ban_chat_member(chat_id, user.id)
                user_link = f"<a href='tg://user?id={user.id}'>{user.full_name}</a>"
                await bot.send_message(
                    chat_id,
                    customization.format_message('spammer_detected',
                        user_link=user_link, reason=spam_reason, warnings=warnings,
                        limit=SPAM_WARN_LIMIT, user_id=user.id),
                    parse_mode="HTML"
                )
                return
            except:
                pass
        
        # Send welcome
        await send_simple_welcome(chat_id, user)

@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    db.set_left_chat(update.chat.id, update.from_user.id)
    await send_to_log_group(update.chat.id, 'leave', f"<b>👋 Выход</b>\n\n👤 {update.from_user.full_name}\n🆔 <code>{update.from_user.id}</code>")

async def send_simple_welcome(chat_id, user):
    is_premium = getattr(user, 'is_premium', False)
    global_user = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
    global_user_data = db.get_global_user(user.id)
    stat = db.get_user_stat(chat_id, user.id)
    join_dt = format_datetime(stat['join_date']) if stat else format_datetime(time.time())
    position = db.get_user_position(chat_id, user.id, 'all')
    warnings = get_spammer_warnings(user.id)
    premium_emoji = get_premium_status_emoji(global_user_data['is_premium'])
    premium_line = customization.format_message('profile_premium') + "\n" if global_user_data['is_premium'] else ""
    
    welcome_text = customization.format_message(
        'welcome_simple',
        premium_emoji=premium_emoji,
        name=user.full_name,
        global_id=global_user_data['global_id'],
        first_seen=format_datetime(global_user_data['first_seen']),
        premium_line=premium_line,
        warnings=warnings,
        limit=SPAM_WARN_LIMIT,
        username=user.username or 'нет',
        user_id=user.id,
        join_dt=join_dt,
        position=position
    )
    
    welcome_text_custom, welcome_photo = db.get_welcome(chat_id)
    
    if welcome_photo:
        await bot.send_photo(
            chat_id,
            photo=welcome_photo,
            caption=welcome_text + (f"\n\n{welcome_text_custom}" if welcome_text_custom else ""),
            reply_markup=get_welcome_buttons(chat_id),
            parse_mode="HTML"
        )
    else:
        await bot.send_message(
            chat_id,
            welcome_text + (f"\n\n{welcome_text_custom}" if welcome_text_custom else ""),
            reply_markup=get_welcome_buttons(chat_id),
            parse_mode="HTML"
        )

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    
    chat_id = message.chat.id
    text = (message.text or message.caption or "").lower().strip()
    
    # Auto responses
    responses = db.get_auto_responses(chat_id)
    
    for trigger, response_text, response_type, media_id in responses:
        trigger_lower = trigger.lower().strip()
        if trigger_lower == text or trigger_lower in text:
            try:
                if response_type == 'text':
                    await message.reply(safe_html(response_text or "", False), parse_mode="HTML", disable_notification=True)
                elif response_type == 'photo' and media_id:
                    await message.reply_photo(media_id, caption=safe_html(response_text or "", False) if response_text else None, parse_mode="HTML", disable_notification=True)
                elif response_type == 'video' and media_id:
                    await message.reply_video(media_id, caption=safe_html(response_text or "", False) if response_text else None, parse_mode="HTML", disable_notification=True)
                elif response_type == 'animation' and media_id:
                    await message.reply_animation(media_id, caption=safe_html(response_text or "", False) if response_text else None, parse_mode="HTML", disable_notification=True)
                elif response_type == 'sticker' and media_id:
                    await message.reply_sticker(media_id, disable_notification=True)
                elif response_type == 'voice' and media_id:
                    await message.reply_voice(media_id, caption=safe_html(response_text or "", False) if response_text else None)
                elif response_type == 'video_note' and media_id:
                    await message.reply_video_note(video_note=media_id)
                elif response_type == 'document' and media_id:
                    await message.reply_document(media_id, caption=safe_html(response_text or "", False) if response_text else None, parse_mode="HTML")
                elif response_type == 'audio' and media_id:
                    await message.reply_audio(media_id, caption=safe_html(response_text or "", False) if response_text else None, parse_mode="HTML")
                return
            except Exception as e:
                logger.error(f"Ошибка автоответа ({response_type}): {e}")

@dp.callback_query(F.data == "back_to_main")
@edit_only()
@check_owner()
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    await callback.message.edit_text(
        "👋 <b>Главное меню</b>\n\nВыберите раздел:",
        reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin)
    )
    await callback.answer()

@dp.callback_query(F.data == "group_manage_main")
@edit_only()
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

@dp.callback_query(F.data.startswith("select_group_"))
@edit_only()
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
@edit_only()
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

# Configuration handlers
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
    await callback.message.edit_text(
        f"🛡️ <b>Антиспам Пульса</b>\n\n"
        f"Статус: {'✅ Включен' if enabled else '❌ Выключен'}\n\n"
        f"Когда функция включена, бот автоматически проверяет всех новых участников "
        f"по глобальной базе спамеров Пульса. Если обнаружен спамер, он сразу банится.\n\n"
        f"Также бот отслеживает явный спам (50+ сообщений в минуту) и добавляет нарушителей в базу.",
        reply_markup=get_puls_antispam_keyboard(enabled)
    )
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
    await callback.answer(f"✅ Антиспам Пульса {'включен' if not current else 'выключен'}!", show_alert=True)
    await puls_antispam_manage(callback, state)

@dp.callback_query(F.data == "puls_antispam_info")
@edit_only()
@check_owner()
async def puls_antispam_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "ℹ️ <b>Что такое Антиспам Пульса?</b>\n\n"
        "Это глобальная система защиты от спамеров:\n\n"
        "1️⃣ Если пользователь отправляет 50+ сообщений за 1 минуту, он получает предупреждение.\n"
        "2️⃣ При 3 предупреждениях он навсегда добавляется в базу спамеров Пульса.\n"
        "3️⃣ При входе в любую группу с ботом он автоматически банится.\n"
        "4️⃣ Админы могут разбанить спамера в своей группе командой /unban.\n"
        "5️⃣ Только разработчики бота могут удалить из базы командой /remove_spammer.\n\n"
        f"Ссылка на поддержку: {SUPPORT_LINK}",
        reply_markup=get_back_keyboard("puls_antispam_manage")
    )
    await callback.answer()

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
    await callback.message.edit_text(
        f"🔗 <b>Анти-ссылки</b>\n\n"
        f"Статус: {'✅ Включен' if settings['enabled'] else '❌ Выключен'}\n"
        f"Наказание: {settings['punishment']}\n"
        f"Длительность: {format_time(settings['duration'])}\n\n"
        f"Блокирует любые ссылки (http, https, t.me, telegram.me)",
        reply_markup=get_links_manage_keyboard(settings)
    )
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
    settings = db.get_links_filter_settings(chat_id)
    db.set_links_filter_settings(chat_id, not settings['enabled'], settings['punishment'], settings['duration'])
    await callback.answer(f"✅ Анти-ссылки {'включен' if not settings['enabled'] else 'выключен'}!", show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "set_links_punishment")
@edit_only()
@check_owner()
async def set_links_punishment(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Выберите наказание для ссылок:",
        reply_markup=get_punishment_keyboard("links")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_links_duration")
@edit_only()
@check_owner()
async def set_links_duration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏱ Введите длительность в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("links_manage")
    )
    await state.set_state(LinksStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "links_filter_info")
@edit_only()
@check_owner()
async def links_filter_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "ℹ️ <b>Что такое анти-ссылки?</b>\n\n"
        "Бот автоматически обнаруживает и блокирует сообщения, содержащие ссылки:\n"
        "• http://, https://\n"
        "• t.me, telegram.me\n\n"
        "При обнаружении ссылки применяется наказание:\n"
        "• Warn - предупреждение\n"
        "• Mute - ограничение на отправку сообщений\n"
        "• Kick - удаление из чата\n"
        "• Ban - блокировка в чате",
        reply_markup=get_back_keyboard("links_manage")
    )
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
    settings = db.get_profanity_filter_settings(chat_id)
    await callback.message.edit_text(
        f"🚫 <b>Антимат</b>\n\n"
        f"Статус: {'✅ Включен' if settings['enabled'] else '❌ Выключен'}\n"
        f"Наказание: {settings['punishment']}\n"
        f"Длительность: {format_time(settings['duration'])}\n\n"
        f"Блокирует нецензурную лексику",
        reply_markup=get_profanity_manage_keyboard(settings)
    )
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
    settings = db.get_profanity_filter_settings(chat_id)
    db.set_profanity_filter_settings(chat_id, not settings['enabled'], settings['punishment'], settings['duration'])
    await callback.answer(f"✅ Антимат {'включен' if not settings['enabled'] else 'выключен'}!", show_alert=True)
    await profanity_manage(callback, state)

@dp.callback_query(F.data == "set_profanity_punishment")
@edit_only()
@check_owner()
async def set_profanity_punishment(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Выберите наказание за мат:",
        reply_markup=get_punishment_keyboard("profanity")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_profanity_duration")
@edit_only()
@check_owner()
async def set_profanity_duration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏱ Введите длительность в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("profanity_manage")
    )
    await state.set_state(ProfanityStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "profanity_filter_info")
@edit_only()
@check_owner()
async def profanity_filter_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "ℹ️ <b>Что такое антимат?</b>\n\n"
        "Бот автоматически обнаруживает и блокирует сообщения, содержащие нецензурную лексику.\n\n"
        "Список матных слов включает основные ругательства и их вариации.\n\n"
        "При обнаружении мата применяется наказание:\n"
        "• Warn - предупреждение\n"
        "• Mute - ограничение на отправку сообщений\n"
        "• Kick - удаление из чата\n"
        "• Ban - блокировка в чате",
        reply_markup=get_back_keyboard("profanity_manage")
    )
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
    settings = db.get_raid_protection_settings(chat_id)
    await callback.message.edit_text(
        f"🛡️ <b>Рейд-защита</b>\n\n"
        f"Статус: {'✅ Включена' if settings['enabled'] else '❌ Выключена'}\n"
        f"Лимит вступлений: {settings['limit']}\n"
        f"Период: {settings['window']} сек\n"
        f"Наказание: {settings['punishment']}\n"
        f"Длительность: {format_time(settings['duration'])}\n\n"
        f"Автоматически блокирует массовые вступления в чат",
        reply_markup=get_raid_manage_keyboard(settings)
    )
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
    settings = db.get_raid_protection_settings(chat_id)
    db.set_raid_protection_settings(chat_id, not settings['enabled'], settings['limit'], settings['window'], settings['punishment'], settings['duration'])
    await callback.answer(f"✅ Рейд-защита {'включена' if not settings['enabled'] else 'выключена'}!", show_alert=True)
    await raid_manage(callback, state)

@dp.callback_query(F.data == "set_raid_limit")
@edit_only()
@check_owner()
async def set_raid_limit(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📊 Введите лимит вступлений (3-50):",
        reply_markup=get_back_keyboard("raid_manage")
    )
    await state.set_state(RaidStates.waiting_for_limit)
    await callback.answer()

@dp.callback_query(F.data == "set_raid_window")
@edit_only()
@check_owner()
async def set_raid_window(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏱ Введите период в секундах (10-300):",
        reply_markup=get_back_keyboard("raid_manage")
    )
    await state.set_state(RaidStates.waiting_for_window)
    await callback.answer()

@dp.callback_query(F.data == "set_raid_punishment")
@edit_only()
@check_owner()
async def set_raid_punishment(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Выберите наказание для рейдеров:",
        reply_markup=get_punishment_keyboard("raid")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_raid_duration")
@edit_only()
@check_owner()
async def set_raid_duration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏱ Введите длительность в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("raid_manage")
    )
    await state.set_state(RaidStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "raid_protection_info")
@edit_only()
@check_owner()
async def raid_protection_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "ℹ️ <b>Что такое рейд-защита?</b>\n\n"
        "Функция автоматически обнаруживает массовые вступления пользователей в чат.\n\n"
        "Если за указанный период вступает больше пользователей, чем лимит:\n"
        "• Все новые участники получают наказание\n"
        "• В чат отправляется предупреждение\n\n"
        "Это защищает от рейд-атак и спам-ботов.",
        reply_markup=get_back_keyboard("raid_manage")
    )
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
    settings = db.get_auto_comment_settings(chat_id)
    await callback.message.edit_text(
        f"💬 <b>Авто-комментарий к постам</b>\n\n"
        f"Статус: {'✅ Включен' if settings['enabled'] else '❌ Выключен'}\n"
        f"Текст: {settings['text'][:100] if settings['text'] else 'не установлен'}\n"
        f"Медиа: {'✅ есть' if settings['media_id'] else '❌ нет'}\n\n"
        f"Бот будет автоматически писать этот комментарий под каждым новым постом канала в обсуждении",
        reply_markup=get_auto_comment_keyboard(settings)
    )
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
    settings = db.get_auto_comment_settings(chat_id)
    db.set_auto_comment_settings(chat_id, not settings['enabled'], settings['text'], settings['media_id'], settings['media_type'])
    await callback.answer(f"✅ Авто-комментарий {'включен' if not settings['enabled'] else 'выключен'}!", show_alert=True)
    await auto_comment_manage(callback, state)

@dp.callback_query(F.data == "set_auto_comment_text")
@edit_only()
@check_owner()
async def set_auto_comment_text(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📝 Отправьте текст для авто-комментария:\n\n"
        "Можно использовать:\n"
        "• {post_link} - ссылка на пост\n"
        "• {post_text} - текст поста\n"
        "• {channel_title} - название канала",
        reply_markup=get_back_keyboard("auto_comment_manage")
    )
    await state.set_state(AutoCommentStates.waiting_for_text)
    await callback.answer()

@dp.callback_query(F.data == "set_auto_comment_media")
@edit_only()
@check_owner()
async def set_auto_comment_media(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🖼 Отправьте медиа для авто-комментария:\n\n"
        "Поддерживается: фото, видео, GIF, стикер",
        reply_markup=get_back_keyboard("auto_comment_manage")
    )
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
    settings = db.get_auto_comment_settings(chat_id)
    if settings['media_id']:
        if settings['media_type'] == 'photo':
            await callback.message.answer_photo(settings['media_id'], caption=settings['text'] or "Авто-комментарий", parse_mode="HTML")
        elif settings['media_type'] == 'video':
            await callback.message.answer_video(settings['media_id'], caption=settings['text'] or "Авто-комментарий", parse_mode="HTML")
        elif settings['media_type'] == 'animation':
            await callback.message.answer_animation(settings['media_id'], caption=settings['text'] or "Авто-комментарий", parse_mode="HTML")
        elif settings['media_type'] == 'sticker':
            await callback.message.answer_sticker(settings['media_id'])
    else:
        await callback.message.answer(settings['text'] or "Авто-комментарий не настроен", parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "auto_comment_info")
@edit_only()
@check_owner()
async def auto_comment_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "ℹ️ <b>Что такое авто-комментарий?</b>\n\n"
        "Функция автоматически публикует комментарий под каждым новым постом канала.\n\n"
        "Для работы необходимо:\n"
        "1️⃣ Канал должен быть привязан к супергруппе как обсуждение\n"
        "2️⃣ Бот должен быть администратором в супергруппе\n\n"
        "Поддерживаются переменные:\n"
        "• {post_link} - ссылка на пост\n"
        "• {post_text} - текст поста\n"
        "• {channel_title} - название канала",
        reply_markup=get_back_keyboard("auto_comment_manage")
    )
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
    await callback.message.edit_text(
        "✅ <b>Подтверждение опасных действий</b>\n\n"
        "Вы можете включить подтверждение для каждого действия отдельно.\n"
        "Если включено, перед выполнением действия бот спросит подтверждение.",
        reply_markup=get_confirmation_actions_keyboard(settings)
    )
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
    await callback.answer(f"✅ Подтверждение бана {'включено' if new_value else 'выключено'}!", show_alert=True)
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
    await callback.answer(f"✅ Подтверждение кика {'включено' if new_value else 'выключено'}!", show_alert=True)
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
    await callback.answer(f"✅ Подтверждение мута {'включено' if new_value else 'выключено'}!", show_alert=True)
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "confirmation_actions_info")
@edit_only()
@check_owner()
async def confirmation_actions_info(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "ℹ️ <b>Что такое подтверждение действий?</b>\n\n"
        "Если функция включена для конкретного действия, то перед его выполнением "
        "бот попросит подтверждение. Это защищает от случайных нажатий.\n\n"
        "Например, если включено подтверждение бана, то после команды /ban "
        "бот сначала покажет информацию и спросит 'Подтверждаете?'. Только после "
        "подтверждения пользователь будет забанен.\n\n"
        "По умолчанию всё выключено.",
        reply_markup=get_back_keyboard("confirmation_actions_manage")
    )
    await callback.answer()

@dp.callback_query(F.data == "log_group_manage")
@edit_only()
@check_owner()
async def log_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    log_group_info = db.get_source_chat_log_group(chat_id)
    has_log_group = log_group_info is not None
    await callback.message.edit_text(
        "📋 <b>Группа логов</b>\n\n"
        "Сюда будут отправляться логи нарушений, действий модераторов и другие события.",
        reply_markup=get_log_group_manage_keyboard(has_log_group, log_group_info)
    )
    await callback.answer()

@dp.callback_query(F.data == "log_group_help")
@edit_only()
@check_owner()
async def log_group_help(callback: CallbackQuery):
    await callback.message.edit_text(
        "📋 <b>Как создать группу логов:</b>\n\n"
        "1️⃣ Создайте отдельную группу в Telegram\n"
        "2️⃣ Добавьте бота в эту группу\n"
        "3️⃣ Выдайте боту права администратора\n"
        "4️⃣ Перешлите любое сообщение из этой группы в ЛС боту\n\n"
        "ИЛИ\n\n"
        "Отправьте команду: /loggroup -100123456789\n"
        "(где -100123456789 - ID вашей группы)\n\n"
        "После создания группы логов, вы сможете привязать её к этому чату.",
        reply_markup=get_back_keyboard("log_group_manage")
    )
    await callback.answer()

@dp.callback_query(F.data == "link_log_group")
@edit_only()
@check_owner()
async def link_log_group(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    log_groups = db.get_user_log_groups(user_id)
    if not log_groups:
        await callback.message.edit_text(
            "❌ У вас ещё нет созданных групп логов!\n\n"
            "Сначала создайте группу логов, следуя инструкции.",
            reply_markup=get_back_keyboard("log_group_manage")
        )
        await callback.answer()
        return
    builder = InlineKeyboardBuilder()
    for log_id, title in log_groups:
        builder.add(create_button(title or f"Группа {log_id}", f"select_log_group_{log_id}", "primary"))
    builder.add(create_button("◀️ Назад", "log_group_manage", "secondary"))
    builder.adjust(1)
    await callback.message.edit_text(
        "📋 <b>Выберите группу логов</b>\n\n"
        "В эту группу будут отправляться события из текущего чата:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("select_log_group_"))
@edit_only()
@check_owner()
async def select_log_group(callback: CallbackQuery, state: FSMContext):
    log_group_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.set_source_chat_log_group(chat_id, log_group_id)
    await callback.answer("✅ Группа логов привязана!", show_alert=True)
    await log_group_manage(callback, state)

@dp.callback_query(F.data == "log_group_settings")
@edit_only()
@check_owner()
async def log_group_settings(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if not log_group_info:
        await callback.answer("❌ Группа логов не привязана!", show_alert=True)
        return
    settings = {
        'send_violations': log_group_info['send_violations'],
        'send_mod_actions': log_group_info['send_mod_actions'],
        'send_joins': log_group_info['send_joins'],
        'send_leaves': log_group_info['send_leaves'],
        'send_messages': log_group_info['send_messages']
    }
    await callback.message.edit_text(
        f"📋 <b>Настройки отправки в лог-группу</b>\n\n"
        f"Группа: {log_group_info['group_title']}\n"
        f"ID: <code>{log_group_info['log_group_id']}</code>\n\n"
        f"Выберите, какие события отправлять:",
        reply_markup=get_log_settings_keyboard(settings),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_log_violations")
@edit_only()
@check_owner()
async def toggle_log_violations(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_violations'] else 1
        db.update_log_group_settings(
            chat_id, log_group_info['log_group_id'],
            send_violations=new_value
        )
        await callback.answer("✅ Настройки обновлены!", show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_mod")
@edit_only()
@check_owner()
async def toggle_log_mod(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_mod_actions'] else 1
        db.update_log_group_settings(
            chat_id, log_group_info['log_group_id'],
            send_mod_actions=new_value
        )
        await callback.answer("✅ Настройки обновлены!", show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_joins")
@edit_only()
@check_owner()
async def toggle_log_joins(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_joins'] else 1
        db.update_log_group_settings(
            chat_id, log_group_info['log_group_id'],
            send_joins=new_value
        )
        await callback.answer("✅ Настройки обновлены!", show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_leaves")
@edit_only()
@check_owner()
async def toggle_log_leaves(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_leaves'] else 1
        db.update_log_group_settings(
            chat_id, log_group_info['log_group_id'],
            send_leaves=new_value
        )
        await callback.answer("✅ Настройки обновлены!", show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_messages")
@edit_only()
@check_owner()
async def toggle_log_messages(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_messages'] else 1
        db.update_log_group_settings(
            chat_id, log_group_info['log_group_id'],
            send_messages=new_value
        )
        await callback.answer("✅ Настройки обновлены!", show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "log_group_info")
@edit_only()
@check_owner()
async def log_group_info(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if not log_group_info:
        await callback.answer("❌ Группа логов не привязана!", show_alert=True)
        return
    stats = {
        'violations': '✅' if log_group_info['send_violations'] else '❌',
        'mod_actions': '✅' if log_group_info['send_mod_actions'] else '❌',
        'joins': '✅' if log_group_info['send_joins'] else '❌',
        'leaves': '✅' if log_group_info['send_leaves'] else '❌',
        'messages': '✅' if log_group_info['send_messages'] else '❌'
    }
    await callback.message.edit_text(
        f"📋 <b>Информация о группе логов</b>\n\n"
        f"Группа: {log_group_info['group_title']}\n"
        f"ID: <code>{log_group_info['log_group_id']}</code>\n\n"
        f"<b>Отправка событий:</b>\n"
        f"• Нарушения: {stats['violations']}\n"
        f"• Действия модераторов: {stats['mod_actions']}\n"
        f"• Входы: {stats['joins']}\n"
        f"• Выходы: {stats['leaves']}\n"
        f"• Сообщения: {stats['messages']}",
        reply_markup=get_back_keyboard("log_group_manage"),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "unlink_log_group")
@edit_only()
@check_owner()
async def unlink_log_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.remove_source_chat_log_group(chat_id)
    await callback.answer("✅ Группа логов отвязана!", show_alert=True)
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
    moderators = db.get_all_moderators(chat_id)
    await callback.message.edit_text(
        "🛡️ <b>Управление модераторами</b>\n\n"
        "Здесь вы можете назначать и забирать права модераторов.",
        reply_markup=get_moderators_manage_keyboard(moderators)
    )
    await callback.answer()

@dp.callback_query(F.data == "give_mod_rights")
@edit_only()
@check_owner()
async def give_mod_rights(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "👤 Ответьте на сообщение пользователя, которому хотите дать права модератора,\n"
        "или отправьте его ID / username."
    )
    await state.set_state(ModerationStates.waiting_for_give_mute_user)

@dp.message(ModerationStates.waiting_for_give_mute_user)
async def process_give_mod_user(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    target_id = None
    target_name = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
    else:
        text = message.text.strip()
        if text.isdigit():
            target_id = int(text)
        elif text.startswith('@'):
            try:
                member = await bot.get_chat_member(chat_id, text)
                target_id = member.user.id
                target_name = member.user.full_name
            except:
                await message.answer("❌ Пользователь не найден в этом чате!")
                return
    if not target_id:
        await message.answer("❌ Не удалось определить пользователя!")
        return
    await state.update_data(target_mod_id=target_id, target_mod_name=target_name)
    await message.answer(
        f"Выберите права для {target_name}:",
        reply_markup=get_mod_rights_keyboard(target_id)
    )

@dp.callback_query(F.data.startswith("give_mute_"))
@edit_only()
@check_owner()
async def give_mute_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_mute', True, callback.from_user.id)
    await callback.answer("✅ Право мутить выдано!", show_alert=True)
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data.startswith("give_kick_"))
@edit_only()
@check_owner()
async def give_kick_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_kick', True, callback.from_user.id)
    await callback.answer("✅ Право кикать выдано!", show_alert=True)
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data.startswith("give_ban_"))
@edit_only()
@check_owner()
async def give_ban_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_ban', True, callback.from_user.id)
    await callback.answer("✅ Право банить выдано!", show_alert=True)
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data.startswith("give_warn_"))
@edit_only()
@check_owner()
async def give_warn_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.set_moderator_permission(chat_id, target_id, 'can_warn', True, callback.from_user.id)
    await callback.answer("✅ Право варнить выдано!", show_alert=True)
    await callback.message.edit_text("✅ Право успешно выдано!")

@dp.callback_query(F.data == "list_moderators")
@edit_only()
@check_owner()
async def list_moderators(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    moderators = db.get_all_moderators(chat_id)
    if not moderators:
        await callback.message.edit_text(
            "📋 Нет назначенных модераторов",
            reply_markup=get_back_keyboard("moderators_manage")
        )
        await callback.answer()
        return
    text = "🛡️ <b>Список модераторов:</b>\n\n"
    for mod in moderators:
        try:
            user = await bot.get_chat_member(chat_id, mod[0])
            name = user.user.full_name
            rights = []
            if mod[1]: rights.append("🔇 мут")
            if mod[2]: rights.append("👢 кик")
            if mod[3]: rights.append("⛔ бан")
            if mod[4]: rights.append("⚠️ варн")
            rights_text = ", ".join(rights) if rights else "нет прав"
            text += f"• <b>{name}</b>\n  Права: {rights_text}\n\n"
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
@edit_only()
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
        "• <u>Подчеркнутый</u> - &lt;u&gt;текст&lt;/u&gt;\n"
        "• <s>Зачеркнутый</s> - &lt;s&gt;текст&lt;/s&gt;\n"
        "• <tg-spoiler>Спойлер</tg-spoiler> - &lt;tg-spoiler&gt;текст&lt;/tg-spoiler&gt;\n"
        "• <blockquote>Цитата</blockquote> - &lt;blockquote&gt;текст&lt;/blockquote&gt;\n"
        "• <blockquote expandable>Свернутая цитата</blockquote> - &lt;blockquote expandable&gt;текст&lt;/blockquote&gt;\n"
        "• <code>Код</code> - &lt;code&gt;текст&lt;/code&gt;\n"
        "• <pre>Блок кода</pre> - &lt;pre&gt;текст&lt;/pre&gt;",
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
    await callback.answer("✅ Готовые правила установлены!", show_alert=True)
    await manage_rules(callback, state)

@dp.callback_query(F.data == "show_rules")
@edit_only()
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
            f"📜 <b>Текущие правила:</b>\n\n{safe_html(rules_html, True)}",
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
@edit_only()
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
    builder.adjust(1)
    await callback.message.edit_text("❓ Вы уверены, что хотите удалить правила?", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "delete_rules")
@edit_only()
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
@edit_only()
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
@edit_only()
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
@edit_only()
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
@edit_only()
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

@dp.callback_query(F.data == "rules_auto")
@edit_only()
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
@edit_only()
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
@edit_only()
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
    settings = db.get_antiflood_settings(chat_id)
    await callback.message.edit_text(
        f"🚫 <b>Антифлуд</b>\n\n"
        f"Статус: {'✅ Включён' if settings['enabled'] else '❌ Выключен'}\n"
        f"• Текст: {settings['msg_limit']} сообщ.\n"
        f"• Медиа: {settings['media_limit']} сообщ.\n"
        f"• Период: {settings['time_window']} сек\n"
        f"• Предупреждений: {settings['warn_count']}\n"
        f"• Первое: {settings['first_punish']} ({format_interval(settings['first_duration'])})\n"
        f"• Повторное: {settings['repeat_punish']} ({format_interval(settings['repeat_duration'])})\n"
        f"• После варнов: {settings['punish_after_warn']} ({format_interval(settings['punish_after_warn_duration'])})\n\n"
        f"Умный антифлуд отличает текстовые и медиа-сообщения.",
        reply_markup=get_antiflood_manage_keyboard(settings)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_antiflood")
@edit_only()
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
@edit_only()
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
    await callback.message.edit_text(
        "⏱ Введите период в секундах (5-300):",
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
    await callback.message.edit_text(
        "🔇 Выберите наказание для первого нарушения:",
        reply_markup=get_punish_type_keyboard("first")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_repeat_punish")
@edit_only()
@check_owner()
async def set_repeat_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text(
        "🔊 Выберите наказание для повторных нарушений:",
        reply_markup=get_punish_type_keyboard("repeat")
    )
    await callback.answer()

@dp.callback_query(F.data == "set_punish_after_warn")
@edit_only()
@check_owner()
async def set_punish_after_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await callback.message.edit_text(
        "⚠️ Выберите наказание после достижения лимита предупреждений:",
        reply_markup=get_punish_type_keyboard("after")
    )
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
    await callback.message.edit_text(
        "⏱ Введите длительность мута в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
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
    elif punish_type == "repeat":
        db.save_antiflood_settings(chat_id, repeat_punish='kick')
    elif punish_type == "after":
        db.save_antiflood_settings(chat_id, punish_after_warn='kick')
    await callback.answer("✅ Наказание: кик", show_alert=True)
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
    await callback.message.edit_text(
        "⏱ Введите длительность бана в минутах (0 = навсегда):",
        reply_markup=get_back_keyboard("antiflood_manage")
    )
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
    if not chat_id or not punish_setting or not punish_action or not await is_creator(chat_id, message.from_user.id):
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

@dp.callback_query(F.data == "auto_response_manage")
@edit_only()
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
        for trigger, resp, resp_type, _ in responses:
            short_resp = resp[:30] + "..." if len(resp) > 30 else resp
            type_emoji = "📝" if resp_type == 'text' else "🖼" if resp_type == 'photo' else "🎬" if resp_type in ['video', 'animation'] else "🎯" if resp_type == 'sticker' else "🎤" if resp_type == 'voice' else "📄" if resp_type == 'document' else "🎵" if resp_type == 'audio' else "❓"
            text += f"• {type_emoji} <code>{trigger}</code> → {short_resp}\n"
    await callback.message.edit_text(text, reply_markup=get_auto_response_keyboard(responses), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "add_auto_trigger")
@edit_only()
@check_owner()
async def add_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    responses = db.get_auto_responses(data.get('selected_chat_id'))
    if len(responses) >= MAX_TRIGGERS:
        await callback.answer(f"❌ Достигнут лимит триггеров ({MAX_TRIGGERS})!", show_alert=True)
        return
    await callback.message.edit_text(
        f"📝 Введите ключевое слово (триггер).\nМакс. длина: {MAX_TRIGGER_LENGTH} символов\nМакс. слов: {MAX_TRIGGER_WORDS}\n\n"
        "Триггер будет проверяться на точное совпадение и вхождение в текст.",
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
    
    trigger = message.text.strip()
    
    is_valid, error_msg = validate_trigger(trigger)
    if not is_valid:
        await message.answer(error_msg)
        return
    
    await state.update_data(auto_trigger=trigger)
    await message.reply(
        f"📝 Введите ответ для триггера '{trigger}'.\n"
        f"Макс. длина: {MAX_RESPONSE_LENGTH} символов\n\n"
        "Вы можете отправить:\n"
        "• Текст с форматированием\n"
        "• Фото с подписью\n"
        "• Видео с подписью\n"
        "• GIF с подписью\n"
        "• Стикер\n"
        "• Голосовое сообщение\n"
        "• Видео-кружок\n"
        "• Документ с подписью\n"
        "• Аудио с подписью",
        reply_markup=get_back_keyboard("auto_response_manage")
    )
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
    
    if not chat_id or not trigger or not await is_creator(chat_id, message.from_user.id):
        await message.answer("❌ Ошибка! Начните заново.")
        await state.clear()
        return
    
    response_type = 'text'
    response = ""
    media_id = None
    
    if message.text:
        response_type = 'text'
        response = message.html_text.strip()
    elif message.photo:
        response_type = 'photo'
        media_id = message.photo[-1].file_id
        response = message.caption or ""
    elif message.video:
        response_type = 'video'
        media_id = message.video.file_id
        response = message.caption or ""
    elif message.animation:
        response_type = 'animation'
        media_id = message.animation.file_id
        response = message.caption or ""
    elif message.sticker:
        response_type = 'sticker'
        media_id = message.sticker.file_id
        response = ""
    elif message.voice:
        response_type = 'voice'
        media_id = message.voice.file_id
        response = ""
    elif message.video_note:
        response_type = 'video_note'
        media_id = message.video_note.file_id
        response = ""
    elif message.document:
        response_type = 'document'
        media_id = message.document.file_id
        response = message.caption or ""
    elif message.audio:
        response_type = 'audio'
        media_id = message.audio.file_id
        response = message.caption or ""
    
    if response_type == 'text' and not response:
        await message.answer("❌ Ответ не может быть пустым!")
        return
    
    if len(response) > MAX_RESPONSE_LENGTH:
        await message.answer(f"❌ Ответ слишком длинный! Максимум {MAX_RESPONSE_LENGTH} символов")
        return
    
    success, msg = db.add_auto_response(chat_id, trigger, response, response_type, media_id)
    await message.reply(msg)
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
@edit_only()
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
        warning = "\n\n⚠️ <b>Внимание:</b> Правила не установлены. Эта настройка не будет работать."
    
    current_text = callback.message.text or callback.message.caption
    new_text = f"✅ <b>Настройки подтверждения</b>\n\nТип: {type_names.get(conf_type, conf_type)}{warning}"
    
    if current_text != new_text:
        await callback.message.edit_text(
            new_text,
            reply_markup=get_confirmation_keyboard(conf_type, has_rules),
            parse_mode="HTML"
        )
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
        if conf_type == 'rules':
            error = "❌ Нельзя включить 'Только правила' - сначала установите правила в группе!"
        else:
            error = "❌ Нельзя включить 'Оба шага' - сначала установите правила в группе!"
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
    await callback.message.edit_text(
        "❓ Вы уверены, что хотите отвязать группу?\n\nВсе настройки будут сохранены, но вы больше не сможете управлять ей.",
        reply_markup=get_unlink_confirm_keyboard(chat_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("unlink_group_"))
@edit_only()
@check_owner()
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET owner_id = NULL WHERE chat_id = ?', (chat_id,))
        conn.commit()
    
    await callback.message.edit_text("✅ Группа отвязана!")
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
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Группа"
    await callback.message.edit_text(
        f"⚙️ <b>Настройка группы:</b> {chat_title}\n\nВыберите действие:",
        reply_markup=get_group_manage_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("show_group_rules_"))
@edit_only()
@check_public()
async def show_group_rules(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    rules = db.get_rules_html(chat_id)
    if rules and db.get_rules_enabled(chat_id):
        await callback.message.answer(safe_html(rules, True), parse_mode="HTML")
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
    is_premium = getattr(user, 'is_premium', False)
    global_user = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
    global_user_data = db.get_global_user(user.id)
    stat = db.get_user_stat(chat_id, user.id)
    position = db.get_user_position(chat_id, user.id, 'all')
    warnings = get_spammer_warnings(user.id)
    
    premium_emoji = get_premium_status_emoji(global_user_data['is_premium'])
    
    if not stat:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        no_stats = customization.get_template('profile_no_stats').get_text()
        
        text = f"{header}\n\n{id_line}\n{first_seen}\n{premium_line}\n{antispam}\n\n{no_stats}"
    else:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        stats_header = customization.get_template('profile_stats_header').get_text()
        day = customization.format_message('profile_day', count=stat['day_messages'])
        week = customization.format_message('profile_week', count=stat['week_messages'])
        month = customization.format_message('profile_month', count=stat['month_messages'])
        total = customization.format_message('profile_total', count=stat['all_messages'])
        position_line = customization.format_message('profile_position', position=position)
        
        text = (
            f"{header}\n\n"
            f"{id_line}\n"
            f"{first_seen}\n"
            f"{premium_line}\n"
            f"{antispam}\n\n"
            f"{stats_header}\n"
            f"{day}\n"
            f"{week}\n"
            f"{month}\n"
            f"{total}\n"
            f"{position_line}"
        )
    
    await callback.message.answer(safe_html(text, True), parse_mode="HTML")
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
    
    header = customization.get_template('top_header').get_text()
    text = f"{header}\n\n"
    
    for i, (uid, count) in enumerate(top, 1):
        try:
            member = await bot.get_chat_member(chat_id, uid)
            name = member.user.full_name
            is_premium = getattr(member.user, 'is_premium', False)
            premium_emoji = get_premium_status_emoji(is_premium)
            warnings = get_spammer_warnings(uid)
        except:
            name = f"ID {uid}"
            premium_emoji = ""
            warnings = 0
        
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        warning_text = f" ⚠️{warnings}" if warnings > 0 else ""
        
        entry = customization.format_message(
            'top_entry',
            medal=medal,
            premium_emoji=premium_emoji,
            name=name,
            count=count,
            warnings=warning_text
        )
        text += f"{entry}\n"
    
    await callback.message.answer(safe_html(text, True), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "about")
@edit_only()
@check_public()
async def about(callback: CallbackQuery):
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    text = (
        "🤖 <b>Puls Chat Manager</b> ⭐\n\n"
        "Версия: 8.0.0\n\n"
        "📌 <b>Возможности:</b>\n"
        "• Управление правилами\n"
        "• Авто-рассылка\n"
        "• Умный антифлуд (текст/медиа)\n"
        "• Антиспам Пульса (глобальная база спамеров)\n"
        "• Анти-ссылки (блокировка любых ссылок)\n"
        "• Антимат (фильтр нецензурной лексики)\n"
        "• Рейд-защита (блокировка массовых вступлений)\n"
        "• Авто-комментарий к постам канала\n"
        "• Автоответчик (до 100 триггеров)\n"
        "• Статистика сообщений\n"
        "• Приветствия\n"
        "• Система модерации (мут/бан/кик/варн)\n"
        "• Кнопка снятия ограничения\n"
        "• Группы логов\n"
        "• Подтверждение входа\n"
        "• Подтверждение опасных действий\n"
        "• Полная кастомизация всех сообщений и фото\n"
        "• Поддержка премиум эмодзи ⭐\n\n"
        "➕ Нажмите «Добавить в группу» чтобы пригласить меня"
    )
    await callback.message.edit_text(
        safe_html(text, False),
        reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin)
    )
    await callback.answer()

@dp.callback_query(F.data == "help")
@edit_only()
@check_public()
async def help(callback: CallbackQuery):
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    text = (
        "🆘 <b>Помощь</b> ⭐\n\n"
        "🔹 <b>Команды в группе:</b>\n"
        "• /rules - показать правила\n"
        "• /stats - моя статистика\n"
        "• /top - топ активных\n"
        "• /profile - профиль пользователя\n"
        "• /group - управление группой\n"
        "• /puls - проверка пинга\n"
        "• /mute [время] [причина] - замутить\n"
        "• /unmute - размутить\n"
        "• /ban [время] [причина] - забанить\n"
        "• /unban - разбанить\n"
        "• /kick [причина] - кикнуть\n"
        "• /warn [причина] - предупредить\n"
        "• /mods - список модераторов\n\n"
        "🔹 <b>Команды для владельца:</b>\n"
        "• /give_mute - дать право мутить\n"
        "• /ungive_mute - забрать право мутить\n"
        "• /give_kick - дать право кикать\n"
        "• /ungive_kick - забрать право кикать\n"
        "• /give_ban - дать право банить\n"
        "• /ungive_ban - забрать право банить\n"
        "• /give_warn - дать право варнить\n"
        "• /ungive_warn - забрать право варнить\n\n"
        "🔹 <b>В ЛС:</b>\n"
        "• /start - главное меню\n"
        "• /groupsettings - управление группами\n"
        "• /loggroup - управление группами логов\n\n"
        "🔹 <b>Антиспам Пульса:</b>\n"
        "• Бот автоматически отслеживает 50+ сообщений в минуту\n"
        "• 3 предупреждения = добавление в базу спамеров\n"
        "• В профиле отображается количество предупреждений\n"
        f"• Поддержка: {SUPPORT_LINK}"
    )
    await callback.message.edit_text(
        safe_html(text, False),
        reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin)
    )
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
    text = (
        f"👑 <b>Панель администратора</b> ⭐\n\n"
        f"Статус бота: {status}\n"
        f"Сообщение: {maintenance_message}\n"
        f"Спамеров в базе: {spammer_count}\n\n"
        "Выберите действие:"
    )
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📊 Статистика", "admin_stats", "primary"))
    builder.add(create_button("📱 Группы", "admin_groups", "primary"))
    builder.add(create_button("👥 Пользователи", "admin_users", "primary"))
    builder.add(create_button("📋 Логи", "admin_logs", "primary"))
    builder.add(create_button("🛠 Техработы", "admin_maintenance", "danger" if technical_maintenance else "secondary"))
    builder.add(create_button("🚫 Спамеры", "admin_spammers", "danger"))
    builder.add(create_button("📢 Рассылка", "admin_broadcast", "success"))
    builder.add(create_button("📦 Бэкап", "admin_backup", "secondary"))
    builder.add(create_button("🎨 Кастомизация", "admin_custom", "primary"))
    builder.add(create_button("❌ Выключить", "admin_shutdown", "danger"))
    builder.add(create_button("◀️ Назад", "back_to_main", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(
        safe_html(text, False), 
        reply_markup=builder.as_markup(), 
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_custom")
@edit_only()
@check_bot_admin()
async def admin_custom(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text(
        "🎨 <b>Кастомизация бота</b>\n\n"
        "Здесь вы можете изменить тексты и фото всех сообщений бота.\n\n"
        "Выберите раздел:",
        reply_markup=get_admin_custom_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_custom_texts")
@edit_only()
@check_bot_admin()
async def admin_custom_texts(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text(
        "📝 <b>Редактирование текстов</b>\n\n"
        "Выберите сообщение для редактирования:",
        reply_markup=get_texts_list_keyboard(0)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("texts_page_"))
@edit_only()
@check_bot_admin()
async def texts_page(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    page = int(callback.data.split('_')[-1])
    await callback.message.edit_text(
        "📝 <b>Редактирование текстов</b>\n\n"
        "Выберите сообщение для редактирования:",
        reply_markup=get_texts_list_keyboard(page)
    )
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
    
    text = (
        f"📝 <b>Редактирование сообщения:</b> <code>{msg_key}</code>\n\n"
        f"Текущий текст:\n{current_text}\n\n"
        f"{'🖼 У сообщения есть фото' if has_photo else ''}\n\n"
        f"Отправьте новый текст для этого сообщения.\n"
        f"Или отправьте /cancel для отмены."
    )
    
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
        await message.answer(f"✅ Сообщение <code>{msg_key}</code> обновлено!")
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
    await callback.message.edit_text(
        "🖼 <b>Редактирование фото</b>\n\n"
        "Выберите сообщение для изменения фото:",
        reply_markup=get_photos_list_keyboard(0)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("photos_page_"))
@edit_only()
@check_bot_admin()
async def photos_page(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    page = int(callback.data.split('_')[-1])
    await callback.message.edit_text(
        "🖼 <b>Редактирование фото</b>\n\n"
        "Выберите сообщение для изменения фото:",
        reply_markup=get_photos_list_keyboard(page)
    )
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
    
    text = (
        f"🖼 <b>Редактирование фото для:</b> <code>{msg_key}</code>\n\n"
        f"{'✅ Текущее фото есть' if current_photo else '❌ Текущего фото нет'}\n\n"
        f"Отправьте новое фото для этого сообщения.\n"
        f"Или отправьте /reset чтобы убрать фото.\n"
        f"Или отправьте /cancel для отмены."
    )
    
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
        await message.answer(f"✅ Фото для <code>{msg_key}</code> обновлено!")
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
        await message.answer(f"✅ Фото для <code>{msg_key}</code> сброшено к стандартному!")
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
    builder.adjust(1)
    
    await callback.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите сбросить все кастомные настройки?</b>\n\n"
        "Все тексты и фото вернутся к стандартным.",
        reply_markup=builder.as_markup()
    )
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
    
    await callback.message.edit_text(
        "✅ Все настройки сброшены к стандартным!",
        reply_markup=get_back_keyboard("admin_custom")
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_spammers")
@edit_only()
@check_bot_admin()
async def admin_spammers(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    if not global_spammers:
        await callback.message.edit_text(
            "✅ База спамеров пуста. Пока никто не спамил.",
            reply_markup=get_back_keyboard("admin_panel")
        )
        await callback.answer()
        return
    text = "🚫 <b>Глобальная база спамеров:</b>\n\n"
    for user_id, info in list(global_spammers.items())[:20]:
        reason = info.get("причина", "неизвестно")
        date = format_datetime(info.get("когда_добавлен", 0))
        unbanned_in = len(info.get("разбанен_в", set()))
        warnings = info.get("предупреждения", 1)
        text += f"• <b>ID:</b> <code>{user_id}</code>\n"
        text += f"  Причина: {reason}\n"
        text += f"  Предупреждений: {warnings}/{SPAM_WARN_LIMIT}\n"
        text += f"  Добавлен: {date}\n"
        text += f"  Разбанен в {unbanned_in} чатах\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔄 Обновить", "admin_spammers", "primary"))
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_maintenance")
@edit_only()
@check_bot_admin()
async def admin_maintenance(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
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
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    builder.adjust(1)
    await callback.message.edit_text(
        safe_html(text, False), 
        reply_markup=builder.as_markup(), 
        parse_mode="HTML"
    )
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
    await notify_all_groups("✅ Бот снова в работе! ⭐")
    await callback.answer("🟢 Техработы ВЫКЛЮЧЕНЫ!", show_alert=True)
    await admin_maintenance(callback)

@dp.callback_query(F.data == "maintenance_message")
@edit_only()
@check_bot_admin()
async def maintenance_message(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
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
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM group_rules')
        total_groups = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM global_users')
        total_users = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM violation_logs')
        total_violations = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM auto_responses')
        total_triggers = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM moderator_logs')
        total_mod_actions = c.fetchone()[0] or 0
    spammer_count = len(global_spammers)
    text = (
        f"📊 <b>Статистика бота</b> ⭐\n\n"
        f"📱 Групп: {total_groups}\n"
        f"👥 Пользователей: {total_users}\n"
        f"🚫 Нарушений: {total_violations}\n"
        f"🛡️ Действий модераторов: {total_mod_actions}\n"
        f"🚫 Спамеров в базе: {spammer_count}\n"
        f"🤖 Триггеров: {total_triggers}/{MAX_TRIGGERS}\n\n"
        f"🕐 Время сервера: {datetime.now(SERVER_TZ).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔄 Обновить", "admin_stats", "primary"))
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_groups")
@edit_only()
@check_bot_admin()
async def admin_groups(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id, chat_title, rules_enabled, welcome_enabled, puls_antispam_enabled FROM group_rules LIMIT 20')
        groups = c.fetchall()
    text = "📱 <b>Группы (первые 20):</b>\n\n"
    for chat_id, title, rules_enabled, welcome_enabled, puls_enabled in groups:
        status = []
        if rules_enabled:
            status.append("📜✅")
        if welcome_enabled:
            status.append("👋✅")
        if puls_enabled:
            status.append("🛡️✅")
        status_text = f" [{''.join(status)}]" if status else ""
        text += f"• {title or 'Без названия'}{status_text} | ID: <code>{chat_id}</code>\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
@edit_only()
@check_bot_admin()
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT full_name, global_id, first_seen, is_premium FROM global_users ORDER BY first_seen DESC LIMIT 20')
        users = c.fetchall()
    text = "👥 <b>Последние пользователи:</b>\n\n"
    for name, gid, ts, is_premium in users:
        date = format_datetime(ts)
        premium_emoji = "⭐" if is_premium else ""
        text += f"• {premium_emoji} {name}\n  ID: <code>{gid}</code> | {date}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_logs")
@edit_only()
@check_bot_admin()
async def admin_logs(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
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
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
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
    builder.adjust(1)
    await callback.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите очистить все логи?</b>\n\nЭто действие нельзя отменить!",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_logs_clear_confirm")
@edit_only()
@check_bot_admin()
async def admin_logs_clear_confirm(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM violation_logs')
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
    builder.add(create_button("◀️ Назад", "admin_panel", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📢 <b>Рассылка сообщений</b>\n\n"
        "Выберите, куда отправлять рассылку:",
        reply_markup=builder.as_markup()
    )
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
    
    await callback.message.edit_text(
        "📝 Отправьте текст или медиа для рассылки.\n\n"
        "Поддерживается: текст, фото, видео, GIF, стикер, документ, аудио.\n\n"
        "Или отправьте /cancel для отмены."
    )
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
    
    # Сохраняем медиа
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
    
    # Получаем список чатов
    chats = []
    if target in ['groups', 'all']:
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id FROM group_rules')
            chats.extend([row[0] for row in c.fetchall()])
    
    if target in ['pm', 'all']:
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT user_id FROM global_users')
            chats.extend([row[0] for row in c.fetchall()])
    
    if not chats:
        await message.answer("❌ Нет получателей для рассылки!")
        await state.clear()
        return
    
    sent, failed = 0, 0
    status_msg = await message.answer(f"📤 Начинаю рассылку...\nВсего получателей: {len(chats)}")
    
    for chat_id in chats:
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
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка отправки в {chat_id}: {e}")
        
        if (sent + failed) % 10 == 0:
            await status_msg.edit_text(f"📤 Прогресс: {sent + failed}/{len(chats)}\n✅ {sent}\n❌ {failed}")
        await asyncio.sleep(0.05)
    
    await status_msg.edit_text(f"✅ Рассылка завершена!\n✅ Успешно: {sent}\n❌ Ошибок: {failed}")
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
        await callback.message.answer_document(
            FSInputFile(backup_name),
            caption=f"✅ Бэкап создан: {backup_name} ⭐"
        )
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
    builder.adjust(1)
    await callback.message.edit_text(
        "⚠️ <b>Вы уверены, что хотите выключить бота?</b>\n\n"
        "Администраторы всё ещё будут иметь доступ.",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
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
    await callback.message.edit_text(
        "🛑 <b>Бот остановлен</b>\n\n"
        "Администраторы всё ещё имеют доступ."
    )
    await callback.answer()

@dp.callback_query(F.data == "show_rules_group")
@edit_only()
@check_public()
async def show_rules_group(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    rules = db.get_rules_html(chat_id)
    if rules and db.get_rules_enabled(chat_id):
        await callback.message.answer(safe_html(rules, True), parse_mode="HTML")
    else:
        await callback.message.answer("❌ В этом чате ещё не установлены правила.")
    await callback.answer()

@dp.callback_query(F.data == "my_stats_group")
@edit_only()
@check_public()
async def my_stats_group(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    user = callback.from_user
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    is_premium = getattr(user, 'is_premium', False)
    global_user = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
    global_user_data = db.get_global_user(user.id)
    stat = db.get_user_stat(chat_id, user.id)
    position = db.get_user_position(chat_id, user.id, 'all')
    warnings = get_spammer_warnings(user.id)
    
    premium_emoji = get_premium_status_emoji(global_user_data['is_premium'])
    
    if not stat:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        no_stats = customization.get_template('profile_no_stats').get_text()
        
        text = f"{header}\n\n{id_line}\n{first_seen}\n{premium_line}\n{antispam}\n\n{no_stats}"
    else:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=user.full_name)
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        stats_header = customization.get_template('profile_stats_header').get_text()
        day = customization.format_message('profile_day', count=stat['day_messages'])
        week = customization.format_message('profile_week', count=stat['week_messages'])
        month = customization.format_message('profile_month', count=stat['month_messages'])
        total = customization.format_message('profile_total', count=stat['all_messages'])
        position_line = customization.format_message('profile_position', position=position)
        
        text = (
            f"{header}\n\n"
            f"{id_line}\n"
            f"{first_seen}\n"
            f"{premium_line}\n"
            f"{antispam}\n\n"
            f"{stats_header}\n"
            f"{day}\n"
            f"{week}\n"
            f"{month}\n"
            f"{total}\n"
            f"{position_line}"
        )
    
    await callback.message.answer(safe_html(text, True), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "top_active_group")
@edit_only()
@check_public()
async def top_active_group(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    top = db.get_top_messages(chat_id, limit=10)
    if not top:
        await callback.message.answer("📊 В этом чате пока нет сообщений")
        await callback.answer()
        return
    
    header = customization.get_template('top_header').get_text()
    text = f"{header}\n\n"
    
    for i, (uid, count) in enumerate(top, 1):
        try:
            member = await bot.get_chat_member(chat_id, uid)
            name = member.user.full_name
            is_premium = getattr(member.user, 'is_premium', False)
            premium_emoji = get_premium_status_emoji(is_premium)
            warnings = get_spammer_warnings(uid)
        except:
            name = f"ID {uid}"
            premium_emoji = ""
            warnings = 0
        
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        warning_text = f" ⚠️{warnings}" if warnings > 0 else ""
        
        entry = customization.format_message(
            'top_entry',
            medal=medal,
            premium_emoji=premium_emoji,
            name=name,
            count=count,
            warnings=warning_text
        )
        text += f"{entry}\n"
    
    await callback.message.answer(safe_html(text, True), parse_mode="HTML")
    await callback.answer()

# Additional handlers for punishment callbacks
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
        db.set_links_filter_settings(chat_id, db.get_links_filter_settings(chat_id)['enabled'], action, 0)
        await callback.answer(f"✅ Наказание: {action}", show_alert=True)
        await links_manage(callback, state)
    elif action == 'mute':
        await state.update_data(links_action='mute')
        await callback.message.edit_text(
            "⏱ Введите длительность мута в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("links_manage")
        )
        await state.set_state(LinksStates.waiting_for_duration)
    elif action == 'ban':
        await state.update_data(links_action='ban')
        await callback.message.edit_text(
            "⏱ Введите длительность бана в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("links_manage")
        )
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
        db.set_profanity_filter_settings(chat_id, db.get_profanity_filter_settings(chat_id)['enabled'], action, 0)
        await callback.answer(f"✅ Наказание: {action}", show_alert=True)
        await profanity_manage(callback, state)
    elif action == 'mute':
        await state.update_data(profanity_action='mute')
        await callback.message.edit_text(
            "⏱ Введите длительность мута в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("profanity_manage")
        )
        await state.set_state(ProfanityStates.waiting_for_duration)
    elif action == 'ban':
        await state.update_data(profanity_action='ban')
        await callback.message.edit_text(
            "⏱ Введите длительность бана в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("profanity_manage")
        )
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
        settings = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, settings['enabled'], settings['limit'], settings['window'], action, settings['duration'])
        await callback.answer(f"✅ Наказание: {action}", show_alert=True)
        await raid_manage(callback, state)
    elif action == 'mute':
        await state.update_data(raid_action='mute')
        await callback.message.edit_text(
            "⏱ Введите длительность мута в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("raid_manage")
        )
        await state.set_state(RaidStates.waiting_for_duration)
    elif action == 'ban':
        await state.update_data(raid_action='ban')
        await callback.message.edit_text(
            "⏱ Введите длительность бана в минутах (0 = навсегда):",
            reply_markup=get_back_keyboard("raid_manage")
        )
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
        db.set_links_filter_settings(chat_id, db.get_links_filter_settings(chat_id)['enabled'], action, duration)
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
        db.set_profanity_filter_settings(chat_id, db.get_profanity_filter_settings(chat_id)['enabled'], action, duration)
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
        settings = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, settings['enabled'], limit, settings['window'], settings['punishment'], settings['duration'])
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
        settings = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, settings['enabled'], settings['limit'], window, settings['punishment'], settings['duration'])
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
        settings = db.get_raid_protection_settings(chat_id)
        db.set_raid_protection_settings(chat_id, settings['enabled'], settings['limit'], settings['window'], action, duration)
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
    settings = db.get_auto_comment_settings(chat_id)
    db.set_auto_comment_settings(chat_id, settings['enabled'], text, settings['media_id'], settings['media_type'])
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
    settings = db.get_auto_comment_settings(chat_id)
    db.set_auto_comment_settings(chat_id, settings['enabled'], caption, media_id, media_type)
    await message.reply("✅ Медиа авто-комментария сохранено!")
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.message(AutoCommentStates.waiting_for_media)
async def process_auto_comment_media_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Отправьте фото, видео, GIF или стикер!")

async def main():
    # Add middlewares
    dp.message.middleware(CommandFloodMiddleware())
    dp.message.middleware(AntiFloodMiddleware())
    dp.message.middleware(MaintenanceMiddleware())
    dp.callback_query.middleware(MaintenanceMiddleware())
    
    # Start background tasks
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    asyncio.create_task(clean_old_messages())
    asyncio.create_task(clean_old_logs())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
