import asyncio
import logging
import time
import random
import string
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict, Any
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
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    Message, CallbackQuery, ChatMemberUpdated, ChatPermissions, 
    InlineKeyboardButton, FSInputFile, InlineKeyboardMarkup,
    ReactionTypeEmoji
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

MAX_BUTTON_PRESSES = 2
BUTTON_CHECK_TIME = 5

user_messages = defaultdict(list)
user_button_presses = defaultdict(list)
user_command_usage = defaultdict(lambda: deque(maxlen=5))
global_spammers = {}
spam_lock = threading.Lock()
stats_lock = threading.Lock()
stats_updating = False
technical_maintenance = False
maintenance_message = "🛠 Бот временно остановлен на технические работы."

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

class I18n:
    def __init__(self):
        self.translations = {
            "ru": {
                "main_menu_title": "👋 <b>Главное меню</b>\n\nВыберите раздел:",
                "about_button": "ℹ️ О боте",
                "help_button": "🆘 Помощь",
                "add_to_group_button": "➕ Добавить в группу",
                "group_settings_button": "⚙️ Настройки групп",
                "rules_button": "📜 Правила",
                "stats_button": "📊 Статистика",
                "top_button": "🏆 Топ",
                "admin_panel_button": "👑 Админ панель",
                "welcome_pm": "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\nЯ помогу вам управлять чатами, следить за порядком и автоматизировать модерацию.\n\nВыберите раздел в меню ниже 👇",
                "welcome_group": "👋 <b>Puls Chat Manager</b>\n\n• /rules - Правила\n• /stats - Моя статистика\n• /top - Топ активных\n• /profile - Профиль пользователя\n• /group - Управление группой\n• /puls - Проверка пинга\n• /mute [время] [причина] - замутить\n• /unmute - размутить\n• /ban [время] [причина] - забанить\n• /unban - разбанить\n• /kick [причина] - кикнуть\n• /warn [причина] - предупредить\n• /mods - список модераторов",
                "bot_added_welcome": "👋 Спасибо, что добавили меня!\n\nЯ Puls Chat Manager - бот для управления чатами.\n\n📌 <b>Для полноценной работы:</b>\n1️⃣ Сделайте меня администратором\n2️⃣ Напишите /group чтобы привязать группу\n3️⃣ Настройте правила, приветствия и антифлуд\n\nВсе настройки доступны в личных сообщениях: @PulsOfficialManager_bot",
                "group_settings_title": "⚙️ <b>Настройка группы:</b> {title}\n\nВыберите действие:",
                "rules_manage": "📝 Правила",
                "welcome_manage": "👋 Приветствие",
                "auto_broadcast": "🔄 Авто-рассылка",
                "antiflood_manage": "🚫 Антифлуд",
                "puls_antispam": "🛡️ Антиспам Пульса",
                "confirm_actions": "✅ Подтверждение действий",
                "log_group": "📋 Группа логов",
                "auto_response": "🤖 Автоответчик",
                "links_manage": "🔗 Ссылки",
                "confirm_entry": "✅ Подтверждение входа",
                "moderators_manage": "🛡️ Модераторы",
                "language_settings": "🌐 Язык бота",
                "unlink_group": "❌ Отвязать",
                "back_button": "◀️ Назад",
                "language_settings_title": "🌐 <b>Настройки языка бота</b>\n\nТекущий язык: {language}\n\nВыберите язык для бота в этой группе:",
                "language_changed": "✅ Язык бота изменен на {language}!",
                "language_in_development": "🌐 Язык {language} находится в разработке и будет доступен в ближайшее время!",
                "language_ru": "🇷🇺 Русский",
                "language_en": "🇬🇧 English",
                "language_uk": "🇺🇦 Українська",
                "language_de": "🇩🇪 Deutsch",
                "language_fr": "🇫🇷 Français",
                "language_es": "🇪🇸 Español",
                "language_it": "🇮🇹 Italiano",
                "language_pt": "🇵🇹 Português",
                "language_tr": "🇹🇷 Türkçe",
                "language_zh": "🇨🇳 中文",
                "language_ja": "🇯🇵 日本語",
                "language_ko": "🇰🇷 한국어",
                "language_ar": "🇸🇦 العربية",
                "language_hi": "🇮🇳 हिन्दी",
                "blacklisted": "🚫 <b>Вы в черном списке бота</b>\n\nК сожалению, вы не можете использовать команды бота.\n\nПричина: {reason}\n\nЕсли считаете это ошибкой, обратитесь к разработчикам: {support_link}",
                "add_to_blacklist": "✅ Пользователь {name} ({user_id}) добавлен в черный список!\nПричина: {reason}",
                "remove_from_blacklist": "✅ Пользователь {user_id} удален из черного списка!",
                "already_blacklisted": "❌ Пользователь уже в черном списке!",
                "not_blacklisted": "❌ Пользователь не найден в черном списке!",
                "blacklist_usage": "❌ Использование: /blacklist <user_id> [причина]\nИли ответьте на сообщение пользователя",
                "global_ban_usage": "❌ Использование: /gban <user_id> [время] [причина]\nПример: /gban 123456789 24ч спам\n\nВремя: 10м, 1ч, 2д, 0 - навсегда",
                "global_ban_success": "✅ <b>Глобальный бан</b>\n\nПользователь: {name} ({user_id})\nПричина: {reason}\nДлительность: {duration}\n\nПользователь забанен во всех группах, где есть бот!",
                "global_ban_error": "❌ Ошибка при глобальном бане: {error}",
                "global_unban_success": "✅ Пользователь {user_id} разбанен глобально!",
                "global_unban_usage": "❌ Использование: /gunban <user_id>",
                "global_mute_usage": "❌ Использование: /gmute <user_id> [время] [причина]\nПример: /gmute 123456789 1ч флуд",
                "global_mute_success": "✅ <b>Глобальный мут</b>\n\nПользователь: {name} ({user_id})\nПричина: {reason}\nДлительность: {duration}\n\nПользователь замьючен во всех группах, где есть бот!",
                "global_unmute_success": "✅ Пользователь {user_id} размучен глобально!",
                "delete_usage": "❌ Использование: -смс <количество>\nИли ответьте на сообщение и напишите -смс <количество>\n\nПример: -смс 10 (удалить 10 последних сообщений)\nПример с ответом: (ответ на сообщение) -смс 5 (удалить 5 сообщений этого пользователя)",
                "delete_invalid_number": "❌ Введите корректное число от 1 до 100!",
                "delete_range_error": "❌ Количество должно быть от 1 до 100!",
                "delete_confirm": "⚠️ <b>Подтвердите удаление {count} сообщений</b>\n\nЭто действие нельзя отменить!",
                "delete_progress": "🗑 Удаляю сообщения... ({current}/{total})",
                "delete_success": "✅ Удалено {count} сообщений!",
                "delete_failed": "❌ Не удалось удалить {count} сообщений",
                "delete_no_messages": "❌ Нет сообщений для удаления",
                "delete_user_success": "✅ Удалено {count} сообщений от пользователя {name}!",
                "delete_give_right": "✅ Пользователю {name} выдано право удалять сообщения!",
                "delete_remove_right": "✅ У пользователя {name} забрано право удалять сообщения!",
                "delete_mod_list": "🗑 <b>Пользователи с правом удаления:</b>\n\n{users}",
                "button_click_log": "🔘 <b>Нажатие на кнопку</b>\n\n👤 Пользователь: {user}\n🆔 ID: {user_id}\n🔘 Кнопка: {button_data}\n📱 Чат: {chat_title}\n🕐 Время: {time}",
                "button_click_group_notify": "👤 {user} использовал функцию",
                "admin_panel_title": "👑 <b>Панель администратора</b>\n\nСтатус бота: {status}\nОсновной язык: {main_lang}\nПользователей в ЧС: {blacklist_count}\nГлобальных банов: {global_bans}\nГлобальных мутов: {global_mutes}",
                "change_main_lang": "🌐 Сменить основной язык",
                "blacklist_manage": "🚫 Черный список",
                "global_bans_manage": "⛔ Глобальные баны",
                "global_mutes_manage": "🔇 Глобальные муты",
                "global_moderators": "👑 Глобальные модераторы",
                "main_lang_changed": "✅ Основной язык изменен на {language}!",
                "main_lang_select": "🌐 <b>Выберите основной язык бота</b>\n\nТекущий: {current}",
                "cmd_mute": "🔇 Пользователь {name} замьючен",
                "cmd_unmute": "🔊 Пользователь {name} размучен",
                "cmd_ban": "⛔ Пользователь {name} забанен",
                "cmd_unban": "✅ Пользователь {name} разбанен",
                "cmd_kick": "👢 Пользователь {name} кикнут",
                "cmd_warn": "⚠️ Предупреждение пользователю {name}",
                "no_groups": "❌ У вас нет привязанных групп.\n\nДобавьте бота в группу и привяжите её командой /group в той группе.",
                "select_group": "📱 <b>Ваши группы</b>\n\nВыберите группу:",
                "rules_not_set": "❌ В этом чате ещё не установлены правила",
                "stats_updating": "📊 Статистика обновляется...",
                "no_messages": "📊 В этом чате пока нет сообщений",
                "profile_not_found": "❌ Пользователь не найден",
                "invalid_command": "❌ Неверная команда",
                "support_link": "https://t.me/support_puls"
            },
            "en": {
                "main_menu_title": "👋 <b>Main Menu</b>\n\nSelect a section:",
                "about_button": "ℹ️ About",
                "help_button": "🆘 Help",
                "add_to_group_button": "➕ Add to Group",
                "group_settings_button": "⚙️ Group Settings",
                "rules_button": "📜 Rules",
                "stats_button": "📊 Statistics",
                "top_button": "🏆 Top",
                "admin_panel_button": "👑 Admin Panel",
                "welcome_pm": "👋 <b>Welcome to Puls Chat Manager!</b>\n\nI'll help you manage chats, monitor order and automate moderation.\n\nSelect a section in the menu below 👇",
                "welcome_group": "👋 <b>Puls Chat Manager</b>\n\n• /rules - Rules\n• /stats - My stats\n• /top - Top active\n• /profile - User profile\n• /group - Group management\n• /puls - Ping check\n• /mute [time] [reason] - mute\n• /unmute - unmute\n• /ban [time] [reason] - ban\n• /unban - unban\n• /kick [reason] - kick\n• /warn [reason] - warn\n• /mods - moderators list",
                "bot_added_welcome": "👋 Thanks for adding me!\n\nI'm Puls Chat Manager - a bot for managing chats.\n\n📌 <b>For full functionality:</b>\n1️⃣ Make me an administrator\n2️⃣ Type /group to link the group\n3️⃣ Configure rules, greetings and anti-flood\n\nAll settings are available in private messages: @PulsOfficialManager_bot",
                "group_settings_title": "⚙️ <b>Group settings:</b> {title}\n\nSelect action:",
                "rules_manage": "📝 Rules",
                "welcome_manage": "👋 Welcome",
                "auto_broadcast": "🔄 Auto broadcast",
                "antiflood_manage": "🚫 Anti-flood",
                "puls_antispam": "🛡️ Puls Antispam",
                "confirm_actions": "✅ Confirm actions",
                "log_group": "📋 Log group",
                "auto_response": "🤖 Auto response",
                "links_manage": "🔗 Links",
                "confirm_entry": "✅ Entry confirmation",
                "moderators_manage": "🛡️ Moderators",
                "language_settings": "🌐 Bot language",
                "unlink_group": "❌ Unlink",
                "back_button": "◀️ Back",
                "language_settings_title": "🌐 <b>Bot language settings</b>\n\nCurrent language: {language}\n\nSelect bot language for this group:",
                "language_changed": "✅ Bot language changed to {language}!",
                "language_in_development": "🌐 {language} language is under development and will be available soon!",
                "language_ru": "🇷🇺 Russian",
                "language_en": "🇬🇧 English",
                "language_uk": "🇺🇦 Ukrainian",
                "language_de": "🇩🇪 German",
                "language_fr": "🇫🇷 French",
                "language_es": "🇪🇸 Spanish",
                "language_it": "🇮🇹 Italian",
                "language_pt": "🇵🇹 Portuguese",
                "language_tr": "🇹🇷 Turkish",
                "language_zh": "🇨🇳 Chinese",
                "language_ja": "🇯🇵 Japanese",
                "language_ko": "🇰🇷 Korean",
                "language_ar": "🇸🇦 Arabic",
                "language_hi": "🇮🇳 Hindi",
                "blacklisted": "🚫 <b>You are in the bot's blacklist</b>\n\nUnfortunately, you cannot use bot commands.\n\nReason: {reason}\n\nIf you think this is a mistake, contact the developers: {support_link}",
                "add_to_blacklist": "✅ User {name} ({user_id}) added to blacklist!\nReason: {reason}",
                "remove_from_blacklist": "✅ User {user_id} removed from blacklist!",
                "already_blacklisted": "❌ User is already in blacklist!",
                "not_blacklisted": "❌ User not found in blacklist!",
                "blacklist_usage": "❌ Usage: /blacklist <user_id> [reason]\nOr reply to user's message",
                "global_ban_usage": "❌ Usage: /gban <user_id> [time] [reason]\nExample: /gban 123456789 24h spam\n\nTime: 10m, 1h, 2d, 0 - forever",
                "global_ban_success": "✅ <b>Global ban</b>\n\nUser: {name} ({user_id})\nReason: {reason}\nDuration: {duration}\n\nUser is banned in all groups where the bot is present!",
                "global_ban_error": "❌ Global ban error: {error}",
                "global_unban_success": "✅ User {user_id} unbanned globally!",
                "global_unban_usage": "❌ Usage: /gunban <user_id>",
                "global_mute_usage": "❌ Usage: /gmute <user_id> [time] [reason]\nExample: /gmute 123456789 1h spam",
                "global_mute_success": "✅ <b>Global mute</b>\n\nUser: {name} ({user_id})\nReason: {reason}\nDuration: {duration}\n\nUser is muted in all groups where the bot is present!",
                "global_unmute_success": "✅ User {user_id} unmuted globally!",
                "delete_usage": "❌ Usage: -del <amount>\nOr reply to a message and write -del <amount>\n\nExample: -del 10 (delete last 10 messages)\nExample with reply: (reply to message) -del 5 (delete 5 messages from that user)",
                "delete_invalid_number": "❌ Enter a valid number from 1 to 100!",
                "delete_range_error": "❌ Amount must be between 1 and 100!",
                "delete_confirm": "⚠️ <b>Confirm deletion of {count} messages</b>\n\nThis action cannot be undone!",
                "delete_progress": "🗑 Deleting messages... ({current}/{total})",
                "delete_success": "✅ Deleted {count} messages!",
                "delete_failed": "❌ Failed to delete {count} messages",
                "delete_no_messages": "❌ No messages to delete",
                "delete_user_success": "✅ Deleted {count} messages from user {name}!",
                "delete_give_right": "✅ User {name} granted message deletion rights!",
                "delete_remove_right": "✅ User {name} revoked message deletion rights!",
                "delete_mod_list": "🗑 <b>Users with deletion rights:</b>\n\n{users}",
                "button_click_log": "🔘 <b>Button click</b>\n\n👤 User: {user}\n🆔 ID: {user_id}\n🔘 Button: {button_data}\n📱 Chat: {chat_title}\n🕐 Time: {time}",
                "button_click_group_notify": "👤 {user} used a function",
                "admin_panel_title": "👑 <b>Admin Panel</b>\n\nBot status: {status}\nMain language: {main_lang}\nBlacklisted users: {blacklist_count}\nGlobal bans: {global_bans}\nGlobal mutes: {global_mutes}",
                "change_main_lang": "🌐 Change main language",
                "blacklist_manage": "🚫 Blacklist",
                "global_bans_manage": "⛔ Global bans",
                "global_mutes_manage": "🔇 Global mutes",
                "global_moderators": "👑 Global moderators",
                "main_lang_changed": "✅ Main language changed to {language}!",
                "main_lang_select": "🌐 <b>Select main bot language</b>\n\nCurrent: {current}",
                "cmd_mute": "🔇 User {name} muted",
                "cmd_unmute": "🔊 User {name} unmuted",
                "cmd_ban": "⛔ User {name} banned",
                "cmd_unban": "✅ User {name} unbanned",
                "cmd_kick": "👢 User {name} kicked",
                "cmd_warn": "⚠️ Warning to user {name}",
                "no_groups": "❌ You have no linked groups.\n\nAdd the bot to a group and link it with /group command in that group.",
                "select_group": "📱 <b>Your groups</b>\n\nSelect a group:",
                "rules_not_set": "❌ Rules are not set in this chat yet",
                "stats_updating": "📊 Statistics is updating...",
                "no_messages": "📊 No messages in this chat yet",
                "profile_not_found": "❌ User not found",
                "invalid_command": "❌ Invalid command",
                "support_link": "https://t.me/support_puls"
            }
        }
        self.main_language = "ru"
    
    def get(self, key: str, lang: str = None, **kwargs) -> str:
        if not lang:
            lang = self.main_language
        if lang not in self.translations:
            lang = "ru"
        text = self.translations[lang].get(key, self.translations["ru"].get(key, key))
        try:
            return text.format(**kwargs)
        except:
            return text
    
    def set_main_language(self, lang: str):
        if lang in self.translations:
            self.main_language = lang
            return True
        return False
    
    def get_available_languages(self) -> List[str]:
        return list(self.translations.keys())
    
    def get_language_name(self, lang: str, display_lang: str = "ru") -> str:
        key = f"language_{lang}"
        return self.get(key, display_lang)

i18n = I18n()

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
            "📊 Всего предупреждений: {warn_count}\n"
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
            "👮 Модератор: {moderator}\n"
            "👤 Пользователь снял ограничение, наложенное в сообщении выше"
        )
        
        self.templates['lift_notification'] = MessageTemplate(
            'lift_notification',
            "✅ Нарушения пользователя сняты модератором {moderator}"
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
        
        self.templates['spammer_pm'] = MessageTemplate(
            'spammer_pm',
            "🚫 <b>Вы были забанены в группе {chat_title}</b>\n\n"
            "Причина: вы находитесь в антиспам базе Пульса.\n"
            "Предупреждений: {warnings}/{limit}\n\n"
            "Для выхода из антиспам базы обратитесь к разработчикам:\n"
            "{support_link}"
        )
        
        self.templates['spam_warning_1'] = MessageTemplate(
            'spam_warning_1',
            "⚠️ <b>Внимание! Обнаружена подозрительная активность</b>\n\n"
            "Вы отправили {count} сообщений за 1 минуту.\n"
            "Это похоже на спам-атаку.\n\n"
            "Предупреждение: {current}/{limit}\n"
            "\nПри 3 предупреждениях вы будете навсегда добавлены в антиспам базу "
            "и не сможете пользоваться ботом.\n\n"
            "Пожалуйста, снизьте активность."
        )
        
        self.templates['spam_warning_2'] = MessageTemplate(
            'spam_warning_2',
            "⚠️ <b>Внимание! Обнаружена подозрительная активность</b>\n\n"
            "Вы отправили {count} сообщений за 1 минуту.\n"
            "Это похоже на спам-атаку.\n\n"
            "Предупреждение: {current}/{limit}\n"
            "\nПри 3 предупреждениях вы будете навсегда добавлены в антиспам базу "
            "и не сможете пользоваться ботом.\n\n"
            "Пожалуйста, снизьте активность."
        )
        
        self.templates['spam_warning_3'] = MessageTemplate(
            'spam_warning_3',
            "⚠️ <b>Внимание! Обнаружена подозрительная активность</b>\n\n"
            "Вы отправили {count} сообщений за 1 минуту.\n"
            "Это похоже на спам-атаку.\n\n"
            "Предупреждение: {current}/{limit}\n"
            "\n❌ <b>Достигнут лимит предупреждений!</b>\n"
            "Вы добавлены в глобальную антиспам базу Пульса.\n"
            "Теперь вы будете автоматически забанены во всех группах, где есть бот.\n\n"
            "Для выхода из базы обратитесь к разработчикам:\n"
            "{support_link}"
        )
        
        self.templates['spammer_added'] = MessageTemplate(
            'spammer_added',
            "🚫 Пользователь {name} добавлен в антиспам базу Пульса!\n"
            "Причина: явный спам (50+ сообщений за минуту)"
        )
        
        self.templates['group_linked'] = MessageTemplate(
            'group_linked',
            "✅ <b>Группа успешно привязана!</b>\n\n"
            "Название: {title}\n"
            "ID: <code>{chat_id}</code>\n\n"
            "Теперь вы можете настроить её в личных сообщениях с ботом.\n"
            "Нажмите /start в ЛС, чтобы открыть главное меню."
        )
        
        self.templates['group_linked_pm'] = MessageTemplate(
            'group_linked_pm',
            "✅ Группа <b>{title}</b> успешно привязана!\n\n"
            "Теперь она доступна в разделе «Настройки групп» в главном меню."
        )
        
        self.templates['group_unlinked'] = MessageTemplate(
            'group_unlinked',
            "✅ Группа отвязана от вашего аккаунта."
        )
        
        self.templates['confirm_action'] = MessageTemplate(
            'confirm_action',
            "⚠️ <b>Подтвердите действие</b>\n\n"
            "Вы хотите {action} {name}\n"
            "{duration_line}"
            "Причина: {reason}\n\n"
            "Подтвердите действие:"
        )
        
        self.templates['action_cancelled'] = MessageTemplate(
            'action_cancelled',
            "❌ Действие отменено"
        )
        
        self.templates['action_completed'] = MessageTemplate(
            'action_completed',
            "✅ Действие выполнено!"
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
        
        self.templates['trigger_empty'] = MessageTemplate(
            'trigger_empty',
            "❌ Триггер не может быть пустым"
        )
        
        self.templates['trigger_too_long'] = MessageTemplate(
            'trigger_too_long',
            "❌ Триггер слишком длинный! Максимум {max_len} символов"
        )
        
        self.templates['trigger_too_many_words'] = MessageTemplate(
            'trigger_too_many_words',
            "❌ Триггер должен содержать максимум {max_words} слово"
        )
        
        self.templates['trigger_removed'] = MessageTemplate(
            'trigger_removed',
            "✅ Триггер '{trigger}' удалён!"
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

class AdminCustomization:
    def __init__(self):
        self.messages = {}
        self.photos = {}
    
    def get_all_templates(self) -> List[Tuple[str, str, bool]]:
        templates = []
        for key, template in customization.templates.items():
            templates.append((
                key,
                template.get_text()[:50] + "..." if len(template.get_text()) > 50 else template.get_text(),
                template.get_photo() is not None
            ))
        return templates
    
    def update_template(self, key: str, text: str = None, photo: str = None):
        template = customization.get_template(key)
        if template:
            template.set_custom(text, photo)
            return True
        return False
    
    def reset_template(self, key: str):
        template = customization.get_template(key)
        if template:
            template.reset()
            return True
        return False

admin_custom = AdminCustomization()

class AntifloodCache:
    def __init__(self, ttl=60):
        self.cache = {}
        self.ttl = ttl
    
    def get(self, chat_id):
        key = f"af_{chat_id}"
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return data
        settings = db.get_antiflood_settings(chat_id)
        self.cache[key] = (settings, time.time())
        return settings
    
    def invalidate(self, chat_id):
        key = f"af_{chat_id}"
        if key in self.cache:
            del self.cache[key]

antiflood_cache = AntifloodCache()

def safe_html(text: str, preserve_quotes: bool = True) -> str:
    if not text:
        return ""
    
    if preserve_quotes:
        allowed_tags = ['blockquote', 'b', 'i', 'u', 's', 'code', 'pre', 'tg-spoiler', 'a', 'strong', 'em', 'ins', 'strike', 'del']
        placeholders = {}
        
        for i, tag in enumerate(allowed_tags):
            pattern_open = f'<{tag}(\\s+expandable)?>'
            pattern_close = f'</{tag}>'
            placeholder_open = f'!!TAG_{i}_OPEN!!'
            placeholder_close = f'!!TAG_{i}_CLOSE!!'
            
            def make_replace_open(tag_name, attrs):
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
            
            text = re.sub(pattern_open, make_replace_open(tag, tag), text, flags=re.IGNORECASE)
            text = re.sub(pattern_close, make_replace_close(tag), text, flags=re.IGNORECASE)
        
        text = html.escape(text)
        
        for placeholder, tag_html in placeholders.items():
            text = text.replace(placeholder, tag_html)
        
        return text
    else:
        return html.escape(text)

async def check_command_flood(user_id: int) -> Tuple[bool, int]:
    now = time.time()
    key = f"cmd_{user_id}"
    if key not in user_command_usage:
        user_command_usage[key] = deque(maxlen=5)
    
    while user_command_usage[key] and now - user_command_usage[key][0] > 2:
        user_command_usage[key].popleft()
    
    if len(user_command_usage[key]) >= 3:
        wait_time = 2 - (now - user_command_usage[key][0])
        return False, int(wait_time) + 1
    
    user_command_usage[key].append(now)
    return True, 0

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
                    logger.info(f"⚠️ Чужой пользователь {callback.from_user.full_name} ({user_id}) пытался нажать чужую кнопку")
                    if callback.message.chat.type in ['group', 'supergroup']:
                        try:
                            await bot.send_message(
                                callback.message.chat.id,
                                f"⚠️ {callback.from_user.full_name} пытался нажать чужую кнопку!"
                            )
                        except:
                            pass
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
                logger.warning(f"⚠️ Неавторизованный доступ: {message.from_user.full_name} ({message.from_user.id})")
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
            button_data = callback.data
            now = time.time()
            key = f"{user_id}_{button_data}"
            
            user_button_presses[key] = [t for t in user_button_presses[key] if now - t < BUTTON_CHECK_TIME]
            
            if len(user_button_presses[key]) >= MAX_BUTTON_PRESSES:
                oldest = user_button_presses[key][0] if user_button_presses[key] else now
                wait_time = int(BUTTON_CHECK_TIME - (now - oldest))
                logger.warning(f"🚫 Слишком частые нажатия: {callback.from_user.full_name}")
                await callback.answer(f"⚠️ Подожди {wait_time} сек.", show_alert=True)
                return
            
            user_button_presses[key].append(now)
            
            if callback.message.chat.type in ['group', 'supergroup']:
                user_link = f"<a href='tg://user?id={user_id}'>{html.escape(callback.from_user.full_name)}</a>"
                try:
                    await bot.send_message(
                        callback.message.chat.id,
                        f"👤 {user_link} использовал функцию",
                        parse_mode="HTML",
                        disable_notification=True
                    )
                except:
                    pass
            
            return await func(callback, *args, **kwargs)
        return wrapper
    return decorator

def parse_time(time_str: str) -> int:
    if not time_str:
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
        return 'gif'
    elif message.sticker:
        return 'sticker'
    elif message.voice:
        return 'voice'
    elif message.video_note:
        return 'video_note'
    elif message.document:
        return 'document'
    return 'other'

def is_media_message(message: Message) -> bool:
    return get_message_type(message) != 'text'

def extract_mentions(text: str) -> int:
    if not text:
        return 0
    username_mentions = len(re.findall(r'@\w+', text))
    hashtag_mentions = len(re.findall(r'#\w+', text))
    links = len(re.findall(r'https?://\S+', text))
    return username_mentions + hashtag_mentions + links

def extract_emojis(text: str) -> List[str]:
    if not text:
        return []
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"
        u"\U0001F300-\U0001F5FF"
        u"\U0001F680-\U0001F6FF"
        u"\U0001F1E0-\U0001F1FF"
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
        u"\ufe0f"
        u"\u3030"
        "]+", flags=re.UNICODE)
    return re.findall(emoji_pattern, text)

def clean_text_with_emojis(text: str) -> str:
    if not text:
        return ""
    emojis = extract_emojis(text)
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"
        u"\U0001F300-\U0001F5FF"
        u"\U0001F680-\U0001F6FF"
        u"\U0001F1E0-\U0001F1FF"
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
        u"\ufe0f"
        u"\u3030"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.lower().strip()
    if emojis:
        text = text + " " + " ".join(emojis)
    return text

def validate_trigger(trigger: str) -> Tuple[bool, str]:
    if not trigger:
        return False, customization.format_message('trigger_empty')
    
    if len(trigger) > MAX_TRIGGER_LENGTH:
        return False, customization.format_message('trigger_too_long', max_len=MAX_TRIGGER_LENGTH)
    
    words = trigger.split()
    if len(words) > MAX_TRIGGER_WORDS:
        return False, customization.format_message('trigger_too_many_words', max_words=MAX_TRIGGER_WORDS)
    
    return True, ""

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
            with db.get_connection() as conn:
                conn.execute('DELETE FROM global_spammers WHERE user_id = ?', (user_id,))
                conn.commit()
            return True
    return False

def get_spammer_warnings(user_id: int) -> int:
    with spam_lock:
        if user_id in global_spammers:
            return global_spammers[user_id].get("предупреждения", 0)
    return 0

def add_spammer_to_db(user_id: int, reason: str, warnings: int = 1):
    with db.get_connection() as conn:
        existing = conn.execute('SELECT unbanned_in FROM global_spammers WHERE user_id = ?', (user_id,)).fetchone()
        if existing:
            unbanned = existing[0]
        else:
            unbanned = '[]'
        conn.execute('''INSERT OR REPLACE INTO global_spammers (user_id, reason, added_at, unbanned_in, warnings)
                        VALUES (?, ?, ?, ?, ?)''',
                    (user_id, reason, int(time.time()), unbanned, warnings))
        conn.commit()

def unban_spammer_in_chat(user_id: int, chat_id: int) -> bool:
    with spam_lock:
        if user_id in global_spammers:
            if "разбанен_в" not in global_spammers[user_id]:
                global_spammers[user_id]["разбанен_в"] = set()
            global_spammers[user_id]["разбанен_в"].add(chat_id)
            unbanned_json = json.dumps(list(global_spammers[user_id]["разбанен_в"]))
            with db.get_connection() as conn:
                conn.execute('UPDATE global_spammers SET unbanned_in = ? WHERE user_id = ?', (unbanned_json, user_id))
                conn.commit()
            logger.info(f"✅ Пользователь {user_id} разбанен в чате {chat_id}")
            return True
    return False

async def check_blacklist(user_id: int) -> Tuple[bool, Optional[str]]:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT reason FROM bot_blacklist WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            return True, result[0]
    return False, None

def add_to_blacklist(user_id: int, reason: str, added_by: int) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT 1 FROM bot_blacklist WHERE user_id = ?', (user_id,))
        if c.fetchone():
            return False
        c.execute('INSERT INTO bot_blacklist (user_id, reason, added_by, added_at) VALUES (?, ?, ?, ?)',
                  (user_id, reason, added_by, int(time.time())))
        conn.commit()
        return True

def remove_from_blacklist(user_id: int) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM bot_blacklist WHERE user_id = ?', (user_id,))
        conn.commit()
        return c.rowcount > 0

def is_global_moderator(user_id: int, permission: str = None) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        if permission:
            c.execute(f'SELECT {permission} FROM global_moderators WHERE user_id = ?', (user_id,))
        else:
            c.execute('SELECT 1 FROM global_moderators WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if permission and result:
            return bool(result[0])
        return result is not None

def add_global_ban(user_id: int, reason: str, moderator_id: int, duration: int = 0) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        expires_at = int(time.time() + duration) if duration > 0 else 0
        c.execute('INSERT OR REPLACE INTO global_bans (user_id, reason, moderator_id, banned_at, expires_at) VALUES (?, ?, ?, ?, ?)',
                  (user_id, reason, moderator_id, int(time.time()), expires_at))
        conn.commit()
        return True

def remove_global_ban(user_id: int) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM global_bans WHERE user_id = ?', (user_id,))
        conn.commit()
        return c.rowcount > 0

def is_global_banned(user_id: int) -> Tuple[bool, Optional[str], Optional[int]]:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT reason, expires_at FROM global_bans WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            expires_at = result[1]
            if expires_at > 0 and time.time() > expires_at:
                remove_global_ban(user_id)
                return False, None, None
            return True, result[0], expires_at
    return False, None, None

def add_global_mute(user_id: int, reason: str, moderator_id: int, duration: int = 0) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        expires_at = int(time.time() + duration) if duration > 0 else 0
        c.execute('INSERT OR REPLACE INTO global_mutes (user_id, reason, moderator_id, muted_at, expires_at) VALUES (?, ?, ?, ?, ?)',
                  (user_id, reason, moderator_id, int(time.time()), expires_at))
        conn.commit()
        return True

def remove_global_mute(user_id: int) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM global_mutes WHERE user_id = ?', (user_id,))
        conn.commit()
        return c.rowcount > 0

def is_global_muted(user_id: int) -> Tuple[bool, Optional[str], Optional[int]]:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT reason, expires_at FROM global_mutes WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            expires_at = result[1]
            if expires_at > 0 and time.time() > expires_at:
                remove_global_mute(user_id)
                return False, None, None
            return True, result[0], expires_at
    return False, None, None

async def apply_global_ban(user_id: int, reason: str, duration: int = 0):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM group_rules')
        for chat_id, in c.fetchall():
            try:
                await bot.ban_chat_member(chat_id, user_id)
                await asyncio.sleep(0.1)
            except:
                pass

async def apply_global_mute(user_id: int, reason: str, duration: int = 0):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM group_rules')
        for chat_id, in c.fetchall():
            try:
                until = int(time.time() + duration) if duration > 0 else None
                await bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
                await asyncio.sleep(0.1)
            except:
                pass

def has_delete_permission(chat_id: int, user_id: int) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT can_delete FROM delete_permissions WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        result = c.fetchone()
        if result:
            return bool(result[0])
    return False

def set_delete_permission(chat_id: int, user_id: int, can_delete: bool, given_by: int) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO delete_permissions (chat_id, user_id, can_delete, given_by, given_at) VALUES (?, ?, ?, ?, ?)',
                  (chat_id, user_id, 1 if can_delete else 0, given_by, int(time.time())))
        conn.commit()
        return True

def get_group_language(chat_id: int) -> str:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT language FROM group_languages WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        if result:
            return result[0]
    return "ru"

def set_group_language(chat_id: int, language: str) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO group_languages (chat_id, language) VALUES (?, ?)', (chat_id, language))
        conn.commit()
        return True

def get_user_language(user_id: int) -> str:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT language FROM user_languages WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            return result[0]
    return "ru"

def set_user_language(user_id: int, language: str) -> bool:
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO user_languages (user_id, language) VALUES (?, ?)', (user_id, language))
        conn.commit()
        return True

async def log_button_click(user_id: int, user_name: str, chat_id: int, button_data: str, message_id: int, action_result: str = "executed"):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('''INSERT INTO button_click_logs (user_id, user_name, chat_id, button_data, message_id, action_result, timestamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, user_name, chat_id, button_data, message_id, action_result, int(time.time())))
        conn.commit()
    
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info and log_group_info.get('log_button_clicks', 0):
        chat_title = "Private"
        if chat_id > 0:
            try:
                chat = await bot.get_chat(chat_id)
                chat_title = chat.title or "Group"
            except:
                chat_title = f"Chat {chat_id}"
        
        log_text = i18n.get('button_click_log', get_group_language(chat_id),
                            user=user_name, user_id=user_id, button_data=button_data,
                            chat_title=chat_title, time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        await send_to_log_group(chat_id, 'button_click', log_text)

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
                          rules_auto_enabled INTEGER DEFAULT 0,
                          rules_interval INTEGER DEFAULT 300,
                          last_rules_message_id INTEGER,
                          last_rules_time INTEGER,
                          chat_title TEXT,
                          chat_username TEXT,
                          report_group_id INTEGER,
                          log_group_id INTEGER,
                          log_settings TEXT DEFAULT 'all',
                          confirmation_type TEXT DEFAULT 'not_bot',
                          puls_antispam_enabled INTEGER DEFAULT 1,
                          confirm_ban INTEGER DEFAULT 0,
                          confirm_kick INTEGER DEFAULT 0,
                          confirm_mute INTEGER DEFAULT 0)''')
            
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
                          punish_after_warn_duration INTEGER DEFAULT 3600,
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
                          can_give_mute INTEGER DEFAULT 0,
                          can_give_kick INTEGER DEFAULT 0,
                          can_give_ban INTEGER DEFAULT 0,
                          can_give_warn INTEGER DEFAULT 0,
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
                          send_button_clicks INTEGER DEFAULT 0,
                          UNIQUE(source_chat_id, log_group_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS button_logs
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          user_name TEXT,
                          chat_id INTEGER,
                          button_data TEXT,
                          timestamp INTEGER)''')
            
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
            
            c.execute('''CREATE TABLE IF NOT EXISTS group_languages
                         (chat_id INTEGER PRIMARY KEY,
                          language TEXT DEFAULT 'ru')''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS user_languages
                         (user_id INTEGER PRIMARY KEY,
                          language TEXT DEFAULT 'ru')''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS bot_blacklist
                         (user_id INTEGER PRIMARY KEY,
                          reason TEXT,
                          added_by INTEGER,
                          added_at INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS global_moderators
                         (user_id INTEGER PRIMARY KEY,
                          can_global_ban INTEGER DEFAULT 0,
                          can_global_mute INTEGER DEFAULT 0,
                          can_global_delete INTEGER DEFAULT 0,
                          given_by INTEGER,
                          given_at INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS global_bans
                         (user_id INTEGER PRIMARY KEY,
                          reason TEXT,
                          moderator_id INTEGER,
                          banned_at INTEGER,
                          expires_at INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS global_mutes
                         (user_id INTEGER PRIMARY KEY,
                          reason TEXT,
                          moderator_id INTEGER,
                          muted_at INTEGER,
                          expires_at INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS delete_permissions
                         (chat_id INTEGER,
                          user_id INTEGER,
                          can_delete INTEGER DEFAULT 0,
                          given_by INTEGER,
                          given_at INTEGER,
                          PRIMARY KEY (chat_id, user_id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS button_click_logs
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          user_name TEXT,
                          chat_id INTEGER,
                          button_data TEXT,
                          message_id INTEGER,
                          action_result TEXT,
                          timestamp INTEGER)''')
            
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
                                punish_after_warn, punish_after_warn_duration,
                                links_enabled, links_punish, links_duration, max_mentions, mention_window
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
                    'punish_after_warn_duration': row[10] or 3600,
                    'links_enabled': bool(row[11]), 
                    'links_punish': row[12] or 'mute',
                    'links_duration': row[13] or 3600, 
                    'max_mentions': row[14] or 3, 
                    'mention_window': row[15] or 60
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
                'punish_after_warn_duration': 3600,
                'links_enabled': False, 
                'links_punish': 'mute',
                'links_duration': 3600, 
                'max_mentions': 3, 
                'mention_window': 60
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
                    'punish_after_warn': 'mute', 'punish_after_warn_duration': 3600,
                    'links_enabled': 0, 'links_punish': 'mute', 'links_duration': 3600, 'max_mentions': 3, 'mention_window': 60
                }
                defaults.update(kwargs)
                c.execute('''INSERT INTO antiflood_settings 
                             (chat_id, enabled, msg_limit, media_limit, time_window, warn_count, 
                              first_punish, first_duration, repeat_punish, repeat_duration,
                              punish_after_warn, punish_after_warn_duration,
                              links_enabled, links_punish, links_duration, max_mentions, mention_window) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (chat_id, defaults['enabled'], defaults['msg_limit'], defaults['media_limit'],
                           defaults['time_window'], defaults['warn_count'],
                           defaults['first_punish'], defaults['first_duration'],
                           defaults['repeat_punish'], defaults['repeat_duration'],
                           defaults['punish_after_warn'], defaults['punish_after_warn_duration'],
                           defaults['links_enabled'], defaults['links_punish'],
                           defaults['links_duration'], defaults['max_mentions'], defaults['mention_window']))
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
    
    def log_button_click(self, user_id, user_name, chat_id, button_data):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO button_logs (user_id, user_name, chat_id, button_data, timestamp)
                         VALUES (?, ?, ?, ?, ?)''',
                     (user_id, user_name, chat_id, button_data, int(time.time())))
            conn.commit()
    
    def get_moderator_permissions(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT can_mute, can_kick, can_ban, can_warn, 
                                can_give_mute, can_give_kick, can_give_ban, can_give_warn 
                         FROM moderators WHERE chat_id = ? AND user_id = ?''', (chat_id, user_id))
            row = c.fetchone()
            if row:
                return {
                    'can_mute': bool(row[0]), 'can_kick': bool(row[1]), 'can_ban': bool(row[2]), 'can_warn': bool(row[3]),
                    'can_give_mute': bool(row[4]), 'can_give_kick': bool(row[5]), 'can_give_ban': bool(row[6]), 'can_give_warn': bool(row[7])
                }
            return {'can_mute': False, 'can_kick': False, 'can_ban': False, 'can_warn': False,
                    'can_give_mute': False, 'can_give_kick': False, 'can_give_ban': False, 'can_give_warn': False}
    
    def set_moderator_permission(self, chat_id, user_id, permission, value, given_by):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM moderators WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            exists = c.fetchone()
            if exists:
                c.execute(f'UPDATE moderators SET {permission} = ?, given_by = ?, given_at = ? WHERE chat_id = ? AND user_id = ?',
                         (1 if value else 0, given_by, int(time.time()), chat_id, user_id))
            else:
                defaults = {
                    'can_mute': 0, 'can_kick': 0, 'can_ban': 0, 'can_warn': 0,
                    'can_give_mute': 0, 'can_give_kick': 0, 'can_give_ban': 0, 'can_give_warn': 0
                }
                defaults[permission] = 1 if value else 0
                c.execute('''INSERT INTO moderators 
                             (chat_id, user_id, can_mute, can_kick, can_ban, can_warn,
                              can_give_mute, can_give_kick, can_give_ban, can_give_warn, given_by, given_at) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (chat_id, user_id, defaults['can_mute'], defaults['can_kick'], 
                          defaults['can_ban'], defaults['can_warn'],
                          defaults['can_give_mute'], defaults['can_give_kick'],
                          defaults['can_give_ban'], defaults['can_give_warn'],
                          given_by, int(time.time())))
            conn.commit()
    
    def get_all_moderators(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT user_id, can_mute, can_kick, can_ban, can_warn,
                                can_give_mute, can_give_kick, can_give_ban, can_give_warn, given_by, given_at 
                         FROM moderators WHERE chat_id = ?''', (chat_id,))
            return c.fetchall()
    
    def log_moderator_action(self, chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO moderator_logs 
                         (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, timestamp)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, int(time.time())))
            conn.commit()
    
    def get_moderator_logs(self, chat_id, limit=20):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT moderator_name, action, target_name, duration, reason, timestamp 
                         FROM moderator_logs WHERE chat_id = ? ORDER BY timestamp DESC LIMIT ?''',
                     (chat_id, limit))
            return c.fetchall()
    
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
                              send_joins, send_leaves, send_messages, send_button_clicks)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                         (source_chat_id, log_group_id,
                          settings.get('send_violations', 1), settings.get('send_mod_actions', 1),
                          settings.get('send_joins', 0), settings.get('send_leaves', 0), 
                          settings.get('send_messages', 0), settings.get('send_button_clicks', 0)))
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

db = Database()

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

class MaintenanceStates(StatesGroup):
    waiting_for_message = State()

class AdminBroadcastStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_media = State()
    waiting_for_target = State()

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

class DeleteMessagesStates(StatesGroup):
    waiting_for_confirm = State()

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

def get_cached_antiflood_settings(chat_id):
    return antiflood_cache.get(chat_id)

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

def get_message_link(chat_id, message_id):
    chat_id_str = str(chat_id)
    if chat_id_str.startswith('-100'):
        chat_id_str = chat_id_str[4:]
    return f"https://t.me/c/{chat_id_str}/{message_id}"

def get_premium_status_emoji(is_premium: bool) -> str:
    return "⭐" if is_premium else ""

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
        'send_messages': log_group_info['send_messages'],
        'send_button_clicks': log_group_info.get('send_button_clicks', 0)
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
    if event_type == 'button_click' and not settings['send_button_clicks']:
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
        
        spammer_pm_text = customization.format_message(
            'spammer_pm',
            chat_title=message.chat.title,
            warnings=warnings,
            limit=SPAM_WARN_LIMIT,
            support_link=SUPPORT_LINK
        )
        try:
            await bot.send_message(user_id, spammer_pm_text, parse_mode="HTML")
        except:
            pass
        try:
            await bot.ban_chat_member(chat_id, user_id)
            logger.info(f"🚫 Спамер {user_id} забанен в чате {chat_id}")
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
            spammer_added_text = customization.format_message(
                'spammer_added',
                name=message.from_user.full_name
            )
            await message.answer(spammer_added_text)
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
        for key in list(user_command_usage.keys()):
            user_command_usage[key] = deque([t for t in user_command_usage[key] if now - t < 2], maxlen=5)
            if not user_command_usage[key]:
                del user_command_usage[key]
        await asyncio.sleep(300)

async def clean_old_logs():
    while True:
        old_time = int(time.time()) - 30 * 86400
        with db.get_connection() as conn:
            conn.execute('DELETE FROM violation_logs WHERE timestamp < ?', (old_time,))
            conn.execute('DELETE FROM moderator_logs WHERE timestamp < ?', (old_time,))
            conn.execute('DELETE FROM button_click_logs WHERE timestamp < ?', (old_time,))
            conn.commit()
        await asyncio.sleep(86400)

async def clean_expired_bans_mutes():
    while True:
        now = int(time.time())
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM global_bans WHERE expires_at > 0 AND expires_at < ?', (now,))
            c.execute('DELETE FROM global_mutes WHERE expires_at > 0 AND expires_at < ?', (now,))
            conn.commit()
        await asyncio.sleep(3600)

def create_button(text: str, callback_data: str, color: str = None):
    if color:
        return InlineKeyboardButton(text=text, callback_data=callback_data, color=color)
    return InlineKeyboardButton(text=text, callback_data=callback_data)

def get_back_keyboard(callback_data):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", callback_data, "secondary"))
    return builder.as_markup()

def get_main_keyboard(is_group: bool = False, is_admin: bool = False, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('about_button', lang), "about", "primary"))
    builder.add(create_button(i18n.get('help_button', lang), "help", "danger"))
    builder.add(create_button(i18n.get('add_to_group_button', lang), f"add_to_group_{BOT_USERNAME}", "success"))
    builder.add(create_button(i18n.get('group_settings_button', lang), "group_manage_main", "primary"))
    if is_group:
        builder.add(create_button(i18n.get('rules_button', lang), "show_rules_group", "secondary"))
        builder.add(create_button(i18n.get('stats_button', lang), "my_stats_group", "secondary"))
        builder.add(create_button(i18n.get('top_button', lang), "top_active_group", "success"))
    if is_admin and not is_group:
        builder.add(create_button(i18n.get('admin_panel_button', lang), "admin_panel", "danger"))
    builder.adjust(2)
    return builder.as_markup()

def get_group_manage_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('rules_manage', lang), "manage_rules", "primary"))
    builder.add(create_button(i18n.get('welcome_manage', lang), "manage_welcome", "secondary"))
    builder.add(create_button(i18n.get('auto_broadcast', lang), "rules_auto", "secondary"))
    builder.add(create_button(i18n.get('antiflood_manage', lang), "antiflood_manage", "primary"))
    builder.add(create_button(i18n.get('puls_antispam', lang), "puls_antispam_manage", "danger"))
    builder.add(create_button(i18n.get('confirm_actions', lang), "confirmation_actions_manage", "primary"))
    builder.add(create_button(i18n.get('log_group', lang), "log_group_manage", "secondary"))
    builder.add(create_button(i18n.get('auto_response', lang), "auto_response_manage", "success"))
    builder.add(create_button(i18n.get('links_manage', lang), "links_manage", "secondary"))
    builder.add(create_button(i18n.get('confirm_entry', lang), "confirmation_manage", "primary"))
    builder.add(create_button(i18n.get('moderators_manage', lang), "moderators_manage", "primary"))
    builder.add(create_button(i18n.get('language_settings', lang), "language_settings", "primary"))
    builder.add(create_button(i18n.get('unlink_group', lang), "unlink_group_confirm", "danger"))
    builder.add(create_button(i18n.get('back_button', lang), "back_to_groups", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_language_settings_keyboard(current_lang: str, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    languages = ["ru", "en", "uk", "de", "fr", "es", "it", "pt", "tr", "zh", "ja", "ko", "ar", "hi"]
    for l in languages:
        name = i18n.get(f"language_{l}", lang)
        if l == current_lang:
            name = f"✅ {name}"
        builder.add(create_button(name, f"set_lang_{l}", "primary"))
    builder.adjust(2)
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    return builder.as_markup()

def get_confirmation_actions_keyboard(settings, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    ban_status = "✅" if settings.get('ban', False) else "❌"
    kick_status = "✅" if settings.get('kick', False) else "❌"
    mute_status = "✅" if settings.get('mute', False) else "❌"
    builder.add(create_button(f"{ban_status} Подтверждение бана", "toggle_confirm_ban", "secondary"))
    builder.add(create_button(f"{kick_status} Подтверждение кика", "toggle_confirm_kick", "secondary"))
    builder.add(create_button(f"{mute_status} Подтверждение мута", "toggle_confirm_mute", "secondary"))
    builder.add(create_button("ℹ️ Что это?", "confirmation_actions_info", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_puls_antispam_keyboard(enabled, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    status_text = "❌ Выключить" if enabled else "✅ Включить"
    status_color = "danger" if enabled else "success"
    builder.add(create_button(status_text, "toggle_puls_antispam", status_color))
    builder.add(create_button("ℹ️ Что это?", "puls_antispam_info", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_confirm_action_keyboard(action, user_id, duration=None, reason=None, lang: str = "ru"):
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

def get_lift_restriction_keyboard(action, user_id, message_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔓 Снять ограничение", f"lift_{action}_{user_id}_{message_id}", "success"))
    return builder.as_markup()

def get_moderators_manage_keyboard(moderators, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Дать права", "give_mod_rights", "success"))
    if moderators:
        builder.add(create_button("❌ Забрать права", "remove_mod_rights", "danger"))
    builder.add(create_button("👁 Список", "list_moderators", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_mod_rights_keyboard(user_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔇 Право мутить", f"give_mute_{user_id}", "primary"))
    builder.add(create_button("👢 Право кикать", f"give_kick_{user_id}", "danger"))
    builder.add(create_button("⛔ Право банить", f"give_ban_{user_id}", "danger"))
    builder.add(create_button("⚠️ Право варнить", f"give_warn_{user_id}", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "moderators_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_log_group_manage_keyboard(has_log_group, log_group_info=None, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    if has_log_group and log_group_info:
        builder.add(create_button("📊 Настройки логов", "log_group_settings", "primary"))
        builder.add(create_button("🔄 Отвязать", "unlink_log_group", "danger"))
        builder.add(create_button("👁 Инфо", "log_group_info", "secondary"))
    else:
        builder.add(create_button("➕ Привязать группу логов", "link_log_group", "success"))
        builder.add(create_button("ℹ️ Как создать", "log_group_help", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_log_settings_keyboard(settings, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    status_violations = "✅" if settings.get('send_violations', 1) else "❌"
    status_mod = "✅" if settings.get('send_mod_actions', 1) else "❌"
    status_joins = "✅" if settings.get('send_joins', 0) else "❌"
    status_leaves = "✅" if settings.get('send_leaves', 0) else "❌"
    status_messages = "✅" if settings.get('send_messages', 0) else "❌"
    status_buttons = "✅" if settings.get('send_button_clicks', 0) else "❌"
    builder.add(create_button(f"{status_violations} Нарушения", "toggle_log_violations", "secondary"))
    builder.add(create_button(f"{status_mod} Действия модераторов", "toggle_log_mod", "secondary"))
    builder.add(create_button(f"{status_joins} Входы", "toggle_log_joins", "secondary"))
    builder.add(create_button(f"{status_leaves} Выходы", "toggle_log_leaves", "secondary"))
    builder.add(create_button(f"{status_messages} Сообщения", "toggle_log_messages", "secondary"))
    builder.add(create_button(f"{status_buttons} Нажатия кнопок", "toggle_log_buttons", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "log_group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard(has_rules, rules_enabled, lang: str = "ru"):
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
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled=False, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if enabled else "success"
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_welcome", toggle_color))
    builder.add(create_button("📝 Текст", "set_welcome_text", "primary"))
    builder.add(create_button("🖼 Фото", "set_welcome_photo", "primary"))
    builder.add(create_button("👁 Посмотреть", "show_welcome", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if enabled else "success"
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_rules_auto", toggle_color))
    builder.add(create_button("⏱ Интервал", "set_interval", "primary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings, lang: str = "ru"):
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
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(punish_type="first", lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"punish_warn_{punish_type}", "secondary"))
    builder.add(create_button("🔇 Мут", f"punish_mute_{punish_type}", "primary"))
    builder.add(create_button("👢 Кик", f"punish_kick_{punish_type}", "danger"))
    builder.add(create_button("⛔️ Бан", f"punish_ban_{punish_type}", "danger"))
    builder.add(create_button(i18n.get('back_button', lang), "antiflood_manage", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_welcome_buttons(chat_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('rules_button', lang), f"show_group_rules_{chat_id}", "primary"))
    builder.add(create_button(i18n.get('stats_button', lang), f"my_stats_{chat_id}", "secondary"))
    builder.add(create_button(i18n.get('top_button', lang), f"top_active_{chat_id}", "success"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id, user_id, msg_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}", "success"))
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id, user_id, msg_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Согласен", f"agree_rules_{chat_id}_{user_id}_{msg_id}", "success"))
    return builder.as_markup()

def get_link_group_keyboard(chat_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Привязать", f"link_group_{chat_id}", "success"))
    builder.add(create_button("🚫 Отмена", "cancel_link", "danger"))
    builder.adjust(1)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("❌ Отвязать", f"unlink_group_{chat_id}", "danger"))
    builder.add(create_button("🚫 Отмена", "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_pm_link_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("💬 Перейти в ЛС", "go_to_pm", "primary"))
    return builder.as_markup()

def get_auto_response_keyboard(responses, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "add_auto_trigger", "success"))
    if responses:
        builder.add(create_button("🗑 Удалить", "remove_auto_trigger", "danger"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_remove_keyboard(responses, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    for i, (trigger, _, _, _) in enumerate(responses):
        short = trigger[:15] + "..." if len(trigger) > 15 else trigger
        builder.add(create_button(short, f"rem_trig_{i}", "danger"))
    builder.add(create_button(i18n.get('back_button', lang), "auto_response_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_manage_keyboard(settings, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    toggle_color = "danger" if settings['links_enabled'] else "success"
    builder.add(create_button(f"{'❌ Выключить' if settings['links_enabled'] else '✅ Включить'}", "toggle_links", toggle_color))
    builder.add(create_button("Наказание", "set_links_punish", "primary"))
    builder.add(create_button(f"Макс: {settings['max_mentions']}", "set_max_mentions", "secondary"))
    builder.add(create_button(f"Период: {settings['mention_window']} сек", "set_mention_window", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_punish_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", "links_punish_warn", "secondary"))
    builder.add(create_button("🔇 Мут", "links_punish_mute", "primary"))
    builder.add(create_button("👢 Кик", "links_punish_kick", "danger"))
    builder.add(create_button("⛔️ Бан", "links_punish_ban", "danger"))
    builder.add(create_button(i18n.get('back_button', lang), "links_manage", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_confirmation_keyboard(current_type, has_rules, lang: str = "ru"):
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
    builder.add(create_button(i18n.get('back_button', lang), "group_manage", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_custom_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Тексты сообщений", "admin_custom_texts", "primary"))
    builder.add(create_button("🖼 Фото сообщений", "admin_custom_photos", "primary"))
    builder.add(create_button("🔄 Сбросить всё", "admin_custom_reset_all", "danger"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_texts_list_keyboard(page=0, lang: str = "ru"):
    templates = admin_custom.get_all_templates()
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
    
    builder.add(create_button(i18n.get('back_button', lang), "admin_custom", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_photos_list_keyboard(page=0, lang: str = "ru"):
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
    
    builder.add(create_button(i18n.get('back_button', lang), "admin_custom", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_delete_confirm_keyboard(count: int, user_id: int = None, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    if user_id:
        builder.add(create_button("✅ Да, удалить", f"confirm_del_user_{count}_{user_id}", "danger"))
    else:
        builder.add(create_button("✅ Да, удалить", f"confirm_del_{count}", "danger"))
    builder.add(create_button("❌ Отмена", "cancel_delete", "secondary"))
    builder.adjust(2)
    return builder.as_markup()

def get_admin_blacklist_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "blacklist_add", "success"))
    builder.add(create_button("🗑 Удалить", "blacklist_remove", "danger"))
    builder.add(create_button("📋 Список", "blacklist_list", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_global_bans_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_ban_add", "success"))
    builder.add(create_button("🗑 Снять", "global_ban_remove", "danger"))
    builder.add(create_button("📋 Список", "global_ban_list", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_global_mutes_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_mute_add", "success"))
    builder.add(create_button("🗑 Снять", "global_mute_remove", "danger"))
    builder.add(create_button("📋 Список", "global_mute_list", "secondary"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel", "secondary"))
    builder.adjust(1)
    return builder.as_markup()

flood_control = defaultdict(lambda: deque(maxlen=50))
mention_control = defaultdict(lambda: deque(maxlen=50))

class AntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if not isinstance(event, Message) or event.chat.type not in {'group', 'supergroup'}:
            return await handler(event, data)
        
        chat_id = event.chat.id
        user = event.from_user
        
        if user.is_bot:
            return await handler(event, data)
        
        is_blacklisted, reason = await check_blacklist(user.id)
        if is_blacklisted:
            lang = get_group_language(chat_id)
            await event.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
            return
        
        is_gbanned, gban_reason, expires = is_global_banned(user.id)
        if is_gbanned:
            await event.delete()
            return
        
        if db.get_puls_antispam_enabled(chat_id):
            is_spam = await check_and_handle_spam(event)
            if is_spam:
                return
        
        db.update_message_count(chat_id, user.id)
        
        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            return await handler(event, data)
        
        settings = get_cached_antiflood_settings(chat_id)
        
        if not settings['enabled']:
            return await handler(event, data)
        
        now = time.time()
        key = f"{chat_id}_{user.id}"
        msg_type = get_message_type(event)
        is_media = msg_type != 'text'
        
        if key not in flood_control:
            flood_control[key] = deque(maxlen=50)
        
        while flood_control[key] and now - flood_control[key][0] > settings['time_window']:
            flood_control[key].popleft()
        
        media_count = sum(1 for t in flood_control[key] if t[1] != 'text')
        text_count = len(flood_control[key]) - media_count
        
        if is_media:
            if media_count >= settings['media_limit']:
                await self.handle_violation(event, chat_id, user, settings, "Медиа-флуд")
                return
        else:
            if text_count >= settings['msg_limit']:
                await self.handle_violation(event, chat_id, user, settings, "Текстовый флуд")
                return
        
        if settings['links_enabled'] and event.text:
            mentions = extract_mentions(event.text)
            if mentions > 0:
                mention_key = f"mentions_{chat_id}_{user.id}"
                if mention_key not in mention_control:
                    mention_control[mention_key] = deque(maxlen=50)
                while mention_control[mention_key] and now - mention_control[mention_key][0] > settings['mention_window']:
                    mention_control[mention_key].popleft()
                for _ in range(mentions):
                    mention_control[mention_key].append(now)
                if len(mention_control[mention_key]) > settings['max_mentions']:
                    await self.handle_violation(event, chat_id, user, settings, "Спам упоминаниями/ссылками", is_links=True)
                    return
        
        flood_control[key].append((now, msg_type))
        return await handler(event, data)
    
    async def handle_violation(self, event: Message, chat_id: int, user: types.User, settings: dict, reason: str, is_links: bool = False):
        warns = db.get_user_warns(chat_id, user.id)
        warn_count = warns['count']
        
        if is_links:
            punish_type = settings['links_punish']
            duration = settings['links_duration']
        else:
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
        
        message_link = get_message_link(chat_id, event.message_id)
        db.log_violation(chat_id, user.id, user.full_name, reason, punish_type, event.message_id, message_link)
        
        log_text = (
            f"<b>🚫 Нарушение</b>\n\n"
            f"Пользователь: {safe_html(user.full_name, False)}\n"
            f"Причина: {safe_html(reason, False)}\n"
            f"Наказание: {punish_type}\n"
            f"Длительность: {format_interval(duration) if duration > 0 else 'навсегда'}\n"
            f"<a href='{message_link}'>Сообщение</a>"
        )
        await send_to_log_group(chat_id, 'violation', log_text)
        
        report_group = db.get_report_group(chat_id)
        if report_group:
            try:
                await bot.send_message(report_group, log_text, parse_mode="HTML")
            except:
                pass
        
        try:
            if punish_type == 'mute':
                until = int(time.time() + duration) if duration > 0 else None
                await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
                
                mute_text = customization.format_message(
                    'mute_message',
                    name=safe_html(user.full_name, False),
                    moderator=safe_html(event.from_user.full_name, False),
                    duration=format_interval(duration) if duration > 0 else 'навсегда',
                    reason=safe_html(reason, False)
                )
                
                msg = await event.reply(
                    mute_text,
                    reply_markup=get_lift_restriction_keyboard('mute', user.id, event.message_id),
                    parse_mode="HTML"
                )
                await add_premium_reaction(event, "🔇")
            elif punish_type == 'ban':
                until = int(time.time() + duration) if duration > 0 else None
                await bot.ban_chat_member(chat_id, user.id, until_date=until)
                
                ban_text = customization.format_message(
                    'ban_message',
                    name=safe_html(user.full_name, False),
                    moderator=safe_html(event.from_user.full_name, False),
                    duration=format_interval(duration) if duration > 0 else 'навсегда',
                    reason=safe_html(reason, False)
                )
                
                msg = await event.reply(
                    ban_text,
                    reply_markup=get_lift_restriction_keyboard('ban', user.id, event.message_id),
                    parse_mode="HTML"
                )
                await add_premium_reaction(event, "⛔️")
            elif punish_type == 'kick':
                await bot.ban_chat_member(chat_id, user.id)
                await bot.unban_chat_member(chat_id, user.id)
                
                kick_text = customization.format_message(
                    'kick_message',
                    name=safe_html(user.full_name, False),
                    moderator=safe_html(event.from_user.full_name, False),
                    reason=safe_html(reason, False)
                )
                await event.reply(kick_text, parse_mode="HTML")
                await add_premium_reaction(event, "👢")
            elif punish_type == 'warn':
                new_warn_count = db.add_user_warn(chat_id, user.id)
                
                warn_text = customization.format_message(
                    'warn_message',
                    name=safe_html(user.full_name, False),
                    moderator=safe_html(event.from_user.full_name, False),
                    warn_count=new_warn_count,
                    reason=safe_html(reason, False)
                )
                await event.reply(warn_text, parse_mode="HTML")
                await add_premium_reaction(event, "⚠️")
        except Exception as e:
            logger.warning(f"Ошибка наказания: {e}")

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

async def reset_periodic_counters():
    global stats_updating
    while True:
        now = datetime.now()
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

DEFAULT_RULES = """
📢 Правила чата

━━━━━━━━━━━━━━━━━━
<blockquote>🔰 1. Администрация</blockquote>
<blockquote expandable>💠 1.1. Администрация следит за порядком и вправе применять наказания.
💠 1.2. Доказательства нарушений хранятся у администрации.
💠 1.3. Обжалование наказания возможно через владельца: @vanezyyy
💠 1.4. Решение администрации окончательное, если владелец не решит иначе.
💠 1.5. Обсуждение действий администрации в чате запрещено
→ (вопросы вроде «за что мут?» или «почему бан?» при первом нарушении — варн, при повторных — мут 1–3 часа).</blockquote>

━━━━━━━━━━━━━━━━━━
<blockquote>🚫 2. Запрещено</blockquote>
<blockquote expandable>🔹 2.1. Неадекватное поведение, агрессия, провокации — мут 1–3 дня
→ (оскорбления без системности, провокации, грубое поведение, угрозы).

🔹 2.2. Очень грубые оскорбления (3 и более грубых высказывания в сторону одного человека) — мут 3–7 дней
→ (несколько матов или оскорбительных слов подряд, направленных на одного участника).

🔹 2.3. Запрещённые слова — наказание на усмотрение администрации
→ (пuдoр, пeтyx, пeдuк, шлюха, проститутка, далбоёбка — если направлено на человека).

🔹 2.4. Оскорбления родных или близких — бан 5–30 дней
→ (оскорбления родителей, братьев, сестёр, родственников участника).

🔹 2.5. Спам, флуд, массовая отправка сообщений/стикеров — мут 1–3 дня
→ (повторяющиеся сообщения, 4+ одинаковых стикеров подряд, бесполезные ссылки).

🔹 2.6. Реклама и продажа без разрешения — мут 3–7 дней, повтор — бан 7–30 дней
→ (продажа внутриигровых предметов, сторонних товаров, рекламы без согласования).

🔹 2.7. Обман участников или администрации — мут 3–7 дней, повтор — бан
→ (ложная информация, введение в заблуждение, обманные обещания).

🔹 2.8. Угрозы (в любом виде) — мут 3–7 дней
→ (угрозы физической расправой, doxxing, swatting, угрозы через личные сообщения или чат).

🔹 2.9. 18+ контент — мут 3–7 дней
→ (материалы сексуального характера, эротические картинки, ссылки на порно, намёки на сексуальный контент).

🔹 2.10. Политика и запрещённая символика — мут 3–7 дней
→ (Z, V, 1488, свастика, символика фашистов или других запрещённых организаций/идеологий).

🔹 2.11. Отправка непроверенных скриптов без согласования — мут 1–3 дня
→ (скрипты, которые могут навредить участникам или чату, без проверки у администрации).

🔹2.12 Ложные жалобы, намеренная подача ложных жалоб на участников запрещена — варн на 1 неделю.

🔹2.13  За умышленный ввод администрации в заблуждение применяется наказание на усмотрение администрации (мут или бан в зависимости от ситуации).</blockquote>

━━━━━━━━━━━━━━━━━━
<blockquote>⭐ 3. Разрешено</blockquote>
<blockquote expandable>✅ 3.1. Обсуждать Roblox и скрипты.
✅ 3.2. Помогать участникам.
✅ 3.3. Поддерживать дружелюбное общение.</blockquote>

━━━━━━━━━━━━━━━━━━
<blockquote>⚠️ Важно</blockquote>
<blockquote>⚠️ Незнание правил не освобождает от ответственности.
⚠️ Вступая в чат, вы соглашаетесь с ними.</blockquote>
━━━━━━━━━━━━━━━━━━
"""

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = "ru"
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    is_premium = getattr(message.from_user, 'is_premium', False)
    is_admin = message.from_user.id in ADMIN_IDS
    is_group = message.chat.type != 'private'
    
    user_lang = get_user_language(user_id)
    
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
            reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin, lang=user_lang),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            welcome_text,
            reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin, lang=user_lang),
            parse_mode="HTML"
        )
    await add_premium_reaction(message, "⭐")

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

@dp.message(Command("groupsettings"))
@pm_only()
async def cmd_group_settings(message: Message, state: FSMContext):
    await state.clear()
    user_lang = get_user_language(message.from_user.id)
    groups = db.get_user_groups(message.from_user.id)
    if not groups:
        await message.answer(i18n.get('no_groups', user_lang))
        return
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button(i18n.get('back_button', user_lang), "back_to_main", "secondary"))
    builder.adjust(1)
    await message.answer(i18n.get('select_group', user_lang), reply_markup=builder.as_markup())
    await add_premium_reaction(message, "📱")

@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["пульс", "понг"]))
async def cmd_ping(message: Message):
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_user_language(user_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    can_use, wait_time = await check_command_flood(user_id)
    if not can_use:
        await message.answer(f"⚠️ Подождите {wait_time} сек перед использованием команды!")
        return
    
    start = time.time()
    msg = await message.reply("⏳ ...")
    ping = round((time.time() - start) * 1000)
    await msg.edit_text(f"📡 <b>Пинг:</b> {ping} мс\n⏱ <b>Время:</b> {ping/1000:.2f} сек", parse_mode="HTML")
    await add_premium_reaction(message, "📡")

@dp.message(Command("stats"))
@group_only()
async def cmd_stats(message: Message):
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(message.chat.id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    can_use, wait_time = await check_command_flood(user_id)
    if not can_use:
        await message.answer(f"⚠️ Подождите {wait_time} сек перед использованием команды!")
        return
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(i18n.get('stats_updating', get_group_language(message.chat.id)))
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
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=safe_html(user.full_name, False))
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        no_stats = customization.get_template('profile_no_stats').get_text()
        
        text = f"{header}\n\n{id_line}\n{first_seen}\n{premium_line}\n{antispam}\n\n{no_stats}"
    else:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=safe_html(user.full_name, False))
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
    
    await message.reply(text, parse_mode="HTML")
    await add_premium_reaction(message, "📊")

@dp.message(Command("top"))
@group_only()
async def cmd_top(message: Message):
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(message.chat.id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    can_use, wait_time = await check_command_flood(user_id)
    if not can_use:
        await message.answer(f"⚠️ Подождите {wait_time} сек перед использованием команды!")
        return
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(i18n.get('stats_updating', get_group_language(message.chat.id)))
        return
    top = db.get_top_messages(message.chat.id, limit=10)
    if not top:
        await message.reply(i18n.get('no_messages', get_group_language(message.chat.id)))
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
            name=safe_html(name, False),
            count=count,
            warnings=warning_text
        )
        text += f"{entry}\n"
    
    await message.reply(text, parse_mode="HTML")
    await add_premium_reaction(message, "🏆")

@dp.message(Command("profile"))
@group_only()
async def cmd_profile(message: Message):
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(message.chat.id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    can_use, wait_time = await check_command_flood(user_id)
    if not can_use:
        await message.answer(f"⚠️ Подождите {wait_time} сек перед использованием команды!")
        return
    
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(i18n.get('stats_updating', get_group_language(message.chat.id)))
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
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=safe_html(target_user.full_name, False))
        id_line = customization.format_message('profile_id', global_id=global_user_data['global_id'])
        first_seen = customization.format_message('profile_first_seen', first_seen=format_datetime(global_user_data['first_seen']))
        premium_line = customization.format_message('profile_premium') if global_user_data['is_premium'] else ""
        antispam = customization.format_message('profile_antispam', warnings=warnings, limit=SPAM_WARN_LIMIT)
        no_stats = customization.get_template('profile_no_stats').get_text()
        
        text = f"{header}\n\n{id_line}\n{first_seen}\n{premium_line}\n{antispam}\n\n{no_stats}"
    else:
        header = customization.format_message('profile_header', premium_emoji=premium_emoji, name=safe_html(target_user.full_name, False))
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
    
    await message.reply(text, parse_mode="HTML")
    await add_premium_reaction(message, "👤")

@dp.message(Command("rules"))
@group_only()
async def cmd_rules(message: Message):
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(message.chat.id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    rules = db.get_rules_html(message.chat.id)
    if rules and db.get_rules_enabled(message.chat.id):
        await message.reply(safe_html(rules, True), parse_mode="HTML")
        await add_premium_reaction(message, "📜")
    else:
        await message.answer(i18n.get('rules_not_set', get_group_language(message.chat.id)))

@dp.message(Command("unban"))
@group_only()
async def cmd_unban(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    if not await is_admin(chat_id, user_id) and not await check_moderator_permission(chat_id, user_id, 'can_ban'):
        await message.answer("❌ У вас нет права разбанивать пользователей!")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "❌ Укажите пользователя!\n\n"
            "Пример: /unban 123456789\n"
            "Или ответьте на сообщение пользователя"
        )
        return
    target_id = None
    target_name = "пользователь"
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
    else:
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
    try:
        await bot.unban_chat_member(chat_id, target_id)
        unban_spammer_in_chat(target_id, chat_id)
        await message.answer(
            f"✅ Пользователь {target_name} разбанен в этом чате!\n\n"
            f"Он всё ещё остаётся в базе спамеров Пульса, но может писать в этой группе."
        )
        log_text = (
            f"<b>✅ Разбан</b>\n\n"
            f"👮 Админ: {safe_html(message.from_user.full_name, False)}\n"
            f"👤 Пользователь: {safe_html(target_name, False)}\n"
            f"🆔 ID: {target_id}"
        )
        await send_to_log_group(chat_id, 'mod_action', log_text)
    except Exception as e:
        await message.answer(f"❌ Ошибка при разбане: {e}")

@dp.message(Command("remove_spammer"))
@check_bot_admin()
@pm_only()
async def cmd_remove_spammer(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите ID пользователя: /remove_spammer 123456789")
        return
    try:
        user_id = int(args[1])
        if remove_spammer_from_db(user_id):
            await message.answer(f"✅ Пользователь {user_id} удален из антиспам базы Пульса!")
            logger.info(f"Админ {message.from_user.id} удалил спамера {user_id} из базы")
        else:
            await message.answer(f"❌ Пользователь {user_id} не найден в базе спамеров")
    except ValueError:
        await message.answer("❌ Некорректный ID пользователя")

@dp.message(Command("mute"))
@group_only()
async def cmd_mute(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
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
            name=safe_html(target_user.full_name, False),
            duration_line=f"⏱ Длительность: {format_time(duration) if duration > 0 else 'навсегда'}\n",
            reason=safe_html(reason, False)
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
            name=safe_html(target_name, False),
            moderator=safe_html(moderator.full_name, False),
            duration=duration_text,
            reason=safe_html(reason, False)
        )
        
        msg = await bot.send_message(
            chat_id,
            mute_text,
            reply_markup=get_lift_restriction_keyboard('mute', target_id, message_id),
            parse_mode="HTML"
        )
        db.log_moderator_action(
            chat_id, moderator.id, moderator.full_name,
            'mute', target_id, target_name, duration, reason
        )
        log_text = (
            f"<b>🔇 Мут</b>\n\n"
            f"👮 Модератор: {safe_html(moderator.full_name, False)}\n"
            f"👤 Пользователь: {safe_html(target_name, False)}\n"
            f"⏱ Длительность: {duration_text}\n"
            f"📝 Причина: {safe_html(reason, False)}"
        )
        await send_to_log_group(chat_id, 'mod_action', log_text)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка при муте: {e}")

@dp.message(Command("unmute"))
@group_only()
async def cmd_unmute(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
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
                can_add_web_page_previews=True,
                can_change_info=False,
                can_invite_users=True,
                can_pin_messages=False
            )
        )
        
        unmute_text = customization.format_message(
            'unmute_message',
            name=safe_html(target_user.full_name, False),
            moderator=safe_html(message.from_user.full_name, False)
        )
        
        await message.answer(
            unmute_text,
            parse_mode="HTML"
        )
        log_text = (
            f"<b>🔊 Размут</b>\n\n"
            f"👮 Модератор: {safe_html(message.from_user.full_name, False)}\n"
            f"👤 Пользователь: {safe_html(target_user.full_name, False)}"
        )
        await send_to_log_group(chat_id, 'mod_action', log_text)
    except Exception as e:
        await message.answer(f"❌ Ошибка при размуте: {e}")

@dp.message(Command("ban"))
@group_only()
async def cmd_ban(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
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
            name=safe_html(target_user.full_name, False),
            duration_line=f"⏱ Длительность: {format_time(duration) if duration > 0 else 'навсегда'}\n",
            reason=safe_html(reason, False)
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
            name=safe_html(target_name, False),
            moderator=safe_html(moderator.full_name, False),
            duration=duration_text,
            reason=safe_html(reason, False)
        )
        
        msg = await bot.send_message(
            chat_id,
            ban_text,
            reply_markup=get_lift_restriction_keyboard('ban', target_id, message_id),
            parse_mode="HTML"
        )
        db.log_moderator_action(
            chat_id, moderator.id, moderator.full_name,
            'ban', target_id, target_name, duration, reason
        )
        log_text = (
            f"<b>⛔ Бан</b>\n\n"
            f"👮 Модератор: {safe_html(moderator.full_name, False)}\n"
            f"👤 Пользователь: {safe_html(target_name, False)}\n"
            f"⏱ Длительность: {duration_text}\n"
            f"📝 Причина: {safe_html(reason, False)}"
        )
        await send_to_log_group(chat_id, 'mod_action', log_text)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка при бане: {e}")

@dp.message(Command("kick"))
@group_only()
async def cmd_kick(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
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
            name=safe_html(target_user.full_name, False),
            duration_line="",
            reason=safe_html(reason, False)
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
            name=safe_html(target_name, False),
            moderator=safe_html(moderator.full_name, False),
            reason=safe_html(reason, False)
        )
        
        await bot.send_message(
            chat_id,
            kick_text,
            parse_mode="HTML"
        )
        db.log_moderator_action(
            chat_id, moderator.id, moderator.full_name,
            'kick', target_id, target_name, 0, reason
        )
        log_text = (
            f"<b>👢 Кик</b>\n\n"
            f"👮 Модератор: {safe_html(moderator.full_name, False)}\n"
            f"👤 Пользователь: {safe_html(target_name, False)}\n"
            f"📝 Причина: {safe_html(reason, False)}"
        )
        await send_to_log_group(chat_id, 'mod_action', log_text)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка при кике: {e}")

@dp.message(Command("warn"))
@group_only()
async def cmd_warn(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
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
        
        warn_text = customization.format_message(
            'warn_message',
            name=safe_html(target_user.full_name, False),
            moderator=safe_html(message.from_user.full_name, False),
            warn_count=warn_count,
            reason=safe_html(reason, False)
        )
        
        await message.answer(
            warn_text,
            parse_mode="HTML"
        )
        db.log_moderator_action(
            chat_id, message.from_user.id, message.from_user.full_name,
            'warn', target_user.id, target_user.full_name, 0, reason
        )
        log_text = (
            f"<b>⚠️ Предупреждение</b>\n\n"
            f"👮 Модератор: {safe_html(message.from_user.full_name, False)}\n"
            f"👤 Пользователь: {safe_html(target_user.full_name, False)}\n"
            f"📊 Предупреждение №{warn_count}\n"
            f"📝 Причина: {safe_html(reason, False)}"
        )
        await send_to_log_group(chat_id, 'mod_action', log_text)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(ModerationStates.waiting_for_confirm_action)
async def process_confirm_action(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    action = data.get('action')
    target_id = data.get('target_id')
    target_name = data.get('target_name')
    duration = data.get('duration')
    reason = data.get('reason')
    moderator = callback.from_user
    message_id = data.get('message_id')
    
    if callback.data.endswith('_yes'):
        if action == 'mute':
            await execute_mute(callback.message.chat.id, target_id, target_name, duration, reason, moderator, message_id)
        elif action == 'ban':
            await execute_ban(callback.message.chat.id, target_id, target_name, duration, reason, moderator, message_id)
        elif action == 'kick':
            await execute_kick(callback.message.chat.id, target_id, target_name, reason, moderator)
        
        completed_text = customization.get_template('action_completed').get_text()
        await callback.message.edit_text(completed_text)
    else:
        cancelled_text = customization.get_template('action_cancelled').get_text()
        await callback.message.edit_text(cancelled_text)
    
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("lift_"))
@edit_only()
@check_public()
async def lift_restriction(callback: CallbackQuery):
    parts = callback.data.split('_')
    action = parts[1]
    target_id = int(parts[2])
    original_message_id = int(parts[3])
    moderator = callback.from_user
    chat_id = callback.message.chat.id
    
    try:
        if action == 'mute':
            await bot.restrict_chat_member(
                chat_id,
                target_id,
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_polls=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                    can_change_info=False,
                    can_invite_users=True,
                    can_pin_messages=False
                )
            )
            
            lift_text = customization.format_message(
                'lift_restriction_message',
                moderator=safe_html(moderator.full_name, False)
            )
            
            await callback.message.edit_text(
                lift_text,
                parse_mode="HTML"
            )
            
            notification_text = customization.format_message(
                'lift_notification',
                moderator=safe_html(moderator.full_name, False)
            )
            
            await bot.send_message(
                chat_id,
                notification_text,
                reply_to_message_id=original_message_id,
                parse_mode="HTML"
            )
            
        elif action == 'ban':
            await bot.unban_chat_member(chat_id, target_id)
            
            await callback.message.edit_text(
                f"✅ <b>Разбанен</b>\n\n"
                f"👮 Модератор: {safe_html(moderator.full_name, False)}\n"
                f"👤 Пользователь разбанен",
                parse_mode="HTML"
            )
            
            await bot.send_message(
                chat_id,
                f"✅ Бан пользователя снят модератором {safe_html(moderator.full_name, False)}",
                reply_to_message_id=original_message_id,
                parse_mode="HTML"
            )
        
        await callback.answer("✅ Ограничение снято!")
        
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

@dp.message(Command("mods"))
@group_only()
async def cmd_mods(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    moderators = db.get_all_moderators(chat_id)
    if not moderators and not await is_creator(chat_id, message.from_user.id):
        await message.answer("📋 В этой группе нет назначенных модераторов")
        return
    text = "🛡️ <b>Модераторы группы:</b>\n\n"
    try:
        creator = await bot.get_chat_member(chat_id, (await bot.get_chat(chat_id)).id)
        text += f"👑 <b>Владелец:</b> {safe_html(creator.user.full_name, False)}\n\n"
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
                rights_text = " ".join(rights) if rights else "❌ нет прав"
                text += f"• {safe_html(name, False)} - {rights_text}\n"
            except:
                continue
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("give_del"))
@group_only()
async def cmd_give_del(message: Message):
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
    set_delete_permission(chat_id, target_user.id, True, user_id)
    await message.answer(i18n.get('delete_give_right', get_group_language(chat_id), name=safe_html(target_user.full_name, False)))
    log_text = (
        f"<b>🗑 Выдача права на удаление</b>\n\n"
        f"👮 Админ: {safe_html(message.from_user.full_name, False)}\n"
        f"👤 Пользователь: {safe_html(target_user.full_name, False)}"
    )
    await send_to_log_group(chat_id, 'mod_action', log_text)

@dp.message(Command("ungive_del"))
@group_only()
async def cmd_ungive_del(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может забирать права!")
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя, у которого хотите забрать права")
        return
    target_user = message.reply_to_message.from_user
    set_delete_permission(chat_id, target_user.id, False, user_id)
    await message.answer(i18n.get('delete_remove_right', get_group_language(chat_id), name=safe_html(target_user.full_name, False)))

@dp.message(Command("delmods"))
@group_only()
async def cmd_delmods(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_group_language(chat_id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Только создатель группы может просматривать эту информацию!")
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user_id, given_by, given_at FROM delete_permissions WHERE chat_id = ? AND can_delete = 1', (chat_id,))
        users = c.fetchall()
    
    if not users:
        await message.answer("📋 Нет пользователей с правом удаления сообщений")
        return
    
    text = i18n.get('delete_mod_list', get_group_language(chat_id), users="")
    for user_id, given_by, given_at in users:
        try:
            user = await bot.get_chat_member(chat_id, user_id)
            name = user.user.full_name
            given_by_name = "создатель"
            try:
                given_by_user = await bot.get_chat_member(chat_id, given_by)
                given_by_name = given_by_user.user.full_name
            except:
                pass
            date = format_datetime(given_at)
            text += f"• <b>{safe_html(name, False)}</b>\n  Выдал: {safe_html(given_by_name, False)} | {date}\n\n"
        except:
            continue
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("gban"))
@check_bot_admin()
async def cmd_global_ban(message: Message):
    user_id = message.from_user.id
    if not is_global_moderator(user_id, 'can_global_ban') and user_id not in ADMIN_IDS:
        await message.answer("❌ У вас нет права глобального бана!")
        return
    
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer(i18n.get('global_ban_usage', 'ru'))
        return
    
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID пользователя!")
        return
    
    duration = 0
    reason = "Не указана"
    
    if len(args) > 2:
        time_match = re.search(r'(\d+)([мчд]|мин|час|дн)', args[2])
        if time_match:
            value = int(time_match.group(1))
            unit = time_match.group(2)
            if unit in ['м', 'мин']:
                duration = value * 60
            elif unit in ['ч', 'час']:
                duration = value * 3600
            elif unit in ['д', 'дн']:
                duration = value * 86400
            reason = args[2].replace(time_match.group(0), '').strip() or "Не указана"
        else:
            reason = args[2]
    
    try:
        user_info = await bot.get_chat(target_id)
        user_name = user_info.full_name
    except:
        user_name = str(target_id)
    
    add_global_ban(target_id, reason, user_id, duration)
    await apply_global_ban(target_id, reason, duration)
    
    duration_text = format_time(duration) if duration > 0 else "навсегда"
    await message.answer(i18n.get('global_ban_success', 'ru', name=user_name, user_id=target_id, reason=reason, duration=duration_text))

@dp.message(Command("gunban"))
@check_bot_admin()
async def cmd_global_unban(message: Message):
    user_id = message.from_user.id
    if not is_global_moderator(user_id, 'can_global_ban') and user_id not in ADMIN_IDS:
        await message.answer("❌ У вас нет права глобального бана!")
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.answer(i18n.get('global_unban_usage', 'ru'))
        return
    
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID пользователя!")
        return
    
    if remove_global_ban(target_id):
        await message.answer(i18n.get('global_unban_success', 'ru', user_id=target_id))
        logger.info(f"Глобальный разбан пользователя {target_id} администратором {user_id}")
    else:
        await message.answer(f"❌ Пользователь {target_id} не найден в глобальных банах")

@dp.message(Command("gmute"))
@check_bot_admin()
async def cmd_global_mute(message: Message):
    user_id = message.from_user.id
    if not is_global_moderator(user_id, 'can_global_mute') and user_id not in ADMIN_IDS:
        await message.answer("❌ У вас нет права глобального мута!")
        return
    
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer(i18n.get('global_mute_usage', 'ru'))
        return
    
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID пользователя!")
        return
    
    duration = 0
    reason = "Не указана"
    
    if len(args) > 2:
        time_match = re.search(r'(\d+)([мчд]|мин|час|дн)', args[2])
        if time_match:
            value = int(time_match.group(1))
            unit = time_match.group(2)
            if unit in ['м', 'мин']:
                duration = value * 60
            elif unit in ['ч', 'час']:
                duration = value * 3600
            elif unit in ['д', 'дн']:
                duration = value * 86400
            reason = args[2].replace(time_match.group(0), '').strip() or "Не указана"
        else:
            reason = args[2]
    
    try:
        user_info = await bot.get_chat(target_id)
        user_name = user_info.full_name
    except:
        user_name = str(target_id)
    
    add_global_mute(target_id, reason, user_id, duration)
    await apply_global_mute(target_id, reason, duration)
    
    duration_text = format_time(duration) if duration > 0 else "навсегда"
    await message.answer(i18n.get('global_mute_success', 'ru', name=user_name, user_id=target_id, reason=reason, duration=duration_text))

@dp.message(Command("gunmute"))
@check_bot_admin()
async def cmd_global_unmute(message: Message):
    user_id = message.from_user.id
    if not is_global_moderator(user_id, 'can_global_mute') and user_id not in ADMIN_IDS:
        await message.answer("❌ У вас нет права глобального мута!")
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Использование: /gunmute <user_id>")
        return
    
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID пользователя!")
        return
    
    if remove_global_mute(target_id):
        await message.answer(i18n.get('global_unmute_success', 'ru', user_id=target_id))
        logger.info(f"Глобальный размут пользователя {target_id} администратором {user_id}")
    else:
        await message.answer(f"❌ Пользователь {target_id} не найден в глобальных мутах")

@dp.message(Command("blacklist"))
@check_bot_admin()
async def cmd_blacklist(message: Message):
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer(i18n.get('blacklist_usage', 'ru'))
        return
    
    target_id = None
    target_name = "пользователь"
    
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
    else:
        try:
            target_id = int(args[1])
            try:
                user = await bot.get_chat(target_id)
                target_name = user.full_name
            except:
                pass
        except:
            await message.answer("❌ Некорректный ID пользователя!")
            return
    
    reason = args[2] if len(args) > 2 else "Не указана"
    
    if add_to_blacklist(target_id, reason, message.from_user.id):
        await message.answer(i18n.get('add_to_blacklist', 'ru', name=target_name, user_id=target_id, reason=reason))
        logger.info(f"Админ {message.from_user.id} добавил пользователя {target_id} в черный список")
    else:
        await message.answer(i18n.get('already_blacklisted', 'ru'))

@dp.message(Command("unblacklist"))
@check_bot_admin()
async def cmd_unblacklist(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Использование: /unblacklist <user_id>")
        return
    
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID пользователя!")
        return
    
    if remove_from_blacklist(target_id):
        await message.answer(i18n.get('remove_from_blacklist', 'ru', user_id=target_id))
        logger.info(f"Админ {message.from_user.id} удалил пользователя {target_id} из черного списка")
    else:
        await message.answer(i18n.get('not_blacklisted', 'ru'))

@dp.message(F.text.regexp(r'^-смс\s+(\d+)$'))
async def cmd_delete_messages(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    if chat_id < 0:
        if not await is_admin(chat_id, user_id) and not has_delete_permission(chat_id, user_id):
            await message.answer("❌ У вас нет права удалять сообщения!")
            return
    
    match = re.search(r'^-смс\s+(\d+)$', message.text)
    count = int(match.group(1))
    
    if count < 1 or count > 100:
        await message.answer(i18n.get('delete_range_error', get_group_language(chat_id)))
        return
    
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        await state.update_data(delete_count=count, delete_user_id=target_user.id, delete_user_name=target_user.full_name)
    else:
        await state.update_data(delete_count=count, delete_user_id=None)
    
    if count >= 50:
        confirm_text = i18n.get('delete_confirm', get_group_language(chat_id), count=count)
        await message.answer(
            confirm_text,
            reply_markup=get_delete_confirm_keyboard(count, target_user.id if message.reply_to_message else None, get_group_language(chat_id))
        )
        await state.set_state(DeleteMessagesStates.waiting_for_confirm)
    else:
        await perform_delete_messages(message, state, chat_id, count, message.reply_to_message.from_user.id if message.reply_to_message else None)

async def perform_delete_messages(message: Message, state: FSMContext, chat_id: int, count: int, target_user_id: int = None):
    status_msg = await message.answer(i18n.get('delete_progress', get_group_language(chat_id), current=0, total=count))
    
    deleted = 0
    if target_user_id:
        async for msg in bot.get_chat_history(chat_id, limit=count):
            if msg.from_user and msg.from_user.id == target_user_id:
                try:
                    await msg.delete()
                    deleted += 1
                    if deleted % 5 == 0:
                        await status_msg.edit_text(i18n.get('delete_progress', get_group_language(chat_id), current=deleted, total=count))
                    await asyncio.sleep(0.1)
                except:
                    pass
        result_text = i18n.get('delete_user_success', get_group_language(chat_id), count=deleted, name=message.reply_to_message.from_user.full_name if message.reply_to_message else "пользователя")
    else:
        async for msg in bot.get_chat_history(chat_id, limit=count):
            try:
                await msg.delete()
                deleted += 1
                if deleted % 5 == 0:
                    await status_msg.edit_text(i18n.get('delete_progress', get_group_language(chat_id), current=deleted, total=count))
                await asyncio.sleep(0.1)
            except:
                pass
        result_text = i18n.get('delete_success', get_group_language(chat_id), count=deleted)
    
    await status_msg.edit_text(result_text)
    await state.clear()

@dp.callback_query(DeleteMessagesStates.waiting_for_confirm, F.data.startswith("confirm_del"))
async def confirm_delete(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    count = data.get('delete_count', 0)
    target_user_id = data.get('delete_user_id')
    
    await callback.message.delete()
    await perform_delete_messages(callback.message, state, callback.message.chat.id, count, target_user_id)
    await callback.answer()

@dp.callback_query(F.data == "cancel_delete")
async def cancel_delete(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await state.clear()
    await callback.answer("❌ Удаление отменено")

@dp.message(F.new_chat_members)
async def on_bot_added(message: Message):
    bot_info = await bot.get_me()
    if any(member.id == bot_info.id for member in message.new_chat_members):
        logger.info(f"⭐ Бот добавлен в группу {message.chat.id}")
        await add_premium_reaction(message, "🎉")
        await message.answer(i18n.get('bot_added_welcome', get_group_language(message.chat.id)))

@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id, user = update.chat.id, update.new_chat_member.user
        
        is_blacklisted, reason = await check_blacklist(user.id)
        if is_blacklisted:
            try:
                await bot.ban_chat_member(chat_id, user.id)
            except:
                pass
            return
        
        is_gbanned, gban_reason, expires = is_global_banned(user.id)
        if is_gbanned:
            try:
                await bot.ban_chat_member(chat_id, user.id)
            except:
                pass
            return
        
        is_gmuted, gmute_reason, expires = is_global_muted(user.id)
        if is_gmuted:
            try:
                await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False))
            except:
                pass
        
        is_premium = getattr(user, 'is_premium', False)
        db.get_or_create_global_user(user.id, user.username or "", user.full_name or "", is_premium)
        db.add_user_stat(chat_id, user.id, int(time.time()))
        log_text = f"<b>👋 Вход</b>\n\n👤 {safe_html(user.full_name, False)}\n🆔 <code>{user.id}</code>"
        await send_to_log_group(chat_id, 'join', log_text)
        
        is_spammer, spam_reason, warnings = check_spammer(user.id, chat_id)
        if is_spammer and db.get_puls_antispam_enabled(chat_id):
            try:
                await bot.ban_chat_member(chat_id, user.id)
                user_link = f"<a href='tg://user?id={user.id}'>{safe_html(user.full_name, False)}</a>"
                
                spammer_text = customization.format_message(
                    'spammer_detected',
                    user_link=user_link,
                    reason=spam_reason,
                    warnings=warnings,
                    limit=SPAM_WARN_LIMIT,
                    user_id=user.id
                )
                
                await bot.send_message(
                    chat_id,
                    spammer_text,
                    parse_mode="HTML"
                )
                
                spammer_pm_text = customization.format_message(
                    'spammer_pm',
                    chat_title=update.chat.title,
                    warnings=warnings,
                    limit=SPAM_WARN_LIMIT,
                    support_link=SUPPORT_LINK
                )
                
                try:
                    await bot.send_message(
                        user.id,
                        spammer_pm_text,
                        parse_mode="HTML"
                    )
                except:
                    pass
                return
            except:
                pass
        
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
        not_bot, rules = db.get_user_confirmation_status(chat_id, user.id)
        if (conf_type == 'both' and not_bot and rules) or (conf_type == 'not_bot' and not_bot) or (conf_type == 'rules' and rules):
            await send_simple_welcome(chat_id, user)
            return
        try:
            await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False))
        except:
            pass
        rules_html = db.get_rules_html(chat_id)
        rules_enabled = db.get_rules_enabled(chat_id)
        builder = InlineKeyboardBuilder()
        msg_text = ""
        if conf_type == 'both':
            msg_text = f"👋 <b>{safe_html(user.full_name, False)}</b>, выполните два шага:\n1️⃣ Подтвердите, что вы не бот\n2️⃣ Прочитайте правила"
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {safe_html(update.chat.title, False)}!\n\nШаг 1: Подтвердите, что вы не бот",
                    reply_markup=get_confirm_not_bot_keyboard(chat_id, user.id, 0)
                )
                if rules_html and rules_enabled:
                    await bot.send_message(
                        user.id,
                        f"Шаг 2: Прочитайте правила:\n\n{safe_html(rules_html, True)}",
                        reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0),
                        parse_mode="HTML"
                    )
            except:
                await bot.send_message(chat_id, "⚠️ Не удалось отправить подтверждение в ЛС")
            builder.add(create_button("💬 Перейти в ЛС", f"go_to_pm_{chat_id}_{user.id}", "primary"))
        elif conf_type == 'not_bot':
            msg_text = f"👋 <b>{safe_html(user.full_name, False)}</b>, подтвердите, что вы не бот"
            builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user.id}_0", "success"))
        elif conf_type == 'rules' and rules_html and rules_enabled:
            msg_text = f"👋 <b>{safe_html(user.full_name, False)}</b>, прочитайте правила"
            builder.add(create_button("💬 Перейти в ЛС", f"go_to_pm_{chat_id}_{user.id}", "primary"))
            try:
                await bot.send_message(
                    user.id,
                    f"Добро пожаловать в {safe_html(update.chat.title, False)}!\n\nПрочитайте правила:\n\n{safe_html(rules_html, True)}",
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
    await bot.send_message(update.chat.id, f"👋 {safe_html(update.from_user.full_name, False)} вышел из чата")
    log_text = f"<b>👋 Выход</b>\n\n👤 {safe_html(update.from_user.full_name, False)}\n🆔 <code>{update.from_user.id}</code>"
    await send_to_log_group(update.chat.id, 'leave', log_text)

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
        name=safe_html(user.full_name, False),
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
            caption=welcome_text + (f"\n\n{safe_html(welcome_text_custom, False)}" if welcome_text_custom else ""),
            reply_markup=get_welcome_buttons(chat_id),
            parse_mode="HTML"
        )
    else:
        await bot.send_message(
            chat_id,
            welcome_text + (f"\n\n{safe_html(welcome_text_custom, False)}" if welcome_text_custom else ""),
            reply_markup=get_welcome_buttons(chat_id),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("confirm_not_bot_"))
@edit_only()
@check_public()
async def process_confirm_not_bot(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id, user_id = int(parts[3]), int(parts[4])
    msg_id = int(parts[5]) if len(parts) > 5 else 0
    await log_button_click(callback.from_user.id, callback.from_user.full_name, chat_id, callback.data, callback.message.message_id, "confirmed")
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
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"✅ {safe_html(callback.from_user.full_name, False)} подтвердил, что не бот"
            )
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
    chat_id, user_id, msg_id = int(parts[2]), int(parts[3]), int(parts[4])
    await log_button_click(callback.from_user.id, callback.from_user.full_name, chat_id, callback.data, callback.message.message_id, "agreed")
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
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"✅ {safe_html(callback.from_user.full_name, False)} согласился с правилами"
            )
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
    parts = callback.data.split('_')
    if len(parts) > 3:
        chat_id, user_id = int(parts[3]), int(parts[4])
    else:
        user_id = callback.from_user.id
        chat_id = 0
    await log_button_click(callback.from_user.id, callback.from_user.full_name, chat_id, callback.data, callback.message.message_id, "clicked")
    if callback.from_user.id != user_id:
        await callback.answer("⚠️ Это не для вас!", show_alert=True)
        return
    await callback.message.answer(
        "💬 Откройте личные сообщения с ботом и завершите подтверждение.",
        reply_markup=get_pm_link_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
@edit_only()
@check_owner()
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    user_lang = get_user_language(callback.from_user.id)
    await callback.message.edit_text(
        i18n.get('main_menu_title', user_lang),
        reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin, lang=user_lang)
    )
    await callback.answer()

@dp.callback_query(F.data == "group_manage_main")
@edit_only()
@check_owner()
async def group_manage_main(callback: CallbackQuery, state: FSMContext):
    user_lang = get_user_language(callback.from_user.id)
    groups = db.get_user_groups(callback.from_user.id)
    if not groups:
        await callback.answer(i18n.get('no_groups', user_lang), show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button(i18n.get('back_button', user_lang), "back_to_main", "secondary"))
    builder.adjust(1)
    await callback.message.edit_text(i18n.get('select_group', user_lang), reply_markup=builder.as_markup())
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
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(
        i18n.get('group_settings_title', group_lang, title=safe_html(chat_title, False)),
        reply_markup=get_group_manage_keyboard(group_lang)
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_groups")
@edit_only()
@check_owner()
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_lang = get_user_language(callback.from_user.id)
    groups = db.get_user_groups(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}", "primary"))
    builder.add(create_button(i18n.get('back_button', user_lang), "back_to_main", "secondary"))
    builder.adjust(1)
    await callback.message.edit_text(i18n.get('select_group', user_lang), reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "language_settings")
@edit_only()
@check_owner()
async def language_settings(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    current_lang = get_group_language(chat_id)
    lang_names = {
        "ru": "Русский", "en": "English", "uk": "Українська",
        "de": "Deutsch", "fr": "Français", "es": "Español",
        "it": "Italiano", "pt": "Português", "tr": "Türkçe",
        "zh": "中文", "ja": "日本語", "ko": "한국어",
        "ar": "العربية", "hi": "हिन्दी"
    }
    lang_name = lang_names.get(current_lang, current_lang)
    await callback.message.edit_text(
        i18n.get('language_settings_title', get_group_language(chat_id), language=lang_name),
        reply_markup=get_language_settings_keyboard(current_lang, get_group_language(chat_id))
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("set_lang_"))
@edit_only()
@check_owner()
async def set_language(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    new_lang = callback.data.split('_')[-1]
    available_langs = ["ru", "en", "uk", "de", "fr", "es", "it", "pt", "tr", "zh", "ja", "ko", "ar", "hi"]
    
    if new_lang not in available_langs:
        await callback.answer("❌ Неизвестный язык!", show_alert=True)
        return
    
    if new_lang not in ["ru", "en"]:
        await callback.answer(i18n.get('language_in_development', get_group_language(chat_id), language=i18n.get(f"language_{new_lang}", get_group_language(chat_id))), show_alert=True)
        return
    
    set_group_language(chat_id, new_lang)
    await callback.answer(i18n.get('language_changed', new_lang, language=i18n.get(f"language_{new_lang}", new_lang)), show_alert=True)
    await language_settings(callback, state)

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
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(
        i18n.get('group_settings_title', group_lang, title=safe_html(chat_title, False)),
        reply_markup=get_group_manage_keyboard(group_lang)
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
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(
        "📋 <b>Группа логов</b>\n\nСюда будут отправляться логи нарушений, действий модераторов и других событий.",
        reply_markup=get_log_group_manage_keyboard(has_log_group, log_group_info, group_lang)
    )
    await callback.answer()

@dp.callback_query(F.data == "link_log_group")
@edit_only()
@check_owner()
async def link_log_group(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    log_groups = db.get_user_log_groups(user_id)
    group_lang = get_group_language(callback.message.chat.id)
    if not log_groups:
        await callback.message.edit_text(
            "❌ У вас ещё нет созданных групп логов!\n\nСначала создайте группу логов, переслав сообщение из группы в ЛС боту.",
            reply_markup=get_back_keyboard("log_group_manage")
        )
        await callback.answer()
        return
    builder = InlineKeyboardBuilder()
    for log_id, title in log_groups:
        builder.add(create_button(title or f"Группа {log_id}", f"select_log_group_{log_id}", "primary"))
    builder.add(create_button(i18n.get('back_button', group_lang), "log_group_manage", "secondary"))
    builder.adjust(1)
    await callback.message.edit_text(
        "📋 <b>Выберите группу логов</b>\n\nВ эту группу будут отправляться события из текущего чата:",
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
        'send_messages': log_group_info['send_messages'],
        'send_button_clicks': log_group_info.get('send_button_clicks', 0)
    }
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(
        f"📋 <b>Настройки отправки в лог-группу</b>\n\n"
        f"Группа: {safe_html(log_group_info['group_title'], False)}\n"
        f"ID: <code>{log_group_info['log_group_id']}</code>\n\n"
        f"Выберите, какие события отправлять:",
        reply_markup=get_log_settings_keyboard(settings, group_lang),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_log_buttons")
@edit_only()
@check_owner()
async def toggle_log_buttons(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info.get('send_button_clicks', 0) else 1
        db.update_log_group_settings(
            chat_id, log_group_info['log_group_id'],
            send_button_clicks=new_value
        )
        await callback.answer("✅ Настройки обновлены!", show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "admin_panel")
@edit_only()
@check_bot_admin()
async def admin_panel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await state.clear()
    status = "🟢 РАБОТАЕТ" if not technical_maintenance else "🔴 ТЕХРАБОТЫ"
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM bot_blacklist')
        blacklist_count = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM global_bans')
        global_bans = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM global_mutes')
        global_mutes = c.fetchone()[0] or 0
    
    text = i18n.get('admin_panel_title', 'ru', 
                    status=status, main_lang=i18n.main_language,
                    blacklist_count=blacklist_count, global_bans=global_bans, global_mutes=global_mutes)
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📊 Статистика", "admin_stats", "primary"))
    builder.add(create_button("📱 Группы", "admin_groups", "primary"))
    builder.add(create_button("👥 Пользователи", "admin_users", "primary"))
    builder.add(create_button("📋 Логи", "admin_logs", "primary"))
    builder.add(create_button("🛠 Техработы", "admin_maintenance", "danger" if technical_maintenance else "secondary"))
    builder.add(create_button(i18n.get('blacklist_manage', 'ru'), "admin_blacklist", "danger"))
    builder.add(create_button(i18n.get('global_bans_manage', 'ru'), "admin_global_bans", "danger"))
    builder.add(create_button(i18n.get('global_mutes_manage', 'ru'), "admin_global_mutes", "danger"))
    builder.add(create_button(i18n.get('change_main_lang', 'ru'), "admin_main_lang", "primary"))
    builder.add(create_button("📢 Рассылка", "admin_broadcast", "success"))
    builder.add(create_button("📦 Бэкап", "admin_backup", "secondary"))
    builder.add(create_button("🎨 Кастомизация", "admin_custom", "primary"))
    builder.add(create_button("❌ Выключить", "admin_shutdown", "danger"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "back_to_main", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(
        safe_html(text, False), 
        reply_markup=builder.as_markup(), 
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_main_lang")
@edit_only()
@check_bot_admin()
async def admin_main_lang(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    languages = ["ru", "en"]
    for lang in languages:
        name = i18n.get(f"language_{lang}", 'ru')
        if lang == i18n.main_language:
            name = f"✅ {name}"
        builder.add(create_button(name, f"set_main_lang_{lang}", "primary"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel", "secondary"))
    builder.adjust(1)
    
    await callback.message.edit_text(
        i18n.get('main_lang_select', 'ru', current=i18n.main_language),
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("set_main_lang_"))
@edit_only()
@check_bot_admin()
async def set_main_lang(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    new_lang = callback.data.split('_')[-1]
    if i18n.set_main_language(new_lang):
        await callback.answer(i18n.get('main_lang_changed', new_lang, language=new_lang), show_alert=True)
    await admin_main_lang(callback)

@dp.callback_query(F.data == "admin_blacklist")
@edit_only()
@check_bot_admin()
async def admin_blacklist(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user_id, reason, added_by, added_at FROM bot_blacklist ORDER BY added_at DESC LIMIT 20')
        blacklist = c.fetchall()
    
    if not blacklist:
        await callback.message.edit_text(
            "✅ Черный список пуст.",
            reply_markup=get_back_keyboard("admin_panel")
        )
        await callback.answer()
        return
    
    text = "🚫 <b>Черный список бота:</b>\n\n"
    for user_id, reason, added_by, added_at in blacklist:
        text += f"• <code>{user_id}</code>\n"
        text += f"  Причина: {safe_html(reason, False)}\n"
        text += f"  Добавил: {added_by}\n"
        text += f"  Дата: {format_datetime(added_at)}\n\n"
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "blacklist_add", "success"))
    builder.add(create_button("🗑 Удалить", "blacklist_remove", "danger"))
    builder.add(create_button("🔄 Обновить", "admin_blacklist", "secondary"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "blacklist_add")
@edit_only()
@check_bot_admin()
async def blacklist_add(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте ID пользователя и причину через пробел:\n\nПример: 123456789 Спам в группах"
    )
    await state.set_state(MaintenanceStates.waiting_for_message)
    await callback.answer()

@dp.callback_query(F.data == "blacklist_remove")
@edit_only()
@check_bot_admin()
async def blacklist_remove(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 Отправьте ID пользователя для удаления из черного списка:\n\nПример: 123456789"
    )
    await state.set_state(MaintenanceStates.waiting_for_message)
    await callback.answer()

@dp.callback_query(F.data == "admin_global_bans")
@edit_only()
@check_bot_admin()
async def admin_global_bans(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user_id, reason, moderator_id, banned_at, expires_at FROM global_bans ORDER BY banned_at DESC LIMIT 20')
        bans = c.fetchall()
    
    if not bans:
        await callback.message.edit_text(
            "✅ Глобальные баны отсутствуют.",
            reply_markup=get_back_keyboard("admin_panel")
        )
        await callback.answer()
        return
    
    text = "⛔ <b>Глобальные баны:</b>\n\n"
    for user_id, reason, moderator_id, banned_at, expires_at in bans:
        expires_text = format_time(expires_at - int(time.time())) if expires_at > 0 else "навсегда"
        text += f"• <code>{user_id}</code>\n"
        text += f"  Причина: {safe_html(reason, False)}\n"
        text += f"  Модератор: {moderator_id}\n"
        text += f"  До: {expires_text}\n"
        text += f"  Дата: {format_datetime(banned_at)}\n\n"
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_ban_add", "success"))
    builder.add(create_button("🗑 Снять", "global_ban_remove", "danger"))
    builder.add(create_button("🔄 Обновить", "admin_global_bans", "secondary"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_global_mutes")
@edit_only()
@check_bot_admin()
async def admin_global_mutes(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user_id, reason, moderator_id, muted_at, expires_at FROM global_mutes ORDER BY muted_at DESC LIMIT 20')
        mutes = c.fetchall()
    
    if not mutes:
        await callback.message.edit_text(
            "✅ Глобальные муты отсутствуют.",
            reply_markup=get_back_keyboard("admin_panel")
        )
        await callback.answer()
        return
    
    text = "🔇 <b>Глобальные муты:</b>\n\n"
    for user_id, reason, moderator_id, muted_at, expires_at in mutes:
        expires_text = format_time(expires_at - int(time.time())) if expires_at > 0 else "навсегда"
        text += f"• <code>{user_id}</code>\n"
        text += f"  Причина: {safe_html(reason, False)}\n"
        text += f"  Модератор: {moderator_id}\n"
        text += f"  До: {expires_text}\n"
        text += f"  Дата: {format_datetime(muted_at)}\n\n"
    
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_mute_add", "success"))
    builder.add(create_button("🗑 Снять", "global_mute_remove", "danger"))
    builder.add(create_button("🔄 Обновить", "admin_global_mutes", "secondary"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel", "secondary"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        return
    
    is_gbanned, gban_reason, expires = is_global_banned(user_id)
    if is_gbanned:
        await message.delete()
        return
    
    text = message.text or message.caption or ""
    
    if text and len(text) < 500:
        log_text = (
            f"<b>💬 Сообщение</b>\n\n"
            f"👤 {safe_html(message.from_user.full_name, False)}\n"
            f"📝 {safe_html(text[:200], False)}{'...' if len(text) > 200 else ''}"
        )
        await send_to_log_group(chat_id, 'message', log_text)
    
    if text:
        cleaned_text = text.lower().strip()
        
        responses = db.get_auto_responses(chat_id)
        
        for trigger, response, response_type, media_id in responses:
            trigger_lower = trigger.lower().strip()
            
            if trigger_lower == cleaned_text or trigger_lower in cleaned_text:
                try:
                    if response_type == 'text':
                        await message.reply(safe_html(response, False), parse_mode="HTML", disable_notification=True)
                    elif response_type == 'photo' and media_id:
                        await message.reply_photo(media_id, caption=safe_html(response, False), parse_mode="HTML")
                    elif response_type == 'animation' and media_id:
                        await message.reply_animation(media_id, caption=safe_html(response, False), parse_mode="HTML")
                    elif response_type == 'sticker' and media_id:
                        await message.reply_sticker(media_id)
                    break
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки: {e}")

async def main():
    dp.message.middleware(AntiFloodMiddleware())
    dp.message.middleware(MaintenanceMiddleware())
    dp.callback_query.middleware(MaintenanceMiddleware())
    
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    asyncio.create_task(clean_old_messages())
    asyncio.create_task(clean_old_logs())
    asyncio.create_task(clean_expired_bans_mutes())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
