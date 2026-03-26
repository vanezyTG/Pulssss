import asyncio
import logging
import time
import random
import string
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict, Any
import sqlite3
from contextlib import contextmanager
from functools import wraps
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

MAX_BUTTON_PRESSES = 3
BUTTON_CHECK_TIME = 30

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
        self.main_language = "ru"
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
                "global_ban_usage": "❌ Использование: /gban <user_id> [время] [причина]\nПример: /gban 123456789 24ч спам",
                "global_ban_success": "✅ <b>Глобальный бан</b>\n\nПользователь: {name} ({user_id})\nПричина: {reason}\nДлительность: {duration}",
                "global_unban_success": "✅ Пользователь {user_id} разбанен глобально!",
                "global_mute_usage": "❌ Использование: /gmute <user_id> [время] [причина]",
                "global_mute_success": "✅ <b>Глобальный мут</b>\n\nПользователь: {name} ({user_id})\nПричина: {reason}\nДлительность: {duration}",
                "global_unmute_success": "✅ Пользователь {user_id} размучен глобально!",
                "delete_usage": "❌ Использование: -смс <количество>\nИли ответьте на сообщение",
                "delete_range_error": "❌ Количество должно быть от 1 до 100!",
                "delete_confirm": "⚠️ <b>Подтвердите удаление {count} сообщений</b>",
                "delete_progress": "🗑 Удаляю сообщения... ({current}/{total})",
                "delete_success": "✅ Удалено {count} сообщений!",
                "delete_user_success": "✅ Удалено {count} сообщений от пользователя {name}!",
                "delete_give_right": "✅ Пользователю {name} выдано право удалять сообщения!",
                "delete_remove_right": "✅ У пользователя {name} забрано право удалять сообщения!",
                "delete_mod_list": "🗑 <b>Пользователи с правом удаления:</b>\n\n{users}",
                "button_click_group_notify": "👤 {user} использовал функцию",
                "admin_panel_title": "👑 <b>Панель администратора</b>\n\nСтатус: {status}\nОсновной язык: {main_lang}",
                "change_main_lang": "🌐 Сменить основной язык",
                "blacklist_manage": "🚫 Черный список",
                "global_bans_manage": "⛔ Глобальные баны",
                "global_mutes_manage": "🔇 Глобальные муты",
                "main_lang_changed": "✅ Основной язык изменен на {language}!",
                "main_lang_select": "🌐 <b>Выберите основной язык</b>\n\nТекущий: {current}",
                "mute_message": "🔇 <b>Пользователь {name} замьючен</b>\n\n👮 Модератор: {moderator}\n⏱ Длительность: {duration}\n📝 Причина: {reason}",
                "unmute_message": "🔊 <b>Пользователь {name} размучен</b>\n\n👮 Модератор: {moderator}",
                "ban_message": "⛔️ <b>Пользователь {name} забанен</b>\n\n👮 Модератор: {moderator}\n⏱ Длительность: {duration}\n📝 Причина: {reason}",
                "kick_message": "👢 <b>Пользователь {name} кикнут</b>\n\n👮 Модератор: {moderator}\n📝 Причина: {reason}",
                "warn_message": "⚠️ <b>Предупреждение пользователю {name}</b>\n\n👮 Модератор: {moderator}\n📊 Всего: {warn_count}\n📝 Причина: {reason}",
                "reply_to_user": "Ответьте на сообщение пользователя!",
                "cant_mute_bot": "Нельзя мутить бота!",
                "cant_ban_bot": "Нельзя банить бота!",
                "cant_kick_bot": "Нельзя кикать бота!",
                "default_reason": "Не указана",
                "forever": "навсегда",
                "no_permission": "❌ У вас нет прав!",
                "group_linked_success": "✅ Группа привязана!",
                "group_already_linked": "❌ Группа уже привязана!",
                "not_creator": "❌ Только создатель может привязать!",
                "select_group": "📱 <b>Ваши группы</b>\n\nВыберите группу:",
                "no_groups": "❌ У вас нет привязанных групп",
                "rules_not_set": "❌ Правила не установлены",
                "stats_updating": "📊 Статистика обновляется...",
                "no_messages": "📊 Нет сообщений",
                "trigger_added": "✅ Триггер '{trigger}' добавлен!",
                "trigger_exists": "❌ Триггер '{trigger}' уже существует",
                "trigger_limit": "❌ Лимит триггеров ({max})",
                "trigger_empty": "❌ Триггер не может быть пустым",
                "trigger_too_long": "❌ Триггер слишком длинный! Макс {max_len} символов",
                "trigger_removed": "✅ Триггер удален!",
                "interval_set": "✅ Интервал установлен: {interval}",
                "welcome_text_set": "✅ Текст приветствия сохранен!",
                "welcome_photo_set": "✅ Фото приветствия сохранено!",
                "welcome_toggled": "✅ Приветствие {'включено' if enabled else 'выключено'}!",
                "rules_toggled": "✅ Правила {'включены' if enabled else 'выключены'}!",
                "antiflood_toggled": "✅ Антифлуд {'включен' if enabled else 'выключен'}!",
                "puls_antispam_toggled": "✅ Антиспам Пульса {'включен' if enabled else 'выключен'}!",
                "confirmation_toggled": "✅ Подтверждение {'включено' if enabled else 'выключено'}!",
                "links_toggled": "✅ Фильтр ссылок {'включен' if enabled else 'выключен'}!",
                "settings_saved": "✅ Настройки сохранены!",
                "invalid_number": "❌ Введите число от 1 до 100!",
                "enter_trigger": "📝 Введите ключевое слово (триггер):",
                "enter_response": "📝 Введите ответ для триггера:",
                "enter_interval": "⏱ Введите интервал в минутах (5-525600):",
                "enter_welcome_text": "📝 Отправьте текст приветствия:",
                "enter_welcome_photo": "🖼 Отправьте фото для приветствия:",
                "enter_msg_limit": "📊 Введите лимит сообщений (3-50):",
                "enter_media_limit": "🎬 Введите лимит медиа (2-20):",
                "enter_window": "⏱ Введите период (5-300 сек):",
                "enter_warn_count": "⚠️ Введите кол-во предупреждений (1-10):",
                "enter_duration": "⏱ Введите длительность в минутах (0=навсегда):",
                "enter_max_mentions": "📊 Введите макс упоминаний (1-50):",
                "enter_mention_window": "⏱ Введите период (10-3600 сек):",
                "select_punish": "🔇 Выберите наказание:",
                "group_unlinked": "✅ Группа отвязана!",
                "log_group_attached": "✅ Группа логов привязана!",
                "log_group_detached": "✅ Группа логов отвязана!",
                "log_settings_updated": "✅ Настройки логов обновлены!",
                "rights_granted": "✅ Права выданы!",
                "rights_revoked": "✅ Права забраны!",
                "moderator_list": "🛡️ <b>Модераторы:</b>\n\n{list}",
                "broadcast_confirm": "📢 Готово к рассылке!\nПолучателей: {count}\nПодтвердите:",
                "broadcast_start": "✅ Начать рассылку",
                "broadcast_cancel": "❌ Отмена",
                "broadcast_progress": "📤 Прогресс: {current}/{total}\n✅ {sent} | ❌ {failed}",
                "broadcast_done": "✅ Рассылка завершена!\n✅ {sent} | ❌ {failed}",
                "broadcast_cancelled": "❌ Рассылка отменена",
                "backup_created": "✅ Бэкап создан!",
                "custom_reset_all": "✅ Все настройки сброшены!",
                "custom_text_updated": "✅ Текст обновлен!",
                "custom_photo_updated": "✅ Фото обновлено!",
                "select_message": "📝 Выберите сообщение:"
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
                "welcome_pm": "👋 <b>Welcome to Puls Chat Manager!</b>\n\nI'll help you manage chats.\n\nSelect a section below 👇",
                "welcome_group": "👋 <b>Puls Chat Manager</b>\n\n• /rules - Rules\n• /stats - My stats\n• /top - Top active\n• /profile - Profile\n• /group - Group management\n• /puls - Ping\n• /mute [time] [reason] - mute\n• /unmute - unmute\n• /ban [time] [reason] - ban\n• /unban - unban\n• /kick [reason] - kick\n• /warn [reason] - warn\n• /mods - moderators list",
                "bot_added_welcome": "👋 Thanks for adding me!\n\nI'm Puls Chat Manager.\n\n📌 <b>For full functionality:</b>\n1️⃣ Make me an admin\n2️⃣ Type /group to link\n3️⃣ Configure settings\n\nSettings in PM: @PulsOfficialManager_bot",
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
                "language_settings": "🌐 Language",
                "unlink_group": "❌ Unlink",
                "back_button": "◀️ Back",
                "language_settings_title": "🌐 <b>Language settings</b>\n\nCurrent: {language}",
                "language_changed": "✅ Language changed to {language}!",
                "language_in_development": "🌐 {language} is under development!",
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
                "blacklisted": "🚫 <b>You are blacklisted</b>\n\nReason: {reason}\n\nContact: {support_link}",
                "add_to_blacklist": "✅ User {name} ({user_id}) blacklisted!\nReason: {reason}",
                "remove_from_blacklist": "✅ User {user_id} removed from blacklist!",
                "already_blacklisted": "❌ User already blacklisted!",
                "not_blacklisted": "❌ User not blacklisted!",
                "blacklist_usage": "❌ Usage: /blacklist <user_id> [reason]",
                "global_ban_usage": "❌ Usage: /gban <user_id> [time] [reason]",
                "global_ban_success": "✅ <b>Global ban</b>\n\nUser: {name} ({user_id})\nReason: {reason}\nDuration: {duration}",
                "global_unban_success": "✅ User {user_id} unbanned globally!",
                "global_mute_usage": "❌ Usage: /gmute <user_id> [time] [reason]",
                "global_mute_success": "✅ <b>Global mute</b>\n\nUser: {name} ({user_id})\nReason: {reason}\nDuration: {duration}",
                "global_unmute_success": "✅ User {user_id} unmuted globally!",
                "delete_usage": "❌ Usage: -del <amount>\nOr reply to a message",
                "delete_range_error": "❌ Amount must be 1-100!",
                "delete_confirm": "⚠️ <b>Confirm delete {count} messages</b>",
                "delete_progress": "🗑 Deleting... ({current}/{total})",
                "delete_success": "✅ Deleted {count} messages!",
                "delete_user_success": "✅ Deleted {count} messages from {name}!",
                "delete_give_right": "✅ User {name} granted delete rights!",
                "delete_remove_right": "✅ User {name} revoked delete rights!",
                "delete_mod_list": "🗑 <b>Users with delete rights:</b>\n\n{users}",
                "button_click_group_notify": "👤 {user} used a function",
                "admin_panel_title": "👑 <b>Admin Panel</b>\n\nStatus: {status}\nMain language: {main_lang}",
                "change_main_lang": "🌐 Change main language",
                "blacklist_manage": "🚫 Blacklist",
                "global_bans_manage": "⛔ Global bans",
                "global_mutes_manage": "🔇 Global mutes",
                "main_lang_changed": "✅ Main language changed to {language}!",
                "main_lang_select": "🌐 <b>Select main language</b>\n\nCurrent: {current}",
                "mute_message": "🔇 <b>User {name} muted</b>\n\n👮 Moderator: {moderator}\n⏱ Duration: {duration}\n📝 Reason: {reason}",
                "unmute_message": "🔊 <b>User {name} unmuted</b>\n\n👮 Moderator: {moderator}",
                "ban_message": "⛔️ <b>User {name} banned</b>\n\n👮 Moderator: {moderator}\n⏱ Duration: {duration}\n📝 Reason: {reason}",
                "kick_message": "👢 <b>User {name} kicked</b>\n\n👮 Moderator: {moderator}\n📝 Reason: {reason}",
                "warn_message": "⚠️ <b>Warning to {name}</b>\n\n👮 Moderator: {moderator}\n📊 Total: {warn_count}\n📝 Reason: {reason}",
                "reply_to_user": "Reply to user's message!",
                "cant_mute_bot": "Can't mute a bot!",
                "cant_ban_bot": "Can't ban a bot!",
                "cant_kick_bot": "Can't kick a bot!",
                "default_reason": "Not specified",
                "forever": "forever",
                "no_permission": "❌ No permission!",
                "group_linked_success": "✅ Group linked!",
                "group_already_linked": "❌ Group already linked!",
                "not_creator": "❌ Only creator can link!",
                "select_group": "📱 <b>Your groups</b>\n\nSelect a group:",
                "no_groups": "❌ No linked groups",
                "rules_not_set": "❌ Rules not set",
                "stats_updating": "📊 Updating...",
                "no_messages": "📊 No messages",
                "trigger_added": "✅ Trigger '{trigger}' added!",
                "trigger_exists": "❌ Trigger '{trigger}' exists!",
                "trigger_limit": "❌ Trigger limit ({max})",
                "trigger_empty": "❌ Trigger cannot be empty!",
                "trigger_too_long": "❌ Trigger too long! Max {max_len} chars",
                "trigger_removed": "✅ Trigger removed!",
                "interval_set": "✅ Interval set: {interval}",
                "welcome_text_set": "✅ Welcome text saved!",
                "welcome_photo_set": "✅ Welcome photo saved!",
                "welcome_toggled": "✅ Welcome {'enabled' if enabled else 'disabled'}!",
                "rules_toggled": "✅ Rules {'enabled' if enabled else 'disabled'}!",
                "antiflood_toggled": "✅ Anti-flood {'enabled' if enabled else 'disabled'}!",
                "puls_antispam_toggled": "✅ Puls Antispam {'enabled' if enabled else 'disabled'}!",
                "confirmation_toggled": "✅ Confirmation {'enabled' if enabled else 'disabled'}!",
                "links_toggled": "✅ Link filter {'enabled' if enabled else 'disabled'}!",
                "settings_saved": "✅ Settings saved!",
                "invalid_number": "❌ Enter a number 1-100!",
                "enter_trigger": "📝 Enter trigger keyword:",
                "enter_response": "📝 Enter response for trigger:",
                "enter_interval": "⏱ Enter interval in minutes (5-525600):",
                "enter_welcome_text": "📝 Send welcome text:",
                "enter_welcome_photo": "🖼 Send welcome photo:",
                "enter_msg_limit": "📊 Enter message limit (3-50):",
                "enter_media_limit": "🎬 Enter media limit (2-20):",
                "enter_window": "⏱ Enter time window (5-300 sec):",
                "enter_warn_count": "⚠️ Enter warn count (1-10):",
                "enter_duration": "⏱ Enter duration in minutes (0=forever):",
                "enter_max_mentions": "📊 Enter max mentions (1-50):",
                "enter_mention_window": "⏱ Enter time window (10-3600 sec):",
                "select_punish": "🔇 Select punishment:",
                "group_unlinked": "✅ Group unlinked!",
                "log_group_attached": "✅ Log group attached!",
                "log_group_detached": "✅ Log group detached!",
                "log_settings_updated": "✅ Log settings updated!",
                "rights_granted": "✅ Rights granted!",
                "rights_revoked": "✅ Rights revoked!",
                "moderator_list": "🛡️ <b>Moderators:</b>\n\n{list}",
                "broadcast_confirm": "📢 Ready to broadcast!\nRecipients: {count}\nConfirm:",
                "broadcast_start": "✅ Start",
                "broadcast_cancel": "❌ Cancel",
                "broadcast_progress": "📤 Progress: {current}/{total}\n✅ {sent} | ❌ {failed}",
                "broadcast_done": "✅ Broadcast done!\n✅ {sent} | ❌ {failed}",
                "broadcast_cancelled": "❌ Broadcast cancelled",
                "backup_created": "✅ Backup created!",
                "custom_reset_all": "✅ All settings reset!",
                "custom_text_updated": "✅ Text updated!",
                "custom_photo_updated": "✅ Photo updated!",
                "select_message": "📝 Select message:"
            }
        }
    
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
        self.templates['welcome_pm'] = MessageTemplate('welcome_pm', "👋 <b>Добро пожаловать в Puls Chat Manager!</b>\n\nЯ помогу вам управлять чатами, следить за порядком и автоматизировать модерацию.\n\nВыберите раздел в меню ниже 👇")
        self.templates['welcome_group'] = MessageTemplate('welcome_group', "👋 <b>Puls Chat Manager</b>\n\n• /rules - Правила\n• /stats - Моя статистика\n• /top - Топ активных\n• /profile - Профиль пользователя\n• /group - Управление группой\n• /puls - Проверка пинга\n• /mute [время] [причина] - замутить\n• /unmute - размутить\n• /ban [время] [причина] - забанить\n• /unban - разбанить\n• /kick [причина] - кикнуть\n• /warn [причина] - предупредить\n• /mods - список модераторов")
        self.templates['profile_header'] = MessageTemplate('profile_header', "<b>Профиль {premium_emoji} {name}</b>")
        self.templates['profile_id'] = MessageTemplate('profile_id', "🆔 <b>ID:</b> <code>{global_id}</code>")
        self.templates['profile_first_seen'] = MessageTemplate('profile_first_seen', "📅 <b>Впервые замечен:</b> {first_seen}")
        self.templates['profile_premium'] = MessageTemplate('profile_premium', "⭐ <b>Премиум пользователь</b>")
        self.templates['profile_antispam'] = MessageTemplate('profile_antispam', "🛡️ <b>Антиспам база Puls:</b> {warnings}/{limit} предупреждений")
        self.templates['profile_stats_header'] = MessageTemplate('profile_stats_header', "📊 <b>Статистика в этом чате:</b>")
        self.templates['profile_day'] = MessageTemplate('profile_day', "• За день: {count} 💬")
        self.templates['profile_week'] = MessageTemplate('profile_week', "• За неделю: {count} 💬")
        self.templates['profile_month'] = MessageTemplate('profile_month', "• За месяц: {count} 💬")
        self.templates['profile_total'] = MessageTemplate('profile_total', "• Всего: {count} 💬")
        self.templates['profile_position'] = MessageTemplate('profile_position', "• Место в топе: {position}")
        self.templates['profile_no_stats'] = MessageTemplate('profile_no_stats', "📊 У пользователя пока нет сообщений в этом чате")
        self.templates['top_header'] = MessageTemplate('top_header', "<b>🏆 Топ активных (всего сообщений):</b>")
        self.templates['top_entry'] = MessageTemplate('top_entry', "{medal} {premium_emoji} {name} — {count} 💬{warnings}")
        self.templates['welcome_simple'] = MessageTemplate('welcome_simple', "Добро пожаловать, {premium_emoji} <b>{name}</b>!\n\n🆔 <b>ID:</b> <code>{global_id}</code>\n📅 <b>Впервые замечен:</b> {first_seen}\n{premium_line}🛡️ <b>Антиспам база Puls:</b> {warnings}/{limit} предупреждений\n\n• Username: @{username}\n• Telegram ID: <code>{user_id}</code>\n• Вошёл: {join_dt}\n• Место в топе: {position}")
        self.templates['group_linked'] = MessageTemplate('group_linked', "✅ <b>Группа успешно привязана!</b>\n\nНазвание: {title}\nID: <code>{chat_id}</code>\n\nТеперь вы можете настроить её в личных сообщениях с ботом.")
        self.templates['group_linked_pm'] = MessageTemplate('group_linked_pm', "✅ Группа <b>{title}</b> успешно привязана!\n\nТеперь она доступна в разделе «Настройки групп».")
        self.templates['trigger_added'] = MessageTemplate('trigger_added', "✅ Триггер '{trigger}' добавлен ({count}/{max})")
        self.templates['trigger_exists'] = MessageTemplate('trigger_exists', "❌ Триггер '{trigger}' уже существует")
        self.templates['trigger_limit'] = MessageTemplate('trigger_limit', "❌ Достигнут лимит триггеров ({max})")
        self.templates['trigger_removed'] = MessageTemplate('trigger_removed', "✅ Триггер '{trigger}' удалён!")
    
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

def safe_html(text: str, preserve_quotes: bool = True) -> str:
    if not text:
        return ""
    if preserve_quotes:
        allowed_tags = ['blockquote', 'b', 'i', 'u', 's', 'code', 'pre', 'tg-spoiler', 'a']
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

def parse_time(time_str: str) -> int:
    if not time_str:
        return 0
    time_str = time_str.lower().strip()
    if time_str.isdigit():
        return int(time_str) * 60
    patterns = [(r'(\d+)\s*с', 1), (r'(\d+)\s*сек', 1), (r'(\d+)\s*м', 60), (r'(\d+)\s*мин', 60), (r'(\d+)\s*ч', 3600), (r'(\d+)\s*час', 3600), (r'(\d+)\s*д', 86400), (r'(\d+)\s*дн', 86400)]
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

async def add_premium_reaction(message: Message, emoji: str = "⭐"):
    try:
        await message.react([ReactionTypeEmoji(emoji=emoji)])
    except:
        pass

def create_button(text: str, callback_data: str, color: str = None):
    return InlineKeyboardButton(text=text, callback_data=callback_data)

def get_back_keyboard(callback_data):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("◀️ Назад", callback_data))
    return builder.as_markup()

def get_main_keyboard(is_group: bool = False, is_admin: bool = False, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('about_button', lang), "about"))
    builder.add(create_button(i18n.get('help_button', lang), "help"))
    builder.add(create_button(i18n.get('add_to_group_button', lang), f"add_to_group_{BOT_USERNAME}"))
    builder.add(create_button(i18n.get('group_settings_button', lang), "group_manage_main"))
    if is_group:
        builder.add(create_button(i18n.get('rules_button', lang), "show_rules_group"))
        builder.add(create_button(i18n.get('stats_button', lang), "my_stats_group"))
        builder.add(create_button(i18n.get('top_button', lang), "top_active_group"))
    if is_admin and not is_group:
        builder.add(create_button(i18n.get('admin_panel_button', lang), "admin_panel"))
    builder.adjust(2)
    return builder.as_markup()

def get_group_manage_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('rules_manage', lang), "manage_rules"))
    builder.add(create_button(i18n.get('welcome_manage', lang), "manage_welcome"))
    builder.add(create_button(i18n.get('auto_broadcast', lang), "rules_auto"))
    builder.add(create_button(i18n.get('antiflood_manage', lang), "antiflood_manage"))
    builder.add(create_button(i18n.get('puls_antispam', lang), "puls_antispam_manage"))
    builder.add(create_button(i18n.get('confirm_actions', lang), "confirmation_actions_manage"))
    builder.add(create_button(i18n.get('log_group', lang), "log_group_manage"))
    builder.add(create_button(i18n.get('auto_response', lang), "auto_response_manage"))
    builder.add(create_button(i18n.get('links_manage', lang), "links_manage"))
    builder.add(create_button(i18n.get('confirm_entry', lang), "confirmation_manage"))
    builder.add(create_button(i18n.get('moderators_manage', lang), "moderators_manage"))
    builder.add(create_button(i18n.get('language_settings', lang), "language_settings"))
    builder.add(create_button(i18n.get('unlink_group', lang), "unlink_group_confirm"))
    builder.add(create_button(i18n.get('back_button', lang), "back_to_groups"))
    builder.adjust(1)
    return builder.as_markup()

def get_language_settings_keyboard(current_lang: str, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    languages = ["ru", "en", "uk", "de", "fr", "es", "it", "pt", "tr", "zh", "ja", "ko", "ar", "hi"]
    for l in languages:
        name = i18n.get(f"language_{l}", lang)
        if l == current_lang:
            name = f"✅ {name}"
        builder.add(create_button(name, f"set_lang_{l}"))
    builder.adjust(2)
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    return builder.as_markup()

def get_link_group_keyboard(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Привязать", f"link_group_{chat_id}"))
    builder.add(create_button("🚫 Отмена", "cancel_link"))
    return builder.as_markup()

def get_pm_link_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(create_button("💬 Перейти в ЛС", "go_to_pm"))
    return builder.as_markup()

def get_lift_restriction_keyboard(action, user_id, message_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔓 Снять ограничение", f"lift_{action}_{user_id}_{message_id}"))
    return builder.as_markup()

def get_confirm_action_keyboard(action, user_id, duration=None, reason=None, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    data_prefix = f"confirm_{action}_{user_id}"
    if duration:
        data_prefix += f"_{duration}"
    if reason:
        data_prefix += f"_{reason[:20] if reason else 'none'}"
    builder.add(create_button("✅ Подтверждаю", f"{data_prefix}_yes"))
    builder.add(create_button("❌ Отмена", f"{data_prefix}_no"))
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id, user_id, msg_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Я не бот", f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}"))
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id, user_id, msg_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Согласен", f"agree_rules_{chat_id}_{user_id}_{msg_id}"))
    return builder.as_markup()

def get_welcome_buttons(chat_id):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📜 Правила", f"show_group_rules_{chat_id}"))
    builder.add(create_button("📊 Моя статистика", f"my_stats_{chat_id}"))
    builder.add(create_button("🏆 Топ", f"top_active_{chat_id}"))
    return builder.as_markup()

def get_delete_confirm_keyboard(count: int, user_id: int = None, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    if user_id:
        builder.add(create_button("✅ Да, удалить", f"confirm_del_user_{count}_{user_id}"))
    else:
        builder.add(create_button("✅ Да, удалить", f"confirm_del_{count}"))
    builder.add(create_button("❌ Отмена", "cancel_delete"))
    return builder.as_markup()

def get_auto_response_remove_keyboard(responses, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    for i, (trigger, _, _, _) in enumerate(responses[:20]):
        short = trigger[:15] + "..." if len(trigger) > 15 else trigger
        builder.add(create_button(short, f"rem_trig_{i}"))
    builder.add(create_button(i18n.get('back_button', lang), "auto_response_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard(has_rules, rules_enabled, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Установить", "set_rules"))
    builder.add(create_button("📋 Готовые", "set_default_rules"))
    if has_rules:
        builder.add(create_button("👁 Посмотреть", "show_rules"))
        builder.add(create_button("✏️ Изменить", "edit_rules"))
        builder.add(create_button("🗑 Удалить", "delete_rules_confirm"))
        status_text = "✅ Включить" if not rules_enabled else "❌ Выключить"
        builder.add(create_button(status_text, "toggle_rules"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled=False, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_welcome"))
    builder.add(create_button("📝 Текст", "set_welcome_text"))
    builder.add(create_button("🖼 Фото", "set_welcome_photo"))
    builder.add(create_button("👁 Посмотреть", "show_welcome"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_rules_auto"))
    builder.add(create_button("⏱ Интервал", "set_interval"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'❌ Выключить' if settings['enabled'] else '✅ Включить'}", "toggle_antiflood"))
    builder.add(create_button(f"📝 Текст: {settings['msg_limit']}", "set_msg_limit"))
    builder.add(create_button(f"🎬 Медиа: {settings['media_limit']}", "set_media_limit"))
    builder.add(create_button(f"⏱ Период: {settings['time_window']} сек", "set_window"))
    builder.add(create_button(f"⚠️ Предупреждений: {settings['warn_count']}", "set_warn_count"))
    builder.add(create_button("🔇 Первое наказание", "set_first_punish"))
    builder.add(create_button("🔊 Повторное", "set_repeat_punish"))
    builder.add(create_button("⚠️ После варнов", "set_punish_after_warn"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(punish_type="first", lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", f"punish_warn_{punish_type}"))
    builder.add(create_button("🔇 Мут", f"punish_mute_{punish_type}"))
    builder.add(create_button("👢 Кик", f"punish_kick_{punish_type}"))
    builder.add(create_button("⛔️ Бан", f"punish_ban_{punish_type}"))
    builder.add(create_button(i18n.get('back_button', lang), "antiflood_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_puls_antispam_keyboard(enabled, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'❌ Выключить' if enabled else '✅ Включить'}", "toggle_puls_antispam"))
    builder.add(create_button("ℹ️ Что это?", "puls_antispam_info"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_actions_keyboard(settings, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    ban_status = "✅" if settings.get('ban', False) else "❌"
    kick_status = "✅" if settings.get('kick', False) else "❌"
    mute_status = "✅" if settings.get('mute', False) else "❌"
    builder.add(create_button(f"{ban_status} Подтверждение бана", "toggle_confirm_ban"))
    builder.add(create_button(f"{kick_status} Подтверждение кика", "toggle_confirm_kick"))
    builder.add(create_button(f"{mute_status} Подтверждение мута", "toggle_confirm_mute"))
    builder.add(create_button("ℹ️ Что это?", "confirmation_actions_info"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_keyboard(current_type, has_rules, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🚫 Отключено" + (" ✅" if current_type == 'disabled' else ""), "confirmation_disabled"))
    builder.add(create_button("🤖 Только не бот" + (" ✅" if current_type == 'not_bot' else ""), "confirmation_not_bot"))
    rules_btn = "📜 Только правила" + (" ✅" if current_type == 'rules' else "")
    builder.add(create_button(rules_btn, "confirmation_rules" if has_rules else "confirmation_disabled"))
    both_btn = "2️⃣ Оба шага" + (" ✅" if current_type == 'both' else "")
    builder.add(create_button(both_btn, "confirmation_both" if has_rules else "confirmation_disabled"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_manage_keyboard(settings, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button(f"{'❌ Выключить' if settings['links_enabled'] else '✅ Включить'}", "toggle_links"))
    builder.add(create_button("Наказание", "set_links_punish"))
    builder.add(create_button(f"Макс: {settings['max_mentions']}", "set_max_mentions"))
    builder.add(create_button(f"Период: {settings['mention_window']} сек", "set_mention_window"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_links_punish_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("⚠️ Warn", "links_punish_warn"))
    builder.add(create_button("🔇 Мут", "links_punish_mute"))
    builder.add(create_button("👢 Кик", "links_punish_kick"))
    builder.add(create_button("⛔️ Бан", "links_punish_ban"))
    builder.add(create_button(i18n.get('back_button', lang), "links_manage"))
    builder.adjust(2)
    return builder.as_markup()

def get_log_group_manage_keyboard(has_log_group, log_group_info=None, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    if has_log_group and log_group_info:
        builder.add(create_button("📊 Настройки логов", "log_group_settings"))
        builder.add(create_button("🔄 Отвязать", "unlink_log_group"))
        builder.add(create_button("👁 Инфо", "log_group_info"))
    else:
        builder.add(create_button("➕ Привязать группу логов", "link_log_group"))
        builder.add(create_button("ℹ️ Как создать", "log_group_help"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
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
    builder.add(create_button(f"{status_violations} Нарушения", "toggle_log_violations"))
    builder.add(create_button(f"{status_mod} Действия модераторов", "toggle_log_mod"))
    builder.add(create_button(f"{status_joins} Входы", "toggle_log_joins"))
    builder.add(create_button(f"{status_leaves} Выходы", "toggle_log_leaves"))
    builder.add(create_button(f"{status_messages} Сообщения", "toggle_log_messages"))
    builder.add(create_button(f"{status_buttons} Нажатия кнопок", "toggle_log_buttons"))
    builder.add(create_button(i18n.get('back_button', lang), "log_group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_moderators_manage_keyboard(moderators, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Дать права", "give_mod_rights"))
    if moderators:
        builder.add(create_button("❌ Забрать права", "remove_mod_rights"))
    builder.add(create_button("👁 Список", "list_moderators"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_mod_rights_keyboard(user_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔇 Право мутить", f"give_mute_{user_id}"))
    builder.add(create_button("👢 Право кикать", f"give_kick_{user_id}"))
    builder.add(create_button("⛔ Право банить", f"give_ban_{user_id}"))
    builder.add(create_button("⚠️ Право варнить", f"give_warn_{user_id}"))
    builder.add(create_button("🗑 Право удалять", f"give_del_{user_id}"))
    builder.add(create_button(i18n.get('back_button', lang), "moderators_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("❌ Отвязать", f"unlink_group_{chat_id}"))
    builder.add(create_button("🚫 Отмена", "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_keyboard(responses, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "add_auto_trigger"))
    if responses:
        builder.add(create_button("🗑 Удалить", "remove_auto_trigger"))
    builder.add(create_button(i18n.get('back_button', lang), "group_manage"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_custom_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Тексты сообщений", "admin_custom_texts"))
    builder.add(create_button("🖼 Фото сообщений", "admin_custom_photos"))
    builder.add(create_button("🔄 Сбросить всё", "admin_custom_reset_all"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_blacklist_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "blacklist_add"))
    builder.add(create_button("🗑 Удалить", "blacklist_remove"))
    builder.add(create_button("📋 Список", "blacklist_list"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_global_bans_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_ban_add"))
    builder.add(create_button("🗑 Снять", "global_ban_remove"))
    builder.add(create_button("📋 Список", "global_ban_list"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_global_mutes_keyboard(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_mute_add"))
    builder.add(create_button("🗑 Снять", "global_mute_remove"))
    builder.add(create_button("📋 Список", "global_mute_list"))
    builder.add(create_button(i18n.get('back_button', lang), "admin_panel"))
    builder.adjust(1)
    return builder.as_markup()

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
            c.execute('''CREATE TABLE IF NOT EXISTS group_rules (
                chat_id INTEGER PRIMARY KEY, owner_id INTEGER, rules_html TEXT, rules_enabled INTEGER DEFAULT 1,
                welcome_enabled INTEGER DEFAULT 0, welcome_text TEXT, welcome_photo_id TEXT,
                rules_auto_enabled INTEGER DEFAULT 0, rules_interval INTEGER DEFAULT 300,
                last_rules_message_id INTEGER, last_rules_time INTEGER, chat_title TEXT, chat_username TEXT,
                confirmation_type TEXT DEFAULT 'not_bot', puls_antispam_enabled INTEGER DEFAULT 1,
                confirm_ban INTEGER DEFAULT 0, confirm_kick INTEGER DEFAULT 0, confirm_mute INTEGER DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_users (user_id INTEGER PRIMARY KEY, global_id TEXT UNIQUE,
                first_seen INTEGER, username TEXT, full_name TEXT, is_premium INTEGER DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS auto_responses (id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER, trigger TEXT, response TEXT, response_type TEXT DEFAULT 'text', media_id TEXT, created_at INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS rules_agreed (chat_id INTEGER, user_id INTEGER,
                agreed_at INTEGER, not_bot_confirmed INTEGER DEFAULT 0, rules_confirmed INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS user_stats (chat_id INTEGER, user_id INTEGER, join_date INTEGER,
                all_messages INTEGER DEFAULT 0, month_messages INTEGER DEFAULT 0, week_messages INTEGER DEFAULT 0,
                day_messages INTEGER DEFAULT 0, last_active INTEGER, left_chat INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS antiflood_settings (chat_id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 0, msg_limit INTEGER DEFAULT 5, media_limit INTEGER DEFAULT 3,
                time_window INTEGER DEFAULT 10, warn_count INTEGER DEFAULT 3, first_punish TEXT DEFAULT 'mute',
                first_duration INTEGER DEFAULT 60, repeat_punish TEXT DEFAULT 'ban', repeat_duration INTEGER DEFAULT 3600,
                punish_after_warn TEXT DEFAULT 'mute', punish_after_warn_duration INTEGER DEFAULT 3600,
                links_enabled INTEGER DEFAULT 0, links_punish TEXT DEFAULT 'mute', links_duration INTEGER DEFAULT 3600,
                max_mentions INTEGER DEFAULT 3, mention_window INTEGER DEFAULT 60)''')
            c.execute('''CREATE TABLE IF NOT EXISTS violation_logs (id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER, user_id INTEGER, user_name TEXT, reason TEXT, punishment TEXT,
                message_id INTEGER, message_link TEXT, timestamp INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS user_warns (chat_id INTEGER, user_id INTEGER,
                warn_count INTEGER DEFAULT 0, last_warn_time INTEGER, PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS moderator_logs (id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER, moderator_id INTEGER, moderator_name TEXT, action TEXT, target_id INTEGER,
                target_name TEXT, duration INTEGER, reason TEXT, timestamp INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS log_groups (log_group_id INTEGER PRIMARY KEY,
                owner_id INTEGER, group_title TEXT, created_at INTEGER, is_active INTEGER DEFAULT 1)''')
            c.execute('''CREATE TABLE IF NOT EXISTS log_group_settings (id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id INTEGER, log_group_id INTEGER, send_violations INTEGER DEFAULT 1,
                send_mod_actions INTEGER DEFAULT 1, send_joins INTEGER DEFAULT 0, send_leaves INTEGER DEFAULT 0,
                send_messages INTEGER DEFAULT 0, send_button_clicks INTEGER DEFAULT 0,
                UNIQUE(source_chat_id, log_group_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_spammers (user_id INTEGER PRIMARY KEY,
                reason TEXT, added_at INTEGER, unbanned_in TEXT DEFAULT '[]', warnings INTEGER DEFAULT 1)''')
            c.execute('''CREATE TABLE IF NOT EXISTS custom_messages (msg_key TEXT PRIMARY KEY,
                custom_text TEXT, custom_photo TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS group_languages (chat_id INTEGER PRIMARY KEY, language TEXT DEFAULT 'ru')''')
            c.execute('''CREATE TABLE IF NOT EXISTS user_languages (user_id INTEGER PRIMARY KEY, language TEXT DEFAULT 'ru')''')
            c.execute('''CREATE TABLE IF NOT EXISTS bot_blacklist (user_id INTEGER PRIMARY KEY,
                reason TEXT, added_by INTEGER, added_at INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_bans (user_id INTEGER PRIMARY KEY,
                reason TEXT, moderator_id INTEGER, banned_at INTEGER, expires_at INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_mutes (user_id INTEGER PRIMARY KEY,
                reason TEXT, moderator_id INTEGER, muted_at INTEGER, expires_at INTEGER)''')
            c.execute('''CREATE TABLE IF NOT EXISTS delete_permissions (chat_id INTEGER, user_id INTEGER,
                can_delete INTEGER DEFAULT 0, given_by INTEGER, given_at INTEGER, PRIMARY KEY (chat_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS button_click_logs (id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, user_name TEXT, chat_id INTEGER, button_data TEXT, message_id INTEGER,
                action_result TEXT, timestamp INTEGER)''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_user_stats_chat ON user_stats(chat_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_user_stats_user ON user_stats(user_id)')
            conn.commit()
    
    def get_group_language(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT language FROM group_languages WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else "ru"
    
    def set_group_language(self, chat_id, language):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO group_languages (chat_id, language) VALUES (?, ?)', (chat_id, language))
            conn.commit()
    
    def get_user_language(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT language FROM user_languages WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            return result[0] if result else "ru"
    
    def set_user_language(self, user_id, language):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO user_languages (user_id, language) VALUES (?, ?)', (user_id, language))
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
                c.execute('INSERT INTO group_rules (chat_id, owner_id, rules_html, chat_title, chat_username, confirmation_type) VALUES (?, ?, ?, ?, ?, ?)',
                         (chat_id, owner_id, rules_html, chat_title, chat_username, 'not_bot'))
            conn.commit()
    
    def get_rules_html(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_html FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else None
    
    def get_rules_enabled(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT rules_enabled FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return bool(result[0]) if result else True
    
    def set_rules_enabled(self, chat_id, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET rules_enabled = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def delete_rules(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE group_rules SET rules_html = NULL WHERE chat_id = ?', (chat_id,))
            conn.commit()
    
    def get_welcome(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT welcome_text, welcome_photo_id FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return (result[0], result[1]) if result else (None, None)
    
    def save_welcome(self, chat_id, welcome_text=None, welcome_photo_id=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            if welcome_text is not None:
                c.execute('UPDATE group_rules SET welcome_text = ? WHERE chat_id = ?', (welcome_text, chat_id))
            if welcome_photo_id is not None:
                c.execute('UPDATE group_rules SET welcome_photo_id = ? WHERE chat_id = ?', (welcome_photo_id, chat_id))
            conn.commit()
    
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
    
    def set_rules_auto_settings(self, chat_id, enabled, interval):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM group_rules WHERE chat_id = ?', (chat_id,))
            existing = c.fetchone()
            if existing:
                c.execute('UPDATE group_rules SET rules_auto_enabled = ?, rules_interval = ? WHERE chat_id = ?',
                         (1 if enabled else 0, interval, chat_id))
            else:
                c.execute('INSERT INTO group_rules (chat_id, rules_auto_enabled, rules_interval) VALUES (?, ?, ?)',
                         (chat_id, 1 if enabled else 0, interval))
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
            c.execute('UPDATE group_rules SET last_rules_message_id = ?, last_rules_time = ? WHERE chat_id = ?',
                     (message_id, int(time.time()), chat_id))
            conn.commit()
    
    def get_user_groups(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id, chat_title FROM group_rules WHERE owner_id = ?', (user_id,))
            return c.fetchall()
    
    def get_or_create_global_user(self, user_id, username, full_name, is_premium=False):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            if result:
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
                return {'global_id': result[0], 'first_seen': result[1], 'username': result[2], 'full_name': result[3], 'is_premium': bool(result[4])}
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
    
    def get_top_messages(self, chat_id, limit=10):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT user_id, all_messages FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY all_messages DESC LIMIT ?', (chat_id, limit))
            return c.fetchall()
    
    def get_user_position(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT user_id FROM user_stats WHERE chat_id = ? AND left_chat = 0 ORDER BY all_messages DESC', (chat_id,))
            users = c.fetchall()
            for i, (uid,) in enumerate(users, 1):
                if uid == user_id:
                    return i
            return 0
    
    def get_antiflood_settings(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT enabled, msg_limit, media_limit, time_window, warn_count, first_punish, first_duration, repeat_punish, repeat_duration, punish_after_warn, punish_after_warn_duration, links_enabled, links_punish, links_duration, max_mentions, mention_window FROM antiflood_settings WHERE chat_id = ?', (chat_id,))
            row = c.fetchone()
            if row:
                return {'enabled': bool(row[0]), 'msg_limit': row[1] or 5, 'media_limit': row[2] or 3, 'time_window': row[3] or 10, 'warn_count': row[4] or 3, 'first_punish': row[5] or 'mute', 'first_duration': row[6] or 60, 'repeat_punish': row[7] or 'ban', 'repeat_duration': row[8] or 3600, 'punish_after_warn': row[9] or 'mute', 'punish_after_warn_duration': row[10] or 3600, 'links_enabled': bool(row[11]), 'links_punish': row[12] or 'mute', 'links_duration': row[13] or 3600, 'max_mentions': row[14] or 3, 'mention_window': row[15] or 60}
            return {'enabled': False, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3, 'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600, 'punish_after_warn': 'mute', 'punish_after_warn_duration': 3600, 'links_enabled': False, 'links_punish': 'mute', 'links_duration': 3600, 'max_mentions': 3, 'mention_window': 60}
    
    def save_antiflood_settings(self, chat_id, **kwargs):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM antiflood_settings WHERE chat_id = ?', (chat_id,))
            if c.fetchone():
                if kwargs:
                    fields = ', '.join(f"{k}=?" for k in kwargs)
                    values = list(kwargs.values()) + [chat_id]
                    c.execute(f'UPDATE antiflood_settings SET {fields} WHERE chat_id = ?', values)
            else:
                defaults = {'enabled': 0, 'msg_limit': 5, 'media_limit': 3, 'time_window': 10, 'warn_count': 3, 'first_punish': 'mute', 'first_duration': 60, 'repeat_punish': 'ban', 'repeat_duration': 3600, 'punish_after_warn': 'mute', 'punish_after_warn_duration': 3600, 'links_enabled': 0, 'links_punish': 'mute', 'links_duration': 3600, 'max_mentions': 3, 'mention_window': 60}
                defaults.update(kwargs)
                c.execute('INSERT INTO antiflood_settings (chat_id, enabled, msg_limit, media_limit, time_window, warn_count, first_punish, first_duration, repeat_punish, repeat_duration, punish_after_warn, punish_after_warn_duration, links_enabled, links_punish, links_duration, max_mentions, mention_window) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                         (chat_id, defaults['enabled'], defaults['msg_limit'], defaults['media_limit'], defaults['time_window'], defaults['warn_count'], defaults['first_punish'], defaults['first_duration'], defaults['repeat_punish'], defaults['repeat_duration'], defaults['punish_after_warn'], defaults['punish_after_warn_duration'], defaults['links_enabled'], defaults['links_punish'], defaults['links_duration'], defaults['max_mentions'], defaults['mention_window']))
            conn.commit()
    
    def get_user_warns(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT warn_count FROM user_warns WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            row = c.fetchone()
            return row[0] if row else 0
    
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
    
    def log_moderator_action(self, chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO moderator_logs (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                     (chat_id, moderator_id, moderator_name, action, target_id, target_name, duration, reason, int(time.time())))
            conn.commit()
    
    def add_auto_response(self, chat_id, trigger, response, response_type='text', media_id=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM auto_responses WHERE chat_id = ?', (chat_id,))
            count = c.fetchone()[0]
            if count >= MAX_TRIGGERS:
                return False, i18n.get('trigger_limit', get_group_language(chat_id), max=MAX_TRIGGERS)
            c.execute('SELECT 1 FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger))
            if c.fetchone():
                return False, i18n.get('trigger_exists', get_group_language(chat_id), trigger=trigger)
            c.execute('INSERT INTO auto_responses (chat_id, trigger, response, response_type, media_id, created_at) VALUES (?, ?, ?, ?, ?, ?)',
                     (chat_id, trigger, response, response_type, media_id, int(time.time())))
            conn.commit()
            return True, i18n.get('trigger_added', get_group_language(chat_id), trigger=trigger, count=count+1, max=MAX_TRIGGERS)
    
    def get_auto_responses(self, chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT trigger, response, response_type, media_id FROM auto_responses WHERE chat_id = ? ORDER BY created_at', (chat_id,))
            return c.fetchall()
    
    def remove_auto_response(self, chat_id, trigger):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM auto_responses WHERE chat_id = ? AND trigger = ?', (chat_id, trigger))
            conn.commit()
            return c.rowcount > 0
    
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
                return {'ban': bool(row[0]), 'kick': bool(row[1]), 'mute': bool(row[2)]}
            return {'ban': False, 'kick': False, 'mute': False}
    
    def set_confirmation_setting(self, chat_id, action, enabled):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute(f'UPDATE group_rules SET confirm_{action} = ? WHERE chat_id = ?', (1 if enabled else 0, chat_id))
            conn.commit()
    
    def mark_user_confirmed(self, chat_id, user_id, not_bot=False, rules=False):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            result = c.fetchone()
            if result:
                c.execute('UPDATE rules_agreed SET not_bot_confirmed = ?, rules_confirmed = ?, agreed_at = ? WHERE chat_id = ? AND user_id = ?',
                         (1 if (result[0] or not_bot) else 0, 1 if (result[1] or rules) else 0, int(time.time()), chat_id, user_id))
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
            if conf_type == 'not_bot':
                return bool(result[0])
            elif conf_type == 'rules':
                return bool(result[1]) and self.get_rules_html(chat_id) is not None
            else:
                return bool(result[0]) and bool(result[1])
    
    def get_user_confirmation_status(self, chat_id, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT not_bot_confirmed, rules_confirmed FROM rules_agreed WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            result = c.fetchone()
            return (bool(result[0]), bool(result[1])) if result else (False, False)
    
    def get_source_chat_log_group(self, source_chat_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM log_group_settings WHERE source_chat_id = ?', (source_chat_id,))
            return c.fetchone()
    
    def set_source_chat_log_group(self, source_chat_id, log_group_id, settings=None):
        with self.get_connection() as conn:
            c = conn.cursor()
            if settings:
                c.execute('INSERT OR REPLACE INTO log_group_settings (source_chat_id, log_group_id, send_violations, send_mod_actions, send_joins, send_leaves, send_messages, send_button_clicks) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                         (source_chat_id, log_group_id, settings.get('send_violations', 1), settings.get('send_mod_actions', 1), settings.get('send_joins', 0), settings.get('send_leaves', 0), settings.get('send_messages', 0), settings.get('send_button_clicks', 0)))
            else:
                c.execute('INSERT OR REPLACE INTO log_group_settings (source_chat_id, log_group_id, send_violations, send_mod_actions) VALUES (?, ?, 1, 1)',
                         (source_chat_id, log_group_id))
            conn.commit()
    
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
    
    def create_log_group(self, log_group_id, owner_id, group_title):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO log_groups (log_group_id, owner_id, group_title, created_at, is_active) VALUES (?, ?, ?, ?, 1)',
                     (log_group_id, owner_id, group_title, int(time.time())))
            conn.commit()
    
    def get_user_log_groups(self, user_id):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT log_group_id, group_title FROM log_groups WHERE owner_id = ?', (user_id,))
            return c.fetchall()
    
    def get_all_chats(self):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT chat_id, chat_title, chat_username FROM group_rules')
            return c.fetchall()
    
    def get_report_group(self, chat_id):
        return None
    
    def set_report_group(self, chat_id, report_group_id):
        pass
    
    def log_violation(self, chat_id, user_id, user_name, reason, punishment, message_id, message_link):
        pass

db = Database()

async def send_to_log_group(source_chat_id, event_type, data):
    log_group_info = db.get_source_chat_log_group(source_chat_id)
    if not log_group_info:
        return False
    try:
        await bot.send_message(log_group_info['log_group_id'], data, parse_mode="HTML")
        return True
    except:
        return False

def get_group_language(chat_id):
    return db.get_group_language(chat_id)

def get_user_language(user_id):
    return db.get_user_language(user_id)

async def check_blacklist(user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT reason FROM bot_blacklist WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            return True, result[0]
    return False, None

def add_to_blacklist(user_id, reason, added_by):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('INSERT INTO bot_blacklist (user_id, reason, added_by, added_at) VALUES (?, ?, ?, ?)',
                 (user_id, reason, added_by, int(time.time())))
        conn.commit()

def remove_from_blacklist(user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM bot_blacklist WHERE user_id = ?', (user_id,))
        conn.commit()
        return c.rowcount > 0

def is_global_banned(user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT reason, expires_at FROM global_bans WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            if result[1] > 0 and time.time() > result[1]:
                c.execute('DELETE FROM global_bans WHERE user_id = ?', (user_id,))
                conn.commit()
                return False, None, None
            return True, result[0], result[1]
    return False, None, None

def add_global_ban(user_id, reason, moderator_id, duration=0):
    with db.get_connection() as conn:
        c = conn.cursor()
        expires_at = int(time.time() + duration) if duration > 0 else 0
        c.execute('INSERT OR REPLACE INTO global_bans (user_id, reason, moderator_id, banned_at, expires_at) VALUES (?, ?, ?, ?, ?)',
                 (user_id, reason, moderator_id, int(time.time()), expires_at))
        conn.commit()

def remove_global_ban(user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM global_bans WHERE user_id = ?', (user_id,))
        conn.commit()
        return c.rowcount > 0

def is_global_muted(user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT reason, expires_at FROM global_mutes WHERE user_id = ?', (user_id,))
        result = c.fetchone()
        if result:
            if result[1] > 0 and time.time() > result[1]:
                c.execute('DELETE FROM global_mutes WHERE user_id = ?', (user_id,))
                conn.commit()
                return False, None, None
            return True, result[0], result[1]
    return False, None, None

def add_global_mute(user_id, reason, moderator_id, duration=0):
    with db.get_connection() as conn:
        c = conn.cursor()
        expires_at = int(time.time() + duration) if duration > 0 else 0
        c.execute('INSERT OR REPLACE INTO global_mutes (user_id, reason, moderator_id, muted_at, expires_at) VALUES (?, ?, ?, ?, ?)',
                 (user_id, reason, moderator_id, int(time.time()), expires_at))
        conn.commit()

def remove_global_mute(user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM global_mutes WHERE user_id = ?', (user_id,))
        conn.commit()
        return c.rowcount > 0

def has_delete_permission(chat_id, user_id):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT can_delete FROM delete_permissions WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        result = c.fetchone()
        return bool(result[0]) if result else False

def set_delete_permission(chat_id, user_id, can_delete, given_by):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO delete_permissions (chat_id, user_id, can_delete, given_by, given_at) VALUES (?, ?, ?, ?, ?)',
                 (chat_id, user_id, 1 if can_delete else 0, given_by, int(time.time())))
        conn.commit()

async def apply_global_ban(user_id, reason, duration=0):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM group_rules')
        for chat_id, in c.fetchall():
            try:
                await bot.ban_chat_member(chat_id, user_id)
                await asyncio.sleep(0.05)
            except:
                pass

async def apply_global_mute(user_id, reason, duration=0):
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_id FROM group_rules')
        for chat_id, in c.fetchall():
            try:
                until = int(time.time() + duration) if duration > 0 else None
                await bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
                await asyncio.sleep(0.05)
            except:
                pass

async def send_simple_welcome(chat_id, user):
    is_premium = getattr(user, 'is_premium', False)
    global_user_data = db.get_global_user(user.id)
    if not global_user_data:
        global_user_data = {'global_id': generate_user_id(), 'first_seen': int(time.time()), 'is_premium': is_premium}
    stat = db.get_user_stat(chat_id, user.id)
    position = db.get_user_position(chat_id, user.id)
    premium_emoji = get_premium_status_emoji(global_user_data['is_premium'])
    premium_line = customization.format_message('profile_premium') + "\n" if global_user_data['is_premium'] else ""
    
    welcome_text = customization.format_message(
        'welcome_simple',
        premium_emoji=premium_emoji,
        name=safe_html(user.full_name, False),
        global_id=global_user_data['global_id'],
        first_seen=format_datetime(global_user_data['first_seen']),
        premium_line=premium_line,
        warnings=0,
        limit=SPAM_WARN_LIMIT,
        username=user.username or 'нет',
        user_id=user.id,
        join_dt=format_datetime(int(time.time())),
        position=position
    )
    
    welcome_text_custom, welcome_photo = db.get_welcome(chat_id)
    
    if welcome_photo:
        await bot.send_photo(chat_id, photo=welcome_photo, caption=welcome_text + (f"\n\n{safe_html(welcome_text_custom, False)}" if welcome_text_custom else ""), reply_markup=get_welcome_buttons(chat_id), parse_mode="HTML")
    else:
        await bot.send_message(chat_id, welcome_text + (f"\n\n{safe_html(welcome_text_custom, False)}" if welcome_text_custom else ""), reply_markup=get_welcome_buttons(chat_id), parse_mode="HTML")

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
    waiting_for_duration = State()

class AutoResponseStates(StatesGroup):
    waiting_for_trigger = State()
    waiting_for_response = State()

class LinksStates(StatesGroup):
    waiting_for_duration = State()
    waiting_for_max_mentions = State()
    waiting_for_mention_window = State()

class MaintenanceStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_user_id = State()

class ModerationStates(StatesGroup):
    waiting_for_confirm_action = State()

class DeleteMessagesStates(StatesGroup):
    waiting_for_confirm = State()

class AdminBroadcastStates(StatesGroup):
    waiting_for_target = State()
    waiting_for_message = State()

class LogGroupStates(StatesGroup):
    waiting_for_log_group_id = State()

class CustomMessageStates(StatesGroup):
    waiting_for_new_text = State()
    waiting_for_new_photo = State()

flood_control = defaultdict(lambda: deque(maxlen=50))

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
        
        db.update_message_count(chat_id, user.id)
        
        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            return await handler(event, data)
        
        settings = db.get_antiflood_settings(chat_id)
        
        if not settings['enabled']:
            return await handler(event, data)
        
        now = time.time()
        key = f"{chat_id}_{user.id}"
        
        if key not in flood_control:
            flood_control[key] = deque(maxlen=50)
        
        while flood_control[key] and now - flood_control[key][0] > settings['time_window']:
            flood_control[key].popleft()
        
        if len(flood_control[key]) >= settings['msg_limit']:
            warn_count = db.get_user_warns(chat_id, user.id)
            if warn_count < settings['warn_count']:
                new_warn_count = db.add_user_warn(chat_id, user.id)
                await event.reply(f"⚠️ {user.full_name}, не флуди! Предупреждение {new_warn_count}/{settings['warn_count']}")
                await add_premium_reaction(event, "⚠️")
                return
            else:
                punish_type = settings['punish_after_warn']
                duration = settings['punish_after_warn_duration']
                db.reset_user_warns(chat_id, user.id)
                try:
                    if punish_type == 'mute':
                        until = int(time.time() + duration) if duration > 0 else None
                        await bot.restrict_chat_member(chat_id, user.id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
                        await event.reply(f"🔇 {user.full_name} замьючен на {format_interval(duration) if duration > 0 else 'навсегда'} за флуд")
                    elif punish_type == 'ban':
                        until = int(time.time() + duration) if duration > 0 else None
                        await bot.ban_chat_member(chat_id, user.id, until_date=until)
                        await event.reply(f"⛔ {user.full_name} забанен на {format_interval(duration) if duration > 0 else 'навсегда'} за флуд")
                    elif punish_type == 'kick':
                        await bot.ban_chat_member(chat_id, user.id)
                        await bot.unban_chat_member(chat_id, user.id)
                        await event.reply(f"👢 {user.full_name} кикнут за флуд")
                except:
                    pass
                return
        
        flood_control[key].append(now)
        return await handler(event, data)

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
                await event.answer("🛠 Бот на техработах", show_alert=True)
                return
        return await handler(event, data)

async def reset_periodic_counters():
    while True:
        now = datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE user_stats SET day_messages = 0 WHERE last_active < ?', (day_start.timestamp(),))
            conn.commit()
        await asyncio.sleep(3600)

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
                    except:
                        pass
        except:
            pass
        await asyncio.sleep(60)

async def clean_expired_bans_mutes():
    while True:
        now = int(time.time())
        with db.get_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM global_bans WHERE expires_at > 0 AND expires_at < ?', (now,))
            c.execute('DELETE FROM global_mutes WHERE expires_at > 0 AND expires_at < ?', (now,))
            conn.commit()
        await asyncio.sleep(3600)

DEFAULT_RULES = """
📢 Правила чата

<blockquote>🔰 1. Администрация</blockquote>
<blockquote expandable>💠 1.1. Администрация следит за порядком и вправе применять наказания.
💠 1.2. Доказательства нарушений хранятся у администрации.</blockquote>

<blockquote>🚫 2. Запрещено</blockquote>
<blockquote expandable>🔹 2.1. Неадекватное поведение, агрессия — мут 1–3 дня
🔹 2.2. Оскорбления — мут 3–7 дней
🔹 2.3. Спам, флуд — мут 1–3 дня</blockquote>
"""

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data({f"msg_owner_{message.message_id}": message.from_user.id})
    
    user_id = message.from_user.id
    is_blacklisted, reason = await check_blacklist(user_id)
    if is_blacklisted:
        lang = get_user_language(user_id) if message.chat.type == 'private' else get_group_language(message.chat.id)
        await message.answer(i18n.get('blacklisted', lang, reason=reason, support_link=SUPPORT_LINK), parse_mode="HTML")
        return
    
    is_premium = getattr(message.from_user, 'is_premium', False)
    is_admin = message.from_user.id in ADMIN_IDS
    is_group = message.chat.type != 'private'
    
    if is_group:
        current_lang = get_group_language(message.chat.id)
        welcome_text = i18n.get('welcome_group', current_lang)
    else:
        current_lang = get_user_language(user_id)
        welcome_text = i18n.get('welcome_pm', current_lang)
    
    photo = customization.get_photo('welcome_pm' if not is_group else 'welcome_group')
    
    if photo:
        await bot.send_photo(message.chat.id, photo=photo, caption=welcome_text, reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin, lang=current_lang), parse_mode="HTML")
    else:
        await message.answer(welcome_text, reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin, lang=current_lang), parse_mode="HTML")
    await add_premium_reaction(message, "⭐")

@dp.message(Command("group"))
@group_only()
async def cmd_group(message: Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer(i18n.get('not_creator', get_group_language(chat_id)))
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        owner_id = result[0] if result else None
    if owner_id == user_id:
        await message.answer(i18n.get('group_already_linked', get_group_language(chat_id)), reply_markup=get_pm_link_keyboard())
    else:
        await message.answer(i18n.get('select_group', get_group_language(chat_id)), reply_markup=get_link_group_keyboard(chat_id))
    await add_premium_reaction(message, "⚙️")

@dp.callback_query(F.data.startswith("link_group_"))
async def link_group(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    lang = get_group_language(chat_id)
    
    if not await is_creator(chat_id, user_id):
        await callback.answer(i18n.get('not_creator', lang), show_alert=True)
        return
    
    try:
        chat = await bot.get_chat(chat_id)
        chat_title = chat.title or "Группа"
        chat_username = chat.username
    except:
        chat_title = "Группа"
        chat_username = None
    
    db.save_rules(chat_id, owner_id=user_id, chat_title=chat_title, chat_username=chat_username)
    
    group_linked_text = customization.format_message('group_linked', title=safe_html(chat_title, False), chat_id=chat_id)
    await callback.message.edit_text(group_linked_text, parse_mode="HTML")
    await callback.answer(i18n.get('group_linked_success', lang), show_alert=True)
    
    try:
        await bot.send_message(user_id, customization.format_message('group_linked_pm', title=safe_html(chat_title, False)), parse_mode="HTML")
    except:
        pass

@dp.callback_query(F.data == "cancel_link")
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

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
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}"))
    builder.add(create_button(i18n.get('back_button', user_lang), "back_to_main"))
    builder.adjust(1)
    await message.answer(i18n.get('select_group', user_lang), reply_markup=builder.as_markup())
    await add_premium_reaction(message, "📱")

@dp.callback_query(F.data.startswith("select_group_"))
async def select_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    if not await is_creator(chat_id, callback.from_user.id):
        await callback.answer(i18n.get('not_creator', get_group_language(chat_id)), show_alert=True)
        return
    await state.update_data(selected_chat_id=chat_id, **{f"msg_owner_{callback.message.message_id}": callback.from_user.id})
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Группа"
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('group_settings_title', group_lang, title=safe_html(chat_title, False)), reply_markup=get_group_manage_keyboard(group_lang))
    await callback.answer()

@dp.callback_query(F.data == "back_to_groups")
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_lang = get_user_language(callback.from_user.id)
    groups = db.get_user_groups(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.add(create_button(title or f"Группа {chat_id}", f"select_group_{chat_id}"))
    builder.add(create_button(i18n.get('back_button', user_lang), "back_to_main"))
    builder.adjust(1)
    await callback.message.edit_text(i18n.get('select_group', user_lang), reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "group_manage")
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
    await callback.message.edit_text(i18n.get('group_settings_title', group_lang, title=safe_html(chat_title, False)), reply_markup=get_group_manage_keyboard(group_lang))
    await callback.answer()

@dp.callback_query(F.data == "language_settings")
async def language_settings(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    current_lang = get_group_language(chat_id)
    lang_names = {"ru": "Русский", "en": "English", "uk": "Українська", "de": "Deutsch", "fr": "Français", "es": "Español", "it": "Italiano", "pt": "Português", "tr": "Türkçe", "zh": "中文", "ja": "日本語", "ko": "한국어", "ar": "العربية", "hi": "हिन्दी"}
    lang_name = lang_names.get(current_lang, current_lang)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('language_settings_title', group_lang, language=lang_name), reply_markup=get_language_settings_keyboard(current_lang, group_lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("set_lang_"))
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
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('language_in_development', group_lang, language=i18n.get(f"language_{new_lang}", group_lang)), show_alert=True)
        return
    db.set_group_language(chat_id, new_lang)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('language_changed', group_lang, language=i18n.get(f"language_{new_lang}", group_lang)), show_alert=True)
    await language_settings(callback, state)

# ============ MANAGE RULES HANDLERS ============

@dp.callback_query(F.data == "manage_rules")
async def manage_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    has_rules = db.get_rules_html(chat_id) is not None
    rules_enabled = db.get_rules_enabled(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(f"📝 <b>Управление правилами</b>\n\nСтатус: {'✅ Включены' if rules_enabled else '❌ Выключены'}", reply_markup=get_rules_manage_keyboard(has_rules, rules_enabled, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "set_rules")
async def set_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_trigger', group_lang), reply_markup=get_back_keyboard("manage_rules"))
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
    group_lang = get_group_language(chat_id)
    await message.reply(i18n.get('settings_saved', group_lang))
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "set_default_rules")
async def set_default_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.save_rules(chat_id, rules_html=DEFAULT_RULES)
    db.set_rules_enabled(chat_id, True)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await manage_rules(callback, state)

@dp.callback_query(F.data == "show_rules")
async def show_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    rules_html = db.get_rules_html(chat_id)
    if rules_html:
        group_lang = get_group_language(chat_id)
        await callback.message.edit_text(f"📜 <b>Текущие правила:</b>\n\n{safe_html(rules_html, True)}", reply_markup=get_back_keyboard("manage_rules"), parse_mode="HTML")
    else:
        await callback.message.edit_text("❌ Правила ещё не установлены", reply_markup=get_back_keyboard("manage_rules"))
    await callback.answer()

@dp.callback_query(F.data == "edit_rules")
async def edit_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_trigger', group_lang), reply_markup=get_back_keyboard("manage_rules"))
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
    group_lang = get_group_language(chat_id)
    await message.reply(i18n.get('settings_saved', group_lang))
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "delete_rules_confirm")
async def delete_rules_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(chat_id)
    builder = InlineKeyboardBuilder()
    builder.add(create_button("✅ Да, удалить", "delete_rules"))
    builder.add(create_button("🚫 Нет", "manage_rules"))
    await callback.message.edit_text("❓ Вы уверены, что хотите удалить правила?", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "delete_rules")
async def delete_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.delete_rules(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await manage_rules(callback, state)

@dp.callback_query(F.data == "toggle_rules")
async def toggle_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    current = db.get_rules_enabled(chat_id)
    db.set_rules_enabled(chat_id, not current)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('rules_toggled', group_lang, enabled=not current), show_alert=True)
    await manage_rules(callback, state)

# ============ MANAGE WELCOME HANDLERS ============

@dp.callback_query(F.data == "manage_welcome")
async def manage_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled = db.get_welcome_enabled(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("👋 <b>Управление приветствием</b>", reply_markup=get_welcome_manage_keyboard(enabled, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_welcome")
async def toggle_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    current = db.get_welcome_enabled(chat_id)
    db.set_welcome_enabled(chat_id, not current)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('welcome_toggled', group_lang, enabled=not current), show_alert=True)
    await manage_welcome(callback, state)

@dp.callback_query(F.data == "set_welcome_text")
async def set_welcome_text(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_welcome_text', group_lang), reply_markup=get_back_keyboard("manage_welcome"))
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
    group_lang = get_group_language(chat_id)
    await message.reply(i18n.get('welcome_text_set', group_lang))
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "set_welcome_photo")
async def set_welcome_photo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_welcome_photo', group_lang), reply_markup=get_back_keyboard("manage_welcome"))
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
    group_lang = get_group_language(chat_id)
    await message.reply(i18n.get('welcome_photo_set', group_lang))
    await add_premium_reaction(message, "✅")
    await state.clear()

@dp.message(WelcomeStates.waiting_for_welcome_photo)
async def process_welcome_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Пожалуйста, отправьте фото!")

@dp.callback_query(F.data == "show_welcome")
async def show_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    text, photo_id = db.get_welcome(chat_id)
    if not text and not photo_id:
        group_lang = get_group_language(chat_id)
        await callback.message.edit_text("❌ Приветствие ещё не настроено", reply_markup=get_back_keyboard("manage_welcome"))
        await callback.answer()
        return
    await callback.message.delete()
    if photo_id:
        await callback.message.answer_photo(photo_id, caption=f"👋 <b>Текущее приветствие:</b>\n\n{safe_html(text, False)}" if text else None, reply_markup=get_back_keyboard("manage_welcome"), parse_mode="HTML")
    else:
        await callback.message.answer(f"👋 <b>Текущее приветствие:</b>\n\n{safe_html(text, False)}", reply_markup=get_back_keyboard("manage_welcome"), parse_mode="HTML")
    await callback.answer()

# ============ AUTO BROADCAST HANDLERS ============

@dp.callback_query(F.data == "rules_auto")
async def rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(f"🔄 <b>Авто-рассылка правил</b>\n\nСтатус: {'✅ Включена' if enabled else '❌ Выключена'}\nИнтервал: {format_interval(interval)}", reply_markup=get_rules_auto_keyboard(bool(enabled), group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_rules_auto")
async def toggle_rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    new_enabled = not enabled
    db.set_rules_auto_settings(chat_id, new_enabled, interval)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await rules_auto(callback, state)

@dp.callback_query(F.data == "set_interval")
async def set_interval(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_interval', group_lang), reply_markup=get_back_keyboard("rules_auto"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('interval_set', group_lang, interval=format_interval(interval * 60)))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

# ============ ANTIFLOOD HANDLERS ============

@dp.callback_query(F.data == "antiflood_manage")
async def antiflood_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_antiflood_settings(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(f"🚫 <b>Антифлуд</b>\n\nСтатус: {'✅ Включён' if settings['enabled'] else '❌ Выключен'}\n• Текст: {settings['msg_limit']} сообщ.\n• Медиа: {settings['media_limit']} сообщ.\n• Период: {settings['time_window']} сек\n• Предупреждений: {settings['warn_count']}", reply_markup=get_antiflood_manage_keyboard(settings, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_antiflood")
async def toggle_antiflood(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_antiflood_settings(chat_id)
    new_enabled = not settings['enabled']
    db.save_antiflood_settings(chat_id, enabled=int(new_enabled))
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('antiflood_toggled', group_lang, enabled=new_enabled), show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_msg_limit")
async def set_msg_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_msg_limit', group_lang), reply_markup=get_back_keyboard("antiflood_manage"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_media_limit")
async def set_media_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_media_limit', group_lang), reply_markup=get_back_keyboard("antiflood_manage"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_window")
async def set_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_window', group_lang), reply_markup=get_back_keyboard("antiflood_manage"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_warn_count")
async def set_warn_count(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_warn_count', group_lang), reply_markup=get_back_keyboard("antiflood_manage"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_first_punish")
async def set_first_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('select_punish', group_lang), reply_markup=get_punish_type_keyboard("first", group_lang))
    await callback.answer()

@dp.callback_query(F.data == "set_repeat_punish")
async def set_repeat_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('select_punish', group_lang), reply_markup=get_punish_type_keyboard("repeat", group_lang))
    await callback.answer()

@dp.callback_query(F.data == "set_punish_after_warn")
async def set_punish_after_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('select_punish', group_lang), reply_markup=get_punish_type_keyboard("after", group_lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_warn_"))
async def punish_warn(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    if punish_type == "first":
        db.save_antiflood_settings(chat_id, first_punish='warn')
    elif punish_type == "repeat":
        db.save_antiflood_settings(chat_id, repeat_punish='warn')
    elif punish_type == "after":
        db.save_antiflood_settings(chat_id, punish_after_warn='warn')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_mute_"))
async def punish_mute(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await state.update_data(punish_setting=punish_type, punish_action='mute')
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('enter_duration', group_lang), reply_markup=get_back_keyboard("antiflood_manage"))
    await state.set_state(AntiFloodStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data.startswith("punish_kick_"))
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
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data.startswith("punish_ban_"))
async def punish_ban(callback: CallbackQuery, state: FSMContext):
    punish_type = callback.data.split('_')[-1]
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await state.update_data(punish_setting=punish_type, punish_action='ban')
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('enter_duration', group_lang), reply_markup=get_back_keyboard("antiflood_manage"))
    await state.set_state(AntiFloodStates.waiting_for_duration)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_duration)
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
        elif punish_setting == "repeat":
            db.save_antiflood_settings(chat_id, repeat_punish=punish_action, repeat_duration=duration)
        elif punish_setting == "after":
            db.save_antiflood_settings(chat_id, punish_after_warn=punish_action, punish_after_warn_duration=duration)
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

# ============ PULS ANTISPAM HANDLERS ============

@dp.callback_query(F.data == "puls_antispam_manage")
async def puls_antispam_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    enabled = db.get_puls_antispam_enabled(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(f"🛡️ <b>Антиспам Пульса</b>\n\nСтатус: {'✅ Включен' if enabled else '❌ Выключен'}", reply_markup=get_puls_antispam_keyboard(enabled, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_puls_antispam")
async def toggle_puls_antispam(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    current = db.get_puls_antispam_enabled(chat_id)
    db.set_puls_antispam_enabled(chat_id, not current)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('puls_antispam_toggled', group_lang, enabled=not current), show_alert=True)
    await puls_antispam_manage(callback, state)

@dp.callback_query(F.data == "puls_antispam_info")
async def puls_antispam_info(callback: CallbackQuery, state: FSMContext):
    group_lang = get_group_language(callback.message.chat.id)
    await callback.message.edit_text("ℹ️ <b>Антиспам Пульса</b>\n\nГлобальная система защиты от спамеров.\n\nПри 50+ сообщениях в минуту пользователь получает предупреждение.\nПри 3 предупреждениях - глобальный бан.", reply_markup=get_back_keyboard("puls_antispam_manage"))
    await callback.answer()

# ============ CONFIRM ACTIONS HANDLERS ============

@dp.callback_query(F.data == "confirmation_actions_manage")
async def confirmation_actions_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("✅ <b>Подтверждение опасных действий</b>", reply_markup=get_confirmation_actions_keyboard(settings, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_confirm_ban")
async def toggle_confirm_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    new_value = not settings.get('ban', False)
    db.set_confirmation_setting(chat_id, 'ban', new_value)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('confirmation_toggled', group_lang, enabled=new_value), show_alert=True)
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "toggle_confirm_kick")
async def toggle_confirm_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    new_value = not settings.get('kick', False)
    db.set_confirmation_setting(chat_id, 'kick', new_value)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('confirmation_toggled', group_lang, enabled=new_value), show_alert=True)
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "toggle_confirm_mute")
async def toggle_confirm_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_confirmation_settings(chat_id)
    new_value = not settings.get('mute', False)
    db.set_confirmation_setting(chat_id, 'mute', new_value)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('confirmation_toggled', group_lang, enabled=new_value), show_alert=True)
    await confirmation_actions_manage(callback, state)

@dp.callback_query(F.data == "confirmation_actions_info")
async def confirmation_actions_info(callback: CallbackQuery, state: FSMContext):
    group_lang = get_group_language(callback.message.chat.id)
    await callback.message.edit_text("ℹ️ <b>Подтверждение действий</b>\n\nЕсли включено, перед выполнением действия бот спросит подтверждение.", reply_markup=get_back_keyboard("confirmation_actions_manage"))
    await callback.answer()

# ============ LOG GROUP HANDLERS ============

@dp.callback_query(F.data == "log_group_manage")
async def log_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    log_group_info = db.get_source_chat_log_group(chat_id)
    has_log_group = log_group_info is not None
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("📋 <b>Группа логов</b>", reply_markup=get_log_group_manage_keyboard(has_log_group, log_group_info, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "link_log_group")
async def link_log_group(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    log_groups = db.get_user_log_groups(user_id)
    group_lang = get_group_language(callback.message.chat.id)
    if not log_groups:
        await callback.message.edit_text("❌ У вас ещё нет созданных групп логов!\n\nСоздайте: перешлите сообщение из группы в ЛС боту.", reply_markup=get_back_keyboard("log_group_manage"))
        await callback.answer()
        return
    builder = InlineKeyboardBuilder()
    for log_id, title in log_groups:
        builder.add(create_button(title or f"Группа {log_id}", f"select_log_group_{log_id}"))
    builder.add(create_button(i18n.get('back_button', group_lang), "log_group_manage"))
    builder.adjust(1)
    await callback.message.edit_text("📋 <b>Выберите группу логов</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("select_log_group_"))
async def select_log_group(callback: CallbackQuery, state: FSMContext):
    log_group_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.set_source_chat_log_group(chat_id, log_group_id)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('log_group_attached', group_lang), show_alert=True)
    await log_group_manage(callback, state)

@dp.callback_query(F.data == "log_group_settings")
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
    await callback.message.edit_text("📋 <b>Настройки логов</b>", reply_markup=get_log_settings_keyboard(settings, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_log_violations")
async def toggle_log_violations(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_violations'] else 1
        db.update_log_group_settings(chat_id, log_group_info['log_group_id'], send_violations=new_value)
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('log_settings_updated', group_lang), show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_mod")
async def toggle_log_mod(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_mod_actions'] else 1
        db.update_log_group_settings(chat_id, log_group_info['log_group_id'], send_mod_actions=new_value)
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('log_settings_updated', group_lang), show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_joins")
async def toggle_log_joins(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_joins'] else 1
        db.update_log_group_settings(chat_id, log_group_info['log_group_id'], send_joins=new_value)
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('log_settings_updated', group_lang), show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_leaves")
async def toggle_log_leaves(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_leaves'] else 1
        db.update_log_group_settings(chat_id, log_group_info['log_group_id'], send_leaves=new_value)
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('log_settings_updated', group_lang), show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_messages")
async def toggle_log_messages(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info['send_messages'] else 1
        db.update_log_group_settings(chat_id, log_group_info['log_group_id'], send_messages=new_value)
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('log_settings_updated', group_lang), show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "toggle_log_buttons")
async def toggle_log_buttons(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if log_group_info:
        new_value = 0 if log_group_info.get('send_button_clicks', 0) else 1
        db.update_log_group_settings(chat_id, log_group_info['log_group_id'], send_button_clicks=new_value)
        group_lang = get_group_language(chat_id)
        await callback.answer(i18n.get('log_settings_updated', group_lang), show_alert=True)
    await log_group_settings(callback, state)

@dp.callback_query(F.data == "log_group_info")
async def log_group_info(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    log_group_info = db.get_source_chat_log_group(chat_id)
    if not log_group_info:
        await callback.answer("❌ Группа логов не привязана!", show_alert=True)
        return
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(f"📋 <b>Группа логов</b>\n\nID: <code>{log_group_info['log_group_id']}</code>", reply_markup=get_back_keyboard("log_group_manage"))
    await callback.answer()

@dp.callback_query(F.data == "unlink_log_group")
async def unlink_log_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    db.remove_source_chat_log_group(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('log_group_detached', group_lang), show_alert=True)
    await log_group_manage(callback, state)

@dp.callback_query(F.data == "log_group_help")
async def log_group_help(callback: CallbackQuery):
    group_lang = get_group_language(callback.message.chat.id)
    await callback.message.edit_text("📋 <b>Как создать группу логов:</b>\n\n1. Создайте группу\n2. Добавьте бота\n3. Перешлите сообщение из группы в ЛС боту", reply_markup=get_back_keyboard("log_group_manage"))
    await callback.answer()

# ============ AUTO RESPONSE HANDLERS ============

@dp.callback_query(F.data == "auto_response_manage")
async def auto_response_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    responses = db.get_auto_responses(chat_id)
    group_lang = get_group_language(chat_id)
    text = f"🤖 <b>Автоответчик</b> ({len(responses)}/{MAX_TRIGGERS})\n\n"
    for trigger, resp, resp_type, _ in responses[:10]:
        short_resp = resp[:30] + "..." if len(resp) > 30 else resp
        text += f"• <code>{safe_html(trigger, False)}</code> → {safe_html(short_resp, False)}\n"
    if not responses:
        text += "Список триггеров пуст."
    await callback.message.edit_text(text, reply_markup=get_auto_response_keyboard(responses, group_lang), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "add_auto_trigger")
async def add_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_trigger', group_lang), reply_markup=get_back_keyboard("auto_response_manage"))
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
        await message.answer(i18n.get('trigger_empty', 'ru'))
        return
    if len(trigger) > MAX_TRIGGER_LENGTH:
        await message.answer(i18n.get('trigger_too_long', 'ru', max_len=MAX_TRIGGER_LENGTH))
        return
    words = trigger.split()
    if len(words) > MAX_TRIGGER_WORDS:
        await message.answer(i18n.get('trigger_too_many_words', 'ru', max_words=MAX_TRIGGER_WORDS))
        return
    await state.update_data(auto_trigger=trigger)
    group_lang = get_group_language(message.chat.id)
    await message.answer(i18n.get('enter_response', group_lang), reply_markup=get_back_keyboard("auto_response_manage"))
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
    elif message.animation:
        response_type = 'animation'
        media_id = message.animation.file_id
        response = message.caption or ""
    elif message.sticker:
        response_type = 'sticker'
        media_id = message.sticker.file_id
        response = ""
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
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("🗑 Выберите триггер для удаления:", reply_markup=get_auto_response_remove_keyboard(responses, group_lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("rem_trig_"))
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
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('trigger_removed', group_lang, trigger=trigger), show_alert=True)
    await auto_response_manage(callback, state)

# ============ LINKS HANDLERS ============

@dp.callback_query(F.data == "links_manage")
async def links_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_antiflood_settings(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(f"🔗 <b>Ссылки и упоминания</b>\n\nФильтр ссылок: {'✅ Вкл' if settings['links_enabled'] else '❌ Выкл'}", reply_markup=get_links_manage_keyboard(settings, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_links")
async def toggle_links(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    settings = db.get_antiflood_settings(chat_id)
    new_enabled = not settings['links_enabled']
    db.save_antiflood_settings(chat_id, links_enabled=int(new_enabled))
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('links_toggled', group_lang, enabled=new_enabled), show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "set_links_punish")
async def set_links_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('select_punish', group_lang), reply_markup=get_links_punish_keyboard(group_lang))
    await callback.answer()

@dp.callback_query(F.data == "links_punish_warn")
async def links_punish_warn(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.save_antiflood_settings(chat_id, links_punish='warn')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "links_punish_mute")
async def links_punish_mute(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await state.update_data(links_punish='mute')
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('enter_duration', group_lang), reply_markup=get_back_keyboard("links_manage"))
    await state.set_state(LinksStates.waiting_for_duration)
    await callback.answer()

@dp.callback_query(F.data == "links_punish_kick")
async def links_punish_kick(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    db.save_antiflood_settings(chat_id, links_punish='kick')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await links_manage(callback, state)

@dp.callback_query(F.data == "links_punish_ban")
async def links_punish_ban(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    await state.update_data(links_punish='ban')
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('enter_duration', group_lang), reply_markup=get_back_keyboard("links_manage"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_max_mentions")
async def set_max_mentions(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_max_mentions', group_lang), reply_markup=get_back_keyboard("links_manage"))
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
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

@dp.callback_query(F.data == "set_mention_window")
async def set_mention_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(i18n.get('enter_mention_window', group_lang), reply_markup=get_back_keyboard("links_manage"))
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
            await message.answer("❌ Период должен быть от 10 до 3600 секунд!")
            return
        db.save_antiflood_settings(chat_id, mention_window=window)
        group_lang = get_group_language(chat_id)
        await message.reply(i18n.get('settings_saved', group_lang))
        await add_premium_reaction(message, "✅")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число!")

# ============ CONFIRM ENTRY HANDLERS ============

@dp.callback_query(F.data == "confirmation_manage")
async def confirmation_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    conf_type = db.get_confirmation_type(chat_id)
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("✅ <b>Подтверждение входа</b>", reply_markup=get_confirmation_keyboard(conf_type, has_rules, group_lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("confirmation_"))
async def process_confirmation_type(callback: CallbackQuery, state: FSMContext):
    conf_type = callback.data.replace("confirmation_", "")
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    if conf_type in ['rules', 'both'] and not has_rules:
        await callback.answer("❌ Сначала установите правила!", show_alert=True)
        return
    db.set_confirmation_type(chat_id, conf_type)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('settings_saved', group_lang), show_alert=True)
    await confirmation_manage(callback, state)

# ============ MODERATORS HANDLERS ============

@dp.callback_query(F.data == "moderators_manage")
async def moderators_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    moderators = []
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("🛡️ <b>Модераторы</b>", reply_markup=get_moderators_manage_keyboard(moderators, group_lang))
    await callback.answer()

@dp.callback_query(F.data == "give_mod_rights")
async def give_mod_rights(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("👤 Ответьте на сообщение пользователя или отправьте ID")
    await state.set_state(ModerationStates.waiting_for_confirm_action)
    await callback.answer()

@dp.callback_query(F.data.startswith("give_mute_"))
async def give_mute_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('rights_granted', group_lang), show_alert=True)
    await moderators_manage(callback, state)

@dp.callback_query(F.data.startswith("give_kick_"))
async def give_kick_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('rights_granted', group_lang), show_alert=True)
    await moderators_manage(callback, state)

@dp.callback_query(F.data.startswith("give_ban_"))
async def give_ban_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('rights_granted', group_lang), show_alert=True)
    await moderators_manage(callback, state)

@dp.callback_query(F.data.startswith("give_warn_"))
async def give_warn_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('rights_granted', group_lang), show_alert=True)
    await moderators_manage(callback, state)

@dp.callback_query(F.data.startswith("give_del_"))
async def give_del_right(callback: CallbackQuery, state: FSMContext):
    target_id = int(callback.data.split('_')[-1])
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    set_delete_permission(chat_id, target_id, True, callback.from_user.id)
    group_lang = get_group_language(chat_id)
    await callback.answer(i18n.get('delete_give_right', group_lang, name=str(target_id)), show_alert=True)
    await moderators_manage(callback, state)

@dp.callback_query(F.data == "list_moderators")
async def list_moderators(callback: CallbackQuery, state: FSMContext):
    group_lang = get_group_language(callback.message.chat.id)
    await callback.message.edit_text(i18n.get('moderator_list', group_lang, list="Нет модераторов"), reply_markup=get_back_keyboard("moderators_manage"))
    await callback.answer()

# ============ UNLINK GROUP HANDLERS ============

@dp.callback_query(F.data == "unlink_group_confirm")
async def unlink_group_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    if not chat_id:
        await callback.answer("❌ Сначала выберите группу!", show_alert=True)
        return
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text("❓ Вы уверены, что хотите отвязать группу?", reply_markup=get_unlink_confirm_keyboard(chat_id, group_lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("unlink_group_"))
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET owner_id = NULL WHERE chat_id = ?', (chat_id,))
        conn.commit()
    group_lang = get_group_language(chat_id)
    await callback.message.edit_text(i18n.get('group_unlinked', group_lang))
    await callback.answer(i18n.get('group_unlinked', group_lang), show_alert=True)
    await state.clear()
    await cmd_start(callback.message, state)

# ============ BASIC COMMANDS ============

@dp.message(Command("puls"))
async def cmd_ping(message: Message):
    start = time.time()
    msg = await message.reply("⏳ ...")
    ping = round((time.time() - start) * 1000)
    await msg.edit_text(f"📡 <b>Пинг:</b> {ping} мс")

@dp.message(Command("stats"))
@group_only()
async def cmd_stats(message: Message):
    chat_id = message.chat.id
    user = message.from_user
    stat = db.get_user_stat(chat_id, user.id)
    if stat:
        await message.reply(f"📊 <b>Ваша статистика:</b>\n\nВсего: {stat['all_messages']}\nЗа день: {stat['day_messages']}\nЗа неделю: {stat['week_messages']}\nЗа месяц: {stat['month_messages']}", parse_mode="HTML")
    else:
        await message.reply(i18n.get('no_messages', get_group_language(chat_id)))

@dp.message(Command("top"))
@group_only()
async def cmd_top(message: Message):
    chat_id = message.chat.id
    top = db.get_top_messages(chat_id, limit=10)
    if not top:
        await message.reply(i18n.get('no_messages', get_group_language(chat_id)))
        return
    text = "🏆 <b>Топ активных:</b>\n\n"
    for i, (uid, count) in enumerate(top, 1):
        try:
            member = await bot.get_chat_member(chat_id, uid)
            name = member.user.full_name
        except:
            name = f"ID {uid}"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        text += f"{medal} {safe_html(name, False)} — {count} 💬\n"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("profile"))
@group_only()
async def cmd_profile(message: Message):
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя!")
        return
    target = message.reply_to_message.from_user
    await message.reply(f"👤 <b>Профиль:</b> {safe_html(target.full_name, False)}\n🆔 ID: <code>{target.id}</code>", parse_mode="HTML")

@dp.message(Command("rules"))
@group_only()
async def cmd_rules(message: Message):
    chat_id = message.chat.id
    rules = db.get_rules_html(chat_id)
    if rules and db.get_rules_enabled(chat_id):
        await message.reply(safe_html(rules, True), parse_mode="HTML")
    else:
        await message.answer(i18n.get('rules_not_set', get_group_language(chat_id)))

@dp.message(Command("mute"))
@group_only()
async def cmd_mute(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id)
    
    if not await is_admin(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    if not message.reply_to_message:
        await message.answer(i18n.get('reply_to_user', group_lang))
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer(i18n.get('cant_mute_bot', group_lang))
        return
    args = message.text.split(maxsplit=2)
    duration_str = args[1] if len(args) > 1 else "0"
    reason = args[2] if len(args) > 2 else i18n.get('default_reason', group_lang)
    duration = parse_time(duration_str)
    try:
        until = int(time.time() + duration) if duration > 0 else None
        await bot.restrict_chat_member(chat_id, target_user.id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
        duration_text = format_time(duration) if duration > 0 else i18n.get('forever', group_lang)
        await message.reply(i18n.get('mute_message', group_lang, name=safe_html(target_user.full_name, False), moderator=safe_html(message.from_user.full_name, False), duration=duration_text, reason=safe_html(reason, False)), parse_mode="HTML")
        await add_premium_reaction(message, "🔇")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("unmute"))
@group_only()
async def cmd_unmute(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id)
    
    if not await is_admin(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    if not message.reply_to_message:
        await message.answer(i18n.get('reply_to_user', group_lang))
        return
    target_user = message.reply_to_message.from_user
    try:
        await bot.restrict_chat_member(chat_id, target_user.id, permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True, can_invite_users=True))
        await message.reply(i18n.get('unmute_message', group_lang, name=safe_html(target_user.full_name, False), moderator=safe_html(message.from_user.full_name, False)), parse_mode="HTML")
        await add_premium_reaction(message, "🔊")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("ban"))
@group_only()
async def cmd_ban(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id)
    
    if not await is_admin(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    if not message.reply_to_message:
        await message.answer(i18n.get('reply_to_user', group_lang))
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer(i18n.get('cant_ban_bot', group_lang))
        return
    args = message.text.split(maxsplit=2)
    duration_str = args[1] if len(args) > 1 else "0"
    reason = args[2] if len(args) > 2 else i18n.get('default_reason', group_lang)
    duration = parse_time(duration_str)
    try:
        until = int(time.time() + duration) if duration > 0 else None
        await bot.ban_chat_member(chat_id, target_user.id, until_date=until)
        duration_text = format_time(duration) if duration > 0 else i18n.get('forever', group_lang)
        await message.reply(i18n.get('ban_message', group_lang, name=safe_html(target_user.full_name, False), moderator=safe_html(message.from_user.full_name, False), duration=duration_text, reason=safe_html(reason, False)), parse_mode="HTML")
        await add_premium_reaction(message, "⛔️")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("unban"))
@group_only()
async def cmd_unban(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id)
    
    if not await is_admin(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    args = message.text.split()
    if len(args) < 2 and not message.reply_to_message:
        await message.answer("❌ Укажите пользователя или ответьте на сообщение!")
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        try:
            target_id = int(args[1])
        except:
            await message.answer("❌ Некорректный ID!")
            return
    try:
        await bot.unban_chat_member(chat_id, target_id)
        await message.answer(i18n.get('unmute_message', group_lang, name=str(target_id), moderator=safe_html(message.from_user.full_name, False)), parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("kick"))
@group_only()
async def cmd_kick(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id)
    
    if not await is_admin(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    if not message.reply_to_message:
        await message.answer(i18n.get('reply_to_user', group_lang))
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer(i18n.get('cant_kick_bot', group_lang))
        return
    args = message.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else i18n.get('default_reason', group_lang)
    try:
        await bot.ban_chat_member(chat_id, target_user.id)
        await bot.unban_chat_member(chat_id, target_user.id)
        await message.reply(i18n.get('kick_message', group_lang, name=safe_html(target_user.full_name, False), moderator=safe_html(message.from_user.full_name, False), reason=safe_html(reason, False)), parse_mode="HTML")
        await add_premium_reaction(message, "👢")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("warn"))
@group_only()
async def cmd_warn(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id)
    
    if not await is_admin(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    if not message.reply_to_message:
        await message.answer(i18n.get('reply_to_user', group_lang))
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя предупреждать бота!")
        return
    args = message.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else i18n.get('default_reason', group_lang)
    try:
        warn_count = db.add_user_warn(chat_id, target_user.id)
        await message.reply(i18n.get('warn_message', group_lang, name=safe_html(target_user.full_name, False), moderator=safe_html(message.from_user.full_name, False), warn_count=warn_count, reason=safe_html(reason, False)), parse_mode="HTML")
        await add_premium_reaction(message, "⚠️")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("mods"))
@group_only()
async def cmd_mods(message: Message):
    await message.answer("👮 <b>Модераторы:</b> все администраторы группы", parse_mode="HTML")

# ============ DELETE MESSAGES HANDLER ============

@dp.message(F.text.regexp(r'^-смс\s+(\d+)$'))
async def cmd_delete_messages(message: Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    group_lang = get_group_language(chat_id) if chat_id < 0 else "ru"
    
    if chat_id < 0 and not await is_admin(chat_id, user_id) and not has_delete_permission(chat_id, user_id):
        await message.answer(i18n.get('no_permission', group_lang))
        return
    
    match = re.search(r'^-смс\s+(\d+)$', message.text)
    count = int(match.group(1))
    
    if count < 1 or count > 100:
        await message.answer(i18n.get('delete_range_error', group_lang))
        return
    
    target_user_id = message.reply_to_message.from_user.id if message.reply_to_message else None
    
    if count >= 50:
        await state.update_data(delete_count=count, delete_user_id=target_user_id)
        await message.answer(i18n.get('delete_confirm', group_lang, count=count), reply_markup=get_delete_confirm_keyboard(count, target_user_id, group_lang))
        await state.set_state(DeleteMessagesStates.waiting_for_confirm)
    else:
        await perform_delete_messages(message, chat_id, count, target_user_id, group_lang)

async def perform_delete_messages(original_message: Message, chat_id: int, count: int, target_user_id: int = None, lang: str = "ru"):
    status_msg = await original_message.answer(i18n.get('delete_progress', lang, current=0, total=count))
    deleted = 0
    async for msg in bot.get_chat_history(chat_id, limit=100):
        if deleted >= count:
            break
        if target_user_id and msg.from_user and msg.from_user.id != target_user_id:
            continue
        if msg.message_id == original_message.message_id or msg.message_id == status_msg.message_id:
            continue
        try:
            await msg.delete()
            deleted += 1
            if deleted % 5 == 0:
                await status_msg.edit_text(i18n.get('delete_progress', lang, current=deleted, total=count))
            await asyncio.sleep(0.1)
        except:
            pass
    if target_user_id:
        await status_msg.edit_text(i18n.get('delete_user_success', lang, count=deleted, name="пользователя"))
    else:
        await status_msg.edit_text(i18n.get('delete_success', lang, count=deleted))

@dp.callback_query(DeleteMessagesStates.waiting_for_confirm, F.data.startswith("confirm_del"))
async def confirm_delete(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    count = data.get('delete_count', 0)
    target_user_id = data.get('delete_user_id')
    lang = get_group_language(callback.message.chat.id)
    await callback.message.delete()
    await perform_delete_messages(callback.message, callback.message.chat.id, count, target_user_id, lang)
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "cancel_delete")
async def cancel_delete(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await state.clear()
    await callback.answer("❌ Удаление отменено")

# ============ GLOBAL COMMANDS ============

@dp.message(Command("gban"))
@check_bot_admin()
async def cmd_global_ban(message: Message):
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer(i18n.get('global_ban_usage', 'ru'))
        return
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID!")
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
    add_global_ban(target_id, reason, message.from_user.id, duration)
    await apply_global_ban(target_id, reason, duration)
    duration_text = format_time(duration) if duration > 0 else "навсегда"
    await message.answer(i18n.get('global_ban_success', 'ru', name=user_name, user_id=target_id, reason=reason, duration=duration_text))

@dp.message(Command("gunban"))
@check_bot_admin()
async def cmd_global_unban(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Использование: /gunban <user_id>")
        return
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID!")
        return
    if remove_global_ban(target_id):
        await message.answer(i18n.get('global_unban_success', 'ru', user_id=target_id))
    else:
        await message.answer(f"❌ Пользователь {target_id} не найден в глобальных банах")

@dp.message(Command("gmute"))
@check_bot_admin()
async def cmd_global_mute(message: Message):
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer(i18n.get('global_mute_usage', 'ru'))
        return
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID!")
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
    add_global_mute(target_id, reason, message.from_user.id, duration)
    await apply_global_mute(target_id, reason, duration)
    duration_text = format_time(duration) if duration > 0 else "навсегда"
    await message.answer(i18n.get('global_mute_success', 'ru', name=user_name, user_id=target_id, reason=reason, duration=duration_text))

@dp.message(Command("gunmute"))
@check_bot_admin()
async def cmd_global_unmute(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Использование: /gunmute <user_id>")
        return
    try:
        target_id = int(args[1])
    except:
        await message.answer("❌ Некорректный ID!")
        return
    if remove_global_mute(target_id):
        await message.answer(i18n.get('global_unmute_success', 'ru', user_id=target_id))
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
            await message.answer("❌ Некорректный ID!")
            return
    reason = args[2] if len(args) > 2 else "Не указана"
    add_to_blacklist(target_id, reason, message.from_user.id)
    await message.answer(i18n.get('add_to_blacklist', 'ru', name=target_name, user_id=target_id, reason=reason))

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
        await message.answer("❌ Некорректный ID!")
        return
    if remove_from_blacklist(target_id):
        await message.answer(i18n.get('remove_from_blacklist', 'ru', user_id=target_id))
    else:
        await message.answer(i18n.get('not_blacklisted', 'ru'))

@dp.message(Command("give_del"))
@group_only()
async def cmd_give_del(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer(i18n.get('no_permission', get_group_language(chat_id)))
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя!")
        return
    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("❌ Нельзя давать права боту!")
        return
    set_delete_permission(chat_id, target_user.id, True, user_id)
    group_lang = get_group_language(chat_id)
    await message.answer(i18n.get('delete_give_right', group_lang, name=safe_html(target_user.full_name, False)))

@dp.message(Command("ungive_del"))
@group_only()
async def cmd_ungive_del(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer(i18n.get('no_permission', get_group_language(chat_id)))
        return
    if not message.reply_to_message:
        await message.answer("❌ Ответьте на сообщение пользователя!")
        return
    target_user = message.reply_to_message.from_user
    set_delete_permission(chat_id, target_user.id, False, user_id)
    group_lang = get_group_language(chat_id)
    await message.answer(i18n.get('delete_remove_right', group_lang, name=safe_html(target_user.full_name, False)))

@dp.message(Command("delmods"))
@group_only()
async def cmd_delmods(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_creator(chat_id, user_id):
        await message.answer(i18n.get('no_permission', get_group_language(chat_id)))
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT user_id, given_by, given_at FROM delete_permissions WHERE chat_id = ? AND can_delete = 1', (chat_id,))
        users = c.fetchall()
    if not users:
        await message.answer("📋 Нет пользователей с правом удаления")
        return
    text = i18n.get('delete_mod_list', get_group_language(chat_id), users="")
    for uid, given_by, given_at in users:
        try:
            user = await bot.get_chat_member(chat_id, uid)
            name = user.user.full_name
            text += f"• {safe_html(name, False)}\n  Дата: {format_datetime(given_at)}\n\n"
        except:
            continue
    await message.answer(text, parse_mode="HTML")

# ============ GROUP EVENTS ============

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
        
        if conf_type == 'not_bot':
            await bot.send_message(chat_id, f"👋 <b>{safe_html(user.full_name, False)}</b>, подтвердите, что вы не бот", reply_markup=get_confirm_not_bot_keyboard(chat_id, user.id, 0), parse_mode="HTML")
        elif conf_type == 'rules' and rules_html and rules_enabled:
            try:
                await bot.send_message(user.id, f"Добро пожаловать в {safe_html(update.chat.title, False)}!\n\nПрочитайте правила:\n\n{safe_html(rules_html, True)}", reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0), parse_mode="HTML")
                await bot.send_message(chat_id, f"👋 <b>{safe_html(user.full_name, False)}</b>, прочитайте правила в ЛС", reply_markup=get_pm_link_keyboard(), parse_mode="HTML")
            except:
                await bot.send_message(chat_id, f"👋 <b>{safe_html(user.full_name, False)}</b>, не удалось отправить правила в ЛС", parse_mode="HTML")
        elif conf_type == 'both':
            try:
                await bot.send_message(user.id, f"Добро пожаловать в {safe_html(update.chat.title, False)}!\n\nШаг 1: Подтвердите, что вы не бот", reply_markup=get_confirm_not_bot_keyboard(chat_id, user.id, 0))
                if rules_html and rules_enabled:
                    await bot.send_message(user.id, f"Шаг 2: Прочитайте правила:\n\n{safe_html(rules_html, True)}", reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0), parse_mode="HTML")
                await bot.send_message(chat_id, f"👋 <b>{safe_html(user.full_name, False)}</b>, подтвердите вход в ЛС", reply_markup=get_pm_link_keyboard(), parse_mode="HTML")
            except:
                await bot.send_message(chat_id, f"👋 <b>{safe_html(user.full_name, False)}</b>, не удалось отправить подтверждение в ЛС", parse_mode="HTML")

@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    db.set_left_chat(update.chat.id, update.from_user.id)
    await bot.send_message(update.chat.id, f"👋 {safe_html(update.from_user.full_name, False)} вышел из чата")

@dp.callback_query(F.data.startswith("confirm_not_bot_"))
async def process_confirm_not_bot(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id, user_id = int(parts[3]), int(parts[4])
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
    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("✅ Спасибо за подтверждение! Теперь вы можете писать в чат.")
    await callback.answer()
    await add_premium_reaction(callback.message, "✅")

@dp.callback_query(F.data.startswith("agree_rules_"))
async def process_agree_rules(callback: CallbackQuery):
    parts = callback.data.split('_')
    chat_id, user_id = int(parts[2]), int(parts[3])
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
    await send_simple_welcome(chat_id, callback.from_user)
    await callback.message.edit_text("✅ Спасибо! Теперь вы можете писать в чат.")
    await callback.answer()
    await add_premium_reaction(callback.message, "✅")

@dp.callback_query(F.data.startswith("show_group_rules_"))
async def show_group_rules(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    rules = db.get_rules_html(chat_id)
    if rules and db.get_rules_enabled(chat_id):
        await callback.message.answer(safe_html(rules, True), parse_mode="HTML")
    else:
        await callback.message.answer(i18n.get('rules_not_set', get_group_language(chat_id)))
    await callback.answer()

@dp.callback_query(F.data.startswith("my_stats_"))
async def my_stats(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    user = callback.from_user
    stat = db.get_user_stat(chat_id, user.id)
    if stat:
        await callback.message.answer(f"📊 <b>Ваша статистика:</b>\n\nВсего: {stat['all_messages']}\nЗа день: {stat['day_messages']}\nЗа неделю: {stat['week_messages']}\nЗа месяц: {stat['month_messages']}", parse_mode="HTML")
    else:
        await callback.message.answer(i18n.get('no_messages', get_group_language(chat_id)))
    await callback.answer()

@dp.callback_query(F.data.startswith("top_active_"))
async def top_active(callback: CallbackQuery):
    chat_id = int(callback.data.split('_')[-1])
    top = db.get_top_messages(chat_id, limit=10)
    if not top:
        await callback.message.answer(i18n.get('no_messages', get_group_language(chat_id)))
        await callback.answer()
        return
    text = "🏆 <b>Топ активных:</b>\n\n"
    for i, (uid, count) in enumerate(top, 1):
        try:
            member = await bot.get_chat_member(chat_id, uid)
            name = member.user.full_name
        except:
            name = f"ID {uid}"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        text += f"{medal} {safe_html(name, False)} — {count} 💬\n"
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = callback.from_user.id in ADMIN_IDS
    is_group = callback.message.chat.type != 'private'
    user_lang = get_user_language(callback.from_user.id)
    await callback.message.edit_text(i18n.get('main_menu_title', user_lang), reply_markup=get_main_keyboard(is_group=is_group, is_admin=is_admin, lang=user_lang))
    await callback.answer()

@dp.callback_query(F.data == "about")
async def about(callback: CallbackQuery):
    user_lang = get_user_language(callback.from_user.id)
    await callback.message.edit_text("🤖 <b>Puls Chat Manager</b> ⭐\n\nВерсия: 7.0.0\n\n📌 <b>Возможности:</b>\n• Управление правилами\n• Авто-рассылка\n• Антифлуд\n• Антиспам Пульса\n• Автоответчик\n• Статистика\n• Приветствия\n• Модерация\n• Группы логов\n• Многоязычность", reply_markup=get_main_keyboard(is_group=False, is_admin=callback.from_user.id in ADMIN_IDS, lang=user_lang))
    await callback.answer()

@dp.callback_query(F.data == "help")
async def help(callback: CallbackQuery):
    user_lang = get_user_language(callback.from_user.id)
    await callback.message.edit_text("🆘 <b>Помощь</b> ⭐\n\n🔹 <b>Команды в группе:</b>\n• /rules - правила\n• /stats - статистика\n• /top - топ\n• /profile - профиль\n• /group - управление\n• /puls - пинг\n• /mute - мут\n• /unmute - размут\n• /ban - бан\n• /unban - разбан\n• /kick - кик\n• /warn - варн\n• /mods - модераторы", reply_markup=get_main_keyboard(is_group=False, is_admin=callback.from_user.id in ADMIN_IDS, lang=user_lang))
    await callback.answer()

# ============ ADMIN PANEL ============

@dp.callback_query(F.data == "admin_panel")
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
    text = i18n.get('admin_panel_title', 'ru', status=status, main_lang=i18n.main_language, blacklist_count=blacklist_count, global_bans=global_bans, global_mutes=global_mutes)
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📊 Статистика", "admin_stats"))
    builder.add(create_button("📱 Группы", "admin_groups"))
    builder.add(create_button("👥 Пользователи", "admin_users"))
    builder.add(create_button("📋 Логи", "admin_logs"))
    builder.add(create_button("🛠 Техработы", "admin_maintenance"))
    builder.add(create_button(i18n.get('blacklist_manage', 'ru'), "admin_blacklist"))
    builder.add(create_button(i18n.get('global_bans_manage', 'ru'), "admin_global_bans"))
    builder.add(create_button(i18n.get('global_mutes_manage', 'ru'), "admin_global_mutes"))
    builder.add(create_button(i18n.get('change_main_lang', 'ru'), "admin_main_lang"))
    builder.add(create_button("📢 Рассылка", "admin_broadcast"))
    builder.add(create_button("📦 Бэкап", "admin_backup"))
    builder.add(create_button("🎨 Кастомизация", "admin_custom"))
    builder.add(create_button("❌ Выключить", "admin_shutdown"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "back_to_main"))
    builder.adjust(2)
    await callback.message.edit_text(safe_html(text, False), reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_main_lang")
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
        builder.add(create_button(name, f"set_main_lang_{lang}"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(1)
    await callback.message.edit_text(i18n.get('main_lang_select', 'ru', current=i18n.main_language), reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("set_main_lang_"))
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
        await callback.message.edit_text("✅ Черный список пуст.", reply_markup=get_back_keyboard("admin_panel"))
        await callback.answer()
        return
    text = "🚫 <b>Черный список бота:</b>\n\n"
    for user_id, reason, added_by, added_at in blacklist:
        text += f"• <code>{user_id}</code>\n  Причина: {safe_html(reason, False)}\n  Добавил: {added_by}\n  Дата: {format_datetime(added_at)}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "blacklist_add"))
    builder.add(create_button("🗑 Удалить", "blacklist_remove"))
    builder.add(create_button("🔄 Обновить", "admin_blacklist"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "blacklist_add")
@check_bot_admin()
async def blacklist_add(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте ID пользователя и причину через пробел:\n\nПример: 123456789 Спам")
    await state.update_data(action='blacklist_add')
    await state.set_state(MaintenanceStates.waiting_for_user_id)
    await callback.answer()

@dp.callback_query(F.data == "blacklist_remove")
@check_bot_admin()
async def blacklist_remove(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте ID пользователя для удаления из черного списка:\n\nПример: 123456789")
    await state.update_data(action='blacklist_remove')
    await state.set_state(MaintenanceStates.waiting_for_user_id)
    await callback.answer()

@dp.message(MaintenanceStates.waiting_for_user_id)
async def process_blacklist_input(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    action = data.get('action', '')
    if action == 'blacklist_add':
        parts = message.text.split(maxsplit=1)
        if len(parts) < 1:
            await message.answer("❌ Укажите ID пользователя!")
            return
        try:
            target_id = int(parts[0])
            reason = parts[1] if len(parts) > 1 else "Не указана"
            add_to_blacklist(target_id, reason, message.from_user.id)
            await message.answer(i18n.get('add_to_blacklist', 'ru', name=str(target_id), user_id=target_id, reason=reason))
        except ValueError:
            await message.answer("❌ Некорректный ID пользователя!")
    elif action == 'blacklist_remove':
        try:
            target_id = int(message.text.strip())
            if remove_from_blacklist(target_id):
                await message.answer(i18n.get('remove_from_blacklist', 'ru', user_id=target_id))
            else:
                await message.answer(i18n.get('not_blacklisted', 'ru'))
        except ValueError:
            await message.answer("❌ Некорректный ID пользователя!")
    await state.clear()

@dp.callback_query(F.data == "admin_global_bans")
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
        await callback.message.edit_text("✅ Глобальные баны отсутствуют.", reply_markup=get_back_keyboard("admin_panel"))
        await callback.answer()
        return
    text = "⛔ <b>Глобальные баны:</b>\n\n"
    for user_id, reason, moderator_id, banned_at, expires_at in bans:
        expires_text = format_time(expires_at - int(time.time())) if expires_at > 0 else "навсегда"
        text += f"• <code>{user_id}</code>\n  Причина: {safe_html(reason, False)}\n  Модератор: {moderator_id}\n  До: {expires_text}\n  Дата: {format_datetime(banned_at)}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_ban_add"))
    builder.add(create_button("🗑 Снять", "global_ban_remove"))
    builder.add(create_button("🔄 Обновить", "admin_global_bans"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "global_ban_add")
@check_bot_admin()
async def global_ban_add(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте ID пользователя, время и причину:\n\nПример: 123456789 24ч спам")
    await state.update_data(action='global_ban_add')
    await state.set_state(MaintenanceStates.waiting_for_user_id)
    await callback.answer()

@dp.callback_query(F.data == "global_ban_remove")
@check_bot_admin()
async def global_ban_remove(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте ID пользователя для снятия глобального бана:\n\nПример: 123456789")
    await state.update_data(action='global_ban_remove')
    await state.set_state(MaintenanceStates.waiting_for_user_id)
    await callback.answer()

@dp.callback_query(F.data == "admin_global_mutes")
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
        await callback.message.edit_text("✅ Глобальные муты отсутствуют.", reply_markup=get_back_keyboard("admin_panel"))
        await callback.answer()
        return
    text = "🔇 <b>Глобальные муты:</b>\n\n"
    for user_id, reason, moderator_id, muted_at, expires_at in mutes:
        expires_text = format_time(expires_at - int(time.time())) if expires_at > 0 else "навсегда"
        text += f"• <code>{user_id}</code>\n  Причина: {safe_html(reason, False)}\n  Модератор: {moderator_id}\n  До: {expires_text}\n  Дата: {format_datetime(muted_at)}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("➕ Добавить", "global_mute_add"))
    builder.add(create_button("🗑 Снять", "global_mute_remove"))
    builder.add(create_button("🔄 Обновить", "admin_global_mutes"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "global_mute_add")
@check_bot_admin()
async def global_mute_add(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте ID пользователя, время и причину:\n\nПример: 123456789 24ч спам")
    await state.update_data(action='global_mute_add')
    await state.set_state(MaintenanceStates.waiting_for_user_id)
    await callback.answer()

@dp.callback_query(F.data == "global_mute_remove")
@check_bot_admin()
async def global_mute_remove(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("📝 Отправьте ID пользователя для снятия глобального мута:\n\nПример: 123456789")
    await state.update_data(action='global_mute_remove')
    await state.set_state(MaintenanceStates.waiting_for_user_id)
    await callback.answer()

@dp.message(MaintenanceStates.waiting_for_user_id)
async def process_global_input(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    action = data.get('action', '')
    if action == 'global_ban_add':
        parts = message.text.split(maxsplit=2)
        if len(parts) < 1:
            await message.answer("❌ Укажите ID пользователя!")
            return
        try:
            target_id = int(parts[0])
            duration = 0
            reason = "Не указана"
            if len(parts) > 1:
                time_match = re.search(r'(\d+)([мчд]|мин|час|дн)', parts[1])
                if time_match:
                    value = int(time_match.group(1))
                    unit = time_match.group(2)
                    if unit in ['м', 'мин']:
                        duration = value * 60
                    elif unit in ['ч', 'час']:
                        duration = value * 3600
                    elif unit in ['д', 'дн']:
                        duration = value * 86400
                    reason = parts[2] if len(parts) > 2 else "Не указана"
                else:
                    reason = parts[1] if len(parts) > 1 else "Не указана"
            add_global_ban(target_id, reason, message.from_user.id, duration)
            await apply_global_ban(target_id, reason, duration)
            duration_text = format_time(duration) if duration > 0 else "навсегда"
            await message.answer(i18n.get('global_ban_success', 'ru', name=str(target_id), user_id=target_id, reason=reason, duration=duration_text))
        except ValueError:
            await message.answer("❌ Некорректный ID пользователя!")
    elif action == 'global_ban_remove':
        try:
            target_id = int(message.text.strip())
            if remove_global_ban(target_id):
                await message.answer(i18n.get('global_unban_success', 'ru', user_id=target_id))
            else:
                await message.answer(f"❌ Пользователь {target_id} не найден в глобальных банах")
        except ValueError:
            await message.answer("❌ Некорректный ID пользователя!")
    elif action == 'global_mute_add':
        parts = message.text.split(maxsplit=2)
        if len(parts) < 1:
            await message.answer("❌ Укажите ID пользователя!")
            return
        try:
            target_id = int(parts[0])
            duration = 0
            reason = "Не указана"
            if len(parts) > 1:
                time_match = re.search(r'(\d+)([мчд]|мин|час|дн)', parts[1])
                if time_match:
                    value = int(time_match.group(1))
                    unit = time_match.group(2)
                    if unit in ['м', 'мин']:
                        duration = value * 60
                    elif unit in ['ч', 'час']:
                        duration = value * 3600
                    elif unit in ['д', 'дн']:
                        duration = value * 86400
                    reason = parts[2] if len(parts) > 2 else "Не указана"
                else:
                    reason = parts[1] if len(parts) > 1 else "Не указана"
            add_global_mute(target_id, reason, message.from_user.id, duration)
            await apply_global_mute(target_id, reason, duration)
            duration_text = format_time(duration) if duration > 0 else "навсегда"
            await message.answer(i18n.get('global_mute_success', 'ru', name=str(target_id), user_id=target_id, reason=reason, duration=duration_text))
        except ValueError:
            await message.answer("❌ Некорректный ID пользователя!")
    elif action == 'global_mute_remove':
        try:
            target_id = int(message.text.strip())
            if remove_global_mute(target_id):
                await message.answer(i18n.get('global_unmute_success', 'ru', user_id=target_id))
            else:
                await message.answer(f"❌ Пользователь {target_id} не найден в глобальных мутах")
        except ValueError:
            await message.answer("❌ Некорректный ID пользователя!")
    await state.clear()

# ============ BROADCAST HANDLERS ============

@dp.callback_query(F.data == "admin_broadcast")
@check_bot_admin()
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📱 В группы", "broadcast_groups"))
    builder.add(create_button("💬 В ЛС пользователям", "broadcast_users"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(1)
    await callback.message.edit_text("📢 <b>Куда отправить рассылку?</b>", reply_markup=builder.as_markup())
    await state.set_state(AdminBroadcastStates.waiting_for_target)
    await callback.answer()

@dp.callback_query(F.data.in_(["broadcast_groups", "broadcast_users"]))
@check_bot_admin()
async def broadcast_target(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    target = "groups" if callback.data == "broadcast_groups" else "users"
    await state.update_data(broadcast_target=target)
    await callback.message.edit_text("📝 Отправьте сообщение для рассылки (текст, фото, видео, GIF, стикер, документ)")
    await state.set_state(AdminBroadcastStates.waiting_for_message)
    await callback.answer()

@dp.message(AdminBroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Доступ запрещён!")
        await state.clear()
        return
    data = await state.get_data()
    target = data.get('broadcast_target', 'groups')
    broadcast_data = {
        'type': 'text',
        'text': message.text or message.caption or "",
        'file_id': None
    }
    if message.photo:
        broadcast_data['type'] = 'photo'
        broadcast_data['file_id'] = message.photo[-1].file_id
    elif message.video:
        broadcast_data['type'] = 'video'
        broadcast_data['file_id'] = message.video.file_id
    elif message.animation:
        broadcast_data['type'] = 'animation'
        broadcast_data['file_id'] = message.animation.file_id
    elif message.sticker:
        broadcast_data['type'] = 'sticker'
        broadcast_data['file_id'] = message.sticker.file_id
    elif message.document:
        broadcast_data['type'] = 'document'
        broadcast_data['file_id'] = message.document.file_id
    await state.update_data(broadcast_message=broadcast_data)
    with db.get_connection() as conn:
        c = conn.cursor()
        if target == 'groups':
            c.execute('SELECT chat_id FROM group_rules')
            recipients = [row[0] for row in c.fetchall()]
        else:
            c.execute('SELECT user_id FROM global_users')
            recipients = [row[0] for row in c.fetchall()]
    if not recipients:
        await message.answer("❌ Нет получателей для рассылки!")
        await state.clear()
        return
    await state.update_data(broadcast_recipients=recipients)
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('broadcast_start', 'ru'), "broadcast_start"))
    builder.add(create_button(i18n.get('broadcast_cancel', 'ru'), "broadcast_cancel"))
    await message.answer(i18n.get('broadcast_confirm', 'ru', count=len(recipients)), reply_markup=builder.as_markup())
    await state.set_state(AdminBroadcastStates.waiting_for_message)

@dp.callback_query(F.data == "broadcast_start")
@check_bot_admin()
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    data = await state.get_data()
    recipients = data.get('broadcast_recipients', [])
    broadcast_data = data.get('broadcast_message', {})
    if not recipients:
        await callback.message.edit_text("❌ Нет получателей!")
        await state.clear()
        return
    await callback.message.edit_text("📤 Начинаю рассылку...")
    sent = 0
    failed = 0
    for i, recipient in enumerate(recipients):
        try:
            if broadcast_data['type'] == 'text':
                await bot.send_message(recipient, broadcast_data['text'], parse_mode="HTML")
            elif broadcast_data['type'] == 'photo':
                await bot.send_photo(recipient, broadcast_data['file_id'], caption=broadcast_data['text'], parse_mode="HTML")
            elif broadcast_data['type'] == 'video':
                await bot.send_video(recipient, broadcast_data['file_id'], caption=broadcast_data['text'], parse_mode="HTML")
            elif broadcast_data['type'] == 'animation':
                await bot.send_animation(recipient, broadcast_data['file_id'], caption=broadcast_data['text'], parse_mode="HTML")
            elif broadcast_data['type'] == 'sticker':
                await bot.send_sticker(recipient, broadcast_data['file_id'])
            elif broadcast_data['type'] == 'document':
                await bot.send_document(recipient, broadcast_data['file_id'], caption=broadcast_data['text'], parse_mode="HTML")
            sent += 1
        except:
            failed += 1
        if (i + 1) % 10 == 0:
            await callback.message.edit_text(i18n.get('broadcast_progress', 'ru', current=i+1, total=len(recipients), sent=sent, failed=failed))
        await asyncio.sleep(0.05)
    await callback.message.edit_text(i18n.get('broadcast_done', 'ru', sent=sent, failed=failed))
    await state.clear()

@dp.callback_query(F.data == "broadcast_cancel")
@check_bot_admin()
async def broadcast_cancel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text(i18n.get('broadcast_cancelled', 'ru'))
    await state.clear()

# ============ ADMIN STATS, GROUPS, USERS, LOGS ============

@dp.callback_query(F.data == "admin_stats")
@check_bot_admin()
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM group_rules')
        groups = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM global_users')
        users = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM auto_responses')
        triggers = c.fetchone()[0] or 0
    text = f"📊 <b>Статистика бота</b>\n\n📱 Групп: {groups}\n👥 Пользователей: {users}\n🤖 Триггеров: {triggers}"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🔄 Обновить", "admin_stats"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_groups")
@check_bot_admin()
async def admin_groups(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    groups = db.get_all_chats()
    text = "📱 <b>Группы:</b>\n\n"
    for chat_id, title, username in groups[:20]:
        text += f"• {safe_html(title, False) or 'Без названия'} | ID: <code>{chat_id}</code>\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
@check_bot_admin()
async def admin_users(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT full_name, global_id, first_seen FROM global_users ORDER BY first_seen DESC LIMIT 20')
        users = c.fetchall()
    text = "👥 <b>Последние пользователи:</b>\n\n"
    for name, gid, ts in users:
        text += f"• {safe_html(name, False)}\n  ID: <code>{gid}</code> | {format_datetime(ts)}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_logs")
@check_bot_admin()
async def admin_logs(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT moderator_name, action, target_name, reason, timestamp FROM moderator_logs ORDER BY timestamp DESC LIMIT 20')
        logs = c.fetchall()
    text = "📋 <b>Последние действия:</b>\n\n"
    for name, action, target, reason, ts in logs:
        text += f"• {safe_html(name, False)} {action} {safe_html(target, False)}\n  {safe_html(reason, False)} | {format_datetime(ts)}\n\n"
    builder = InlineKeyboardBuilder()
    builder.add(create_button("🗑 Очистить", "admin_logs_clear"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(2)
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_logs_clear")
@check_bot_admin()
async def admin_logs_clear(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('DELETE FROM moderator_logs')
        conn.commit()
    await callback.answer("✅ Логи очищены!", show_alert=True)
    await admin_logs(callback)

# ============ ADMIN BACKUP ============

@dp.callback_query(F.data == "admin_backup")
@check_bot_admin()
async def admin_backup(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2("puls_manager.db", backup_name)
        await callback.message.answer_document(FSInputFile(backup_name), caption=i18n.get('backup_created', 'ru'))
        os.remove(backup_name)
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

# ============ ADMIN CUSTOMIZATION ============

@dp.callback_query(F.data == "admin_custom")
@check_bot_admin()
async def admin_custom(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    builder.add(create_button("📝 Тексты сообщений", "admin_custom_texts"))
    builder.add(create_button("🖼 Фото сообщений", "admin_custom_photos"))
    builder.add(create_button("🔄 Сбросить всё", "admin_custom_reset_all"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_panel"))
    builder.adjust(1)
    await callback.message.edit_text("🎨 <b>Кастомизация бота</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_custom_texts")
@check_bot_admin()
async def admin_custom_texts(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    for key in list(customization.templates.keys())[:20]:
        builder.add(create_button(key, f"edit_text_{key}"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_custom"))
    builder.adjust(1)
    await callback.message.edit_text(i18n.get('select_message', 'ru'), reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_text_"))
@check_bot_admin()
async def edit_text(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    msg_key = callback.data.replace("edit_text_", "")
    await state.update_data(edit_msg_key=msg_key)
    await callback.message.edit_text(f"📝 Отправьте новый текст для <code>{msg_key}</code>\n\nТекущий:\n{customization.get_template(msg_key).get_text()}", parse_mode="HTML")
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
        await message.answer("❌ Ошибка!")
        await state.clear()
        return
    new_text = message.html_text.strip()
    if not new_text:
        await message.answer("❌ Текст не может быть пустым!")
        return
    template = customization.get_template(msg_key)
    if template:
        template.set_custom(text=new_text)
        db.save_custom_message(msg_key, text=new_text)
        await message.answer(i18n.get('custom_text_updated', 'ru'))
        await add_premium_reaction(message, "✅")
    await state.clear()

@dp.callback_query(F.data == "admin_custom_photos")
@check_bot_admin()
async def admin_custom_photos(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    builder = InlineKeyboardBuilder()
    for key, template in customization.templates.items():
        if template.get_photo():
            builder.add(create_button(key, f"edit_photo_{key}"))
    builder.add(create_button(i18n.get('back_button', 'ru'), "admin_custom"))
    builder.adjust(1)
    await callback.message.edit_text(i18n.get('select_message', 'ru'), reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_photo_"))
@check_bot_admin()
async def edit_photo(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    msg_key = callback.data.replace("edit_photo_", "")
    await state.update_data(edit_photo_key=msg_key)
    await callback.message.edit_text(f"🖼 Отправьте новое фото для <code>{msg_key}</code>", parse_mode="HTML")
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
        await message.answer("❌ Ошибка!")
        await state.clear()
        return
    photo_id = message.photo[-1].file_id
    template = customization.get_template(msg_key)
    if template:
        template.set_custom(photo=photo_id)
        db.save_custom_message(msg_key, photo=photo_id)
        await message.answer(i18n.get('custom_photo_updated', 'ru'))
        await add_premium_reaction(message, "✅")
    await state.clear()

@dp.message(CustomMessageStates.waiting_for_new_photo)
async def process_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Отправьте фото!")

@dp.callback_query(F.data == "admin_custom_reset_all")
@check_bot_admin()
async def admin_custom_reset_all(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    for key, template in customization.templates.items():
        template.reset()
        db.reset_custom_message(key)
    await callback.answer(i18n.get('custom_reset_all', 'ru'), show_alert=True)
    await admin_custom(callback)

# ============ ADMIN MAINTENANCE & SHUTDOWN ============

@dp.callback_query(F.data == "admin_maintenance")
@check_bot_admin()
async def admin_maintenance(callback: CallbackQuery):
    global technical_maintenance
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    technical_maintenance = not technical_maintenance
    status = "Включен" if technical_maintenance else "Выключен"
    await callback.answer(f"🛠 Режим техработ {status}!", show_alert=True)
    await admin_panel(callback, None)

@dp.callback_query(F.data == "admin_shutdown")
@check_bot_admin()
async def admin_shutdown(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Доступ запрещён!", show_alert=True)
        return
    await callback.message.edit_text("🛑 Бот остановлен администратором")
    await callback.answer()

# ============ AUTO RESPONSE HANDLER ============

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    chat_id = message.chat.id
    text = message.text or message.caption or ""
    if not text:
        return
    responses = db.get_auto_responses(chat_id)
    for trigger, response, response_type, media_id in responses:
        if trigger.lower() in text.lower():
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
            except:
                pass

# ============ MAIN ============

async def main():
    dp.message.middleware(AntiFlenterddleware())
    dp.message.middleware(MaintenanceMiddleware())
    dp.callback_query.middleware(MaintenanceMiddleware())
    
    asyncio.create_task(rules_broadcast_task())
    asyncio.create_task(reset_periodic_counters())
    asyncio.create_task(clean_old_messages())
    asyncio.create_task(clean_expired_bans_mutes())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
