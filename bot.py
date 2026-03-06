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

# Московское время (UTC+3)
MOSCOW_TZ = timezone(timedelta(hours=3))

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

# Таблица для личных языков пользователей
user_languages = {}

# Готовый текст правил с expandable цитатой (без <br>)
DEFAULT_RULES = """
Chat Rules:

<blockquote expandable>
1. No spamming, flooding, or writing in all caps.
2. Respect other group members.
3. Advertising, links, and calls to action - only with admin permission.
4. No insults, threats, discrimination of any kind.
5. Do not distribute prohibited content (porn, violence, drugs, etc.).
6. Administration has the right to mute/ban without explanation.
7. If you don't agree with the rules - leave the group.
8. If rules are violated - contact admins in PM.
</blockquote>

Thank you for your attention and enjoy your communication!
"""

# Функция для получения перевода
def get_text(chat_id: int = None, user_id: int = None, key: str = None, **kwargs) -> str:
    """
    Универсальная функция для получения перевода.
    Приоритет: язык группы (если chat_id) -> личный язык пользователя -> английский
    """
    lang = 'en'  # по умолчанию английский
    
    if chat_id:
        # Пытаемся получить язык группы
        try:
            with db.get_connection() as conn:
                c = conn.cursor()
                c.execute('SELECT language FROM group_rules WHERE chat_id = ?', (chat_id,))
                result = c.fetchone()
                if result:
                    lang = result[0]
        except:
            pass
    
    if user_id and lang == 'en':  # если группа не дала язык, пробуем личный
        lang = user_languages.get(user_id, 'en')
    
    # Получаем перевод
    text = TRANSLATIONS.get(lang, TRANSLATIONS['en']).get(key, key)
    
    # Форматируем, если есть параметры
    if kwargs:
        try:
            text = text.format(**kwargs)
        except:
            pass
    
    return text

# Словарь переводов (ПОЛНЫЙ)
TRANSLATIONS = {
    'en': {
        # ========== ПРИВЕТСТВИЯ И ОБЩЕЕ ==========
        'welcome': "Welcome, <b>{name}</b>!",
        'no_username': "none",
        'username': "Username",
        'id': "ID",
        'joined': "Joined",
        'last_active': "Last active",
        'place_in_top': "Place in top",
        'user_id': "User ID",
        'first_seen': "First seen",
        'messages_count': "Messages",
        
        # ========== ПОДТВЕРЖДЕНИЕ ==========
        'confirm_not_bot': "I'm not a bot",
        'agree_rules': "✅ I agree with the rules",
        'muted_forever': "You are muted **forever** until you confirm the rules",
        'go_to_pm': "📜 Go to PM",
        'rules_sent': "Rules sent to private messages",
        'confirmed_not_bot': "✅ {name} confirmed they're not a bot and can now write in the chat.",
        'confirmed_rules': "✅ {name} agreed to the rules and can now write in the chat.",
        'thanks_confirmation': "Thank you for confirmation! You can now write in the chat.",
        'need_confirm_both': "You need to complete TWO steps:\n1. Confirm you're not a bot\n2. Read and agree to the rules",
        'step_1_completed': "✅ Step 1 completed! Now complete step 2: agree to the rules.",
        'step_2_completed': "✅ Step 2 completed! Now complete step 1: confirm you're not a bot.",
        'confirmation_disabled': "✅ Confirmation is disabled. New members can write immediately.",
        
        # ========== СООБЩЕНИЯ ПРИ ВХОДЕ ==========
        'user_joined': "👋 <b>{name}</b> joined the chat!",
        'need_confirm_rules': "You are muted **forever** until you confirm the rules.\nGo to bot's PM, read the rules and confirm — mute will be removed.",
        'need_confirm_not_bot': "You are muted **forever** until you confirm you're not a bot.\nClick the button below — mute will be removed.",
        
        # ========== СТАТИСТИКА ==========
        'stats_empty': "📊 No statistics yet",
        'stats_updating': "📊 Statistics are updating, wait 5–10 seconds",
        'top_active': "🏆 Top active (total messages):",
        'profile': "Profile {name}",
        'per_day': "Per day",
        'per_week': "Per week",
        'per_month': "Per month",
        'total': "Total",
        'messages': "messages",
        
        # ========== НАСТРОЙКИ ==========
        'language': "🌐 Bot language",
        'current_language': "Current language: {lang}",
        'choose_language': "Choose bot language for this group:",
        'language_changed': "✅ Language changed to {lang}",
        'russian': "🇷🇺 Russian",
        'ukrainian': "🇺🇦 Ukrainian",
        'english': "🇬🇧 English",
        'group_language': "🌐 Group language",
        'personal_language': "👤 Personal language",
        
        # ========== КОМАНДЫ ==========
        'pulse': "pulse",
        'pong': "pong",
        'ping': "Ping: {ping} ms\nResponse time: {response} sec",
        'start': "Start",
        'main_menu': "Main menu",
        
        # ========== ВЫХОД ==========
        'user_left': "👋 User {name} left the chat.",
        
        # ========== ОШИБКИ ==========
        'error_no_group': "❌ Select a group first!",
        'error_not_creator': "❌ You are not the creator of this group!",
        'error_not_yours': "⚠️ This is not your confirmation!",
        'error_no_rules': "❌ No rules set in this chat yet.",
        'error_rules_short': "❌ Rules are too short! Send a more meaningful text.",
        'group_only': "❌ This command works only in groups!",
        'pm_only': "❌ This command works only in private messages!",
        'rules_not_set': "❌ Rules are not set in this chat!",
        'group_not_found': "❌ Group not found!",
        'user_not_found': "❌ User not found!",
        
        # ========== АВТО-РАССЫЛКА ==========
        'rules_reminder': "📢 Reminder: chat rules",
        
        # ========== ГЛАВНОЕ МЕНЮ ==========
        'about': "📋 About",
        'help': "🆘 Help",
        'add_to_group': "➕ Add to group",
        'group_manage': "⚙️ Group management",
        'back': "◀️ Back",
        'language_menu': "🌐 Language",
        
        # ========== ПРИВЯЗКА ГРУППЫ ==========
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
        'select_group': "📱 Select a group to configure:",
        
        # ========== ГРУППА РЕПОРТОВ ==========
        'report_group': "📋 Report group",
        'current_report_group': "📋 Current report group: {group}",
        'set_report_group': "Set report group",
        'report_group_info': "Select a group where violation logs will be sent:",
        'report_group_set': "✅ Report group set successfully!",
        'report_group_removed': "❌ Report group removed.",
        'violation_report': "🚫 Violation Report",
        'user': "User",
        'reason': "Reason",
        'punishment': "Punishment",
        'message_link': "🔗 Go to message",
        'time': "Time",
        'violations_list': "📋 Recent violations",
        'no_violations': "No violations recorded yet.",
        
        # ========== ПРОФИЛЬ ==========
        'profile_info': "👤 User Profile",
        'profile_stats': "Statistics for {name}:",
        'reply_to_user': "Reply to a user's message with /profile to see their stats",
        
        # ========== АВТООТВЕТЧИК ==========
        'auto_responder': "🤖 Auto responder",
        'auto_responder_empty': "Auto responder is empty.\nAdd your first keyword and response.",
        'auto_responder_list': "🤖 Auto responder:\n\n",
        'add_trigger': "➕ Add trigger",
        'remove_trigger': "🗑 Remove trigger",
        'enter_trigger': "Enter the keyword (trigger):",
        'enter_response': "Enter the response text:",
        'trigger_added': "✅ Trigger '{trigger}' added successfully!",
        'trigger_removed': "✅ Trigger '{trigger}' removed successfully!",
        'select_trigger_to_remove': "Select a trigger to remove:",
        'trigger_exists': "❌ Trigger '{trigger}' already exists!",
        
        # ========== ССЫЛКИ И УПОМИНАНИЯ ==========
        'links_mentions': "🔗 Links and mentions",
        'links_enabled': "Link filter: {status}",
        'mentions_enabled': "Mention filter: {status}",
        'links_punish': "Punishment: {punish}",
        'max_mentions': "Max mentions: {count} per {window} sec",
        'toggle_links': "Toggle link filter",
        'toggle_mentions': "Toggle mention filter",
        'set_links_punish': "Set link punishment",
        'set_mentions_punish': "Set mention punishment",
        'set_max_mentions': "Set max mentions",
        'set_mention_window': "Set mention window",
        'choose_punish': "Choose punishment:",
        'enter_duration': "Enter duration in minutes (0 = forever):",
        'enter_max_mentions': "Enter max mentions per minute:",
        'enter_mention_window': "Enter mention window in seconds:",
        'punish_set': "✅ Punishment set to {punish}",
        'max_mentions_set': "✅ Max mentions set to {count}",
        'mention_window_set': "✅ Mention window set to {window} sec",
        'filter_enabled': "✅ Filter enabled",
        'filter_disabled': "❌ Filter disabled",
        'punishment_saved': "✅ <b>Settings saved!</b>\n\nPunishment: {punish}\nDuration: {duration}",
        
        # ========== НАСТРОЙКИ ПОДТВЕРЖДЕНИЯ ==========
        'confirmation_settings': "✅ Confirmation settings",
        'confirmation_type': "Confirmation type: {type}",
        'disabled': "🚫 Disabled",
        'not_bot_only': "🤖 Not bot only",
        'rules_only': "📜 Rules only",
        'both_steps': "2️⃣ Both steps",
        'set_confirmation_type': "Set confirmation type",
        'confirmation_updated': "✅ Confirmation settings updated!",
        'cant_use_rules': "❌ Cannot select this option because no rules are set in this group! Please set rules first.",
        'cant_use_both': "❌ Cannot select 'Both steps' because no rules are set in this group! Please set rules first.",
        'need_rules_first': "⚠️ No rules set. This option requires rules.",
        
        # ========== УПРАВЛЕНИЕ ПРАВИЛАМИ ==========
        'rules_management': "📝 Rules management",
        'set_rules': "📝 Set rules",
        'set_default_rules': "📋 Set default rules",
        'edit_rules': "✏️ Edit rules",
        'delete_rules': "🗑 Delete rules",
        'toggle_rules': "🔄 Enable/Disable rules",
        'rules_enabled': "✅ Rules are enabled",
        'rules_disabled': "❌ Rules are disabled",
        'rules_deleted': "✅ Rules deleted successfully!",
        'rules_enabled_status': "Rules enabled: {status}",
        'enter_new_rules': "📝 Send the new rules text:",
        'rules_updated': "✅ Rules updated successfully!",
        'rules_set': "✅ Rules set successfully!",
        'default_rules_set': "✅ Default rules set successfully!",
        
        # ========== КНОПКИ ==========
        'status_enabled': "✅ Enabled",
        'status_disabled': "❌ Disabled",
        'set_text': "📝 Set text",
        'set_photo': "🖼 Set photo",
        'view': "👁 View",
        'rules': "📜 Rules",
        'my_stats': "📊 My stats",
        'top_active_btn': "🏆 Top active",
        'interval': "⏱ Interval",
        'limit': "📊 Limit",
        'window': "⏱ Window",
        'warn_count': "⚠️ Warn count",
        'first_punish': "🔇 First",
        'repeat_punish': "🔊 Repeat",
        'enable': "✅ Enable",
        'disable': "❌ Disable",
        'duration': "Duration",
        'minutes': "minutes",
        'forever': "forever",
    },
    'ru': {
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
        'language': "🌐 Язык бота",
        'current_language': "Текущий язык: {lang}",
        'choose_language': "Выберите язык бота для этой группы:",
        'language_changed': "✅ Язык изменён на {lang}",
        'russian': "🇷🇺 Русский",
        'ukrainian': "🇺🇦 Українська",
        'english': "🇬🇧 English",
        'group_language': "🌐 Язык группы",
        'personal_language': "👤 Личный язык",
        
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
        
        # ========== АВТО-РАССЫЛКА ==========
        'rules_reminder': "📢 Напоминание правил чата",
        
        # ========== ГЛАВНОЕ МЕНЮ ==========
        'about': "📋 О боте",
        'help': "🆘 Помощь",
        'add_to_group': "➕ Добавить в группу",
        'group_manage': "⚙️ Управление группой",
        'back': "◀️ Назад",
        'language_menu': "🌐 Язык",
        
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
        'mentions_enabled': "Фильтр упоминаний: {status}",
        'links_punish': "Наказание: {punish}",
        'max_mentions': "Макс упоминаний: {count} за {window} сек",
        'toggle_links': "Вкл/выкл фильтр ссылок",
        'toggle_mentions': "Вкл/выкл фильтр упоминаний",
        'set_links_punish': "Установить наказание за ссылки",
        'set_mentions_punish': "Установить наказание за упоминания",
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
    },
    'uk': {
        # ========== ПРИВІТАННЯ ТА ЗАГАЛЬНЕ ==========
        'welcome': "Ласкаво просимо, <b>{name}</b>!",
        'no_username': "немає",
        'username': "Юзернейм",
        'id': "ID",
        'joined': "Увійшов",
        'last_active': "Остання активність",
        'place_in_top': "Місце в топі",
        'user_id': "ID користувача",
        'first_seen': "Вперше помічений",
        'messages_count': "Повідомлень",
        
        # ========== ПІДТВЕРДЖЕННЯ ==========
        'confirm_not_bot': "Я не бот",
        'agree_rules': "✅ Згоден з правилами",
        'muted_forever': "Ви зам'ючені **назавжди**, поки не підтвердите правила",
        'go_to_pm': "📜 Перейти в ЛС",
        'rules_sent': "Правила надіслано в особисті повідомлення",
        'confirmed_not_bot': "✅ {name} підтвердив, що не бот і тепер може писати в чат.",
        'confirmed_rules': "✅ {name} погодився з правилами і тепер може писати в чат.",
        'thanks_confirmation': "Дякую за підтвердження! Тепер ви можете писати в чат.",
        'need_confirm_both': "Вам потрібно виконати ДВА кроки:\n1. Підтвердити, що ви не бот\n2. Прочитати та погодитися з правилами",
        'step_1_completed': "✅ Крок 1 виконано! Тепер виконайте крок 2: погодьтеся з правилами.",
        'step_2_completed': "✅ Крок 2 виконано! Тепер виконайте крок 1: підтвердьте, що ви не бот.",
        'confirmation_disabled': "✅ Підтвердження вимкнено. Нові учасники можуть писати відразу.",
        
        # ========== ПОВІДОМЛЕННЯ ПРИ ВХОДІ ==========
        'user_joined': "👋 <b>{name}</b> зайшов у чат!",
        'need_confirm_rules': "Ви зам'ючені **назавжди**, поки не підтвердите правила.\nПерейдіть в ЛС бота, прочитайте правила і підтвердьте згоду — мут зніметься.",
        'need_confirm_not_bot': "Ви зам'ючені **назавжди**, поки не підтвердите, що ви не бот.\nНатисніть кнопку нижче — мут зніметься.",
        
        # ========== СТАТИСТИКА ==========
        'stats_empty': "📊 Статистика ще не зібрана",
        'stats_updating': "📊 Статистика оновлюється, зачекайте 5–10 секунд",
        'top_active': "🏆 Топ активних (всього повідомлень):",
        'profile': "Профіль {name}",
        'per_day': "За день",
        'per_week': "За тиждень",
        'per_month': "За місяць",
        'total': "Всього",
        'messages': "повідомлень",
        
        # ========== НАЛАШТУВАННЯ ==========
        'language': "🌐 Мова бота",
        'current_language': "Поточна мова: {lang}",
        'choose_language': "Виберіть мову бота для цієї групи:",
        'language_changed': "✅ Мову змінено на {lang}",
        'russian': "🇷🇺 Русский",
        'ukrainian': "🇺🇦 Українська",
        'english': "🇬🇧 English",
        'group_language': "🌐 Мова групи",
        'personal_language': "👤 Особиста мова",
        
        # ========== КОМАНДИ ==========
        'pulse': "пульс",
        'pong': "понг",
        'ping': "Пінг: {ping} мс\nЧас відповіді: {response} сек",
        'start': "Старт",
        'main_menu': "Головне меню",
        
        # ========== ВИХІД ==========
        'user_left': "👋 Користувач {name} вийшов з чату.",
        
        # ========== ПОМИЛКИ ==========
        'error_no_group': "❌ Спочатку виберіть групу!",
        'error_not_creator': "❌ Ви не є творцем цієї групи!",
        'error_not_yours': "⚠️ Це не ваше підтвердження!",
        'error_no_rules': "❌ У цьому чаті ще не встановлені правила.",
        'error_rules_short': "❌ Правила занадто короткі! Відправте більш змістовний текст.",
        'group_only': "❌ Ця команда працює тільки в групах!",
        'pm_only': "❌ Ця команда працює тільки в особистих повідомленнях!",
        'rules_not_set': "❌ У цьому чаті не встановлені правила!",
        'group_not_found': "❌ Група не знайдена!",
        'user_not_found': "❌ Користувач не знайдений!",
        
        # ========== АВТОРОЗСИЛКА ==========
        'rules_reminder': "📢 Нагадування правил чату",
        
        # ========== ГОЛОВНЕ МЕНЮ ==========
        'about': "📋 Про бота",
        'help': "🆘 Допомога",
        'add_to_group': "➕ Додати в групу",
        'group_manage': "⚙️ Керування групою",
        'back': "◀️ Назад",
        'language_menu': "🌐 Мова",
        
        # ========== ПРИВ'ЯЗКА ГРУПИ ==========
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
        'select_group': "📱 Виберіть групу для налаштування:",
        
        # ========== ГРУПА РЕПОРТІВ ==========
        'report_group': "📋 Група репортів",
        'current_report_group': "📋 Поточна група репортів: {group}",
        'set_report_group': "Встановити групу репортів",
        'report_group_info': "Виберіть групу, куди будуть надсилатися логи порушень:",
        'report_group_set': "✅ Групу репортів успішно встановлено!",
        'report_group_removed': "❌ Групу репортів видалено.",
        'violation_report': "🚫 Звіт про порушення",
        'user': "Користувач",
        'reason': "Причина",
        'punishment': "Покарання",
        'message_link': "🔗 Перейти до повідомлення",
        'time': "Час",
        'violations_list': "📋 Останні порушення",
        'no_violations': "Порушень поки що немає.",
        
        # ========== ПРОФІЛЬ ==========
        'profile_info': "👤 Профіль користувача",
        'profile_stats': "Статистика для {name}:",
        'reply_to_user': "Дайте відповідь на повідомлення користувача командою /profile щоб побачити його статистику",
        
        # ========== АВТОВІДПОВІДАЧ ==========
        'auto_responder': "🤖 Автовідповідач",
        'auto_responder_empty': "Автовідповідач порожній.\nДодайте перше ключове слово та відповідь.",
        'auto_responder_list': "🤖 Автовідповідач:\n\n",
        'add_trigger': "➕ Додати тригер",
        'remove_trigger': "🗑 Видалити тригер",
        'enter_trigger': "Введіть ключове слово (тригер):",
        'enter_response': "Введіть текст відповіді:",
        'trigger_added': "✅ Тригер '{trigger}' успішно додано!",
        'trigger_removed': "✅ Тригер '{trigger}' успішно видалено!",
        'select_trigger_to_remove': "Виберіть тригер для видалення:",
        'trigger_exists': "❌ Тригер '{trigger}' вже існує!",
        
        # ========== ПОСИЛАННЯ ТА ЗГАДУВАННЯ ==========
        'links_mentions': "🔗 Посилання та згадування",
        'links_enabled': "Фільтр посилань: {status}",
        'mentions_enabled': "Фільтр згадувань: {status}",
        'links_punish': "Покарання: {punish}",
        'max_mentions': "Макс згадувань: {count} за {window} сек",
        'toggle_links': "Вкл/викл фільтр посилань",
        'toggle_mentions': "Вкл/викл фільтр згадувань",
        'set_links_punish': "Встановити покарання за посилання",
        'set_mentions_punish': "Встановити покарання за згадування",
        'set_max_mentions': "Встановити макс згадувань",
        'set_mention_window': "Встановити вікно згадувань",
        'choose_punish': "Виберіть покарання:",
        'enter_duration': "Введіть тривалість у хвилинах (0 = назавжди):",
        'enter_max_mentions': "Введіть максимальну кількість згадувань за хвилину:",
        'enter_mention_window': "Введіть вікно згадувань у секундах:",
        'punish_set': "✅ Покарання встановлено: {punish}",
        'max_mentions_set': "✅ Макс згадувань встановлено: {count}",
        'mention_window_set': "✅ Вікно згадувань встановлено: {window} сек",
        'filter_enabled': "✅ Фільтр увімкнено",
        'filter_disabled': "❌ Фільтр вимкнено",
        'punishment_saved': "✅ <b>Налаштування збережено!</b>\n\nПокарання: {punish}\nТривалість: {duration}",
        
        # ========== НАЛАШТУВАННЯ ПІДТВЕРДЖЕННЯ ==========
        'confirmation_settings': "✅ Налаштування підтвердження",
        'confirmation_type': "Тип підтвердження: {type}",
        'disabled': "🚫 Вимкнено",
        'not_bot_only': "🤖 Тільки не бот",
        'rules_only': "📜 Тільки правила",
        'both_steps': "2️⃣ Обидва кроки",
        'set_confirmation_type': "Встановити тип підтвердження",
        'confirmation_updated': "✅ Налаштування підтвердження оновлено!",
        'cant_use_rules': "❌ Не можна вибрати цей варіант, тому що в групі не встановлені правила! Спочатку встановіть правила.",
        'cant_use_both': "❌ Не можна вибрати 'Обидва кроки', тому що в групі не встановлені правила! Спочатку встановіть правила.",
        'need_rules_first': "⚠️ Правила не встановлені. Цей варіант вимагає наявності правил.",
        
        # ========== УПРАВЛІННЯ ПРАВИЛАМИ ==========
        'rules_management': "📝 Управління правилами",
        'set_rules': "📝 Встановити правила",
        'set_default_rules': "📋 Встановити готові правила",
        'edit_rules': "✏️ Змінити правила",
        'delete_rules': "🗑 Видалити правила",
        'toggle_rules': "🔄 Вкл/Викл правила",
        'rules_enabled': "✅ Правила включені",
        'rules_disabled': "❌ Правила виключені",
        'rules_deleted': "✅ Правила успішно видалені!",
        'rules_enabled_status': "Правила включені: {status}",
        'enter_new_rules': "📝 Надішліть новий текст правил:",
        'rules_updated': "✅ Правила успішно оновлені!",
        'rules_set': "✅ Правила успішно встановлені!",
        'default_rules_set': "✅ Готові правила успішно встановлені!",
        
        # ========== КНОПКИ ==========
        'status_enabled': "✅ Увімкнено",
        'status_disabled': "❌ Вимкнено",
        'set_text': "📝 Встановити текст",
        'set_photo': "🖼 Встановити фото",
        'view': "👁 Переглянути",
        'rules': "📜 Правила",
        'my_stats': "📊 Моя статистика",
        'top_active_btn': "🏆 Топ активних",
        'interval': "⏱ Інтервал",
        'limit': "📊 Ліміт",
        'window': "⏱ Вікно",
        'warn_count': "⚠️ Попереджень",
        'first_punish': "🔇 Перше",
        'repeat_punish': "🔊 Повторне",
        'enable': "✅ Увімкнути",
        'disable': "❌ Вимкнути",
        'duration': "Тривалість",
        'minutes': "хвилин",
        'forever': "назавжди",
    }
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
            
            # Добавляем поле language, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN language TEXT DEFAULT "en"')
            except sqlite3.OperationalError:
                pass  # колонка уже есть
            
            # Добавляем поле report_group_id, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN report_group_id INTEGER')
            except sqlite3.OperationalError:
                pass  # колонка уже есть
            
            # Добавляем поле confirmation_type, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN confirmation_type TEXT DEFAULT "both"')
            except sqlite3.OperationalError:
                pass  # колонка уже есть
            
            # Добавляем поле rules_enabled, если его нет
            try:
                c.execute('ALTER TABLE group_rules ADD COLUMN rules_enabled INTEGER DEFAULT 1')
            except sqlite3.OperationalError:
                pass  # колонка уже есть
            
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
    
    # Методы для глобальных пользователей
    def get_or_create_global_user(self, user_id: int, username: str, full_name: str) -> str:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT global_id FROM global_users WHERE user_id = ?', (user_id,))
            result = c.fetchone()
            
            if result:
                return result[0]
            
            # Создаем нового пользователя
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
    
    # Методы для правил
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
    
    # Метод для получения языка группы
    def get_group_language(self, chat_id: int) -> str:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT language FROM group_rules WHERE chat_id = ?', (chat_id,))
            result = c.fetchone()
            return result[0] if result else 'en'
    
    # Методы для группы репортов
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
            return result[0] if result else f"Group {report_group_id}"
    
    # Методы для настроек подтверждения
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
    
    # Методы для автоответчика
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
    
    # Методы для согласия с правилами
    def mark_user_confirmed(self, chat_id: int, user_id: int, not_bot: bool = False, rules: bool = False):
        with self.get_connection() as conn:
            c = conn.cursor()
            
            # Проверяем, есть ли запись
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
            return True  # Если подтверждение отключено, считаем что пользователь подтвердил
        
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
            else:  # 'both'
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
    
    # Методы для антифлуда
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
    
    # Методы для логов нарушений
    def log_violation(self, chat_id: int, user_id: int, user_name: str, reason: str, punishment: str, message_id: int, message_link: str):
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO violation_logs 
                         (chat_id, user_id, user_name, reason, punishment, message_id, message_link, timestamp) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (chat_id, user_id, user_name, reason, punishment, message_id, message_link, int(time.time())))
            conn.commit()
    
    def get_recent_violations(self, chat_id: int, limit: int = 10) -> List[Tuple]:
        with self.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT user_name, reason, punishment, timestamp, message_link 
                         FROM violation_logs WHERE chat_id = ? ORDER BY timestamp DESC LIMIT ?''',
                      (chat_id, limit))
            return c.fetchall()

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
        return f"{seconds} sec"
    elif seconds < 3600:
        return f"{seconds // 60} min"
    elif seconds < 86400:
        return f"{seconds // 3600} h"
    else:
        return f"{seconds // 86400} d"

def format_duration(minutes: int) -> str:
    if minutes == 0:
        return get_text(None, None, 'forever')
    return f"{minutes} {get_text(None, None, 'minutes')}"

def get_message_link(chat_id: int, message_id: int) -> str:
    # Для супергрупп ID обычно отрицательный и начинается с -100
    chat_id_str = str(chat_id)
    if chat_id_str.startswith('-100'):
        chat_id_str = chat_id_str[4:]
    return f"https://t.me/c/{chat_id_str}/{message_id}"

# Клавиатуры
def get_back_keyboard(callback_data: str, lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['back'], callback_data=callback_data)
    return builder.as_markup()

def get_main_keyboard(lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['about'], callback_data="about")
    builder.button(text=tr['help'], callback_data="help")
    builder.button(text=tr['add_to_group'], url=f"https://t.me/{BOT_USERNAME}?startgroup=true")
    builder.button(text=tr['group_manage'], callback_data="group_manage_main")
    builder.button(text=tr['language_menu'], callback_data="personal_language")
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_main_keyboard(lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['group_manage'], callback_data="group_manage")
    builder.button(text=tr['back'], callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def get_group_manage_keyboard(lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 " + tr['rules_management'], callback_data="manage_rules")
    builder.button(text="👋 " + tr['welcome'].format(name=""), callback_data="manage_welcome")
    builder.button(text="🔄 " + tr['rules_reminder'], callback_data="rules_auto")
    builder.button(text="🚫 Anti-flood", callback_data="antiflood_manage")
    builder.button(text=tr['group_language'], callback_data="set_language")
    builder.button(text=tr['report_group'], callback_data="set_report_group")
    builder.button(text=tr['auto_responder'], callback_data="auto_response_manage")
    builder.button(text=tr['links_mentions'], callback_data="links_manage")
    builder.button(text=tr['confirmation_settings'], callback_data="confirmation_manage")
    builder.button(text=tr['unlink_group'], callback_data="unlink_group_confirm")
    builder.button(text=tr['back'], callback_data="back_to_groups")
    builder.adjust(1)
    return builder.as_markup()

def get_rules_manage_keyboard(has_rules: bool, rules_enabled: bool, lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 " + tr['set_rules'], callback_data="set_rules")
    builder.button(text="📋 " + tr['set_default_rules'], callback_data="set_default_rules")
    
    if has_rules:
        builder.button(text="👁 " + tr['view'], callback_data="show_rules")
        builder.button(text="✏️ " + tr['edit_rules'], callback_data="edit_rules")
        builder.button(text="🗑 " + tr['delete_rules'], callback_data="delete_rules_confirm")
        
        status_text = tr['enable'] if not rules_enabled else tr['disable']
        builder.button(text=f"🔄 {status_text} rules", callback_data="toggle_rules")
    
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_manage_keyboard(enabled: bool = False, lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    status = tr['status_enabled'] if enabled else tr['status_disabled']
    builder.button(text=f"{tr['enable'] if not enabled else tr['disable']}: {status}", callback_data="toggle_welcome")
    builder.button(text=tr['set_text'], callback_data="set_welcome_text")
    builder.button(text=tr['set_photo'], callback_data="set_welcome_photo")
    builder.button(text=tr['view'], callback_data="show_welcome")
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_rules_auto_keyboard(enabled: bool, lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    status = tr['status_enabled'] if enabled else tr['status_disabled']
    builder.button(text=f"{tr['enable'] if not enabled else tr['disable']}: {status}", callback_data="toggle_rules_auto")
    builder.button(text=tr['interval'], callback_data="set_interval")
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_antiflood_manage_keyboard(settings: dict, lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    status = tr['status_enabled'] if settings['enabled'] else tr['status_disabled']
    builder.button(text=f"{tr['enable'] if not settings['enabled'] else tr['disable']}: {status}", callback_data="toggle_antiflood")
    builder.button(text=f"{tr['limit']}: {settings['msg_limit']}", callback_data="set_limit")
    builder.button(text=f"{tr['window']}: {settings['time_window']} sec", callback_data="set_window")
    builder.button(text=f"{tr['warn_count']}: {settings['warn_count']}", callback_data="set_warn_count")
    builder.button(text=f"{tr['first_punish']}: {settings['first_punish']} ({settings['first_duration']} sec)", callback_data="set_first_punish")
    builder.button(text=f"{tr['repeat_punish']}: {settings['repeat_punish']} ({settings['repeat_duration']} sec)", callback_data="set_repeat_punish")
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_punish_type_keyboard(is_first: bool = True, lang: str = 'en'):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    prefix = "first" if is_first else "repeat"
    builder = InlineKeyboardBuilder()
    builder.button(text="⚠️ Warn", callback_data=f"punish_warn_{prefix}")
    builder.button(text="🔇 Mute", callback_data=f"punish_mute_{prefix}")
    builder.button(text="👢 Kick", callback_data=f"punish_kick_{prefix}")
    builder.button(text="⛔️ Ban", callback_data=f"punish_ban_{prefix}")
    builder.button(text=tr['back'], callback_data="antiflood_manage")
    builder.adjust(2)
    return builder.as_markup()

def get_language_keyboard(current_lang: str):
    builder = InlineKeyboardBuilder()
    
    # English
    en_text = TRANSLATIONS['en']['english']
    if current_lang == 'en':
        en_text += " ✅"
    builder.button(text=en_text, callback_data="lang_group_en")
    
    # Russian
    ru_text = TRANSLATIONS['ru']['russian']
    if current_lang == 'ru':
        ru_text += " ✅"
    builder.button(text=ru_text, callback_data="lang_group_ru")
    
    # Ukrainian
    uk_text = TRANSLATIONS['uk']['ukrainian']
    if current_lang == 'uk':
        uk_text += " ✅"
    builder.button(text=uk_text, callback_data="lang_group_uk")
    
    builder.button(text="◀️ Back", callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_personal_language_keyboard(current_lang: str):
    builder = InlineKeyboardBuilder()
    
    # English
    en_text = TRANSLATIONS['en']['english']
    if current_lang == 'en':
        en_text += " ✅"
    builder.button(text=en_text, callback_data="personal_lang_en")
    
    # Russian
    ru_text = TRANSLATIONS['ru']['russian']
    if current_lang == 'ru':
        ru_text += " ✅"
    builder.button(text=ru_text, callback_data="personal_lang_ru")
    
    # Ukrainian
    uk_text = TRANSLATIONS['uk']['ukrainian']
    if current_lang == 'uk':
        uk_text += " ✅"
    builder.button(text=uk_text, callback_data="personal_lang_uk")
    
    builder.button(text="◀️ Back", callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def get_welcome_buttons(chat_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['rules'], callback_data=f"show_group_rules_{chat_id}")
    builder.button(text=tr['my_stats'], callback_data=f"my_stats_{chat_id}")
    builder.button(text=tr['top_active_btn'], callback_data=f"top_active_{chat_id}")
    builder.adjust(2)
    return builder.as_markup()

def get_confirm_not_bot_keyboard(chat_id: int, user_id: int, msg_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['confirm_not_bot'], callback_data=f"confirm_not_bot_{chat_id}_{user_id}_{msg_id}")
    return builder.as_markup()

def get_rules_agree_keyboard(chat_id: int, user_id: int, msg_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['agree_rules'], callback_data=f"agree_rules_{chat_id}_{user_id}_{msg_id}")
    return builder.as_markup()

def get_link_group_keyboard(chat_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['link_group'], callback_data=f"link_group_{chat_id}")
    builder.button(text=tr['cancel'], callback_data="cancel_link")
    builder.adjust(1)
    return builder.as_markup()

def get_unlink_confirm_keyboard(chat_id: int, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['unlink_group'], callback_data=f"unlink_group_{chat_id}")
    builder.button(text=tr['cancel'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_pm_link_keyboard(lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['go_to_pm_settings'], url=f"https://t.me/{BOT_USERNAME}?start")
    return builder.as_markup()

def get_report_group_keyboard(groups: List[Tuple[int, str]], lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.button(text=title or f"Group {chat_id}", callback_data=f"set_report_group_{chat_id}")
    builder.button(text="❌ " + tr['unlink_group'], callback_data="remove_report_group")
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_keyboard(responses: List[Tuple[str, str]], lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['add_trigger'], callback_data="add_auto_trigger")
    if responses:
        builder.button(text=tr['remove_trigger'], callback_data="remove_auto_trigger")
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_auto_response_remove_keyboard(responses: List[Tuple[str, str]], lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    for trigger, _ in responses:
        builder.button(text=trigger, callback_data=f"remove_trigger_{trigger}")
    builder.button(text=tr['back'], callback_data="auto_response_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_links_manage_keyboard(settings: dict, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    links_status = tr['status_enabled'] if settings['links_enabled'] else tr['status_disabled']
    builder.button(text=f"{tr['enable'] if not settings['links_enabled'] else tr['disable']}: {tr['links_enabled'].format(status=links_status)}", callback_data="toggle_links")
    builder.button(text=tr['set_links_punish'], callback_data="set_links_punish")
    builder.button(text=tr['set_max_mentions'], callback_data="set_max_mentions")
    builder.button(text=tr['set_mention_window'], callback_data="set_mention_window")
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

def get_links_punish_keyboard(lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⚠️ Warn", callback_data="links_punish_warn")
    builder.button(text="🔇 Mute", callback_data="links_punish_mute")
    builder.button(text="👢 Kick", callback_data="links_punish_kick")
    builder.button(text="⛔️ Ban", callback_data="links_punish_ban")
    builder.button(text=tr['back'], callback_data="links_manage")
    builder.adjust(2)
    return builder.as_markup()

def get_confirmation_keyboard(current_type: str, has_rules: bool, lang: str):
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    
    # Disabled
    disabled_text = tr['disabled']
    if current_type == 'disabled':
        disabled_text += " ✅"
    builder.button(text=disabled_text, callback_data="confirmation_disabled")
    
    # Not bot only
    not_bot_text = tr['not_bot_only']
    if current_type == 'not_bot':
        not_bot_text += " ✅"
    builder.button(text=not_bot_text, callback_data="confirmation_not_bot")
    
    # Rules only - доступно только если есть правила
    rules_text = tr['rules_only']
    if not has_rules:
        rules_text = "❌ " + rules_text
    elif current_type == 'rules':
        rules_text += " ✅"
    builder.button(text=rules_text, callback_data="confirmation_rules" if has_rules else "confirmation_disabled")
    
    # Both steps - доступно только если есть правила
    both_text = tr['both_steps']
    if not has_rules:
        both_text = "❌ " + both_text
    elif current_type == 'both':
        both_text += " ✅"
    builder.button(text=both_text, callback_data="confirmation_both" if has_rules else "confirmation_disabled")
    
    builder.button(text=tr['back'], callback_data="group_manage")
    builder.adjust(1)
    return builder.as_markup()

# Middleware для антифлуда и фильтра ссылок
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
        conf_type = db.get_confirmation_type(chat_id)
        if not db.has_user_confirmed(chat_id, user.id, conf_type):
            # Если не подтвердил - не обрабатываем флуд
            return await handler(event, data)

        settings = db.get_antiflood_settings(chat_id)
        
        # Проверка на флуд
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
                    await event.reply(f"⚠️ {user.full_name}, don't flood! ({violations}/{settings['warn_count']})")
                else:
                    await self.apply_punishment(chat_id, user, punish_type, duration, "Flood", event)
                
                flood_control[key].clear()
                return

        # Проверка на ссылки и упоминания
        if settings['links_enabled'] and event.text:
            text = event.text.lower()
            has_external_link = False
            mention_count = 0
            
            if event.entities:
                for entity in event.entities:
                    if entity.type == 'url':
                        url = event.text[entity.offset:entity.offset + entity.length]
                        # Разрешенные домены (можно расширить)
                        allowed_domains = ['t.me', 'telegram.me', 'youtube.com', 'youtu.be']
                        if not any(domain in url for domain in allowed_domains):
                            has_external_link = True
                            break
                    
                    if entity.type in ('mention', 'text_mention'):
                        mention_count += 1
            
            if has_external_link:
                await self.apply_punishment(chat_id, user, settings['links_punish'], settings['links_duration'], "External link", event)
                return
            
            if mention_count > settings['max_mentions']:
                await self.apply_punishment(chat_id, user, settings['links_punish'], settings['links_duration'], f"Too many mentions ({mention_count})", event)
                return

        return await handler(event, data)
    
    async def apply_punishment(self, chat_id: int, user: types.User, punish_type: str, duration: int, reason: str, event: Message):
        now = time.time()
        
        # Получаем язык группы для отчета
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        # Создаем ссылку на сообщение
        message_link = get_message_link(chat_id, event.message_id)
        
        # Логируем нарушение
        db.log_violation(
            chat_id, user.id, user.full_name,
            reason, punish_type, event.message_id, message_link
        )
        
        # Отправляем отчет в группу репортов
        report_group_id = db.get_report_group(chat_id)
        if report_group_id:
            try:
                report_text = (
                    f"<b>{tr['violation_report']}</b>\n\n"
                    f"<b>{tr['user']}:</b> {user.full_name} (@{user.username})\n"
                    f"<b>ID:</b> <code>{user.id}</code>\n"
                    f"<b>{tr['reason']}:</b> {reason}\n"
                    f"<b>{tr['punishment']}:</b> {punish_type}\n"
                    f"<b>{tr['time']}:</b> {format_datetime(int(now))}\n\n"
                    f"<a href='{message_link}'>{tr['message_link']}</a>"
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
                await event.reply(f"🔇 {user.full_name} muted for {duration // 60} min")
            elif punish_type == 'kick':
                await bot.ban_chat_member(chat_id, user.id)
                await bot.unban_chat_member(chat_id, user.id)
                await event.reply(f"👢 {user.full_name} kicked: {reason}")
            elif punish_type == 'ban':
                until = int(now + duration) if duration > 0 else None
                await bot.ban_chat_member(chat_id, user.id, until_date=until)
                await event.reply(f"⛔️ {user.full_name} banned for {duration // 60} min")
        except Exception as e:
            logger.warning(f"Error punishing in {chat_id}: {e}")

# Фоновая задача для сброса счетчиков
async def reset_periodic_counters():
    global stats_updating
    
    while True:
        now = datetime.now(MOSCOW_TZ)
        
        # Начало текущего дня (00:00 MSK)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Начало текущей недели (понедельник 00:00 MSK)
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
                    logger.info("Statistics counters reset")
            except Exception as e:
                logger.error(f"Error resetting counters: {e}")
            
            stats_updating = False
        
        # Ждём 1 час
        await asyncio.sleep(3600)

# Фоновая задача для авто-рассылки правил
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
                        lang = db.get_group_language(chat_id)
                        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
                        
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
                        logger.error(f"Error sending rules to chat {chat_id}: {e}")
                        
        except Exception as e:
            logger.error(f"Error in broadcast task: {e}")
        
        await asyncio.sleep(60)

# Команда для проверки пинга
@dp.message(Command("puls"))
@dp.message(Command("startpuls"))
@dp.message(F.text.lower().in_(["pulse", "пульс", "понг"]))
async def cmd_ping(message: Message):
    start_time = time.time()
    msg = await message.reply("⏳ ...")
    end_time = time.time()
    
    ping = round((end_time - start_time) * 1000)  # в миллисекундах
    response_time = round(end_time - start_time, 2)  # в секундах
    
    lang = 'en'  # по умолчанию
    if message.chat.type in {'group', 'supergroup'}:
        lang = db.get_group_language(message.chat.id)
    else:
        lang = user_languages.get(message.from_user.id, 'en')
    
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await msg.edit_text(
        tr['ping'].format(ping=ping, response=response_time),
        parse_mode="HTML"
    )

# Команда /start в группах
@dp.message(CommandStart())
@group_only()
async def cmd_start_group(message: Message):
    chat_id = message.chat.id
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    text = (
        f"👋 <b>Puls Chat Manager</b>\n\n"
        f"{tr['main_menu']}\n\n"
        f"• /rules - {tr['rules']}\n"
        f"• /stats - {tr['my_stats']}\n"
        f"• /top - {tr['top_active_btn']}\n"
        f"• /group - {tr['group_manage']}\n"
        f"• /puls - {tr['ping'].format(ping='?', response='?')}"
    )
    
    await message.reply(text, parse_mode="HTML")

# Команда /start в ЛС
@dp.message(CommandStart())
@pm_only()
async def cmd_start_pm(message: Message, state: FSMContext):
    await state.clear()
    
    user_id = message.from_user.id
    lang = user_languages.get(user_id, 'en')
    
    groups = db.get_user_groups(user_id)
    
    if not groups:
        text = (
            "👋 <b>Welcome to Puls Chat Manager!</b>\n\n"
            "You don't have any linked groups yet.\n\n"
            "🔹 <b>How to start:</b>\n"
            "1. Click the «➕ Add to group» button\n"
            "2. Select the chat to add the bot\n"
            "3. Make the bot an administrator\n"
            "4. In the group, write /group or click the management button\n"
            "5. Confirm group linking\n\n"
            "After that, the group will appear in the list for configuration."
        )
        await message.answer(text, reply_markup=get_main_keyboard(lang))
        return
    
    # Если есть группы, показываем их для выбора
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.button(text=title or f"Group {chat_id}", callback_data=f"select_group_{chat_id}")
    builder.button(text="◀️ Back", callback_data="back_to_main")
    builder.adjust(1)
    
    await message.answer(
        "📱 <b>Your groups:</b>\n\n"
        "Select a group to configure:",
        reply_markup=builder.as_markup()
    )

# Команды админа
@dp.message(Command("adminstats"))
@check_bot_admin()
async def cmd_admin_stats(message: Message):
    chats = db.get_all_chats()
    
    text = (
        "📊 <b>Bot Statistics</b>\n\n"
        f"📱 Total groups: {len(chats)}\n\n"
    )
    
    if chats:
        text += "<b>📋 Groups list:</b>\n"
        for chat_id, title, username, rules_enabled, welcome_enabled in chats:
            if username:
                link = f"https://t.me/{username}"
                chat_info = f"<a href='{link}'>{title or 'No name'}</a>"
            else:
                chat_info = f"{title or 'No name'} (private)"
            
            rules_status = "✅" if rules_enabled else "❌"
            welcome_status = "✅" if welcome_enabled else "❌"
            text += f"• {chat_info} | Rules:{rules_status} Welcome:{welcome_status}\n"
    
    await message.answer(text)

# Обработчик добавления бота в группу
@dp.message(F.new_chat_members)
async def on_bot_added(message: Message):
    bot_info = await bot.get_me()
    if any(member.id == bot_info.id for member in message.new_chat_members):
        # Не привязываем автоматически, просто логируем
        logger.info(f"Bot added to group {message.chat.id}")

# Обработчик команды управления группой в чате
@dp.message(Command("group"))
@dp.message(Command("manage"))
@group_only()
async def cmd_group_manage(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, является ли пользователь создателем
    if not await is_creator(chat_id, user_id):
        await message.answer("❌ Only the group creator can configure the bot!")
        return
    
    # Получаем язык
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Проверяем, привязана ли группа к пользователю
    owner_id = None
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT owner_id FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        owner_id = result[0] if result else None
    
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
        await callback.answer("❌ You are not the creator of this group!", show_alert=True)
        return
    
    # Получаем информацию о группе
    try:
        chat = await bot.get_chat(chat_id)
        chat_title = chat.title
        chat_username = chat.username
    except:
        chat_title = "Group"
        chat_username = None
    
    # Сохраняем привязку
    db.save_rules(
        chat_id=chat_id,
        owner_id=user_id,
        chat_title=chat_title,
        chat_username=chat_username
    )
    
    # Устанавливаем язык по умолчанию (английский)
    lang = 'en'
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET language = ? WHERE chat_id = ?', (lang, chat_id))
        conn.commit()
    
    tr = TRANSLATIONS[lang]
    
    await callback.message.edit_text(tr['group_linked'])
    await callback.answer("✅ Group linked!")
    
    # Отправляем сообщение в ЛС
    try:
        await bot.send_message(
            user_id,
            f"✅ Group <b>{chat_title}</b> successfully linked!\n\n"
            f"Now you can configure it by selecting it in the menu.",
            reply_markup=get_main_keyboard(lang)
        )
    except:
        pass

# Отмена привязки
@dp.callback_query(F.data == "cancel_link")
async def cancel_link(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()

# Правила
@dp.message(Command("rules"))
@group_only()
async def cmd_rules(message: Message):
    chat_id = message.chat.id
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    rules_html = db.get_rules_html(chat_id)
    if rules_html and db.get_rules_enabled(chat_id):
        await message.reply(f"<b>{tr['rules_reminder']}</b>\n\n{rules_html}", parse_mode="HTML")
    else:
        await message.answer(tr['error_no_rules'])

# Топ активных
@dp.message(Command("top"))
@group_only()
async def cmd_top_messages(message: Message):
    global stats_updating
    
    # Ждём, пока сброс закончится (максимум 5 секунд)
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(get_text(message.chat.id, message.from_user.id, 'stats_updating'))
        return
    
    chat_id = message.chat.id
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
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
    
    await message.reply(text, parse_mode="HTML")

# Моя статистика
@dp.message(Command("stats"))
@group_only()
async def cmd_my_stats(message: Message):
    global stats_updating
    
    # Ждём, пока сброс закончится (максимум 5 секунд)
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(get_text(message.chat.id, message.from_user.id, 'stats_updating'))
        return
    
    chat_id = message.chat.id
    user = message.from_user
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Получаем глобальный ID пользователя
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    
    if not stat:
        text = tr['stats_empty']
    else:
        join_dt = format_datetime(stat['join_date'])
        last_dt = format_datetime(stat['last_active'])
        position = db.get_user_position(chat_id, user.id, 'all')
        
        text = (
            f"<b>{tr['profile'].format(name=user.full_name)}</b>\n\n"
            f"<b>{tr['user_id']}:</b> <code>{global_user['global_id']}</code>\n"
            f"<b>{tr['first_seen']}:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"• {tr['per_day']}: {stat['day_messages']} {tr['messages']}\n"
            f"• {tr['per_week']}: {stat['week_messages']} {tr['messages']}\n"
            f"• {tr['per_month']}: {stat['month_messages']} {tr['messages']}\n"
            f"• {tr['total']}: {stat['all_messages']} {tr['messages']}\n"
            f"• {tr['place_in_top']}: {position}\n"
            f"• {tr['joined']}: {join_dt}\n"
            f"• {tr['last_active']}: {last_dt}"
        )
    
    await message.reply(text, parse_mode="HTML")

# Профиль пользователя (по ответу)
@dp.message(Command("profile"))
@group_only()
async def cmd_profile(message: Message):
    global stats_updating
    
    # Ждём, пока сброс закончится (максимум 5 секунд)
    for _ in range(50):
        if not stats_updating:
            break
        await asyncio.sleep(0.1)
    else:
        await message.reply(get_text(message.chat.id, message.from_user.id, 'stats_updating'))
        return
    
    chat_id = message.chat.id
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Проверяем, есть ли reply
    if not message.reply_to_message:
        await message.reply(tr['reply_to_user'])
        return
    
    target_user = message.reply_to_message.from_user
    
    # Получаем глобальный ID пользователя
    global_user = db.get_global_user(target_user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(target_user.id, target_user.username or "", target_user.full_name or "")
        global_user = db.get_global_user(target_user.id)
    
    stat = db.get_user_stat(chat_id, target_user.id)
    
    if not stat:
        text = tr['stats_empty']
    else:
        join_dt = format_datetime(stat['join_date'])
        last_dt = format_datetime(stat['last_active'])
        position = db.get_user_position(chat_id, target_user.id, 'all')
        
        text = (
            f"<b>{tr['profile'].format(name=target_user.full_name)}</b>\n\n"
            f"<b>{tr['user_id']}:</b> <code>{global_user['global_id']}</code>\n"
            f"<b>{tr['first_seen']}:</b> {format_datetime(global_user['first_seen'])}\n\n"
            f"• {tr['per_day']}: {stat['day_messages']} {tr['messages']}\n"
            f"• {tr['per_week']}: {stat['week_messages']} {tr['messages']}\n"
            f"• {tr['per_month']}: {stat['month_messages']} {tr['messages']}\n"
            f"• {tr['total']}: {stat['all_messages']} {tr['messages']}\n"
            f"• {tr['place_in_top']}: {position}\n"
            f"• {tr['joined']}: {join_dt}\n"
            f"• {tr['last_active']}: {last_dt}"
        )
    
    await message.reply(text, parse_mode="HTML")

# Вход нового участника
@dp.chat_member()
async def on_member_join(update: ChatMemberUpdated):
    if update.new_chat_member.status == "member" and update.old_chat_member.status in ("left", "kicked"):
        chat_id = update.chat.id
        user = update.new_chat_member.user
        
        # Добавляем в глобальную таблицу пользователей
        db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        
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
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])

        # Проверяем тип подтверждения
        conf_type = db.get_confirmation_type(chat_id)
        
        # Если подтверждение отключено - просто приветствуем
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

        # МУТ НАВСЕГДА
        try:
            await bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user.id,
                permissions=types.ChatPermissions(can_send_messages=False)
            )
        except Exception as e:
            logger.warning(f"Failed to mute {user.id}: {e}")

        rules_html = db.get_rules_html(chat_id)
        rules_enabled = db.get_rules_enabled(chat_id)
        builder = InlineKeyboardBuilder()
        msg_text = ""

        if conf_type == 'both':
            msg_text = (
                f"{tr['user_joined'].format(name=user.full_name)}\n\n"
                f"{tr['need_confirm_both']}"
            )
            # Отправляем в ЛС оба шага
            try:
                # Шаг 1: Подтверждение не бот
                msg1 = await bot.send_message(
                    user.id,
                    f"Welcome to {update.chat.title}!\n\n"
                    "Step 1: Confirm you're not a bot",
                    reply_markup=get_confirm_not_bot_keyboard(chat_id, user.id, 0, lang)
                )
                
                # Шаг 2: Правила (если есть и включены)
                if rules_html and rules_enabled:
                    await bot.send_message(
                        user.id,
                        f"Step 2: Read and agree to the rules:\n\n{rules_html}",
                        reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0, lang),
                        parse_mode="HTML"
                    )
            except Exception as e:
                logger.warning(f"Failed to send confirmation to PM {user.id}: {e}")
                await bot.send_message(chat_id, f"Failed to send confirmation to {user.full_name} in PM. Please open PM with the bot.")
            
            # Отправляем сообщение в группе с кнопкой
            builder.button(
                text=tr['go_to_pm'],
                url=f"https://t.me/{BOT_USERNAME}?start"
            )
            
        elif conf_type == 'not_bot':
            msg_text = (
                f"{tr['user_joined'].format(name=user.full_name)}\n\n"
                f"{tr['need_confirm_not_bot']}"
            )
            builder.button(
                text=tr['confirm_not_bot'],
                callback_data=f"confirm_not_bot_{chat_id}_{user.id}_0"
            )
        elif conf_type == 'rules' and rules_html and rules_enabled:
            msg_text = (
                f"{tr['user_joined'].format(name=user.full_name)}\n\n"
                f"{tr['need_confirm_rules']}"
            )
            builder.button(
                text=tr['go_to_pm'],
                url=f"https://t.me/{BOT_USERNAME}?start"
            )
            try:
                await bot.send_message(
                    user.id,
                    f"Welcome to {update.chat.title}!\n\n"
                    "Please read the rules below and confirm your agreement.\n"
                    "Without this, you won't be able to write in the chat.\n\n"
                    f"<b>Rules:</b>\n\n{rules_html}",
                    reply_markup=get_rules_agree_keyboard(chat_id, user.id, 0, lang),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Failed to send rules to PM {user.id}: {e}")
                await bot.send_message(chat_id, f"Failed to send rules to {user.full_name} in PM. Please open PM with the bot.")
        else:
            # Если нет правил и тип 'rules' или правила выключены - не мутим
            await send_simple_welcome(chat_id, user)
            return
        
        if msg_text:
            msg = await bot.send_message(chat_id, msg_text, reply_markup=builder.as_markup(), parse_mode="HTML")

# Простое приветствие
async def send_simple_welcome(chat_id: int, user: types.User):
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Получаем глобальный ID пользователя
    global_user = db.get_global_user(user.id)
    if not global_user:
        global_id = db.get_or_create_global_user(user.id, user.username or "", user.full_name or "")
        global_user = db.get_global_user(user.id)
    
    stat = db.get_user_stat(chat_id, user.id)
    join_dt = format_datetime(stat['join_date']) if stat else format_datetime(time.time())
    
    # Получаем позицию в топе
    position = db.get_user_position(chat_id, user.id, 'all')
    
    text = (
        f"{tr['welcome'].format(name=user.full_name)}\n\n"
        f"<b>{tr['user_id']}:</b> <code>{global_user['global_id']}</code>\n"
        f"<b>{tr['first_seen']}:</b> {format_datetime(global_user['first_seen'])}\n\n"
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
            reply_markup=get_welcome_buttons(chat_id, lang),
            parse_mode="HTML"
        )
    else:
        await bot.send_message(
            chat_id,
            text + (f"\n\n{welcome_text}" if welcome_text else ""),
            reply_markup=get_welcome_buttons(chat_id, lang),
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
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await callback.answer(tr['error_not_yours'], show_alert=True)
        return

    db.mark_user_confirmed(chat_id, user_id, not_bot=True, rules=False)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Проверяем тип подтверждения
    conf_type = db.get_confirmation_type(chat_id)
    not_bot_confirmed, rules_confirmed = db.get_user_confirmation_status(chat_id, user_id)
    
    # Если тип both и правила еще не подтверждены - не снимаем мут
    if conf_type == 'both' and not rules_confirmed:
        await callback.message.edit_text(tr['step_1_completed'])
        await callback.answer("Step 1 completed!")
        return

    # Снимаем мут
    try:
        await bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions=types.ChatPermissions(can_send_messages=True)
        )
    except Exception as e:
        logger.warning(f"Failed to unmute {user_id}: {e}")

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
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await callback.answer(tr['error_not_yours'], show_alert=True)
        return

    db.mark_user_confirmed(chat_id, user_id, not_bot=False, rules=True)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Проверяем тип подтверждения
    conf_type = db.get_confirmation_type(chat_id)
    not_bot_confirmed, rules_confirmed = db.get_user_confirmation_status(chat_id, user_id)
    
    # Если тип both и not bot еще не подтвержден - не снимаем мут
    if conf_type == 'both' and not not_bot_confirmed:
        await callback.message.edit_text(tr['step_2_completed'])
        await callback.answer("Step 2 completed!")
        return

    # Снимаем мут
    try:
        await bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions=types.ChatPermissions(can_send_messages=True)
        )
    except Exception as e:
        logger.warning(f"Failed to unmute {user_id}: {e}")

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
    if not message.from_user.is_bot:
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Проверяем, подтвердил ли пользователь
        conf_type = db.get_confirmation_type(chat_id)
        if db.has_user_confirmed(chat_id, user_id, conf_type):
            db.update_message_count(chat_id, user_id)

# Автоответчик - обработка сообщений
@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def auto_response_handler(message: Message):
    if message.from_user.is_bot:
        return
    
    chat_id = message.chat.id
    text = message.text.lower() if message.text else ""
    
    if not text:
        return
    
    # Проверяем, подтвердил ли пользователь
    conf_type = db.get_confirmation_type(chat_id)
    if not db.has_user_confirmed(chat_id, message.from_user.id, conf_type):
        return
    
    responses = db.get_auto_responses(chat_id)
    for trigger, response in responses:
        if trigger in text:
            try:
                await message.reply(response, parse_mode="HTML", disable_notification=True)
            except Exception as e:
                logger.warning(f"Auto response error in {chat_id}: {e}")
            # Отвечаем только на первое совпадение
            break

# Выход из группы
@dp.chat_member(F.new_chat_member.status == "left")
async def on_member_left(update: ChatMemberUpdated):
    chat_id = update.chat.id
    user = update.from_user
    db.set_left_chat(chat_id, user.id)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await bot.send_message(chat_id, tr['user_left'].format(name=user.full_name))

# Возврат в главное меню
@dp.callback_query(F.data == "back_to_main")
@check_owner()
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    lang = user_languages.get(user_id, 'en')
    
    await callback.message.edit_text(
        "👋 Main menu:",
        reply_markup=get_main_keyboard(lang)
    )
    await callback.answer()

# Управление группой (главное меню)
@dp.callback_query(F.data == "group_manage_main")
@check_owner()
async def group_manage_main(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    lang = user_languages.get(user_id, 'en')
    
    groups = db.get_user_groups(user_id)
    
    if not groups:
        await callback.answer("❌ You don't have any linked groups!", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.button(text=title or f"Group {chat_id}", callback_data=f"select_group_{chat_id}")
    builder.button(text="◀️ Back", callback_data="back_to_main")
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📱 <b>Your groups:</b>\n\n"
        "Select a group to configure:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# Выбор группы
@dp.callback_query(F.data.startswith("select_group_"))
@check_owner()
async def select_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    
    if not await is_creator(chat_id, callback.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await callback.answer(tr['error_not_creator'], show_alert=True)
        return
    
    await state.update_data(selected_chat_id=chat_id)
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Group"
    
    lang = db.get_group_language(chat_id)
    
    await callback.message.edit_text(
        f"⚙️ <b>Configuring group:</b> {chat_title}\n\n"
        f"Choose what to configure:",
        reply_markup=get_group_manage_keyboard(lang)
    )
    await callback.answer()

# Возврат к списку групп
@dp.callback_query(F.data == "back_to_groups")
@check_owner()
async def back_to_groups(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    lang = user_languages.get(user_id, 'en')
    
    groups = db.get_user_groups(user_id)
    
    builder = InlineKeyboardBuilder()
    for chat_id, title in groups:
        builder.button(text=title or f"Group {chat_id}", callback_data=f"select_group_{chat_id}")
    builder.button(text="◀️ Back", callback_data="back_to_main")
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📱 <b>Your groups:</b>\n\n"
        "Select a group to configure:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# Личный язык
@dp.callback_query(F.data == "personal_language")
@check_owner()
async def personal_language(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    current_lang = user_languages.get(user_id, 'en')
    
    await callback.message.edit_text(
        "🌐 <b>Select your personal language:</b>",
        reply_markup=get_personal_language_keyboard(current_lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("personal_lang_"))
@check_owner()
async def process_personal_language(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split('_')[-1]  # en / ru / uk
    user_id = callback.from_user.id
    
    # Сохраняем личный язык
    user_languages[user_id] = lang
    
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['language_changed'].format(lang=lang.upper()), show_alert=True)
    
    await callback.message.edit_text(
        "👋 Main menu:",
        reply_markup=get_main_keyboard(lang)
    )

# Управление правилами
@dp.callback_query(F.data == "manage_rules")
@check_owner()
async def manage_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    has_rules = db.get_rules_html(chat_id) is not None
    rules_enabled = db.get_rules_enabled(chat_id)
    
    status_text = get_text(chat_id, None, 'rules_enabled') if rules_enabled else get_text(chat_id, None, 'rules_disabled')
    
    await callback.message.edit_text(
        f"<b>{get_text(chat_id, None, 'rules_management')}</b>\n\n"
        f"{get_text(chat_id, None, 'rules_enabled_status').format(status=status_text)}\n\n"
        f"{get_text(chat_id, None, 'choose_language') if not lang else ''}",
        reply_markup=get_rules_manage_keyboard(has_rules, rules_enabled, lang),
        parse_mode="HTML"
    )
    await callback.answer()

# Установка правил
@dp.callback_query(F.data == "set_rules")
@check_owner()
async def set_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        get_text(data.get('selected_chat_id'), None, 'enter_new_rules') + "\n\n"
        "• <b>Bold</b> - &lt;b&gt;text&lt;/b&gt;\n"
        "• <i>Italic</i> - &lt;i&gt;text&lt;/i&gt;\n"
        "• <tg-spoiler>Spoiler</tg-spoiler> - &lt;tg-spoiler&gt;text&lt;/tg-spoiler&gt;\n"
        "• <blockquote>Quote</blockquote> - &lt;blockquote&gt;text&lt;/blockquote&gt;\n"
        "• <blockquote expandable>Expandable quote\nLine 2\nLine 3</blockquote> - &lt;blockquote expandable&gt;text\nlines&lt;/blockquote&gt;",
        reply_markup=get_back_keyboard("manage_rules", lang),
        parse_mode="HTML"
    )
    await state.set_state(RulesStates.waiting_for_rules_text)
    await callback.answer()

# Установка готовых правил
@dp.callback_query(F.data == "set_default_rules")
@check_owner()
async def set_default_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    db.save_rules(chat_id, rules_html=DEFAULT_RULES)
    db.set_rules_enabled(chat_id, True)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['default_rules_set'], show_alert=True)
    await manage_rules(callback, state)

# Показать правила
@dp.callback_query(F.data == "show_rules")
@check_owner()
async def show_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    rules_html = db.get_rules_html(chat_id)
    
    if rules_html:
        await callback.message.edit_text(
            f"📜 <b>Current rules:</b>\n\n{rules_html}",
            parse_mode="HTML",
            reply_markup=get_back_keyboard("manage_rules", db.get_group_language(chat_id))
        )
    else:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await callback.message.edit_text(
            tr['error_no_rules'],
            reply_markup=get_back_keyboard("manage_rules", lang)
        )
    
    await callback.answer()

# Редактирование правил
@dp.callback_query(F.data == "edit_rules")
@check_owner()
async def edit_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        get_text(data.get('selected_chat_id'), None, 'enter_new_rules'),
        reply_markup=get_back_keyboard("manage_rules", lang)
    )
    await state.set_state(RulesStates.waiting_for_new_rules_text)
    await callback.answer()

# Удаление правил (подтверждение)
@dp.callback_query(F.data == "delete_rules_confirm")
@check_owner()
async def delete_rules_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Yes, delete", callback_data="delete_rules")
    builder.button(text=tr['cancel'], callback_data="manage_rules")
    builder.adjust(1)
    
    await callback.message.edit_text(
        "❓ Are you sure you want to delete the rules?",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# Удаление правил
@dp.callback_query(F.data == "delete_rules")
@check_owner()
async def delete_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    db.delete_rules(chat_id)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['rules_deleted'], show_alert=True)
    await manage_rules(callback, state)

# Включение/выключение правил
@dp.callback_query(F.data == "toggle_rules")
@check_owner()
async def toggle_rules(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    current = db.get_rules_enabled(chat_id)
    db.set_rules_enabled(chat_id, not current)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    status = "enabled" if not current else "disabled"
    await callback.answer(f"Rules {status}!", show_alert=True)
    await manage_rules(callback, state)

# Обработка текста правил
@dp.message(RulesStates.waiting_for_rules_text)
async def process_rules_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    rules_html = message.html_text.strip()
    
    if not rules_html or len(rules_html) < 10:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_rules_short'])
        return
    
    db.save_rules(chat_id, rules_html=rules_html)
    db.set_rules_enabled(chat_id, True)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await message.reply(
        tr['rules_set'],
        parse_mode="HTML"
    )
    await state.clear()

# Обработка редактирования правил
@dp.message(RulesStates.waiting_for_new_rules_text)
async def process_edit_rules_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    rules_html = message.html_text.strip()
    
    if not rules_html or len(rules_html) < 10:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_rules_short'])
        return
    
    db.save_rules(chat_id, rules_html=rules_html)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await message.reply(
        tr['rules_updated'],
        parse_mode="HTML"
    )
    await state.clear()

# Управление приветствием
@dp.callback_query(F.data == "manage_welcome")
@check_owner()
async def manage_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    enabled = db.get_welcome_enabled(chat_id)
    lang = db.get_group_language(chat_id)
    
    await callback.message.edit_text(
        "👋 <b>Welcome message management</b>\n\n"
        "Configure the welcome message for new members.",
        reply_markup=get_welcome_manage_keyboard(enabled, lang)
    )
    await callback.answer()

# Включение/выключение приветствия
@dp.callback_query(F.data == "toggle_welcome")
@check_owner()
async def toggle_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    current = db.get_welcome_enabled(chat_id)
    db.set_welcome_enabled(chat_id, not current)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    new_status = "enabled" if not current else "disabled"
    await callback.answer(f"Welcome {new_status}!", show_alert=True)
    
    await manage_welcome(callback, state)

# Установка текста приветствия
@dp.callback_query(F.data == "set_welcome_text")
@check_owner()
async def set_welcome_text(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "📝 Send the welcome text for new members.\n\n"
        "You can use:\n"
        "• {name} - user's name\n"
        "• {username} - username\n"
        "• {chat} - group name\n\n"
        "Example:\n"
        "<code>Welcome, {name}!</code>",
        reply_markup=get_back_keyboard("manage_welcome", lang)
    )
    await state.set_state(WelcomeStates.waiting_for_welcome_text)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    welcome_text = message.html_text.strip()
    
    if not welcome_text:
        await message.answer("❌ Text cannot be empty!")
        return
    
    db.save_welcome(chat_id, welcome_text=welcome_text)
    
    await message.reply("✅ Welcome text saved!")
    await state.clear()

# Установка фото для приветствия
@dp.callback_query(F.data == "set_welcome_photo")
@check_owner()
async def set_welcome_photo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "🖼 Send a photo for the welcome message.\n\n"
        "It will be sent along with the text.",
        reply_markup=get_back_keyboard("manage_welcome", lang)
    )
    await state.set_state(WelcomeStates.waiting_for_welcome_photo)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_photo, F.photo)
async def process_welcome_photo(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    photo_id = message.photo[-1].file_id
    
    db.save_welcome(chat_id, welcome_photo_id=photo_id)
    
    await message.reply("✅ Welcome photo saved!")
    await state.clear()

@dp.message(WelcomeStates.waiting_for_welcome_photo)
async def process_welcome_photo_invalid(message: Message, state: FSMContext):
    await message.answer("❌ Please send a photo!")

# Показать приветствие
@dp.callback_query(F.data == "show_welcome")
@check_owner()
async def show_welcome(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    text, photo_id = db.get_welcome(chat_id)
    
    if not text and not photo_id:
        lang = db.get_group_language(chat_id)
        await callback.message.edit_text(
            "❌ Welcome message not set yet.",
            reply_markup=get_back_keyboard("manage_welcome", lang)
        )
        await callback.answer()
        return
    
    await callback.message.delete()
    
    lang = db.get_group_language(chat_id)
    
    if photo_id:
        await callback.message.answer_photo(
            photo=photo_id,
            caption=f"👋 <b>Current welcome message:</b>\n\n{text}" if text else None,
            reply_markup=get_back_keyboard("manage_welcome", lang),
            parse_mode="HTML"
        )
    elif text:
        await callback.message.answer(
            f"👋 <b>Current welcome message:</b>\n\n{text}",
            reply_markup=get_back_keyboard("manage_welcome", lang),
            parse_mode="HTML"
        )
    
    await callback.answer()

# Авто-рассылка правил
@dp.callback_query(F.data == "rules_auto")
@check_owner()
async def rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    lang = db.get_group_language(chat_id)
    
    text = (
        "🔄 <b>Auto rules broadcast</b>\n\n"
        f"Status: {'✅ Enabled' if enabled else '❌ Disabled'}\n"
        f"Interval: {format_interval(interval)}\n\n"
        "The bot will automatically send and pin the rules "
        "at the specified interval."
    )
    
    await callback.message.edit_text(text, reply_markup=get_rules_auto_keyboard(bool(enabled), lang))
    await callback.answer()

@dp.callback_query(F.data == "toggle_rules_auto")
@check_owner()
async def toggle_rules_auto(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    enabled, interval, _, _ = db.get_rules_auto_settings(chat_id)
    new_enabled = not bool(enabled)
    
    db.set_rules_auto_settings(chat_id, new_enabled, interval)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(f"Auto broadcast {'enabled' if new_enabled else 'disabled'}!", show_alert=True)
    await rules_auto(callback, state)

@dp.callback_query(F.data == "set_interval")
@check_owner()
async def set_interval(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "⏱ Enter the interval in minutes (from 5 to 525600):\n"
        "Examples:\n"
        "• 60 = 1 hour\n"
        "• 1440 = 1 day\n"
        "• 10080 = 1 week\n"
        "• 43200 = 1 month",
        reply_markup=get_back_keyboard("rules_auto", lang)
    )
    await state.set_state(RulesStates.waiting_for_interval)
    await callback.answer()

@dp.message(RulesStates.waiting_for_interval)
async def process_interval(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        interval = int(message.text)
        if interval < 5 or interval > 525600:
            await message.answer("❌ Interval must be from 5 to 525600 minutes!")
            return
        
        interval_seconds = interval * 60
        enabled, _, _, _ = db.get_rules_auto_settings(chat_id)
        db.set_rules_auto_settings(chat_id, bool(enabled), interval_seconds)
        
        await message.reply(f"✅ Interval set: {format_interval(interval_seconds)}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

# Управление антифлудом
@dp.callback_query(F.data == "antiflood_manage")
@check_owner()
async def antiflood_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    lang = db.get_group_language(chat_id)
    
    await callback.message.edit_text(
        "🚫 <b>Anti-flood management</b>\n\n"
        "Configure flood protection in the chat.",
        reply_markup=get_antiflood_manage_keyboard(settings, lang)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_antiflood")
@check_owner()
async def toggle_antiflood(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    db.set_antiflood_enabled(chat_id, not settings['enabled'])
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    new_status = "enabled" if not settings['enabled'] else "disabled"
    await callback.answer(f"Anti-flood {new_status}!", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_limit")
@check_owner()
async def set_limit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "📊 Enter the message limit per interval (from 3 to 20):",
        reply_markup=get_back_keyboard("antiflood_manage", lang)
    )
    await state.set_state(AntiFloodStates.waiting_for_limit)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_limit)
async def process_limit(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        limit = int(message.text)
        if limit < 3 or limit > 20:
            await message.answer("❌ Limit must be from 3 to 20!")
            return
        
        db.save_antiflood_settings(chat_id, msg_limit=limit)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(f"✅ {tr['limit']}: {limit}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

@dp.callback_query(F.data == "set_window")
@check_owner()
async def set_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "⏱ Enter the time window in seconds (from 5 to 300):",
        reply_markup=get_back_keyboard("antiflood_manage", lang)
    )
    await state.set_state(AntiFloodStates.waiting_for_window)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_window)
async def process_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        window = int(message.text)
        if window < 5 or window > 300:
            await message.answer("❌ Window must be from 5 to 300 seconds!")
            return
        
        db.save_antiflood_settings(chat_id, time_window=window)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(f"✅ {tr['window']}: {window} sec")
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

@dp.callback_query(F.data == "set_warn_count")
@check_owner()
async def set_warn_count(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "⚠️ Enter the number of warnings before punishment (from 1 to 5):",
        reply_markup=get_back_keyboard("antiflood_manage", lang)
    )
    await state.set_state(AntiFloodStates.waiting_for_warn_count)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_warn_count)
async def process_warn_count(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        count = int(message.text)
        if count < 1 or count > 5:
            await message.answer("❌ Warning count must be from 1 to 5!")
            return
        
        db.save_antiflood_settings(chat_id, warn_count=count)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(f"✅ {tr['warn_count']}: {count}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

@dp.callback_query(F.data == "set_first_punish")
@check_owner()
async def set_first_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(
        "🔇 Choose punishment for first violation:",
        reply_markup=get_punish_type_keyboard(is_first=True, lang=lang)
    )
    await callback.answer()

@dp.callback_query(F.data == "set_repeat_punish")
@check_owner()
async def set_repeat_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    await callback.message.edit_text(
        "🔊 Choose punishment for repeated violations:",
        reply_markup=get_punish_type_keyboard(is_first=False, lang=lang)
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
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    if is_first:
        db.save_antiflood_settings(chat_id, first_punish=punish_type)
    else:
        db.save_antiflood_settings(chat_id, repeat_punish=punish_type)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(f"Punishment set to {punish_type}", show_alert=True)
    await antiflood_manage(callback, state)

@dp.callback_query(F.data == "set_first_duration")
@check_owner()
async def set_first_duration(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "⏱ Enter punishment duration in seconds for first violation (from 30 to 86400):",
        reply_markup=get_back_keyboard("antiflood_manage", lang)
    )
    await state.set_state(AntiFloodStates.waiting_for_first_duration)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_first_duration)
async def process_first_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        if duration < 30 or duration > 86400:
            await message.answer("❌ Duration must be from 30 to 86400 seconds!")
            return
        
        db.save_antiflood_settings(chat_id, first_duration=duration)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(f"✅ {tr['first_punish']} duration: {duration} sec")
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

@dp.callback_query(F.data == "set_repeat_duration")
@check_owner()
async def set_repeat_duration(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        "⏱ Enter punishment duration in seconds for repeated violations (from 60 to 604800):",
        reply_markup=get_back_keyboard("antiflood_manage", lang)
    )
    await state.set_state(AntiFloodStates.waiting_for_repeat_duration)
    await callback.answer()

@dp.message(AntiFloodStates.waiting_for_repeat_duration)
async def process_repeat_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over with /start.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        if duration < 60 or duration > 604800:
            await message.answer("❌ Duration must be from 60 to 604800 seconds!")
            return
        
        db.save_antiflood_settings(chat_id, repeat_duration=duration)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(f"✅ {tr['repeat_punish']} duration: {duration} sec")
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

# Настройка языка группы
@dp.callback_query(F.data == "set_language")
@check_owner()
async def set_language(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    current_lang = db.get_group_language(chat_id)
    
    await callback.message.edit_text(
        TRANSLATIONS[current_lang]['current_language'].format(lang=current_lang.upper()) + "\n\n" +
        TRANSLATIONS[current_lang]['choose_language'],
        reply_markup=get_language_keyboard(current_lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("lang_group_"))
@check_owner()
async def process_group_language(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split('_')[-1]  # en / ru / uk
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET language = ? WHERE chat_id = ?', (lang, chat_id))
        conn.commit()
    
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['language_changed'].format(lang=lang.upper()), show_alert=True)
    await set_language(callback, state)

# Настройка группы репортов
@dp.callback_query(F.data == "set_report_group")
@check_owner()
async def set_report_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    user_id = callback.from_user.id
    groups = db.get_user_groups(user_id)
    
    if not groups:
        await callback.answer("❌ You don't have any linked groups!", show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    current_report_group = db.get_report_group_name(chat_id)
    text = tr['report_group_info']
    if current_report_group:
        text = tr['current_report_group'].format(group=current_report_group) + "\n\n" + text
    
    await callback.message.edit_text(
        text,
        reply_markup=get_report_group_keyboard(groups, lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("set_report_group_"))
@check_owner()
async def process_set_report_group(callback: CallbackQuery, state: FSMContext):
    report_group_id = int(callback.data.split('_')[-1])
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    db.set_report_group(chat_id, report_group_id)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['report_group_set'], show_alert=True)
    await set_report_group(callback, state)

@dp.callback_query(F.data == "remove_report_group")
@check_owner()
async def remove_report_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    db.set_report_group(chat_id, None)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['report_group_removed'], show_alert=True)
    await set_report_group(callback, state)

# Автоответчик
@dp.callback_query(F.data == "auto_response_manage")
@check_owner()
async def auto_response_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    responses = db.get_auto_responses(chat_id)
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    if not responses:
        text = tr['auto_responder_empty']
    else:
        text = tr['auto_responder_list']
        for trigger, resp in responses:
            text += f"• <code>{trigger}</code> → {resp[:50]}{'...' if len(resp) > 50 else ''}\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_auto_response_keyboard(responses, lang),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "add_auto_trigger")
@check_owner()
async def add_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_chat_id'):
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(data.get('selected_chat_id'))
    
    await callback.message.edit_text(
        get_text(data.get('selected_chat_id'), None, 'enter_trigger'),
        reply_markup=get_back_keyboard("auto_response_manage", lang)
    )
    await state.set_state(AutoResponseStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponseStates.waiting_for_trigger)
async def process_auto_trigger(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    trigger = message.text.strip()
    if not trigger:
        await message.answer("❌ Trigger cannot be empty!")
        return
    
    await state.update_data(auto_trigger=trigger)
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    lang = db.get_group_language(chat_id)
    
    await message.reply(
        get_text(chat_id, None, 'enter_response'),
        reply_markup=get_back_keyboard("auto_response_manage", lang).as_markup()
    )
    await state.set_state(AutoResponseStates.waiting_for_response)

@dp.message(AutoResponseStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    response = message.html_text.strip()
    if not response:
        await message.answer("❌ Response cannot be empty!")
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    trigger = data.get('auto_trigger')
    
    if not chat_id or not trigger:
        await message.answer("❌ Error! Start over.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    # Проверяем, не существует ли уже такой триггер
    responses = db.get_auto_responses(chat_id)
    for t, _ in responses:
        if t.lower() == trigger.lower():
            lang = db.get_group_language(chat_id)
            tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
            await message.answer(tr['trigger_exists'].format(trigger=trigger))
            await state.clear()
            return
    
    db.add_auto_response(chat_id, trigger, response)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await message.reply(tr['trigger_added'].format(trigger=trigger))
    await state.clear()

@dp.callback_query(F.data == "remove_auto_trigger")
@check_owner()
async def remove_auto_trigger(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    responses = db.get_auto_responses(chat_id)
    if not responses:
        await callback.answer("❌ No triggers to remove!", show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.message.edit_text(
        tr['select_trigger_to_remove'],
        reply_markup=get_auto_response_remove_keyboard(responses, lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("remove_trigger_"))
@check_owner()
async def process_remove_trigger(callback: CallbackQuery, state: FSMContext):
    trigger = callback.data.replace("remove_trigger_", "")
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    db.remove_auto_response(chat_id, trigger)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['trigger_removed'].format(trigger=trigger), show_alert=True)
    await auto_response_manage(callback, state)

# Управление ссылками и упоминаниями
@dp.callback_query(F.data == "links_manage")
@check_owner()
async def links_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.message.edit_text(
        f"🔗 <b>{tr['links_mentions']}</b>\n\n"
        f"{tr['links_enabled'].format(status='✅' if settings['links_enabled'] else '❌')}\n"
        f"{tr['links_punish'].format(punish=settings['links_punish'].capitalize())}\n"
        f"{tr['max_mentions'].format(count=settings['max_mentions'], window=settings['mention_window'])}\n\n"
        f"Choose what to configure:",
        reply_markup=get_links_manage_keyboard(settings, lang)
    )
    await callback.answer()

@dp.callback_query(F.data == "toggle_links")
@check_owner()
async def toggle_links(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    settings = db.get_antiflood_settings(chat_id)
    new_enabled = not settings['links_enabled']
    
    db.save_antiflood_settings(chat_id, links_enabled=int(new_enabled))
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    status = tr['filter_enabled'] if new_enabled else tr['filter_disabled']
    await callback.answer(status, show_alert=True)
    
    await links_manage(callback, state)

@dp.callback_query(F.data == "set_links_punish")
@check_owner()
async def set_links_punish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.message.edit_text(
        tr['choose_punish'],
        reply_markup=get_links_punish_keyboard(lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("links_punish_"))
@check_owner()
async def process_links_punish(callback: CallbackQuery, state: FSMContext):
    punish = callback.data.split('_')[-1]  # warn / mute / kick / ban
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    if punish in ['warn', 'kick']:
        # Для warn и kick время не нужно
        db.save_antiflood_settings(chat_id, links_punish=punish)
        
        # Показываем красивое сообщение
        duration_text = tr['forever']
        await callback.message.edit_text(
            tr['punishment_saved'].format(punish=punish.capitalize(), duration=duration_text),
            reply_markup=get_back_keyboard("links_manage", lang)
        )
    else:
        # Для mute и ban спрашиваем время
        await state.update_data(links_punish=punish)
        await callback.message.edit_text(
            tr['enter_duration'],
            reply_markup=get_back_keyboard("links_manage", lang)
        )
        await state.set_state(LinksStates.waiting_for_duration)
    
    await callback.answer()

@dp.message(LinksStates.waiting_for_duration)
async def process_links_duration(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    punish = data.get('links_punish')
    
    if not chat_id or not punish:
        await message.answer("❌ Error! Start over.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        minutes = int(message.text)
        if minutes < 0:
            await message.answer("❌ Enter a positive number or 0 (forever)")
            return
        
        duration_sec = minutes * 60
        db.save_antiflood_settings(chat_id, links_punish=punish, links_duration=duration_sec)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        duration_text = format_duration(minutes) if minutes > 0 else tr['forever']
        
        # Показываем красивое сообщение
        await message.reply(
            tr['punishment_saved'].format(punish=punish.capitalize(), duration=duration_text),
            reply_markup=get_back_keyboard("links_manage", lang).as_markup()
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

@dp.callback_query(F.data == "set_max_mentions")
@check_owner()
async def set_max_mentions(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.message.edit_text(
        tr['enter_max_mentions'],
        reply_markup=get_back_keyboard("links_manage", lang)
    )
    await state.set_state(LinksStates.waiting_for_max_mentions)
    await callback.answer()

@dp.message(LinksStates.waiting_for_max_mentions)
async def process_max_mentions(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        count = int(message.text)
        if count < 1 or count > 20:
            await message.answer("❌ Max mentions must be from 1 to 20!")
            return
        
        db.save_antiflood_settings(chat_id, max_mentions=count)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(tr['max_mentions_set'].format(count=count))
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

@dp.callback_query(F.data == "set_mention_window")
@check_owner()
async def set_mention_window(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.message.edit_text(
        tr['enter_mention_window'],
        reply_markup=get_back_keyboard("links_manage", lang)
    )
    await state.set_state(LinksStates.waiting_for_mention_window)
    await callback.answer()

@dp.message(LinksStates.waiting_for_mention_window)
async def process_mention_window(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await message.answer("❌ Settings only in private messages!")
        await state.clear()
        return
    
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await message.answer("❌ Error! Start over.")
        await state.clear()
        return
    
    if not await is_creator(chat_id, message.from_user.id):
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        await message.answer(tr['error_not_creator'])
        await state.clear()
        return
    
    try:
        window = int(message.text)
        if window < 10 or window > 3600:
            await message.answer("❌ Mention window must be from 10 to 3600 seconds!")
            return
        
        db.save_antiflood_settings(chat_id, mention_window=window)
        
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        
        await message.reply(tr['mention_window_set'].format(window=window))
        await state.clear()
    except ValueError:
        await message.answer("❌ Please enter a number!")

# Настройка типа подтверждения
@dp.callback_query(F.data == "confirmation_manage")
@check_owner()
async def confirmation_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    conf_type = db.get_confirmation_type(chat_id)
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    
    type_names = {
        'disabled': tr['disabled'],
        'not_bot': tr['not_bot_only'],
        'rules': tr['rules_only'],
        'both': tr['both_steps']
    }
    
    warning_text = ""
    if not has_rules and conf_type in ['rules', 'both']:
        warning_text = "\n\n⚠️ <b>Warning:</b> " + tr['need_rules_first']
    
    await callback.message.edit_text(
        f"{tr['confirmation_settings']}\n\n"
        f"{tr['confirmation_type'].format(type=type_names.get(conf_type, conf_type))}"
        f"{warning_text}\n\n"
        f"Choose confirmation type:",
        reply_markup=get_confirmation_keyboard(conf_type, has_rules, lang),
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
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    # Проверяем, можно ли выбрать этот тип
    has_rules = db.get_rules_html(chat_id) is not None and db.get_rules_enabled(chat_id)
    
    if conf_type in ['rules', 'both'] and not has_rules:
        lang = db.get_group_language(chat_id)
        tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
        error_text = tr['cant_use_rules'] if conf_type == 'rules' else tr['cant_use_both']
        await callback.answer(error_text, show_alert=True)
        return
    
    db.set_confirmation_type(chat_id, conf_type)
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    await callback.answer(tr['confirmation_updated'], show_alert=True)
    await confirmation_manage(callback, state)

# Подтверждение отвязки группы
@dp.callback_query(F.data == "unlink_group_confirm")
@check_owner()
async def unlink_group_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.answer(get_text(None, callback.from_user.id, 'error_no_group'), show_alert=True)
        return
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    builder = InlineKeyboardBuilder()
    builder.button(text=tr['unlink_group'], callback_data=f"unlink_group_{chat_id}")
    builder.button(text=tr['cancel'], callback_data="group_manage")
    builder.adjust(1)
    
    await callback.message.edit_text(
        tr['confirm_unlink'],
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# Отвязка группы
@dp.callback_query(F.data.startswith("unlink_group_"))
@check_owner()
async def unlink_group(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    lang = db.get_group_language(chat_id)
    tr = TRANSLATIONS.get(lang, TRANSLATIONS['en'])
    
    # Удаляем owner_id (отвязываем группу)
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('UPDATE group_rules SET owner_id = NULL WHERE chat_id = ?', (chat_id,))
        conn.commit()
    
    await callback.message.edit_text(tr['group_unlinked'])
    await callback.answer("✅ Group unlinked!")
    
    # Возвращаемся к списку групп
    await state.clear()
    await cmd_start_pm(callback.message, state)

# Возврат в меню группы
@dp.callback_query(F.data == "group_manage")
@check_owner()
async def back_to_group_manage(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get('selected_chat_id')
    
    if not chat_id:
        await callback.message.edit_text("❌ Error! Start over with /start.")
        return
    
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute('SELECT chat_title FROM group_rules WHERE chat_id = ?', (chat_id,))
        result = c.fetchone()
        chat_title = result[0] if result else "Group"
    
    lang = db.get_group_language(chat_id)
    
    await callback.message.edit_text(
        f"⚙️ <b>Configuring group:</b> {chat_title}\n\n"
        f"Choose what to configure:",
        reply_markup=get_group_manage_keyboard(lang)
    )
    await callback.answer()

# О боте
@dp.callback_query(F.data == "about")
@check_owner()
async def callback_about(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    lang = user_languages.get(user_id, 'en')
    
    text = (
        "🤖 <b>Puls Chat Manager</b>\n\n"
        "📌 <b>What I can do:</b>\n"
        "• Automatic moderation of new members\n"
        "• Rules confirmation in PM\n"
        "• Message statistics (day/week/month/total)\n"
        "• Top active members\n"
        "• Anti-flood with customizable punishments\n"
        "• Links and mentions filter\n"
        "• Auto responder with keywords\n"
        "• Report system with admin buttons\n"
        "• Welcome message with photo/text\n"
        "• Auto rules broadcast\n"
        "• Support for 3 languages (English, Russian, Ukrainian)\n"
        "• Ping check (/puls, /startpuls, pulse)\n\n"
        "👇 Click «➕ Add to group» to invite me to your chat"
    )
    
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(lang))
    await callback.answer()

# Помощь
@dp.callback_query(F.data == "help")
@check_owner()
async def callback_help(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    lang = user_languages.get(user_id, 'en')
    
    text = (
        "🆘 <b>Puls Chat Manager Help</b>\n\n"
        "🔹 <b>Group commands:</b>\n"
        "• /rules - Show rules\n"
        "• /stats - My statistics\n"
        "• /top - Top active\n"
        "• /profile - View user profile (reply to their message)\n"
        "• /puls, /startpuls, pulse - Ping check\n"
        "• /group - Group management (for creator)\n\n"
        "🔹 <b>How to add bot to group:</b>\n"
        "1. Click «➕ Add to group» button\n"
        "2. Select the chat\n"
        "3. Make the bot an administrator\n"
        "4. In the group, type /group and link the group\n"
        "5. Configure in PM via /start\n\n"
        "🔹 <b>For new members:</b>\n"
        "• Bot automatically mutes until confirmation\n"
        "• Need to agree to rules in PM\n"
        "• Mute is removed after confirmation\n\n"
        "🔹 <b>Statistics:</b>\n"
        "• Messages are counted per day/week/month\n"
        "• Reset happens automatically\n"
        "• Top shows the most active\n\n"
        "🔹 <b>Languages:</b>\n"
        "• Bot supports English, Russian, and Ukrainian\n"
        "• Personal language can be changed in the main menu\n"
        "• Group language can be changed in group settings"
    )
    
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
