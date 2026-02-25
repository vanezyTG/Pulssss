import asyncio
import sqlite3
import random
import datetime
import string
from typing import Dict, List, Tuple, Optional
from contextlib import contextmanager

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup,
    KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = '7966298894:'8557190026:AAGSBViDE6P8TZx15HAi5IF-G9MBRjnsmaY'
DB_FILE = 'puls_bot.db'

# ========== СИСТЕМА УРОВНЕЙ ==========
LEVELS = {
    1:  {"exp": 0,       "reward_coins": 0,    "bonus_win": 0.00, "bonus_daily": 0.00, "bonus_salary": 0.00, "max_attempts_bonus": 0,  "double_win_chance": 0.00},
    2:  {"exp": 300,     "reward_coins": 10,   "bonus_win": 0.005, "bonus_daily": 0.00, "bonus_salary": 0.00, "max_attempts_bonus": 0,  "double_win_chance": 0.00},
    3:  {"exp": 700,     "reward_coins": 20,   "bonus_win": 0.01,  "bonus_daily": 0.02, "bonus_salary": 0.00, "max_attempts_bonus": 0,  "double_win_chance": 0.005},
    4:  {"exp": 1200,    "reward_coins": 30,   "bonus_win": 0.015, "bonus_daily": 0.04, "bonus_salary": 0.00, "max_attempts_bonus": 0,  "double_win_chance": 0.01},
    5:  {"exp": 2000,    "reward_coins": 50,   "bonus_win": 0.02,  "bonus_daily": 0.06, "bonus_salary": 0.00, "max_attempts_bonus": 1,  "double_win_chance": 0.015},
    6:  {"exp": 3500,    "reward_coins": 60,   "bonus_win": 0.025, "bonus_daily": 0.08, "bonus_salary": 0.02, "max_attempts_bonus": 1,  "double_win_chance": 0.02},
    7:  {"exp": 6000,    "reward_coins": 70,   "bonus_win": 0.03,  "bonus_daily": 0.10, "bonus_salary": 0.03, "max_attempts_bonus": 1,  "double_win_chance": 0.025},
    8:  {"exp": 10000,   "reward_coins": 80,   "bonus_win": 0.035, "bonus_daily": 0.12, "bonus_salary": 0.04, "max_attempts_bonus": 1,  "double_win_chance": 0.03},
    9:  {"exp": 17000,   "reward_coins": 100,  "bonus_win": 0.04,  "bonus_daily": 0.14, "bonus_salary": 0.05, "max_attempts_bonus": 1,  "double_win_chance": 0.035},
    10: {"exp": 28000,   "reward_coins": 125,  "bonus_win": 0.045, "bonus_daily": 0.16, "bonus_salary": 0.06, "max_attempts_bonus": 2,  "double_win_chance": 0.04},
    11: {"exp": 45000,   "reward_coins": 150,  "bonus_win": 0.05,  "bonus_daily": 0.18, "bonus_salary": 0.07, "max_attempts_bonus": 2,  "double_win_chance": 0.045},
    12: {"exp": 70000,   "reward_coins": 180,  "bonus_win": 0.055, "bonus_daily": 0.20, "bonus_salary": 0.08, "max_attempts_bonus": 2,  "double_win_chance": 0.05},
    13: {"exp": 110000,  "reward_coins": 220,  "bonus_win": 0.06,  "bonus_daily": 0.22, "bonus_salary": 0.09, "max_attempts_bonus": 2,  "double_win_chance": 0.055},
    14: {"exp": 170000,  "reward_coins": 270,  "bonus_win": 0.065, "bonus_daily": 0.24, "bonus_salary": 0.10, "max_attempts_bonus": 2,  "double_win_chance": 0.06},
    15: {"exp": 250000,  "reward_coins": 320,  "bonus_win": 0.07,  "bonus_daily": 0.26, "bonus_salary": 0.11, "max_attempts_bonus": 2,  "double_win_chance": 0.065},
    16: {"exp": 380000,  "reward_coins": 380,  "bonus_win": 0.075, "bonus_daily": 0.28, "bonus_salary": 0.12, "max_attempts_bonus": 3,  "double_win_chance": 0.07},
    17: {"exp": 550000,  "reward_coins": 450,  "bonus_win": 0.08,  "bonus_daily": 0.30, "bonus_salary": 0.13, "max_attempts_bonus": 3,  "double_win_chance": 0.075},
    18: {"exp": 800000,  "reward_coins": 530,  "bonus_win": 0.085, "bonus_daily": 0.35, "bonus_salary": 0.14, "max_attempts_bonus": 3,  "double_win_chance": 0.08},
    19: {"exp": 1150000, "reward_coins": 620,  "bonus_win": 0.09,  "bonus_daily": 0.40, "bonus_salary": 0.15, "max_attempts_bonus": 3,  "double_win_chance": 0.085},
    20: {"exp": 1650000, "reward_coins": 750,  "bonus_win": 0.095, "bonus_daily": 0.45, "bonus_salary": 0.16, "max_attempts_bonus": 3,  "double_win_chance": 0.09},
    21: {"exp": 2300000, "reward_coins": 900,  "bonus_win": 0.10,  "bonus_daily": 0.50, "bonus_salary": 0.17, "max_attempts_bonus": 4,  "double_win_chance": 0.095},
    22: {"exp": 3200000, "reward_coins": 1100, "bonus_win": 0.105, "bonus_daily": 0.52, "bonus_salary": 0.175, "max_attempts_bonus": 4,  "double_win_chance": 0.10},
    23: {"exp": 4300000, "reward_coins": 1350, "bonus_win": 0.11,  "bonus_daily": 0.54, "bonus_salary": 0.18, "max_attempts_bonus": 4,  "double_win_chance": 0.105},
    24: {"exp": 5700000, "reward_coins": 1650, "bonus_win": 0.115, "bonus_daily": 0.56, "bonus_salary": 0.185, "max_attempts_bonus": 4,  "double_win_chance": 0.11},
    25: {"exp": 7500000, "reward_coins": 2000, "bonus_win": 0.12,  "bonus_daily": 0.58, "bonus_salary": 0.19, "max_attempts_bonus": 4,  "double_win_chance": 0.115},
    26: {"exp": 10000000,"reward_coins": 2500, "bonus_win": 0.125, "bonus_daily": 0.59, "bonus_salary": 0.195, "max_attempts_bonus": 5,  "double_win_chance": 0.12},
    27: {"exp": 13000000,"reward_coins": 3000, "bonus_win": 0.13,  "bonus_daily": 0.595, "bonus_salary": 0.198, "max_attempts_bonus": 5,  "double_win_chance": 0.125},
    28: {"exp": 17000000,"reward_coins": 3700, "bonus_win": 0.135, "bonus_daily": 0.597, "bonus_salary": 0.199, "max_attempts_bonus": 5,  "double_win_chance": 0.13},
    29: {"exp": 22000000,"reward_coins": 4500, "bonus_win": 0.14,  "bonus_daily": 0.598, "bonus_salary": 0.1995, "max_attempts_bonus": 5, "double_win_chance": 0.135},
    30: {"exp": 28000000,"reward_coins": 5000, "bonus_win": 0.15,  "bonus_daily": 0.60, "bonus_salary": 0.20,  "max_attempts_bonus": 5, "double_win_chance": 0.14},
}

# ========== ПРОФЕССИИ ==========
PROFESSIONS = {
    "none": 0,
    "junior": 50,
    "middle": 100,
    "senior": 300,
    "manager": 400,
    "director": 500
}

# ========== FSM СОСТОЯНИЯ ==========
class AuthStates(StatesGroup):
    login = State()
    password = State()
    new_username = State()
    new_password = State()

class AdminStates(StatesGroup):
    password = State()
    manage_prices = State()
    create_giveaway = State()
    set_max_accounts_all = State()
    set_max_accounts_user = State()
    add_quest = State()
    add_quest_reward = State()
    broadcast = State()
    view_account = State()
    create_promotion = State()

class GameStates(StatesGroup):
    choose_difficulty = State()
    choose_game = State()
    bet = State()
    play = State()
    rps_choice = State()
    ttt_move = State()

class ShopStates(StatesGroup):
    browsing = State()
    select_quantity = State()
    confirm_purchase = State()

class SettingsStates(StatesGroup):
    main = State()
    language = State()
    auto_bet = State()
    account_settings = State()
    change_username = State()
    change_password_old = State()
    change_password_new = State()
    add_account = State()

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ========== БАЗА ДАННЫХ ==========
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        tg_id           INTEGER PRIMARY KEY,
        max_accounts    INTEGER DEFAULT 3,
        admin           INTEGER DEFAULT 0,
        language        TEXT DEFAULT 'ru',
        auto_bet        INTEGER DEFAULT 25
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS accounts (
        account_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id               INTEGER,
        username            TEXT,
        password            TEXT,
        coins               INTEGER DEFAULT 100,
        created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_bonus          DATETIME,
        games_played        INTEGER DEFAULT 0,
        profession          TEXT DEFAULT 'none',
        quest_count_today   INTEGER DEFAULT 0,
        last_quest_date     DATE,
        level               INTEGER DEFAULT 1,
        exp                 INTEGER DEFAULT 0,
        total_exp           INTEGER DEFAULT 0,
        daily_games         INTEGER DEFAULT 0,
        daily_wins          INTEGER DEFAULT 0,
        weekly_games        INTEGER DEFAULT 0,
        weekly_wins         INTEGER DEFAULT 0,
        monthly_games       INTEGER DEFAULT 0,
        monthly_wins        INTEGER DEFAULT 0,
        last_daily_reset    DATE,
        last_week_reset     DATE,
        last_month_reset    DATE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS actions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id  INTEGER,
        action      TEXT,
        timestamp   DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS quests (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        type            TEXT,
        description     TEXT,
        reward          INTEGER,
        link            TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS completed_quests (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id      INTEGER,
        quest_id        INTEGER,
        completed_at    DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shop_prices (
        item    TEXT PRIMARY KEY,
        price   INTEGER
    )
    ''')
    
    default_prices = [
        ('junior', 500),
        ('middle', 1000),
        ('senior', 3000),
        ('manager', 7000),
        ('director', 10000),
        ('temp_attempts', 50),
        ('perm_attempts', 800)
    ]
    cursor.executemany('''
    INSERT OR IGNORE INTO shop_prices (item, price) VALUES (?, ?)
    ''', default_prices)
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS giveaways (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        prize       TEXT,
        end_time    DATETIME,
        status      TEXT DEFAULT 'active'
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS giveaway_participants (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        giveaway_id     INTEGER,
        account_id      INTEGER,
        UNIQUE(giveaway_id, account_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS promotions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        item            TEXT,
        discount_percent INTEGER,
        end_time        DATETIME
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS game_attempts (
        account_id      INTEGER,
        game_name       TEXT,
        daily_attempts  INTEGER DEFAULT 0,
        last_date       DATE,
        permanent_max   INTEGER DEFAULT 5,
        extra_attempts  INTEGER DEFAULT 0,
        PRIMARY KEY (account_id, game_name)
    )
    ''')
    
    conn.commit()
    conn.close()

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def get_level_info(account):
    level = account['level']
    exp = account['exp']
    next_level = level + 1
    next_req = LEVELS.get(next_level, {"exp": 9999999999})["exp"]
    to_next = next_req - exp
    progress = exp / next_req if next_req > 0 else 1.0
    current = LEVELS.get(level, LEVELS[1])
    return {
        "level": level,
        "exp": exp,
        "to_next": to_next,
        "progress": progress,
        "bonus_win": current["bonus_win"],
        "bonus_daily": current["bonus_daily"],
        "bonus_salary": current["bonus_salary"],
        "max_attempts_bonus": current["max_attempts_bonus"],
        "double_win_chance": current["double_win_chance"]
    }

async def add_exp(account_id: int, amount: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE accounts SET exp = exp + ?, total_exp = total_exp + ? WHERE account_id = ?",
            (amount, amount, account_id)
        )
        cursor.execute("SELECT level, exp, tg_id FROM accounts WHERE account_id = ?", (account_id,))
        level, exp, tg_id = cursor.fetchone()
        
        while level < 30:
            next_req = LEVELS.get(level + 1, {"exp": 9999999999})["exp"]
            if exp >= next_req:
                level += 1
                reward = LEVELS[level]["reward_coins"]
                cursor.execute(
                    "UPDATE accounts SET level = ?, coins = coins + ?, exp = exp - ? WHERE account_id = ?",
                    (level, reward, next_req, account_id)
                )
                exp -= next_req
            else:
                break
        
        conn.commit()

def check_attempts(account_id: int, game_name: str) -> Tuple[bool, int]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT level FROM accounts WHERE account_id = ?", (account_id,))
        level = cursor.fetchone()['level']
        level_bonus = LEVELS.get(level, LEVELS[1])["max_attempts_bonus"]
        
        cursor.execute('''
        SELECT daily_attempts, last_date, permanent_max, extra_attempts 
        FROM game_attempts 
        WHERE account_id = ? AND game_name = ?
        ''', (account_id, game_name))
        
        result = cursor.fetchone()
        today = datetime.date.today().isoformat()
        
        if result:
            daily_attempts, last_date, permanent_max, extra_attempts = result
            if last_date != today:
                daily_attempts = 0
                cursor.execute('''
                UPDATE game_attempts 
                SET daily_attempts = 0, last_date = ?
                WHERE account_id = ? AND game_name = ?
                ''', (today, account_id, game_name))
                conn.commit()
            
            total_max = permanent_max + extra_attempts + level_bonus
            return daily_attempts < total_max, total_max - daily_attempts
        else:
            total_max = 5 + level_bonus
            cursor.execute('''
            INSERT INTO game_attempts 
            (account_id, game_name, daily_attempts, last_date, permanent_max, extra_attempts)
            VALUES (?, ?, 0, ?, 5, 0)
            ''', (account_id, game_name, today))
            conn.commit()
            return True, total_max

def use_attempt(account_id: int, game_name: str):
    with get_db() as conn:
        cursor = conn.cursor()
        today = datetime.date.today().isoformat()
        cursor.execute('''
        UPDATE game_attempts 
        SET daily_attempts = daily_attempts + 1, last_date = ?
        WHERE account_id = ? AND game_name = ?
        ''', (today, account_id, game_name))
        conn.commit()

def get_promotion_discount(item: str) -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        cursor.execute('''
        SELECT discount_percent FROM promotions 
        WHERE item = ? AND end_time > ? AND discount_percent > 0
        ORDER BY end_time DESC LIMIT 1
        ''', (item, now))
        result = cursor.fetchone()
        return result['discount_percent'] if result else 0

# ========== КЛАВИАТУРЫ ==========
def main_menu_keyboard(is_admin=False):
    """Главное меню с Reply-кнопками"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        KeyboardButton(text="🎮 Игры"),
        KeyboardButton(text="🛒 Магазин"),
        KeyboardButton(text="📜 Квесты"),
        KeyboardButton(text="💼 Работа"),
        KeyboardButton(text="🎁 Бонус"),
        KeyboardButton(text="🏆 Топ"),
        KeyboardButton(text="📊 Профиль"),
        KeyboardButton(text="⚙️ Настройки")
    ]
    kb.add(*buttons)
    if is_admin:
        kb.add(KeyboardButton(text="👑 Админка"))
    return kb

def login_keyboard():
    """Клавиатура для входа/регистрации"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Войти", callback_data="auth_login"),
         InlineKeyboardButton(text="📝 Регистрация", callback_data="auth_register")]
    ])
    return kb

def back_keyboard():
    """Кнопка отмены"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="cancel_action")]
    ])
    return kb

def generate_password_keyboard():
    """Клавиатура для генерации пароля"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сгенерировать надёжный пароль", callback_data="generate_password")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="cancel_action")]
    ])
    return kb

def settings_keyboard():
    """Клавиатура настроек"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌐 Язык", callback_data="settings_language")],
        [InlineKeyboardButton(text="💰 Автоматическая ставка", callback_data="settings_auto_bet")],
        [InlineKeyboardButton(text="👤 Управление аккаунтом", callback_data="settings_account")],
        [InlineKeyboardButton(text="➕ Создать новый аккаунт", callback_data="auth_register")],
        [InlineKeyboardButton(text="🚪 Выйти из аккаунта", callback_data="settings_logout")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")]
    ])
    return kb

def account_settings_keyboard():
    """Клавиатура управления аккаунтом"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Сменить логин", callback_data="settings_change_username")],
        [InlineKeyboardButton(text="🔐 Сменить пароль", callback_data="settings_change_password")],
        [InlineKeyboardButton(text="💾 Сохранить данные", callback_data="settings_save")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_settings")]
    ])
    return kb

def games_keyboard():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Угадай число", callback_data="game_guess")],
        [InlineKeyboardButton(text="✊✋✌️ Камень-Ножницы-Бумага", callback_data="game_rps")],
        [InlineKeyboardButton(text="❌⭕️ Крестики-Нолики", callback_data="game_ttt")],
        [InlineKeyboardButton(text="🎰 Казик", callback_data="game_slots")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")]
    ])
    return kb

def shop_keyboard(account_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT profession FROM accounts WHERE account_id = ?", (account_id,))
        current_prof = cursor.fetchone()['profession']
    
    kb = InlineKeyboardBuilder()
    
    professions = [
        ("👨‍💻 Junior (50 PC/час)", "shop_junior"),
        ("👨‍💼 Middle (100 PC/час)", "shop_middle"),
        ("👨‍🔬 Senior (300 PC/час)", "shop_senior"),
        ("👨‍💼 Manager (400 PC/час)", "shop_manager"),
        ("👨‍💼 Director (500 PC/час)", "shop_director")
    ]
    
    for text, data in professions:
        prof_name = data.replace("shop_", "")
        if current_prof == prof_name:
            kb.button(text=f"✓ {text}", callback_data="already_owned")
        else:
            discount = get_promotion_discount(prof_name)
            if discount > 0:
                kb.button(text=f"🏷️ {text} -{discount}%", callback_data=data)
            else:
                kb.button(text=text, callback_data=data)
    
    kb.button(text="🔄 Временные попытки (+5 на день)", callback_data="shop_temp_attempts")
    kb.button(text="⭐ Перманентные попытки (+1 макс.)", callback_data="shop_perm_attempts")
    kb.button(text="◀️ Назад", callback_data="back_to_menu")
    kb.adjust(1)
    return kb.as_markup()

def confirm_keyboard(item: str, quantity: int = 1):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Подтверждаю ({quantity} шт.)", callback_data=f"buy_{item}_{quantity}"),
         InlineKeyboardButton(text="❌ Отмена", callback_data="shop_cancel")],
        [InlineKeyboardButton(text="➖", callback_data=f"dec_{item}"),
         InlineKeyboardButton(text="➕", callback_data=f"inc_{item}")]
    ])
    return kb

def admin_keyboard():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👤 Управление аккаунтами", callback_data="admin_accounts")],
        [InlineKeyboardButton(text="💰 Изменить цены", callback_data="admin_prices")],
        [InlineKeyboardButton(text="🎁 Создать розыгрыш", callback_data="admin_giveaway")],
        [InlineKeyboardButton(text="📈 Установить макс. аккаунтов", callback_data="admin_max_accounts")],
        [InlineKeyboardButton(text="📝 Добавить задание", callback_data="admin_add_quest")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🏷️ Создать акцию", callback_data="admin_promotion")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")]
    ])
    return kb

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@router.message(CommandStart())
@router.message(Command("startpuls"))
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик /start и /startpuls"""
    await state.clear()
    
    try:
        await message.delete()
    except:
        pass
    
    # Проверяем, личное сообщение или группа
    if message.chat.type != "private":
        await message.answer(
            "❌ Регистрация и вход доступны только в личных сообщениях с ботом.\n"
            "Напиши мне в личку: @PulsOfficialManager_bot"
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE tg_id = ?", (message.from_user.id,))
        user = cursor.fetchone()
        
        if not user:
            cursor.execute(
                "INSERT INTO users (tg_id, max_accounts, admin, auto_bet) VALUES (?, 3, 0, 25)",
                (message.from_user.id,)
            )
            conn.commit()
            
            await message.answer_photo(
                photo="https://kappa.lol/v3Fqcl",
                caption="👋 Привет! Я Puls Bot\n\n"
                        "Экономический бот с играми, профессиями и системой уровней.\n\n"
                        "🎮 Зарабатывай монеты в играх\n"
                        "💼 Покупай профессии и получай зарплату\n"
                        "📜 Выполняй квесты\n"
                        "⭐ Прокачивай уровень и открывай бонусы\n\n"
                        "🔐 Войди или создай аккаунт, чтобы начать:",
                reply_markup=login_keyboard()
            )
        else:
            cursor.execute("SELECT * FROM accounts WHERE tg_id = ?", (message.from_user.id,))
            accounts = cursor.fetchall()
            
            if accounts:
                kb = InlineKeyboardBuilder()
                for acc in accounts[:3]:
                    kb.button(
                        text=f"👤 {acc['username']} | 💰 {acc['coins']} PC | ⭐ {acc['level']} ур.",
                        callback_data=f"select_acc_{acc['account_id']}"
                    )
                kb.button(text="➕ Создать новый аккаунт", callback_data="auth_register")
                kb.adjust(1)
                
                await message.answer_photo(
                    photo="https://kappa.lol/v3Fqcl",
                    caption="🔑 Выбери аккаунт:",
                    reply_markup=kb.as_markup()
                )
            else:
                await message.answer_photo(
                    photo="https://kappa.lol/v3Fqcl",
                    caption="👋 У тебя пока нет аккаунтов.\nСоздай новый:",
                    reply_markup=login_keyboard()
                )

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help"""
    help_text = (
        "🎮 Puls Bot - Помощь\n\n"
        "Основные команды:\n"
        "/start - Запустить бота\n"
        "/startpuls - Запустить бота\n"
        "/help - Показать это сообщение\n\n"
        "Основные функции:\n"
        "🎮 Игры - Зарабатывай монеты\n"
        "🛒 Магазин - Покупай профессии и попытки\n"
        "📜 Квесты - Выполняй задания\n"
        "💼 Работа - Получай зарплату каждый час\n"
        "🎁 Бонус - Забирай ежедневный бонус\n"
        "🏆 Топ - Соревнуйся с другими\n"
        "📊 Профиль - Смотри свой уровень\n"
        "⚙️ Настройки - Настрой бота под себя"
    )
    
    # Проверяем, есть ли у пользователя аккаунт
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM accounts WHERE tg_id = ?", (message.from_user.id,))
        account = cursor.fetchone()
    
    if not account and message.chat.type == "private":
        await message.answer(
            help_text + "\n\n🔐 Войди или зарегистрируйся, чтобы пользоваться всеми функциями!",
            reply_markup=login_keyboard()
        )
    else:
        await message.answer(help_text)

# ========== ОБРАБОТЧИКИ АВТОРИЗАЦИИ ==========
@router.callback_query(F.data.startswith("auth_"))
async def auth_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик авторизации"""
    if callback.message.chat.type != "private":
        await callback.answer("❌ Это можно делать только в личных сообщениях", show_alert=True)
        return
    
    action = callback.data.split("_")[1]
    await callback.message.delete()
    
    if action == "login":
        await callback.message.answer(
            "🔑 Вход в аккаунт\n\n"
            "Введи имя пользователя:",
            reply_markup=back_keyboard()
        )
        await state.set_state(AuthStates.login)
    
    elif action == "register":
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as count FROM accounts WHERE tg_id = ?",
                (callback.from_user.id,)
            )
            total_acc = cursor.fetchone()['count']
            
            if total_acc >= 3:
                await callback.message.answer(
                    "❌ Достигнут лимит аккаунтов (максимум 3)\n"
                    "Удали старый аккаунт или используй существующий."
                )
                await callback.answer()
                return
        
        await callback.message.answer(
            "📝 Создание аккаунта\n\n"
            "Придумай логин (3-20 символов, только буквы и цифры):",
            reply_markup=back_keyboard()
        )
        await state.set_state(AuthStates.new_username)
    
    await callback.answer()

@router.message(AuthStates.login)
async def process_login_username(message: Message, state: FSMContext):
    """Ввод логина при входе"""
    if message.chat.type != "private":
        return
    
    username = message.text.strip()
    await message.delete()
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM accounts WHERE tg_id = ? AND username = ?",
            (message.from_user.id, username)
        )
        account = cursor.fetchone()
        
        if not account:
            await message.answer(
                "❌ Аккаунт не найден.\n"
                "Проверь логин или создай новый аккаунт:",
                reply_markup=login_keyboard()
            )
            await state.clear()
            return
        
        await state.update_data(account_id=account['account_id'])
        await message.answer(
            "🔐 Введи пароль:",
            reply_markup=back_keyboard()
        )
        await state.set_state(AuthStates.password)

@router.message(AuthStates.password)
async def process_login_password(message: Message, state: FSMContext):
    """Ввод пароля при входе"""
    if message.chat.type != "private":
        return
    
    password = message.text.strip()
    await message.delete()
    
    data = await state.get_data()
    account_id = data['account_id']
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM accounts WHERE account_id = ? AND password = ?",
            (account_id, password)
        )
        account = cursor.fetchone()
        
        if not account:
            await message.answer(
                "❌ Неверный пароль.\n"
                "Попробуй снова:",
                reply_markup=back_keyboard()
            )
            return
        
        cursor.execute("SELECT admin, auto_bet FROM users WHERE tg_id = ?", (message.from_user.id,))
        user = cursor.fetchone()
        is_admin = user['admin'] == 1 if user else False
    
    await state.update_data(current_account=account_id)
    
    await message.answer_photo(
        photo="https://kappa.lol/v3Fqcl",
        caption=f"✅ С возвращением, {account['username']}!\n\n"
                f"💰 Баланс: {account['coins']} PC\n"
                f"⭐ Уровень: {account['level']}\n"
                f"💼 Профессия: {account['profession']}\n\n"
                f"👇 Выбери действие:",
        reply_markup=main_menu_keyboard(is_admin)
    )
    
    await state.clear()

@router.message(AuthStates.new_username)
async def process_new_username(message: Message, state: FSMContext):
    """Создание нового логина"""
    if message.chat.type != "private":
        return
    
    username = message.text.strip()
    await message.delete()
    
    if len(username) < 3 or len(username) > 20:
        await message.answer(
            "❌ Логин должен быть от 3 до 20 символов.\n"
            "Попробуй снова:",
            reply_markup=back_keyboard()
        )
        return
    
    if not username.isalnum():
        await message.answer(
            "❌ Логин должен содержать только буквы и цифры.\n"
            "Попробуй снова:",
            reply_markup=back_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM accounts WHERE tg_id = ? AND username = ?",
            (message.from_user.id, username)
        )
        if cursor.fetchone():
            await message.answer(
                "❌ Этот логин уже занят.\n"
                "Выбери другой:",
                reply_markup=back_keyboard()
            )
            return
    
    await state.update_data(new_username=username)
    await message.answer(
        "🔐 Отлично, логин свободен!\n\n"
        "Теперь придумай пароль (6-20 символов)\n"
        "или нажми кнопку для генерации надёжного пароля:",
        reply_markup=generate_password_keyboard()
    )
    await state.set_state(AuthStates.new_password)

@router.callback_query(F.data == "generate_password")
async def generate_password(callback: CallbackQuery, state: FSMContext):
    """Генерация надёжного пароля"""
    await callback.message.delete()
    
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(random.choice(chars) for _ in range(12))
    
    # Имитируем ввод пароля пользователем
    await callback.message.answer(
        f"🔄 Сгенерирован пароль: {password}\n\n"
        f"✅ Нажми кнопку ниже, чтобы подтвердить:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Использовать этот пароль", callback_data=f"use_password_{password}")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data.startswith("use_password_"))
async def use_generated_password(callback: CallbackQuery, state: FSMContext):
    """Использовать сгенерированный пароль"""
    await callback.message.delete()
    password = callback.data.replace("use_password_", "")
    
    await state.update_data(new_password=password)
    
    # Переходим к созданию аккаунта
    await process_new_password(callback.message, state, password)
    await callback.answer()

@router.message(AuthStates.new_password)
async def process_new_password(message: Message, state: FSMContext, password: str = None):
    """Создание аккаунта с паролем"""
    if message.chat.type != "private":
        return
    
    if password is None:
        password = message.text.strip()
        await message.delete()
    
    if len(password) < 6 or len(password) > 20:
        await message.answer(
            "❌ Пароль должен быть от 6 до 20 символов.\n"
            "Попробуй снова:",
            reply_markup=generate_password_keyboard()
        )
        return
    
    data = await state.get_data()
    username = data['new_username']
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Проверяем лимит создания аккаунтов (1 раз в 3 дня)
        cursor.execute(
            "SELECT COUNT(*) as count FROM accounts WHERE tg_id = ? AND created_at > datetime('now', '-3 days')",
            (message.from_user.id,)
        )
        recent = cursor.fetchone()['count']
        
        if recent >= 1:
            await message.answer(
                "⏳ Ты уже создавал аккаунт за последние 3 дня.\n"
                "Подожди немного или войди в существующий.",
                reply_markup=login_keyboard()
            )
            await state.clear()
            return
        
        cursor.execute(
            "SELECT COUNT(*) as count FROM accounts WHERE tg_id = ?",
            (message.from_user.id,)
        )
        total_acc = cursor.fetchone()['count']
        
        if total_acc >= 3:
            await message.answer(
                "❌ Достигнут лимит аккаунтов (максимум 3).\n"
                "Удали старый аккаунт или используй существующий."
            )
            await state.clear()
            return
        
        # Создаём аккаунт
        cursor.execute('''
        INSERT INTO accounts (tg_id, username, password, coins, level, exp)
        VALUES (?, ?, ?, 100, 1, 0)
        ''', (message.from_user.id, username, password))
        
        account_id = cursor.lastrowid
        
        games = ["Угадай число", "Камень-Ножницы-Бумага", "Крестики-Нолики", "Слот-машина"]
        for game in games:
            cursor.execute('''
            INSERT INTO game_attempts (account_id, game_name, daily_attempts, last_date, permanent_max, extra_attempts)
            VALUES (?, ?, 0, ?, 5, 0)
            ''', (account_id, game, datetime.date.today().isoformat()))
        
        conn.commit()
        
        cursor.execute("SELECT admin FROM users WHERE tg_id = ?", (message.from_user.id,))
        user = cursor.fetchone()
        is_admin = user['admin'] == 1 if user else False
    
    await state.update_data(current_account=account_id)
    
    await message.answer_photo(
        photo="https://kappa.lol/v3Fqcl",
        caption=f"🎉 Аккаунт создан!\n\n"
                f"👤 Логин: {username}\n"
                f"🔐 Пароль: {password}\n\n"
                f"❗ ВАЖНО: Администрация НИКОГДА не попросит эти данные.\n"
                f"⚠️ Никому не передавай доступ к аккаунту!\n\n"
                f"👇 Выбери действие:",
        reply_markup=main_menu_keyboard(is_admin)
    )
    
    await state.clear()

@router.callback_query(F.data.startswith("select_acc_"))
async def select_account_handler(callback: CallbackQuery, state: FSMContext):
    """Выбор аккаунта из списка"""
    account_id = int(callback.data.split("_")[-1])
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM accounts WHERE account_id = ?", (account_id,))
        account = cursor.fetchone()
        
        if not account or account['tg_id'] != callback.from_user.id:
            await callback.answer("❌ Аккаунт не найден", show_alert=True)
            return
        
        cursor.execute("SELECT admin FROM users WHERE tg_id = ?", (callback.from_user.id,))
        user = cursor.fetchone()
        is_admin = user['admin'] == 1 if user else False
    
    await state.update_data(current_account=account_id)
    await callback.message.delete()
    
    await callback.message.answer_photo(
        photo="https://kappa.lol/v3Fqcl",
        caption=f"✅ Аккаунт выбран!\n\n"
                f"👤 Логин: {account['username']}\n"
                f"💰 Баланс: {account['coins']} PC\n"
                f"⭐ Уровень: {account['level']}\n"
                f"💼 Профессия: {account['profession']}\n\n"
                f"👇 Выбери действие:",
        reply_markup=main_menu_keyboard(is_admin)
    )
    
    await callback.answer()

# ========== ОБРАБОТЧИКИ НАСТРОЕК ==========
@router.message(F.text == "⚙️ Настройки")
async def settings_menu(message: Message, state: FSMContext):
    """Меню настроек"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT auto_bet FROM users WHERE tg_id = ?", (message.from_user.id,))
        user = cursor.fetchone()
        auto_bet = user['auto_bet'] if user else 25
    
    await message.answer(
        f"⚙️ Настройки\n\n"
        f"💰 Твоя автоматическая ставка: {auto_bet} PC\n\n"
        f"Что хочешь изменить?",
        reply_markup=settings_keyboard()
    )

@router.callback_query(F.data == "back_to_settings")
async def back_to_settings(callback: CallbackQuery, state: FSMContext):
    """Назад к настройкам"""
    await callback.message.delete()
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT auto_bet FROM users WHERE tg_id = ?", (callback.from_user.id,))
        user = cursor.fetchone()
        auto_bet = user['auto_bet'] if user else 25
    
    await callback.message.answer(
        f"⚙️ Настройки\n\n"
        f"💰 Твоя автоматическая ставка: {auto_bet} PC\n\n"
        f"Что хочешь изменить?",
        reply_markup=settings_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "settings_auto_bet")
async def settings_auto_bet(callback: CallbackQuery, state: FSMContext):
    """Настройка автоматической ставки"""
    await callback.message.delete()
    await callback.message.answer(
        "💰 Автоматическая ставка\n\n"
        "Введи число от 25 до 1000,\n"
        "которое будет использоваться по умолчанию в играх:",
        reply_markup=back_keyboard()
    )
    await state.set_state(SettingsStates.auto_bet)
    await callback.answer()

@router.message(SettingsStates.auto_bet)
async def process_auto_bet(message: Message, state: FSMContext):
    """Обработка изменения автоставки"""
    try:
        bet = int(message.text.strip())
        await message.delete()
        
        if bet < 25 or bet > 1000:
            await message.answer(
                "❌ Ставка должна быть от 25 до 1000 PC.\n"
                "Попробуй снова:",
                reply_markup=back_keyboard()
            )
            return
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET auto_bet = ? WHERE tg_id = ?",
                (bet, message.from_user.id)
            )
            conn.commit()
        
        await message.answer(
            f"✅ Автоматическая ставка установлена: {bet} PC",
            reply_markup=settings_keyboard()
        )
        await state.clear()
        
    except ValueError:
        await message.answer(
            "❌ Введи целое число.",
            reply_markup=back_keyboard()
        )

@router.callback_query(F.data == "settings_account")
async def settings_account(callback: CallbackQuery, state: FSMContext):
    """Управление аккаунтом"""
    await callback.message.delete()
    
    data = await state.get_data()
    account_id = data.get('current_account')
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT username, created_at FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        account = cursor.fetchone()
    
    await callback.message.answer(
        f"👤 Управление аккаунтом\n\n"
        f"Логин: {account['username']}\n"
        f"Создан: {account['created_at'][:10]}\n\n"
        f"Выбери действие:",
        reply_markup=account_settings_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "settings_logout")
async def settings_logout(callback: CallbackQuery, state: FSMContext):
    """Выход из аккаунта"""
    await state.update_data(current_account=None)
    await callback.message.delete()
    await callback.message.answer(
        "👋 Ты вышел из аккаунта.",
        reply_markup=login_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "settings_save")
async def settings_save(callback: CallbackQuery, state: FSMContext):
    """Сохранить настройки"""
    await callback.message.delete()
    await callback.message.answer(
        "💾 Настройки сохранены!",
        reply_markup=settings_keyboard()
    )
    await callback.answer()

# ========== ОБРАБОТЧИКИ ГЛАВНОГО МЕНЮ ==========
@router.message(F.text == "🎮 Игры")
async def play_menu(message: Message, state: FSMContext):
    """Меню игр"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    await message.answer(
        "🎮 Выбери игру:\n\n"
        "🎲 Угадай число - угадай число от 1 до 100\n"
        "✊✋✌️ Камень-Ножницы-Бумага - сыграй против бота\n"
        "❌⭕️ Крестики-Нолики - сыграй против бота\n"
        "🎰 Казик - испытай удачу\n\n"
        "У тебя ограниченное количество попыток в день!",
        reply_markup=games_keyboard()
    )

@router.message(F.text == "🛒 Магазин")
async def shop_menu(message: Message, state: FSMContext):
    """Меню магазина"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT coins FROM accounts WHERE account_id = ?", (account_id,))
        coins = cursor.fetchone()['coins']
    
    await message.answer(
        f"🛒 Магазин\n\n"
        f"💰 Твой баланс: {coins} PC\n\n"
        f"Доступные товары:",
        reply_markup=shop_keyboard(account_id)
    )
    await state.set_state(ShopStates.browsing)

@router.message(F.text == "📜 Квесты")
async def quests_menu(message: Message, state: FSMContext):
    """Меню квестов"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT q.*, 
               CASE WHEN cq.quest_id IS NOT NULL THEN 1 ELSE 0 END as completed
        FROM quests q
        LEFT JOIN completed_quests cq ON q.id = cq.quest_id AND cq.account_id = ?
        ORDER BY q.type, q.reward DESC
        ''', (account_id,))
        
        quests = cursor.fetchall()
        
        if not quests:
            text = "📜 Квесты\n\nНа данный момент квестов нет."
        else:
            text = "📜 Квесты\n\n"
            for quest in quests:
                status = "✅ Выполнено" if quest['completed'] else "🔄 Доступно"
                text += f"{quest['description']}\n"
                text += f"Награда: {quest['reward']} PC\n"
                if quest['link']:
                    text += f"Ссылка: {quest['link']}\n"
                text += f"Статус: {status}\n\n"
        
        await message.answer(text)

@router.message(F.text == "💼 Работа")
async def work_menu(message: Message, state: FSMContext):
    """Меню работы"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT profession, coins, level FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        account = cursor.fetchone()
        
        level_info = get_level_info(account)
        base_salary = PROFESSIONS.get(account['profession'], 0)
        salary = int(base_salary * (1 + level_info['bonus_salary']))
        
        text = f"💼 Работа\n\n"
        text += f"Твоя профессия: {account['profession']}\n"
        text += f"Базовая зарплата: {base_salary} PC/час\n"
        if level_info['bonus_salary'] > 0:
            text += f"Бонус уровня: +{int(level_info['bonus_salary']*100)}%\n"
        text += f"Итоговая зарплата: {salary} PC/час\n\n"
        text += "🕐 Зарплата начисляется автоматически каждый час\n"
        text += "🛒 Новые профессии можно купить в магазине"
        
        await message.answer(text)
        
        cursor.execute('''
        SELECT timestamp FROM actions 
        WHERE account_id = ? AND action LIKE 'work_salary%'
        ORDER BY timestamp DESC LIMIT 1
        ''', (account_id,))
        
        last_salary = cursor.fetchone()
        now = datetime.datetime.now()
        
        if not last_salary or (now - datetime.datetime.fromisoformat(last_salary['timestamp'])).seconds >= 3600:
            cursor.execute(
                "UPDATE accounts SET coins = coins + ? WHERE account_id = ?",
                (salary, account_id)
            )
            cursor.execute(
                "INSERT INTO actions (account_id, action) VALUES (?, ?)",
                (account_id, f"work_salary_{salary}")
            )
            conn.commit()
            
            await message.answer(
                f"💰 Зарплата получена!\n\n"
                f"+{salary} Puls Coins\n"
                f"💳 Новый баланс: {account['coins'] + salary} PC"
            )

@router.message(F.text == "🎁 Бонус")
async def daily_bonus(message: Message, state: FSMContext):
    """Ежедневный бонус"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT coins, level, last_bonus FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        account = cursor.fetchone()
        
        level_info = get_level_info(account)
        now = datetime.datetime.now()
        last_bonus = account['last_bonus']
        
        if last_bonus:
            last_bonus_dt = datetime.datetime.fromisoformat(last_bonus)
            if (now - last_bonus_dt).days < 1:
                next_bonus = last_bonus_dt + datetime.timedelta(days=1)
                wait_time = next_bonus - now
                hours = wait_time.seconds // 3600
                minutes = (wait_time.seconds % 3600) // 60
                
                await message.answer(
                    f"⏳ Ты уже получал бонус сегодня\n\n"
                    f"Следующий бонус через: {hours}ч {minutes}м\n"
                    f"Приходи завтра!"
                )
                return
        
        base_bonus = random.randint(200, 300)
        bonus = int(base_bonus * (1 + level_info['bonus_daily']))
        
        cursor.execute('''
        UPDATE accounts 
        SET coins = coins + ?, last_bonus = ?
        WHERE account_id = ?
        ''', (bonus, now.isoformat(), account_id))
        
        cursor.execute(
            "INSERT INTO actions (account_id, action) VALUES (?, ?)",
            (account_id, f"daily_bonus_{bonus}")
        )
        
        conn.commit()
        
        await message.answer(
            f"🎁 Ежедневный бонус!\n\n"
            f"💰 Базовый бонус: {base_bonus} PC\n"
            f"⭐ Бонус уровня: +{int(level_info['bonus_daily']*100)}%\n"
            f"💰 Итоговый бонус: {bonus} PC\n"
            f"💳 Новый баланс: {account['coins'] + bonus} PC\n\n"
            f"Приходи завтра за новым бонусом!"
        )

@router.message(F.text == "🏆 Топ")
async def leaderboard_menu(message: Message, state: FSMContext):
    """Лидерборд"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT username, coins, level, total_exp FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        current = cursor.fetchone()
        
        cursor.execute('''
        SELECT username, coins, level 
        FROM accounts 
        ORDER BY coins DESC 
        LIMIT 10
        ''')
        top_balance = cursor.fetchall()
        
        cursor.execute('''
        SELECT username, total_exp, level 
        FROM accounts 
        ORDER BY total_exp DESC 
        LIMIT 10
        ''')
        top_exp = cursor.fetchall()
        
        text = "🏆 Лидерборд\n\n"
        
        text += "Топ-10 по балансу:\n"
        for i, player in enumerate(top_balance, 1):
            medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"{i}."
            text += f"{medal} {player['username']} - {player['coins']} PC (Ур. {player['level']})\n"
        
        text += f"\nТвоё место: "
        cursor.execute('''
        SELECT COUNT(*) + 1 as rank
        FROM accounts 
        WHERE coins > ?
        ''', (current['coins'],))
        rank = cursor.fetchone()['rank']
        text += f"{rank}\n"
        text += f"👤 {current['username']} - {current['coins']} PC (Ур. {current['level']})\n\n"
        
        text += "Топ-10 по опыту:\n"
        for i, player in enumerate(top_exp, 1):
            medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"{i}."
            text += f"{medal} {player['username']} - {player['total_exp']} опыта (Ур. {player['level']})\n"
        
        await message.answer(text)

@router.message(F.text == "📊 Профиль")
async def my_level(message: Message, state: FSMContext):
    """Информация об уровне"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await message.answer(
            "❌ Сначала войди в аккаунт.",
            reply_markup=login_keyboard()
        )
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT username, level, exp, coins, profession, games_played FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        account = cursor.fetchone()
        
        level_info = get_level_info(account)
        
        progress_bar_length = 20
        filled = int(level_info['progress'] * progress_bar_length)
        progress_bar = "█" * filled + "░" * (progress_bar_length - filled)
        
        text = f"📊 Профиль: {account['username']}\n\n"
        text += f"⭐ Уровень: {level_info['level']}\n"
        text += f"💰 Баланс: {account['coins']} PC\n"
        text += f"💼 Профессия: {account['profession']}\n"
        text += f"🎮 Сыграно игр: {account['games_played']}\n\n"
        
        text += f"Опыт: {level_info['exp']} / {LEVELS.get(level_info['level'] + 1, {'exp': 'MAX'})['exp']}\n"
        text += f"{progress_bar} {int(level_info['progress']*100)}%\n"
        text += f"До следующего уровня: {level_info['to_next']} опыта\n\n"
        
        text += "Твои бонусы:\n"
        if level_info['bonus_win'] > 0:
            text += f"• +{int(level_info['bonus_win']*100)}% к выигрышам\n"
        if level_info['bonus_daily'] > 0:
            text += f"• +{int(level_info['bonus_daily']*100)}% к ежедневке\n"
        if level_info['bonus_salary'] > 0:
            text += f"• +{int(level_info['bonus_salary']*100)}% к зарплате\n"
        if level_info['max_attempts_bonus'] > 0:
            text += f"• +{level_info['max_attempts_bonus']} попыток в день\n"
        if level_info['double_win_chance'] > 0:
            text += f"• {int(level_info['double_win_chance']*100)}% шанс удвоить выигрыш\n"
        
        await message.answer(text)

# ========== ОБРАБОТЧИКИ ИГР ==========
@router.callback_query(F.data.startswith("game_"))
async def game_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора игры"""
    game_type = callback.data.split("_")[1]
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await callback.answer("❌ Сначала войди в аккаунт", show_alert=True)
        return
    
    game_names = {
        "guess": "Угадай число",
        "rps": "Камень-Ножницы-Бумага",
        "ttt": "Крестики-Нолики",
        "slots": "Слот-машина"
    }
    
    game_name = game_names.get(game_type)
    if not game_name:
        await callback.answer("❌ Игра не найдена", show_alert=True)
        return
    
    available, remaining = check_attempts(account_id, game_name)
    
    if not available:
        await callback.answer(
            f"❌ Попытки закончились. Доступно {remaining}/день",
            show_alert=True
        )
        return
    
    await state.update_data(game_type=game_type, game_name=game_name)
    
    # Получаем автоставку пользователя
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT auto_bet FROM users WHERE tg_id = ?", (callback.from_user.id,))
        user = cursor.fetchone()
        auto_bet = user['auto_bet'] if user else 25
    
    if game_type == "guess":
        # Автоматическая ставка
        bet = auto_bet
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT coins FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            coins = cursor.fetchone()['coins']
            
            if bet > coins:
                bet = min(25, coins)
        
        secret = random.randint(1, 100)
        await state.update_data(
            bet=bet,
            secret_number=secret,
            attempts_left=7
        )
        
        await callback.message.edit_text(
            f"🎲 Угадай число\n\n"
            f"✅ Ставка: {bet} PC (авто)\n"
            f"Я загадал число от 1 до 100.\n"
            f"У тебя 7 попыток.\n\n"
            f"Введи число:"
        )
        await state.set_state(GameStates.play)
    
    elif game_type == "rps":
        await callback.message.edit_text(
            "✊✋✌️ Камень-Ножницы-Бумага\n\n"
            "Выбери свой ход:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✊ Камень", callback_data="rps_rock"),
                 InlineKeyboardButton(text="✋ Бумага", callback_data="rps_paper"),
                 InlineKeyboardButton(text="✌️ Ножницы", callback_data="rps_scissors")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_games")]
            ])
        )
        await state.set_state(GameStates.rps_choice)
    
    elif game_type == "ttt":
        await callback.message.edit_text(
            "❌⭕️ Крестики-Нолики\n\n"
            "Ты играешь за ❌. Сделай первый ход:"
        )
        board = [[" " for _ in range(3)] for _ in range(3)]
        await state.update_data(ttt_board=board, ttt_turn="X")
        await show_ttt_board(callback.message, board)
        await state.set_state(GameStates.ttt_move)
    
    elif game_type == "slots":
        bet = auto_bet
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT coins FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            coins = cursor.fetchone()['coins']
            
            if bet > coins:
                bet = min(25, coins)
        
        await state.update_data(bet=bet)
        
        await callback.message.edit_text(
            f"🎰 Казик\n\n"
            f"✅ Ставка: {bet} PC (авто)\n\n"
            f"Нажми кнопку, чтобы крутить:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎰 Крутить!", callback_data="spin_slots")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_games")]
            ])
        )
        await state.set_state(GameStates.play)
    
    await callback.answer()

@router.message(GameStates.play)
async def process_guess(message: Message, state: FSMContext):
    """Обработка угадывания числа"""
    data = await state.get_data()
    game_type = data.get('game_type')
    
    if game_type != "guess":
        return
    
    try:
        guess = int(message.text.strip())
        await message.delete()
        
        secret = data['secret_number']
        attempts_left = data['attempts_left'] - 1
        bet = data['bet']
        account_id = data['current_account']
        
        if guess < 1 or guess > 100:
            await message.answer("❌ Число должно быть от 1 до 100. Попробуй снова:")
            return
        
        if guess < secret:
            hint = "⬆️ Загаданное число больше"
        elif guess > secret:
            hint = "⬇️ Загаданное число меньше"
        else:
            await finish_game(message, state, account_id, bet, 3.0, "win")
            return
        
        if attempts_left <= 0:
            await finish_game(message, state, account_id, bet, 0.0, "loss")
            return
        
        await state.update_data(attempts_left=attempts_left)
        await message.answer(
            f"{hint}\n"
            f"Осталось попыток: {attempts_left}\n"
            f"Введи следующее число:"
        )
    
    except ValueError:
        await message.answer("❌ Введи целое число.")

async def show_ttt_board(message: Message, board: List[List[str]]):
    """Показать поле крестиков-ноликов"""
    symbols = {" ": "⬜", "X": "❌", "O": "⭕️"}
    
    board_text = ""
    for i in range(3):
        row = []
        for j in range(3):
            cell_id = i * 3 + j + 1
            if board[i][j] == " ":
                row.append(f"{cell_id}")
            else:
                row.append(symbols[board[i][j]])
        board_text += " | ".join(row) + "\n"
        if i < 2:
            board_text += "───┼───┼───\n"
    
    await message.answer(
        f"❌⭕️ Крестики-Нолики\n\n{board_text}\nТы играешь за ❌\n\nВыбери номер клетки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="ttt_1"),
             InlineKeyboardButton(text="2", callback_data="ttt_2"),
             InlineKeyboardButton(text="3", callback_data="ttt_3")],
            [InlineKeyboardButton(text="4", callback_data="ttt_4"),
             InlineKeyboardButton(text="5", callback_data="ttt_5"),
             InlineKeyboardButton(text="6", callback_data="ttt_6")],
            [InlineKeyboardButton(text="7", callback_data="ttt_7"),
             InlineKeyboardButton(text="8", callback_data="ttt_8"),
             InlineKeyboardButton(text="9", callback_data="ttt_9")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_games")]
        ])
    )

@router.callback_query(GameStates.ttt_move, F.data.startswith("ttt_"))
async def process_ttt_move(callback: CallbackQuery, state: FSMContext):
    """Обработка хода в крестиках-ноликах"""
    try:
        cell = int(callback.data.split("_")[1]) - 1
        row, col = cell // 3, cell % 3
        
        data = await state.get_data()
        board = data['ttt_board']
        account_id = data['current_account']
        
        # Получаем автоставку
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT auto_bet FROM users WHERE tg_id = ?", (callback.from_user.id,))
            user = cursor.fetchone()
            auto_bet = user['auto_bet'] if user else 25
            
            cursor.execute(
                "SELECT coins FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            coins = cursor.fetchone()['coins']
            bet = min(auto_bet, coins)
        
        if board[row][col] != " ":
            await callback.answer("❌ Эта клетка уже занята!", show_alert=True)
            return
        
        board[row][col] = "X"
        
        if check_ttt_win(board, "X"):
            await finish_game(callback, state, account_id, bet, 2.0, "win")
            return
        
        if all(cell != " " for row in board for cell in row):
            await finish_game(callback, state, account_id, bet, 1.0, "draw")
            return
        
        bot_move = get_bot_move(board)
        if bot_move:
            br, bc = bot_move
            board[br][bc] = "O"
            
            if check_ttt_win(board, "O"):
                await finish_game(callback, state, account_id, bet, 0.0, "loss")
                return
            
            if all(cell != " " for row in board for cell in row):
                await finish_game(callback, state, account_id, bet, 1.0, "draw")
                return
        
        await state.update_data(ttt_board=board)
        await callback.message.delete()
        await show_ttt_board(callback.message, board)
        await callback.answer()
    
    except Exception as e:
        await callback.answer(f"❌ Ошибка", show_alert=True)

def check_ttt_win(board: List[List[str]], player: str) -> bool:
    for i in range(3):
        if all(board[i][j] == player for j in range(3)):
            return True
        if all(board[j][i] == player for j in range(3)):
            return True
    if all(board[i][i] == player for i in range(3)):
        return True
    if all(board[i][2-i] == player for i in range(3)):
        return True
    return False

def get_bot_move(board: List[List[str]]) -> Optional[Tuple[int, int]]:
    for i in range(3):
        for j in range(3):
            if board[i][j] == " ":
                board[i][j] = "O"
                if check_ttt_win(board, "O"):
                    board[i][j] = " "
                    return (i, j)
                board[i][j] = " "
    
    for i in range(3):
        for j in range(3):
            if board[i][j] == " ":
                board[i][j] = "X"
                if check_ttt_win(board, "X"):
                    board[i][j] = " "
                    return (i, j)
                board[i][j] = " "
    
    if board[1][1] == " ":
        return (1, 1)
    
    corners = [(0, 0), (0, 2), (2, 0), (2, 2)]
    random.shuffle(corners)
    for i, j in corners:
        if board[i][j] == " ":
            return (i, j)
    
    for i in range(3):
        for j in range(3):
            if board[i][j] == " ":
                return (i, j)
    
    return None

@router.callback_query(GameStates.rps_choice, F.data.startswith("rps_"))
async def process_rps_choice(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора в камень-ножницы-бумага"""
    choice = callback.data.split("_")[1]
    choices = {"rock": "✊", "paper": "✋", "scissors": "✌️"}
    
    data = await state.get_data()
    account_id = data.get('current_account')
    
    # Получаем автоставку
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT auto_bet FROM users WHERE tg_id = ?", (callback.from_user.id,))
        user = cursor.fetchone()
        auto_bet = user['auto_bet'] if user else 25
        
        cursor.execute(
            "SELECT coins FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        coins = cursor.fetchone()['coins']
        bet = min(auto_bet, coins)
    
    await state.update_data(rps_choice=choice, bet=bet)
    
    # Ход бота
    bot_choice = random.choice(["rock", "paper", "scissors"])
    bot_emoji = choices[bot_choice]
    
    # Определяем результат
    if choice == bot_choice:
        result = "draw"
        multiplier = 1.0
    elif (choice == "rock" and bot_choice == "scissors") or \
         (choice == "paper" and bot_choice == "rock") or \
         (choice == "scissors" and bot_choice == "paper"):
        result = "win"
        multiplier = 2.0
    else:
        result = "loss"
        multiplier = 0.0
    
    await finish_game(callback, state, account_id, bet, multiplier, result)
    await callback.answer()

@router.callback_query(GameStates.play, F.data == "spin_slots")
async def spin_slots(callback: CallbackQuery, state: FSMContext):
    """Крутить слот-машину"""
    data = await state.get_data()
    account_id = data.get('current_account')
    bet = data.get('bet')
    
    symbols = ["🍒", "🍋", "🍊", "🍇", "🔔", "⭐", "7️⃣"]
    reels = [random.choice(symbols) for _ in range(3)]
    
    if reels[0] == reels[1] == reels[2]:
        if reels[0] == "7️⃣":
            multiplier = 10.0
        elif reels[0] == "⭐":
            multiplier = 5.0
        else:
            multiplier = 3.0
    elif reels[0] == reels[1] or reels[1] == reels[2]:
        multiplier = 1.5
    else:
        multiplier = 0.0
    
    await callback.message.edit_text(
        f"🎰 {' | '.join(reels)}\n\n"
        f"{'✅ Победа!' if multiplier > 0 else '❌ Проигрыш'}"
    )
    
    await finish_game(callback, state, account_id, bet, multiplier, "win" if multiplier > 0 else "loss")
    await callback.answer()

async def finish_game(source, state: FSMContext, account_id: int, bet: int, multiplier: float, result: str):
    """Завершение игры"""
    data = await state.get_data()
    game_name = data.get('game_name')
    
    use_attempt(account_id, game_name)
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT level, exp FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        account = cursor.fetchone()
        level_info = get_level_info(account)
        
        win_multiplier = multiplier * (1 + level_info["bonus_win"])
        
        double_win = False
        if result == "win" and random.random() < level_info["double_win_chance"]:
            win_multiplier *= 2
            double_win = True
        
        win_amount = int(bet * win_multiplier)
        
        if result == "win":
            cursor.execute(
                "UPDATE accounts SET coins = coins + ? WHERE account_id = ?",
                (win_amount, account_id)
            )
            profit = win_amount - bet
        elif result == "loss":
            cursor.execute(
                "UPDATE accounts SET coins = coins - ? WHERE account_id = ?",
                (bet, account_id)
            )
            profit = -bet
        else:
            profit = 0
        
        exp_gained = int(bet * 0.1)
        await add_exp(account_id, exp_gained)
        
        cursor.execute(
            "UPDATE accounts SET games_played = games_played + 1 WHERE account_id = ?",
            (account_id,)
        )
        
        if result == "win":
            cursor.execute(
                "UPDATE accounts SET daily_wins = daily_wins + 1, weekly_wins = weekly_wins + 1, monthly_wins = monthly_wins + 1 WHERE account_id = ?",
                (account_id,)
            )
        
        cursor.execute(
            "INSERT INTO actions (account_id, action) VALUES (?, ?)",
            (account_id, f"game_{game_name}_{result}_{profit}")
        )
        
        cursor.execute(
            "SELECT coins FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        new_balance = cursor.fetchone()['coins']
        
        conn.commit()
    
    if isinstance(source, CallbackQuery):
        message = source.message
    else:
        message = source
    
    result_text = ""
    if result == "win":
        result_text = f"✅ Победа!\n\n"
        result_text += f"Ты выиграл: {win_amount} PC\n"
        result_text += f"Множитель: {multiplier}x\n"
        if level_info["bonus_win"] > 0:
            result_text += f"Бонус уровня: +{int(level_info['bonus_win']*100)}%\n"
        if double_win:
            result_text += f"✨ ДВОЙНОЙ ВЫИГРЫШ!\n"
        result_text += f"💰 Чистая прибыль: +{profit} PC\n"
    elif result == "loss":
        result_text = f"❌ Поражение\n\n"
        result_text += f"Ты проиграл: {bet} PC\n"
        result_text += f"💰 Потеря: -{bet} PC\n"
    else:
        result_text = f"🤝 Ничья\n\n"
        result_text += f"Ставка возвращена\n"
        result_text += f"💰 Изменение: 0 PC\n"
    
    result_text += f"\n💳 Новый баланс: {new_balance} PC"
    
    await message.answer(
        result_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Играть снова", callback_data="game_" + data.get('game_type'))],
            [InlineKeyboardButton(text="📊 Главное меню", callback_data="back_to_menu")]
        ])
    )
    
    await state.clear()

# ========== ОБРАБОТЧИКИ МАГАЗИНА ==========
@router.callback_query(ShopStates.browsing, F.data.startswith("shop_"))
async def shop_item_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора товара"""
    item = callback.data.split("_")[1]
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await callback.answer("❌ Сначала войди в аккаунт", show_alert=True)
        return
    
    if item == "cancel":
        await callback.message.delete()
        await state.clear()
        await callback.answer()
        return
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT price FROM shop_prices WHERE item = ?", (item,))
        price_info = cursor.fetchone()
        
        if not price_info:
            await callback.answer("❌ Товар не найден", show_alert=True)
            return
        
        base_price = price_info['price']
        discount = get_promotion_discount(item)
        final_price = int(base_price * (1 - discount/100))
        
        if item in PROFESSIONS:
            cursor.execute(
                "SELECT profession FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            current_prof = cursor.fetchone()['profession']
            if current_prof == item:
                await callback.answer("❌ У тебя уже есть эта профессия", show_alert=True)
                return
        
        await state.update_data(
            shop_item=item,
            shop_price=final_price,
            shop_quantity=1
        )
        
        item_names = {
            "junior": "👨‍💻 Профессия Junior",
            "middle": "👨‍💼 Профессия Middle",
            "senior": "👨‍🔬 Профессия Senior",
            "manager": "👨‍💼 Профессия Manager",
            "director": "👨‍💼 Профессия Director",
            "temp_attempts": "🔄 Временные попытки",
            "perm_attempts": "⭐ Перманентные попытки"
        }
        
        item_name = item_names.get(item, item)
        
        text = f"🛒 Покупка\n\n"
        text += f"Товар: {item_name}\n"
        text += f"Цена: {final_price} PC"
        if discount > 0:
            text += f" (скидка {discount}%)\n"
        else:
            text += "\n"
        
        if item in ["temp_attempts", "perm_attempts"]:
            text += f"\nКоличество: 1\n\n"
            text += "Подтверди покупку:"
            await callback.message.edit_text(
                text,
                reply_markup=confirm_keyboard(item, 1)
            )
        else:
            await callback.message.edit_text(
                text + "\nПодтверди покупку:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Купить", callback_data=f"buy_{item}_1"),
                     InlineKeyboardButton(text="❌ Отмена", callback_data="shop_cancel")]
                ])
            )
    
    await callback.answer()

@router.callback_query(ShopStates.browsing, F.data.startswith(("buy_", "inc_", "dec_")))
async def shop_purchase_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик покупки"""
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if not account_id:
        await callback.answer("❌ Сначала войди в аккаунт", show_alert=True)
        return
    
    action, item, *rest = callback.data.split("_")
    quantity = int(rest[0]) if rest else 1
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT coins FROM accounts WHERE account_id = ?", (account_id,))
        balance = cursor.fetchone()['coins']
        
        cursor.execute("SELECT price FROM shop_prices WHERE item = ?", (item,))
        price_info = cursor.fetchone()
        
        if not price_info:
            await callback.answer("❌ Товар не найден", show_alert=True)
            return
        
        base_price = price_info['price']
        discount = get_promotion_discount(item)
        final_price = int(base_price * (1 - discount/100))
        total_price = final_price * quantity
        
        if action == "buy":
            if balance < total_price:
                await callback.answer(
                    f"❌ Недостаточно средств. Нужно: {total_price} PC",
                    show_alert=True
                )
                return
            
            if item in PROFESSIONS:
                cursor.execute(
                    "UPDATE accounts SET profession = ?, coins = coins - ? WHERE account_id = ?",
                    (item, total_price, account_id)
                )
                cursor.execute(
                    "INSERT INTO actions (account_id, action) VALUES (?, ?)",
                    (account_id, f"buy_profession_{item}_{total_price}")
                )
                
                await callback.message.edit_text(
                    f"✅ Покупка совершена!\n\n"
                    f"Ты приобрёл профессию: {item}\n"
                    f"Списано: {total_price} PC\n"
                    f"Новый баланс: {balance - total_price} PC\n\n"
                    f"Теперь ты получаешь {PROFESSIONS[item]} PC каждый час!"
                )
            
            elif item == "temp_attempts":
                cursor.execute(
                    "UPDATE game_attempts SET extra_attempts = extra_attempts + ? WHERE account_id = ?",
                    (5 * quantity, account_id)
                )
                cursor.execute(
                    "UPDATE accounts SET coins = coins - ? WHERE account_id = ?",
                    (total_price, account_id)
                )
                cursor.execute(
                    "INSERT INTO actions (account_id, action) VALUES (?, ?)",
                    (account_id, f"buy_temp_attempts_{total_price}")
                )
                
                await callback.message.edit_text(
                    f"✅ Покупка совершена!\n\n"
                    f"Ты приобрёл временные попытки\n"
                    f"+{5 * quantity} попыток ко всем играм на сегодня\n"
                    f"Списано: {total_price} PC\n"
                    f"Новый баланс: {balance - total_price} PC"
                )
            
            elif item == "perm_attempts":
                cursor.execute(
                    "UPDATE game_attempts SET permanent_max = permanent_max + ? WHERE account_id = ?",
                    (quantity, account_id)
                )
                cursor.execute(
                    "UPDATE accounts SET coins = coins - ? WHERE account_id = ?",
                    (total_price, account_id)
                )
                cursor.execute(
                    "INSERT INTO actions (account_id, action) VALUES (?, ?)",
                    (account_id, f"buy_perm_attempts_{total_price}")
                )
                
                await callback.message.edit_text(
                    f"✅ Покупка совершена!\n\n"
                    f"Ты приобрёл перманентные попытки\n"
                    f"+{quantity} к макс. количеству попыток во всех играх\n"
                    f"Списано: {total_price} PC\n"
                    f"Новый баланс: {balance - total_price} PC"
                )
            
            conn.commit()
            await state.clear()
        
        elif action in ["inc", "dec"]:
            current_qty = data.get('shop_quantity', 1)
            
            if action == "inc":
                new_qty = current_qty + 1
                if new_qty > 10:
                    await callback.answer("❌ Максимум 10 штук", show_alert=True)
                    return
            else:
                new_qty = current_qty - 1
                if new_qty < 1:
                    await callback.answer("❌ Минимум 1 штука", show_alert=True)
                    return
            
            total_price = final_price * new_qty
            
            item_names = {
                "temp_attempts": "🔄 Временные попытки",
                "perm_attempts": "⭐ Перманентные попытки"
            }
            
            item_name = item_names.get(item, item)
            
            text = f"🛒 Покупка\n\n"
            text += f"Товар: {item_name}\n"
            text += f"Цена за шт: {final_price} PC"
            if discount > 0:
                text += f" (скидка {discount}%)\n"
            else:
                text += "\n"
            text += f"Количество: {new_qty}\n"
            text += f"Итого: {total_price} PC\n\n"
            text += f"Твой баланс: {balance} PC\n\n"
            
            if balance < total_price:
                text += "❌ Недостаточно средств\n"
            
            text += "Подтверди покупку:"
            
            await state.update_data(shop_quantity=new_qty)
            await callback.message.edit_text(
                text,
                reply_markup=confirm_keyboard(item, new_qty)
            )
    
    await callback.answer()

# ========== ОБРАБОТЧИКИ ВОЗВРАТА ==========
@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    
    data = await state.get_data()
    account_id = data.get('current_account')
    
    if account_id:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT u.admin 
            FROM users u
            JOIN accounts a ON u.tg_id = a.tg_id
            WHERE a.account_id = ?
            ''', (account_id,))
            
            result = cursor.fetchone()
            is_admin = result['admin'] == 1 if result else False
        
        await callback.message.delete()
        await callback.message.answer_photo(
            photo="https://kappa.lol/v3Fqcl",
            caption="📊 Главное меню\n\nВыбери действие:",
            reply_markup=main_menu_keyboard(is_admin)
        )
    else:
        await callback.message.delete()
        await callback.message.answer(
            "Главное меню",
            reply_markup=login_keyboard()
        )
    
    await callback.answer()

@router.callback_query(F.data == "back_to_games")
async def back_to_games(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору игры"""
    await state.clear()
    await callback.message.delete()
    await callback.message.answer(
        "🎮 Выбери игру:",
        reply_markup=games_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "cancel_action")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.clear()
    await callback.message.delete()
    await callback.message.answer(
        "❌ Действие отменено",
        reply_markup=login_keyboard()
    )
    await callback.answer()

# ========== ЗАЩИТА ОТ ЧУЖИХ КНОПОК ==========
@router.callback_query()
async def unknown_callback(callback: CallbackQuery):
    """Обработчик неизвестных callback-ов"""
    messages = [
        "❌ Это не твоя кнопка!",
        "🚫 Доступ запрещён!",
        "⚠️ Эту кнопку нажал не ты!",
        "🔒 Кнопка заблокирована!",
        "🙅‍♂️ Не твоя кнопка!"
    ]
    await callback.answer(random.choice(messages), show_alert=True)
    
    # Если в группе, отправляем в ЛС
    if callback.message.chat.type != "private":
        try:
            await bot.send_message(
                callback.from_user.id,
                f"⚠️ Ты нажал кнопку в группе {callback.message.chat.title}\n"
                f"Кнопка: {callback.data}\n\n"
                f"Используй бота в личных сообщениях!"
            )
        except:
            pass

# ========== ЗАПУСК БОТА ==========
async def main():
    """Запуск бота"""
    init_db()
    asyncio.create_task(periodic_tasks())
    await dp.start_polling(bot)

async def periodic_tasks():
    """Периодические задачи"""
    while True:
        now = datetime.datetime.now()
        if now.hour == 0 and now.minute == 0:
            with get_db() as conn:
                cursor = conn.cursor()
                today = datetime.date.today().isoformat()
                cursor.execute('''
                UPDATE accounts 
                SET daily_games = 0, daily_wins = 0, last_daily_reset = ?
                WHERE last_daily_reset IS NULL OR last_daily_reset < ?
                ''', (today, today))
                conn.commit()
        
        with get_db() as conn:
            cursor = conn.cursor()
            now_iso = datetime.datetime.now().isoformat()
            cursor.execute(
                "DELETE FROM promotions WHERE end_time < ?",
                (now_iso,)
            )
            conn.commit()
        
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
