import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ChatMemberHandler
from datetime import datetime, timedelta
import asyncio
import re
import aiohttp

TOKEN = "8533732699:AAH9zGR8qmcxQanWOZk3h8uUdm7gaEPIKPc"
ADMIN_IDS = [6708209142]

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

admin_names = {}
user_requests = {}
request_counter = 0
support_chats = {}
chat_history = {}
group_welcome_settings = {}
group_goodbye_settings = {}
pending_group_settings = {}
group_admins_cache = {}

bot_clones = {}
clone_creation_sessions = {}
technical_breaks = False
tech_break_message = "🔧 В боте сейчас технические работы. Приходите позже!"
bot_owners = {}
accepted_rules = {}
pending_requests = {}
blacklisted_users = {}
request_status = {}
support_assignments = {}
pinned_messages = {}
active_chats = {}
chat_timers = {}

REQUEST_TOPICS = {
    "problem": "🔧 Проблема",
    "question": "❓ Вопрос",
    "suggestion": "💡 Предложение",
    "complaint": "⚠️ Жалоба",
    "other": "📝 Другое"
}

async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        chat_member = await context.bot.get_chat_member(chat_id, user_id)
        return chat_member.status in ['administrator', 'creator']
    except Exception as e:
        logger.error(f"Ошибка проверки админа: {e}")
        return False

def get_new_request_id():
    global request_counter
    request_counter += 1
    return f"REQ-{request_counter:06d}"

def validate_admin_name(name: str) -> bool:
    pattern = r'^[А-ЯЁ][а-яё]+ [А-ЯЁ]\.$'
    return bool(re.match(pattern, name))

async def validate_bot_token(token: str) -> tuple:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.telegram.org/bot{token}/getMe") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('ok'):
                        return True, data['result'].get('username', 'Unknown')
                return False, None
    except:
        return False, None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    
    if user.id in blacklisted_users:
        await update.message.reply_text("⛔ Вам заблокирован доступ к поддержке.")
        return
    
    if chat.type in ['group', 'supergroup']:
        await update.message.reply_text(
            f"👋 Привет, {user.first_name}!\n\nЯ бот поддержки Puls. Чтобы связаться с поддержкой, напишите мне в личные сообщения: @{context.bot.username}"
        )
        return
    
    if user.id in ADMIN_IDS:
        if user.id not in admin_names:
            await update.message.reply_text(
                "👋 Добро пожаловать в систему поддержки Puls!\n\n"
                "Пожалуйста, введите ваше имя по примеру: Иван З.\n"
                "(Первая буква заглавная, фамилия сокращенно с точкой)"
            )
            context.user_data['awaiting_name'] = True
        else:
            await show_admin_menu(update, context)
    else:
        if technical_breaks:
            await update.message.reply_text(tech_break_message)
            return
        await show_main_menu(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📞 Связаться с поддержкой", callback_data="contact_support")],
        [InlineKeyboardButton("ℹ️ О боте", callback_data="about_bot")],
        [InlineKeyboardButton("📊 Мои обращения", callback_data="my_requests")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"👋 Привет, {update.effective_user.first_name}!\n\n"
        f"Я бот поддержки Puls. Помогу связаться с операторами, отвечу на вопросы и решу проблемы.\n\n"
        f"Выберите действие:",
        reply_markup=reply_markup
    )

async def show_contact_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_query=None):
    keyboard = [
        [InlineKeyboardButton("🔧 Проблема", callback_data="topic_problem")],
        [InlineKeyboardButton("❓ Вопрос", callback_data="topic_question")],
        [InlineKeyboardButton("💡 Предложение", callback_data="topic_suggestion")],
        [InlineKeyboardButton("⚠️ Жалоба", callback_data="topic_complaint")],
        [InlineKeyboardButton("📝 Другое", callback_data="topic_other")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "📋 Выберите тему обращения:"
    
    if callback_query:
        await callback_query.message.edit_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = ""
    if technical_breaks:
        status_text = "\n\n🔧 ТЕХНИЧЕСКИЙ ПЕРЕРЫВ ВКЛЮЧЕН"
    
    keyboard = [
        [InlineKeyboardButton("📨 Новые обращения", callback_data="admin_new_requests")],
        [InlineKeyboardButton("📨 Активные чаты", callback_data="admin_active_chats")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("🤖 Создать копию бота", callback_data="admin_create_clone")],
        [InlineKeyboardButton("🔧 Технический перерыв", callback_data="admin_tech_break")],
        [InlineKeyboardButton("⛔ Черный список", callback_data="admin_blacklist")],
        [InlineKeyboardButton("⚙️ Настройки групп", callback_data="admin_settings")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"👨‍💼 Панель администратора\n\nДобро пожаловать, {admin_names.get(update.effective_user.id, 'Администратор')}!{status_text}",
        reply_markup=reply_markup
    )

async def create_clone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав для создания копий бота")
        return
    
    clone_creation_sessions[user.id] = {
        'status': 'awaiting_token',
        'expires': datetime.now() + timedelta(minutes=10)
    }
    
    keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel_clone")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🤖 Создание копии бота\n\n"
        "Отправьте токен нового бота в течение 10 минут:\n"
        "(получите у @BotFather через команду /newbot)\n\n"
        "Токен выглядит так: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
        reply_markup=reply_markup
    )
    
    asyncio.create_task(check_clone_creation_timeout(user.id, context))

async def check_clone_creation_timeout(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(600)
    if user_id in clone_creation_sessions and clone_creation_sessions[user_id]['status'] == 'awaiting_token':
        del clone_creation_sessions[user_id]
        try:
            await context.bot.send_message(
                user_id,
                "⏰ Время на отправку токена истекло. Создание копии отменено."
            )
        except:
            pass

async def handle_clone_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    token = update.message.text.strip()
    
    if user.id not in clone_creation_sessions or clone_creation_sessions[user.id]['status'] != 'awaiting_token':
        return
    
    if datetime.now() > clone_creation_sessions[user.id]['expires']:
        del clone_creation_sessions[user.id]
        await update.message.reply_text("⏰ Время истекло. Начните создание копии заново.")
        return
    
    loading_msg = await update.message.reply_text("⏳ Проверяю токен...")
    
    is_valid, bot_username = await validate_bot_token(token)
    
    if not is_valid:
        await loading_msg.edit_text(
            "❌ Токен недействителен. Проверьте правильность и попробуйте снова.\n\n"
            "Токен можно получить у @BotFather"
        )
        return
    
    await loading_msg.delete()
    
    clone_creation_sessions[user.id]['token'] = token
    clone_creation_sessions[user.id]['bot_username'] = bot_username
    clone_creation_sessions[user.id]['status'] = 'awaiting_admins'
    
    keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel_clone")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"✅ Токен принят! Бот: @{bot_username}\n\n"
        f"Теперь отправьте ID администраторов поддержки через запятую\n"
        f"(например: 123456789, 987654321)\n\n"
        f"Ваш ID ({user.id}) будет добавлен автоматически",
        reply_markup=reply_markup
    )

async def handle_clone_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    admins_text = update.message.text.strip()
    
    if user.id not in clone_creation_sessions or clone_creation_sessions[user.id]['status'] != 'awaiting_admins':
        return
    
    try:
        admin_ids = [int(x.strip()) for x in admins_text.split(',') if x.strip().isdigit()]
        
        if user.id not in admin_ids:
            admin_ids.append(user.id)
        
        admin_ids = list(set(admin_ids))
        
        clone_data = clone_creation_sessions[user.id]
        token = clone_data['token']
        bot_username = clone_data['bot_username']
        
        clone_id = f"clone_{len(bot_clones) + 1}"
        bot_clones[clone_id] = {
            'token': token,
            'admin_ids': admin_ids,
            'owner_id': user.id,
            'bot_username': bot_username,
            'created_at': datetime.now().strftime("%d.%m.%Y %H:%M"),
            'status': 'active'
        }
        
        bot_owners[clone_id] = user.id
        
        del clone_creation_sessions[user.id]
        
        admins_list = ', '.join(map(str, admin_ids))
        
        instruction_text = (
            f"✅ Копия бота успешно создана!\n\n"
            f"🤖 Бот: @{bot_username}\n"
            f"🆔 ID копии: {clone_id}\n"
            f"👥 Администраторы: {admins_list}\n\n"
            f"📋 ЧТО ДЕЛАТЬ ДАЛЬШЕ:\n\n"
            f"1. Зайдите на https://bothost.ru/\n"
            f"2. Нажмите «Создать бота»\n"
            f"3. Выберите Python\n"
            f"4. Вставьте этот токен:\n"
            f"<code>{token}</code>\n"
            f"5. В переменные окружения добавьте:\n"
            f"<code>ADMIN_IDS={','.join(map(str, admin_ids))}</code>\n"
            f"6. Загрузите код бота и нажмите «Запустить»\n\n"
            f"🔗 Ссылка: https://bothost.ru/\n\n"
            f"После запуска бот будет работать как точная копия!"
        )
        
        keyboard = [
            [InlineKeyboardButton("🚀 Перейти на bothost.ru", url="https://bothost.ru/")],
            [InlineKeyboardButton("👤 BotFather", url="https://t.me/botfather")],
            [InlineKeyboardButton("◀️ В меню", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            instruction_text,
            parse_mode='HTML',
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}. Попробуйте снова.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not (update.message.text or update.message.photo or update.message.video):
        return
    
    user = update.effective_user
    chat = update.effective_chat
    
    if user.id in blacklisted_users:
        await update.message.reply_text("⛔ Вам заблокирован доступ к поддержке.")
        return
    
    if chat.type in ['group', 'supergroup']:
        return
    
    if user.id in ADMIN_IDS:
        if context.user_data.get('awaiting_name'):
            name = update.message.text.strip()
            if validate_admin_name(name):
                admin_names[user.id] = name
                context.user_data['awaiting_name'] = False
                await update.message.reply_text(f"✅ Принято, {name}! Теперь вы в системе поддержки.")
                await show_admin_menu(update, context)
            else:
                await update.message.reply_text(
                    "❌ Неверный формат. Введите имя по примеру: Иван З.\n"
                    "(Первая буква заглавная, фамилия сокращенно с точкой)"
                )
            return
        
        if context.user_data.get('active_chat'):
            user_id = context.user_data['active_chat']
            if user_id in active_chats:
                message_text = update.message.text
                timestamp = datetime.now().strftime("%H:%M")
                
                if user_id not in chat_history:
                    chat_history[user_id] = []
                
                chat_history[user_id].append({
                    'from': 'support',
                    'name': admin_names.get(user.id, 'Оператор'),
                    'text': message_text,
                    'time': timestamp
                })
                
                await context.bot.send_message(
                    user_id,
                    f"💬 {admin_names.get(user.id, 'Оператор')} ({timestamp}):\n{message_text}"
                )
                await update.message.reply_text("✅ Сообщение отправлено")
            return
        
        if context.user_data.get('replying_to'):
            request_id = context.user_data['replying_to']
            if request_id in user_requests and request_status.get(request_id) == 'active':
                user_id = user_requests[request_id]['user_id']
                
                active_chats[user_id] = {
                    'request_id': request_id,
                    'admin_id': user.id,
                    'started': datetime.now().strftime("%d.%m.%Y %H:%M")
                }
                
                context.user_data['active_chat'] = user_id
                context.user_data['replying_to'] = None
                
                if user_id not in chat_history:
                    chat_history[user_id] = []
                
                chat_history[user_id].append({
                    'from': 'system',
                    'text': f'Чат начат оператором {admin_names.get(user.id, "Оператор")}',
                    'time': datetime.now().strftime("%H:%M")
                })
                
                await show_chat_controls(update, context, user_id, request_id)
                
                await context.bot.send_message(
                    user_id,
                    f"👋 С вами начал диалог оператор {admin_names.get(user.id, 'Оператор')}"
                )
            return
        
        if context.user_data.get('awaiting_tech_message'):
            tech_message = update.message.text
            global tech_break_message
            tech_break_message = tech_message
            context.user_data['awaiting_tech_message'] = False
            await update.message.reply_text(f"✅ Сообщение для техперерыва изменено на:\n{tech_message}")
            return
        
        return
    
    if technical_breaks:
        await update.message.reply_text(tech_break_message)
        return
    
    if user.id in clone_creation_sessions:
        if clone_creation_sessions[user.id]['status'] == 'awaiting_token':
            await handle_clone_token(update, context)
        elif clone_creation_sessions[user.id]['status'] == 'awaiting_admins':
            await handle_clone_admins(update, context)
        return
    
    if user.id in pending_requests:
        request_data = pending_requests[user.id]
        
        if request_data['stage'] == 'awaiting_custom_topic':
            if request_data.get('cancel_timer'):
                request_data['cancel_timer']()
            
            topic = update.message.text
            if 5 <= len(topic) <= 30:
                request_data['topic'] = topic
                request_data['stage'] = 'awaiting_message'
                
                keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel_request")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(
                    "✅ Тема принята! Теперь напишите ваше обращение (от 10 до 500 символов):\n\n⏰ У вас 5 минут",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text("❌ Тема должна быть от 5 до 30 символов. Попробуйте снова:")
        
        elif request_data['stage'] == 'awaiting_message':
            if request_data.get('cancel_timer'):
                request_data['cancel_timer']()
            
            message_text = update.message.text
            if 10 <= len(message_text) <= 500:
                request_id = get_new_request_id()
                
                user_requests[request_id] = {
                    'user_id': user.id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'topic': request_data['topic'],
                    'message': message_text,
                    'status': 'new',
                    'date': datetime.now().strftime("%d.%m.%Y %H:%M")
                }
                
                request_status[request_id] = 'new'
                
                await update.message.reply_text("✅ Обращение отправлено! Ожидайте ответа оператора.")
                
                await notify_admins_new_request(request_id, context)
                
                del pending_requests[user.id]
            else:
                await update.message.reply_text("❌ Текст должен быть от 10 до 500 символов. Попробуйте снова:")
    
    elif user.id in active_chats:
        message_text = update.message.text
        timestamp = datetime.now().strftime("%H:%M")
        admin_id = active_chats[user.id]['admin_id']
        
        if user.id not in chat_history:
            chat_history[user.id] = []
        
        chat_history[user.id].append({
            'from': 'user',
            'name': user.first_name,
            'text': message_text,
            'time': timestamp
        })
        
        await context.bot.send_message(
            admin_id,
            f"💬 {user.first_name} ({timestamp}):\n{message_text}"
        )
        await update.message.reply_text("✅ Сообщение отправлено оператору")
    else:
        await show_main_menu(update, context)

async def show_chat_controls(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, request_id: str):
    user = update.effective_user
    request = user_requests[request_id]
    
    history_text = f"📋 Чат с пользователем {request['first_name']}\n"
    history_text += f"Тема: {request['topic']}\n"
    history_text += f"Обращение #{request_id}\n\n"
    
    if user_id in chat_history and chat_history[user_id]:
        history_text += "📝 История диалога:\n"
        for msg in chat_history[user_id]:
            if msg['from'] == 'user':
                history_text += f"👤 {msg['name']} ({msg['time']}): {msg['text']}\n"
            elif msg['from'] == 'support':
                history_text += f"👨‍💼 {msg['name']} ({msg['time']}): {msg['text']}\n"
            else:
                history_text += f"🔄 {msg['text']} ({msg['time']})\n"
    else:
        history_text += f"📝 Сообщение пользователя:\n{request['message']}\n"
    
    keyboard = [
        [InlineKeyboardButton("⛔ Заблокировать", callback_data=f"block_user_{user_id}_{request_id}")],
        [InlineKeyboardButton("✅ Завершить диалог", callback_data=f"end_chat_{user_id}_{request_id}")],
        [InlineKeyboardButton("🚪 Выйти из диалога", callback_data=f"exit_chat_{user_id}_{request_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    sent_message = await update.message.reply_text(history_text, reply_markup=reply_markup)
    
    try:
        await context.bot.pin_chat_message(chat_id=update.effective_chat.id, message_id=sent_message.message_id)
        pinned_messages[request_id] = sent_message.message_id
    except:
        pass

async def notify_admins_new_request(request_id: str, context: ContextTypes.DEFAULT_TYPE):
    request = user_requests[request_id]
    
    for admin_id in ADMIN_IDS:
        try:
            keyboard = [
                [InlineKeyboardButton("✅ Принять", callback_data=f"accept_{request_id}"),
                 InlineKeyboardButton("⛔ Отклонить", callback_data=f"reject_{request_id}")],
                [InlineKeyboardButton("🚫 В ЧС", callback_data=f"blacklist_{request['user_id']}_{request_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await context.bot.send_message(
                admin_id,
                f"🆕 Новое обращение #{request_id}\n\n"
                f"От: {request['first_name']} (@{request['username']})\n"
                f"ID: {request['user_id']}\n"
                f"Тема: {request['topic']}\n"
                f"Текст: {request['message']}\n"
                f"Время: {request['date']}",
                reply_markup=reply_markup
            )
        except:
            continue

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    data = query.data
    
    if data == "back_to_main":
        keyboard = [
            [InlineKeyboardButton("📞 Связаться с поддержкой", callback_data="contact_support")],
            [InlineKeyboardButton("ℹ️ О боте", callback_data="about_bot")],
            [InlineKeyboardButton("📊 Мои обращения", callback_data="my_requests")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"👋 Привет, {user.first_name}!\n\nВыберите действие:",
            reply_markup=reply_markup
        )
        return
    
    if data == "contact_support":
        await show_contact_menu(update, context, query)
        return
    
    if data == "about_bot":
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "ℹ️ Puls Bot - система поддержки пользователей\n"
            "Версия: 3.0\n"
            "Разработчик: @username\n\n"
            "Возможности:\n"
            "• Связь с поддержкой\n"
            "• Умные обращения\n"
            "• История диалогов\n"
            "• Создание копий бота",
            reply_markup=reply_markup
        )
        return
    
    if data == "my_requests":
        user_reqs = [(rid, req) for rid, req in user_requests.items() if req['user_id'] == user.id]
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if user_reqs:
            text = "📊 Ваши обращения:\n\n"
            for rid, req in user_reqs[-5:]:
                status_emoji = "✅" if request_status.get(rid) == 'answered' else "⏳"
                text += f"{status_emoji} #{rid}: {req['topic']} ({req['date']})\n"
        else:
            text = "📊 У вас пока нет обращений"
        
        await query.message.edit_text(text, reply_markup=reply_markup)
        return
    
    if data == "cancel_request":
        if user.id in pending_requests:
            if pending_requests[user.id].get('cancel_timer'):
                pending_requests[user.id]['cancel_timer']()
            del pending_requests[user.id]
        await query.message.edit_text("❌ Создание обращения отменено")
        await show_main_menu_callback(query, context)
        return
    
    if data == "cancel_clone":
        if user.id in clone_creation_sessions:
            del clone_creation_sessions[user.id]
        await query.message.edit_text("❌ Создание копии отменено")
        await show_admin_menu_callback(query, context)
        return
    
    if data.startswith('topic_'):
        topic_key = data.replace('topic_', '')
        
        async def cancel_pending(user_id):
            await asyncio.sleep(300)
            if user_id in pending_requests:
                del pending_requests[user_id]
                try:
                    await context.bot.send_message(
                        user_id,
                        "⏰ Время на написание обращения истекло. Начните заново."
                    )
                except:
                    pass
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel_request")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if topic_key == 'other':
            pending_requests[user.id] = {
                'stage': 'awaiting_custom_topic'
            }
            timer_task = asyncio.create_task(cancel_pending(user.id))
            pending_requests[user.id]['cancel_timer'] = timer_task.cancel
            await query.message.edit_text(
                "📝 Введите свою тему обращения (от 5 до 30 символов):\n\n⏰ У вас 5 минут",
                reply_markup=reply_markup
            )
        else:
            pending_requests[user.id] = {
                'stage': 'awaiting_message',
                'topic': REQUEST_TOPICS[topic_key]
            }
            timer_task = asyncio.create_task(cancel_pending(user.id))
            pending_requests[user.id]['cancel_timer'] = timer_task.cancel
            await query.message.edit_text(
                "📝 Напишите ваше обращение (от 10 до 500 символов):\n\n⏰ У вас 5 минут",
                reply_markup=reply_markup
            )
        return
    
    if data.startswith('accept_'):
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        request_id = data.replace('accept_', '')
        if request_status.get(request_id) == 'new':
            request_status[request_id] = 'active'
            support_assignments[request_id] = user.id
            
            for admin_id in ADMIN_IDS:
                if admin_id != user.id:
                    try:
                        await context.bot.send_message(
                            admin_id,
                            f"ℹ️ Обращение #{request_id} принято оператором {admin_names.get(user.id, 'Администратор')}"
                        )
                    except:
                        continue
            
            await query.message.edit_text(
                f"✅ Вы приняли обращение #{request_id}\n\n"
                f"Теперь напишите первое сообщение пользователю:"
            )
            context.user_data['replying_to'] = request_id
        else:
            await query.message.edit_text("❌ Это обращение уже обработано другим оператором")
        return
    
    if data.startswith('reject_'):
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        request_id = data.replace('reject_', '')
        if request_status.get(request_id) == 'new':
            request_status[request_id] = 'rejected'
            await query.message.edit_text(f"❌ Обращение #{request_id} отклонено")
        return
    
    if data.startswith('blacklist_'):
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        parts = data.replace('blacklist_', '').split('_')
        user_id = int(parts[0])
        request_id = parts[1]
        
        blacklisted_users[user_id] = True
        request_status[request_id] = 'blacklisted'
        
        await query.message.edit_text(f"⛔ Пользователь {user_id} добавлен в черный список")
        
        try:
            await context.bot.send_message(
                user_id,
                "⛔ Вам заблокирован доступ к поддержке."
            )
        except:
            pass
        return
    
    if data.startswith('block_user_'):
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        parts = data.replace('block_user_', '').split('_')
        user_id = int(parts[0])
        request_id = parts[1]
        
        blacklisted_users[user_id] = True
        
        if user_id in active_chats:
            del active_chats[user_id]
        
        if request_id in pinned_messages:
            try:
                await context.bot.unpin_chat_message(chat_id=user.id, message_id=pinned_messages[request_id])
            except:
                pass
        
        await query.message.edit_text(f"⛔ Пользователь заблокирован")
        
        try:
            await context.bot.send_message(
                user_id,
                "⛔ Вам заблокирован доступ к поддержке."
            )
        except:
            pass
        return
    
    if data.startswith('end_chat_'):
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        parts = data.replace('end_chat_', '').split('_')
        user_id = int(parts[0])
        request_id = parts[1]
        
        if user_id in active_chats:
            del active_chats[user_id]
        
        if request_id in pinned_messages:
            try:
                await context.bot.unpin_chat_message(chat_id=user.id, message_id=pinned_messages[request_id])
            except:
                pass
        
        request_status[request_id] = 'ended'
        
        if user_id in context.user_data and context.user_data.get('active_chat') == user_id:
            context.user_data['active_chat'] = None
        
        await query.message.edit_text(f"✅ Диалог завершен")
        
        try:
            await context.bot.send_message(
                user_id,
                "✅ Диалог с поддержкой завершен. Спасибо за обращение!"
            )
        except:
            pass
        return
    
    if data.startswith('exit_chat_'):
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        parts = data.replace('exit_chat_', '').split('_')
        user_id = int(parts[0])
        request_id = parts[1]
        
        if user_id in active_chats:
            del active_chats[user_id]
        
        if request_id in pinned_messages:
            try:
                await context.bot.unpin_chat_message(chat_id=user.id, message_id=pinned_messages[request_id])
            except:
                pass
        
        if user_id in context.user_data and context.user_data.get('active_chat') == user_id:
            context.user_data['active_chat'] = None
        
        await query.message.edit_text(f"🚪 Вы вышли из диалога")
        
        try:
            await context.bot.send_message(
                user_id,
                "🚪 Оператор вышел из диалога. Ожидайте, скоро к вам подключатся."
            )
        except:
            pass
        return
    
    if data == "admin_new_requests":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        new_requests = [(rid, req) for rid, req in user_requests.items() if request_status.get(rid) == 'new']
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if new_requests:
            text = "📨 Новые обращения:\n\n"
            for rid, req in new_requests:
                text += f"#{rid}: {req['topic']} от {req['first_name']}\n"
        else:
            text = "📨 Нет новых обращений"
        
        await query.message.edit_text(text, reply_markup=reply_markup)
        return
    
    if data == "admin_active_chats":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if active_chats:
            text = "📨 Активные чаты:\n\n"
            for user_id, chat_info in active_chats.items():
                try:
                    user_chat = await context.bot.get_chat(user_id)
                    text += f"👤 {user_chat.first_name}: #{chat_info['request_id']}\n"
                except:
                    continue
        else:
            text = "📨 Нет активных чатов"
        
        await query.message.edit_text(text, reply_markup=reply_markup)
        return
    
    if data == "admin_stats":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        total = len(user_requests)
        new = len([r for r in request_status.values() if r == 'new'])
        active = len([r for r in request_status.values() if r == 'active'])
        blacklisted = len(blacklisted_users)
        
        stats = (
            f"📊 Статистика поддержки\n\n"
            f"Всего обращений: {total}\n"
            f"Новых: {new}\n"
            f"Активных: {active}\n"
            f"В черном списке: {blacklisted}"
        )
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(stats, reply_markup=reply_markup)
        return
    
    if data == "admin_blacklist":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        if blacklisted_users:
            text = "⛔ Черный список:\n\n"
            for uid in blacklisted_users:
                text += f"• ID: {uid}\n"
        else:
            text = "⛔ Черный список пуст"
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(text, reply_markup=reply_markup)
        return
    
    if data == "admin_create_clone":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        await create_clone_callback(query, context)
        return
    
    if data == "admin_tech_break":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        global technical_breaks
        status = "🔧 ВКЛЮЧЕН" if technical_breaks else "✅ ВЫКЛЮЧЕН"
        
        keyboard = [
            [InlineKeyboardButton("🔧 Включить техперерыв", callback_data="tech_break_on")],
            [InlineKeyboardButton("✅ Выключить техперерыв", callback_data="tech_break_off")],
            [InlineKeyboardButton("✏️ Изменить сообщение", callback_data="tech_break_message")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"🔧 Управление техническим перерывом\n\n"
            f"Текущий статус: {status}\n"
            f"Текущее сообщение: {tech_break_message}",
            reply_markup=reply_markup
        )
        return
    
    if data == "tech_break_on":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        global technical_breaks
        technical_breaks = True
        await query.message.edit_text("✅ Технический перерыв включен. Пользователи теперь видят сообщение о техперерыве.")
        return
    
    if data == "tech_break_off":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        global technical_breaks
        technical_breaks = False
        await query.message.edit_text("✅ Технический перерыв выключен. Пользователи могут обращаться в поддержку.")
        return
    
    if data == "tech_break_message":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        context.user_data['awaiting_tech_message'] = True
        await query.message.edit_text(
            f"✏️ Отправьте новое сообщение для технического перерыва:\n\n"
            f"Текущее сообщение:\n{tech_break_message}"
        )
        return
    
    if data == "admin_settings":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        keyboard = [
            [InlineKeyboardButton("👥 Мои группы", callback_data="admin_my_groups")],
            [InlineKeyboardButton("🔧 Техперерыв", callback_data="admin_tech_break")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text("⚙️ Настройки бота:", reply_markup=reply_markup)
        return
    
    if data == "admin_my_groups":
        if user.id not in ADMIN_IDS:
            await query.message.reply_text("❌ У вас нет прав для этого действия")
            return
        
        groups = []
        for chat_id in group_welcome_settings.keys():
            try:
                chat = await context.bot.get_chat(chat_id)
                if await is_group_admin(update, context, chat_id, user.id):
                    groups.append(f"👥 {chat.title}")
            except:
                continue
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if groups:
            text = "Ваши группы:\n\n" + "\n".join(groups)
        else:
            text = "У вас нет групп с настроенными приветствиями"
        
        await query.message.edit_text(text, reply_markup=reply_markup)
        return
    
    if data == "admin_back":
        await show_admin_menu_callback(query, context)
        return
    
    if data.startswith('confirm_welcome_'):
        chat_id = int(data.replace('confirm_welcome_', ''))
        if chat_id in pending_group_settings:
            settings = pending_group_settings[chat_id]
            if settings['user_id'] == user.id:
                group_welcome_settings[chat_id] = settings['data']
                del pending_group_settings[chat_id]
                await query.message.edit_text("✅ Приветствие успешно сохранено!")
            else:
                await query.message.reply_text("❌ Только владелец может подтвердить изменения")
        return
    
    if data.startswith('cancel_welcome_'):
        chat_id = int(data.replace('cancel_welcome_', ''))
        if chat_id in pending_group_settings:
            if pending_group_settings[chat_id]['user_id'] == user.id:
                del pending_group_settings[chat_id]
                await query.message.edit_text("❌ Изменения отменены")
            else:
                await query.message.reply_text("❌ Только владелец может отменить изменения")
        return
    
    if data.startswith('confirm_goodbye_'):
        chat_id = int(data.replace('confirm_goodbye_', ''))
        if chat_id in pending_group_settings:
            settings = pending_group_settings[chat_id]
            if settings['user_id'] == user.id:
                group_goodbye_settings[chat_id] = settings['data']
                del pending_group_settings[chat_id]
                await query.message.edit_text("✅ Сообщение о выходе успешно сохранено!")
            else:
                await query.message.reply_text("❌ Только владелец может подтвердить изменения")
        return
    
    if data.startswith('cancel_goodbye_'):
        chat_id = int(data.replace('cancel_goodbye_', ''))
        if chat_id in pending_group_settings:
            if pending_group_settings[chat_id]['user_id'] == user.id:
                del pending_group_settings[chat_id]
                await query.message.edit_text("❌ Изменения отменены")
            else:
                await query.message.reply_text("❌ Только владелец может отменить изменения")
        return

async def create_clone_callback(query, context):
    user = query.from_user
    
    clone_creation_sessions[user.id] = {
        'status': 'awaiting_token',
        'expires': datetime.now() + timedelta(minutes=10)
    }
    
    keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel_clone")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.edit_text(
        "🤖 Создание копии бота\n\n"
        "Отправьте токен нового бота в течение 10 минут:\n"
        "(получите у @BotFather через команду /newbot)\n\n"
        "Токен выглядит так: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
        reply_markup=reply_markup
    )
    
    asyncio.create_task(check_clone_creation_timeout(user.id, context))

async def show_admin_menu_callback(query, context):
    status_text = ""
    if technical_breaks:
        status_text = "\n\n🔧 ТЕХНИЧЕСКИЙ ПЕРЕРЫВ ВКЛЮЧЕН"
    
    keyboard = [
        [InlineKeyboardButton("📨 Новые обращения", callback_data="admin_new_requests")],
        [InlineKeyboardButton("📨 Активные чаты", callback_data="admin_active_chats")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("🤖 Создать копию бота", callback_data="admin_create_clone")],
        [InlineKeyboardButton("🔧 Технический перерыв", callback_data="admin_tech_break")],
        [InlineKeyboardButton("⛔ Черный список", callback_data="admin_blacklist")],
        [InlineKeyboardButton("⚙️ Настройки групп", callback_data="admin_settings")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.edit_text(
        f"👨‍💼 Панель администратора\n\nДобро пожаловать, {admin_names.get(query.from_user.id, 'Администратор')}!{status_text}",
        reply_markup=reply_markup
    )

async def show_main_menu_callback(query, context):
    keyboard = [
        [InlineKeyboardButton("📞 Связаться с поддержкой", callback_data="contact_support")],
        [InlineKeyboardButton("ℹ️ О боте", callback_data="about_bot")],
        [InlineKeyboardButton("📊 Мои обращения", callback_data="my_requests")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.edit_text(
        f"👋 Привет, {query.from_user.first_name}!\n\nВыберите действие:",
        reply_markup=reply_markup
    )

async def group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    
    user = update.effective_user
    chat = update.effective_chat
    
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ Эта команда работает только в группах")
        return
    
    if not await is_group_admin(update, context, chat.id, user.id):
        await update.message.reply_text("❌ Только администраторы группы могут использовать эту команду")
        return
    
    if len(context.args) == 0:
        await update.message.reply_text(
            "Использование:\n"
            "/welcome текст - установить текстовое приветствие\n"
            "/welcome (с фото/видео) - установить приветствие с медиа\n"
            "/goodbye текст - установить текстовое сообщение о выходе\n"
            "/goodbye (с фото/видео) - установить сообщение о выходе с медиа"
        )
        return
    
    command = context.args[0].lower()
    
    if command in ['welcome', 'goodbye']:
        context.user_data['awaiting_group_' + command] = chat.id
        await update.message.reply_text(
            f"📝 Отправьте текст и при необходимости приложите фото или видео (до 20 секунд)\n\n"
            f"Используйте %username% для имени пользователя"
        )

async def handle_group_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.type not in ['group', 'supergroup']:
        return
    
    if not await is_group_admin(update, context, chat.id, user.id):
        return
    
    setting_type = None
    if 'awaiting_group_welcome' in context.user_data and context.user_data['awaiting_group_welcome'] == chat.id:
        setting_type = 'welcome'
        del context.user_data['awaiting_group_welcome']
    elif 'awaiting_group_goodbye' in context.user_data and context.user_data['awaiting_group_goodbye'] == chat.id:
        setting_type = 'goodbye'
        del context.user_data['awaiting_group_goodbye']
    else:
        return
    
    caption = update.message.caption or ""
    message_text = update.message.text or caption
    
    media_data = {}
    
    if update.message.photo:
        photo = update.message.photo[-1]
        media_data = {
            'type': 'photo',
            'content': photo.file_id,
            'caption': message_text
        }
    elif update.message.video:
        video = update.message.video
        if video.duration > 20:
            await update.message.reply_text("❌ Видео должно быть не длиннее 20 секунд")
            return
        media_data = {
            'type': 'video',
            'content': video.file_id,
            'caption': message_text
        }
    elif message_text:
        media_data = {
            'type': 'text',
            'content': message_text,
            'caption': None
        }
    else:
        await update.message.reply_text("❌ Отправьте текст или медиа с подписью")
        return
    
    preview_text = "Предпросмотр:\n\n"
    if setting_type == 'welcome':
        preview_text += media_data['content'].replace('%username%', user.first_name) if media_data['type'] == 'text' else media_data['caption'].replace('%username%', user.first_name)
    else:
        preview_text += media_data['content'].replace('%username%', user.first_name) if media_data['type'] == 'text' else media_data['caption'].replace('%username%', user.first_name)
    
    keyboard = [
        [InlineKeyboardButton("✅ Да", callback_data=f"confirm_{setting_type}_{chat.id}"),
         InlineKeyboardButton("❌ Нет", callback_data=f"cancel_{setting_type}_{chat.id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    pending_group_settings[chat.id] = {
        'user_id': user.id,
        'data': media_data
    }
    
    await update.message.reply_text(preview_text, reply_markup=reply_markup)

async def chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.chat_member:
        return
    
    chat = update.effective_chat
    new_member = update.chat_member.new_chat_member
    old_member = update.chat_member.old_chat_member
    
    if chat.type not in ['group', 'supergroup']:
        return
    
    if new_member.status == 'member' and old_member.status == 'left':
        user = new_member.user
        if chat.id in group_welcome_settings:
            settings = group_welcome_settings[chat.id]
            try:
                text = settings['content'] if settings['type'] == 'text' else settings['caption']
                text = text.replace('%username%', user.first_name)
                
                if settings['type'] == 'text':
                    await context.bot.send_message(chat.id, text)
                elif settings['type'] == 'photo':
                    await context.bot.send_photo(chat.id, settings['content'], caption=text)
                elif settings['type'] == 'video':
                    await context.bot.send_video(chat.id, settings['content'], caption=text)
            except Exception as e:
                logger.error(f"Ошибка отправки приветствия: {e}")
        else:
            await context.bot.send_message(
                chat.id,
                f"🥳 {user.first_name} зашел в группу! Будем знакомы! Рады видеть нового участника 🎉"
            )
    
    elif old_member.status == 'member' and new_member.status == 'left':
        user = old_member.user
        if chat.id in group_goodbye_settings:
            settings = group_goodbye_settings[chat.id]
            try:
                text = settings['content'] if settings['type'] == 'text' else settings['caption']
                text = text.replace('%username%', user.first_name)
                
                if settings['type'] == 'text':
                    await context.bot.send_message(chat.id, text)
                elif settings['type'] == 'photo':
                    await context.bot.send_photo(chat.id, settings['content'], caption=text)
                elif settings['type'] == 'video':
                    await context.bot.send_video(chat.id, settings['content'], caption=text)
            except Exception as e:
                logger.error(f"Ошибка отправки прощания: {e}")
        else:
            await context.bot.send_message(
                chat.id,
                f"👋 {user.first_name} покинул группу... Жалко терять таких участников 😢"
            )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    
    if chat.type in ['group', 'supergroup']:
        await update.message.reply_text(
            "👋 Команды для администраторов группы:\n"
            "/welcome - установить приветствие\n"
            "/goodbye - установить сообщение о выходе\n\n"
            "Используйте %username% в тексте для имени пользователя"
        )
    else:
        await update.message.reply_text(
            "👋 Команды:\n"
            "/start - начать работу\n"
            "/help - это сообщение"
        )

async def clone_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав для создания копий бота")
        return
    
    await create_clone(update, context)

def main():
    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("clone", clone_command))
    application.add_handler(CommandHandler("welcome", group_command))
    application.add_handler(CommandHandler("goodbye", group_command))
    
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(ChatMemberHandler(chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.CAPTION, handle_group_media))
    
    print("🤖 Бот Puls запущен...")
    application.run_polling()

if __name__ == '__main__':
    main()
