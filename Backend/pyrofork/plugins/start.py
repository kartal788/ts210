from pyrogram import filters, Client, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from Backend.config import Telegram
from Backend import db
from datetime import datetime

print("DEBUG: start.py PLUGIN LOADED SUCCESSFULLY!")

@Client.on_message(filters.command('start'), group=10)
async def send_start_message(client: Client, message: Message):
    try:
        user_id = (message.from_user.id if message.from_user else None) or (message.sender_chat.id if message.sender_chat else None) or message.chat.id
        print(f"DEBUG: Received /start command from {user_id}")
        # await message.reply_text("DEBUG: Bot received the start command.")
        
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # If subscriptions are NOT enabled
        if not Telegram.SUBSCRIPTION:
            # Generate or fetch API token for free user
            user_name = (message.from_user.first_name or message.from_user.username or f"User {user_id}") if message.from_user else f"Chat {user_id}"
            try:
                # Ensure user gets a token
                token_doc = await db.add_api_token(name=user_name, user_id=user_id)
                token_str = token_doc.get("token")
                addon_url = f"{base_url}/stremio/{token_str}/manifest.json"
            except Exception as e:
                print(f"DEBUG: Error ensuring token for free user: {e}")
                
            await message.reply_text(
                '🎉 <b>Telegram Stremio Medya Sunucusuna Hoş Geldiniz</b>\n\n'
                'Kişisel Stremio Eklenti bağlantınız aşağıdadır:\n\n'
                '🎬 <b>Stremio Eklentisi:</b>\n'
                f'<code>{addon_url}</code>\n\n'
                'Linki kopyalayıp stremio eklentiler bölümüne ekleyin',
                quote=True,
                parse_mode=enums.ParseMode.HTML
            )
            return

        # Subscription logic (When SUBSCRIPTION=True)
        user = await db.get_user(user_id)
        now = datetime.utcnow()
        
        # Check if user has an active subscription
        is_active = False
        if user and user.get("subscription_status") == "active":
            if user.get("subscription_expiry") and user.get("subscription_expiry") > now:
                is_active = True
            else:
                await db.mark_user_expired(user_id)

        if not is_active:
            plans = await db.get_subscription_plans()
            if not plans:
                return await message.reply_text(
                    '<b>Telegram Stremio Özel Grubuna Hoş Geldiniz!</b>\n\n'
                    'Şu anda herhangi bir abonelik planı tanımlanmamıştır. Lütfen yönetici ile iletişime geçin.',
                    quote=True,
                    parse_mode=enums.ParseMode.HTML
                )
            
            keyboard_buttons = []
            for plan in plans:
                keyboard_buttons.append([InlineKeyboardButton(f"{plan['days']} Gün - {plan['price']} TL", callback_data=f"plan_{plan['_id']}")])
            
            keyboard = InlineKeyboardMarkup(keyboard_buttons)
            
            return await message.reply_text(
                '<bTelegram Stremio Özel Grubuna Hoş Geldiniz</b>\n\n'
                'Stremio eklentisine erişmek için aktif bir abonelik gereklidir.\n'
                'Devam etmek için lütfen aşağıdan bir abonelik planı seçin:',
                reply_markup=keyboard,
                quote=True,
                parse_mode=enums.ParseMode.HTML
            )
        
        # User is active, fetch their token
        all_tokens = await db.get_all_api_tokens()
        token_doc = next((t for t in all_tokens if t.get("user_id") == user_id), None)
        
        if token_doc and "token" in token_doc:
            token_str = token_doc["token"]
            addon_url = f"{base_url}/stremio/{token_str}/manifest.json"

        await message.reply_text(
            '🎉 <bTelegram Stremio Abonelik Paneline Tekrar Hoş Geldiniz</b>\n\n'
            'Aboneliğiniz aktif durumdadır. Kişisel eklenti bağlantınız aşağıdadır:\n\n'
            '🎬 <b>Stremio eklentisi:</b>\n'
            f'<code>{addon_url}</code>\n\n'
            'Linki kopyalayıp stremio eklentiler bölümüne ekleyin',
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Error: {e}")
        print(f"Error in /start handler: {e}")
