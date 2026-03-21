from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os
import re

# -------------------------- gizlikomutlar ----------------------
@Client.on_message(filters.command("gizlikomutlar") & filters.private & CustomFilters.owner)
async def gizli_komutlar(client, message: Message):
    await message.reply_text(
        "/cevir 🇹🇷 Açıklamaları Türkçeye çevirir.\n"
        "/posterturkce 🖼️ Poster Yerelleştirme. \n"
        "/turkcebaslik 🏷️ Başlık Yerelleştirme. \n"
        "/linklerisil 🔗 Link içeren videoları siler.\n"
        "/sil 🗑️ Tüm filmleri ve dizileri siler.\n"
    )

