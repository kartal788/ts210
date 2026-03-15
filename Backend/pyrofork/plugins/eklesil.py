import os
import re
import aiohttp
import time
import asyncio  # <--- BU EKSİKTİ, EKLENDİ
from datetime import timezone, datetime
from dateutil.parser import parse as parse_date
from Backend.helper.metadata import tmdb
from pyrogram.errors import FloodWait

from pyrogram import Client, filters
from pyrogram.types import Message

from motor.motor_asyncio import AsyncIOMotorClient
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.metadata import metadata
from Backend.logger import LOGGER

# ----------------- ENV -----------------
DATABASE_RAW = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_RAW.split(",") if u.strip().startswith("mongodb")]
MONGO_URL = db_urls[1]
DB_NAME = "dbFyvio"

# ----------------- MongoDB -----------------
mongo = AsyncIOMotorClient(MONGO_URL)
db = mongo[DB_NAME]
movie_col = db["movie"]
series_col = db["tv"]

# ----------------- Helpers -----------------
def pixeldrain_to_api(url: str) -> str:
    m = re.match(r"https?://pixeldrain\.com/u/([A-Za-z0-9]+)", url)
    if not m:
        return url
    return f"https://pixeldrain.com/api/file/{m.group(1)}"

async def head(url, key):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.head(url, allow_redirects=True, timeout=20) as r:
                return r.headers.get(key)
    except:
        return None

async def filename_from_url(url):
    try:
        cd = await head(url, "Content-Disposition")
        if cd:
            m = re.search(r'filename="(.+?)"', cd)
            if m:
                return m.group(1)
        return url.split("/")[-1]
    except:
        return url.split("/")[-1]

async def filesize(url):
    try:
        size = await head(url, "Content-Length")
        if not size:
            return "YOK"
        size = int(size)
        for u in ["B", "KB", "MB", "GB"]:
            if size < 1024:
                return f"{size:.2f}{u}"
            size /= 1024
        return "YOK"
    except:
        return "YOK"

# ----------ekle ----------
@Client.on_message(filters.command("ekle") & filters.private & CustomFilters.owner)
async def ekle(client: Client, message: Message):
    text = message.text.strip()

    if "\n" in text:
        lines = text.split("\n")[1:]
    else:
        parts = text.split(maxsplit=1)
        lines = [parts[1]] if len(parts) > 1 else []

    if not lines:
        return await message.reply_text(
            "Kullanım:\n/ekle link\nveya her satıra bir link gelecek şekilde toplu gönderin."
        )

    total_links = len(lines)
    status = await message.reply_text(f"📥 İşlem başlatıldı: 0/{total_links} tamamlandı...")

    movie_count = 0
    series_count = 0
    failed = []
    added_names = []

    for index, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue

        parts = line.split(maxsplit=1)
        link = parts[0]
        extra_info = parts[1] if len(parts) > 1 else None

        try:
            # Durum Güncelleme
            await status.edit_text(
                f"⏳ İşleniyor ({index}/{total_links}):\n`{link}`\n\n"
                f"✅ Başarılı: {movie_count + series_count}\n"
                f"❌ Hatalı: {len(failed)}"
            )

            api_link = pixeldrain_to_api(link) if "pixeldrain.com" in link else link

            try:
                size = await filesize(api_link)
                cd_filename = await filename_from_url(api_link)
                meta_filename = extra_info or cd_filename or link.split("/")[-1]
            except:
                size = "YOK"
                meta_filename = extra_info or link.split("/")[-1]

            meta = await metadata(
                filename=meta_filename,
                channel=message.chat.id,
                msg_id=message.id
            )

            if not meta:
                meta = {
                    "media_type": "movie", "tmdb_id": None, "imdb_id": None,
                    "title": meta_filename, "genres": [], "description": "",
                    "rate": 0, "year": None, "poster": "", "backdrop": "",
                    "logo": "", "cast": [], "runtime": 0, "season_number": 1,
                    "episode_number": 1, "episode_title": meta_filename,
                    "episode_backdrop": "", "episode_overview": "",
                    "episode_released": None, "quality": "Unknown"
                }

            telegram_obj = {
                "quality": meta.get("quality", "Unknown"),
                "id": api_link,
                "name": meta_filename,
                "size": size
            }

            if meta["media_type"] == "movie":
                doc = await movie_col.find_one({"tmdb_id": meta["tmdb_id"]})
                if not doc:
                    doc = {
                        "tmdb_id": meta["tmdb_id"], "imdb_id": meta["imdb_id"],
                        "db_index": 1, "title": meta["title"], "genres": meta["genres"],
                        "description": meta["description"], "rating": meta["rate"],
                        "release_year": meta["year"], "poster": meta["poster"],
                        "backdrop": meta["backdrop"], "logo": meta["logo"],
                        "cast": meta["cast"], "runtime": meta["runtime"],
                        "media_type": "movie", "updated_on": str(datetime.utcnow()),
                        "telegram": [telegram_obj]
                    }
                    await movie_col.insert_one(doc)
                else:
                    doc["telegram"].append(telegram_obj)
                    doc["updated_on"] = str(datetime.utcnow())
                    await movie_col.replace_one({"_id": doc["_id"]}, doc)
                movie_count += 1
                added_names.append(f"🎬 {meta['title']}")
            else:
                doc = await series_col.find_one({"tmdb_id": meta["tmdb_id"]})
                episode_obj = {
                    "episode_number": meta["episode_number"],
                    "title": meta["episode_title"],
                    "episode_backdrop": meta["episode_backdrop"],
                    "overview": meta["episode_overview"],
                    "released": meta["episode_released"],
                    "telegram": [telegram_obj]
                }
                if not doc:
                    doc = {
                        "tmdb_id": meta["tmdb_id"], "imdb_id": meta["imdb_id"],
                        "db_index": 1, "title": meta["title"], "genres": meta["genres"],
                        "description": meta["description"], "rating": meta["rate"],
                        "release_year": meta["year"], "poster": meta["poster"],
                        "backdrop": meta["backdrop"], "logo": meta["logo"],
                        "cast": meta["cast"], "runtime": meta["runtime"],
                        "media_type": "tv", "updated_on": str(datetime.utcnow()),
                        "seasons": [{"season_number": meta["season_number"], "episodes": [episode_obj]}]
                    }
                    await series_col.insert_one(doc)
                else:
                    season = next((s for s in doc["seasons"] if s["season_number"] == meta["season_number"]), None)
                    if not season:
                        season = {"season_number": meta["season_number"], "episodes": []}
                        doc["seasons"].append(season)

                    ep = next((e for e in season["episodes"] if e["episode_number"] == meta["episode_number"]), None)
                    if not ep:
                        season["episodes"].append(episode_obj)
                    else:
                        ep["telegram"].append(telegram_obj)

                    doc["updated_on"] = str(datetime.utcnow())
                    await series_col.replace_one({"_id": doc["_id"]}, doc)
                series_count += 1
                added_names.append(f"📺 {meta['title']}")

        except Exception as e:
            LOGGER.exception(e)
            failed.append(line)

        # 30 Saniye Bekleme
        if index < total_links:
            await asyncio.sleep(30)

    # Sonuç Raporu
    result_text = f"✅ **İşlem Tamamlandı**\n\n🎬 Film: {movie_count}\n📺 Dizi: {series_count}\n❌ Hatalı: {len(failed)}"
    
    if added_names and len(added_names) <= 15:
        result_text += "\n\n**Eklenenler:**\n" + "\n".join(added_names)
    
    if failed:
        result_text += "\n\n**⚠️ Hatalı Linkler:**\n" + "\n".join([f"`{f}`" for f in failed[:15]])

    await status.edit_text(result_text)

# (Silme ve temizlik komutları aşağıda değişmeden devam ediyor...)
    
# ----------------- /SİL -----------------
awaiting_confirmation = {}

@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def sil(client: Client, message: Message):
    uid = message.from_user.id

    movie_count = await movie_col.count_documents({})
    tv_count = await series_col.count_documents({})

    if movie_count == 0 and tv_count == 0:
        return await message.reply_text("ℹ️ Veritabanı zaten boş.")

    awaiting_confirmation[uid] = True

    await message.reply_text(
        "⚠️ TÜM VERİLER SİLİNECEK ⚠️\n\n"
        f"🎬 Filmler: {movie_count}\n"
        f"📺 Diziler: {tv_count}\n\n"
        "Onaylamak için **Evet** yaz.\n"
        "İptal için **Hayır** yaz."
    )

@Client.on_message(filters.private & CustomFilters.owner & filters.regex("(?i)^(evet|hayır)$"))
async def sil_onay(client: Client, message: Message):
    uid = message.from_user.id

    if uid not in awaiting_confirmation:
        return

    awaiting_confirmation.pop(uid)

    if message.text.lower() == "evet":
        m = await movie_col.count_documents({})
        t = await series_col.count_documents({})
        await movie_col.delete_many({})
        await series_col.delete_many({})
        await message.reply_text(
            f"✅ Silme tamamlandı\n🎬 {m} film\n📺 {t} dizi"
        )
    else:
        await message.reply_text("❌ Silme iptal edildi.")

# ------------------calismayanlinklerisil------------------
@Client.on_message(filters.command("calismayanlinklerisil") & filters.private & CustomFilters.owner)
async def calismayan_linkleri_sil(client: Client, message: Message):

    status = await message.reply_text("🔍 Linkler kontrol ediliyor...")

    async def link_calismiyor_mu(url: str) -> bool:
        if not url.startswith(("http://", "https://")):
            return False
        try:
            async with aiohttp.ClientSession() as s:
                async with s.head(url, allow_redirects=True, timeout=20) as r:
                    size = r.headers.get("Content-Length")
                    if not size:
                        return True
                    return int(size) < (5 * 1024 * 1024)
        except:
            return True

    silinen_film = 0
    silinen_dizi = 0
    silinen_bolum = 0
    silinen_link = 0

    silinen_isimler = []

    # ---------------- MOVIES ----------------
    async for movie in movie_col.find({}):
        telegramlar = movie.get("telegram", [])
        yeni_telegram = []

        for t in telegramlar:
            if await link_calismiyor_mu(t.get("id", "")):
                silinen_link += 1
                silinen_isimler.append(f"🎬 {t.get('name')}")
            else:
                yeni_telegram.append(t)

        if not yeni_telegram:
            await movie_col.delete_one({"_id": movie["_id"]})
            silinen_film += 1
        elif len(yeni_telegram) != len(telegramlar):
            await movie_col.update_one(
                {"_id": movie["_id"]},
                {"$set": {"telegram": yeni_telegram}}
            )

    # ---------------- TV ----------------
    async for tv in series_col.find({}):
        sezonlar = []
        dizi_bos = True

        for season in tv.get("seasons", []):
            bolumler = []

            for ep in season.get("episodes", []):
                telegramlar = ep.get("telegram", [])
                yeni_telegram = []

                for t in telegramlar:
                    if await link_calismiyor_mu(t.get("id", "")):
                        silinen_link += 1
                        silinen_isimler.append(f"📺 {t.get('name')}")
                    else:
                        yeni_telegram.append(t)

                if yeni_telegram:
                    ep["telegram"] = yeni_telegram
                    bolumler.append(ep)
                    dizi_bos = False
                else:
                    silinen_bolum += 1

            if bolumler:
                season["episodes"] = bolumler
                sezonlar.append(season)

        if dizi_bos:
            await series_col.delete_one({"_id": tv["_id"]})
            silinen_dizi += 1
        else:
            await series_col.update_one(
                {"_id": tv["_id"]},
                {"$set": {"seasons": sezonlar}}
            )

    # ---------------- SONUÇ ----------------
    header = (
        "✅ Temizlik tamamlandı\n\n"
        f"🔗 Silinen link: {silinen_link}\n"
    )

    if len(silinen_isimler) <= 15:
        detay = "\n".join(silinen_isimler)
        await status.edit_text(header + detay)
    else:
        txt_path = "/tmp/silinen_linkler.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(silinen_isimler))

        await client.send_document(
            chat_id=message.chat.id,
            document=txt_path,
            caption=header + "\n📄 Silinen içerik listesi dosya olarak gönderildi."
        )
        await status.delete()

@Client.on_message(filters.command("turkcebaslik") & filters.private & CustomFilters.owner)
async def toplu_turkce_yap(client, message):
    status = await message.reply_text("🔄 İşlem başlatılıyor...")
    
    g_film, g_dizi, hata = 0, 0, 0
    last_update_time = time.time()
    last_status_text = "" # Mesaj içeriği değişmiş mi kontrolü için

    async def update_status(text, force=False):
        nonlocal last_update_time, last_status_text
        
        # İçerik aynıysa veya 15 saniye geçmediyse (force değilse) güncelleme yapma
        if not force:
            if text == last_status_text or (time.time() - last_update_time < 15):
                return

        try:
            await status.edit_text(text)
            last_update_time = time.time()
            last_status_text = text
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            pass

    # 1. FİLMLER
    async for movie in movie_col.find({"tmdb_id": {"$ne": None}}):
        try:
            m_id = movie.get("tmdb_id")
            details = await tmdb.movie(m_id).details()
            tr_title = details.title

            if tr_title and tr_title != movie.get("title"):
                await movie_col.update_one({"_id": movie["_id"]}, {"$set": {"title": tr_title}})
                g_film += 1
                # Sadece bir şey değiştiğinde mesajı güncellemeyi dene
                await update_status(f"🔄 Güncelleniyor...\n🎬 Film: {g_film}\n📺 Dizi: {g_dizi}")
            
            await asyncio.sleep(0.25)
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            hata += 1

    # 2. DİZİLER
    async for tv in series_col.find({"tmdb_id": {"$ne": None}}):
        try:
            t_id = tv.get("tmdb_id")
            details = await tmdb.tv(t_id).details()
            tr_title = details.name

            if tr_title and tr_title != tv.get("title"):
                await series_col.update_one({"_id": tv["_id"]}, {"$set": {"title": tr_title}})
                g_dizi += 1
                await update_status(f"🔄 Güncelleniyor...\n🎬 Film: {g_film}\n📺 Dizi: {g_dizi}")

            await asyncio.sleep(0.25)
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            hata += 1

    # SONUÇ
    final_text = (
        f"✅ **Güncelleme Tamamlandı!**\n\n"
        f"🎬 Güncellenen Film: {g_film}\n"
        f"📺 Güncellenen Dizi: {g_dizi}\n"
        f"⚠️ Hata: {hata}"
    )
    await update_status(final_text, force=True)
