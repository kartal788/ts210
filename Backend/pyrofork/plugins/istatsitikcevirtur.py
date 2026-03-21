import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, UpdateOne
from collections import defaultdict
import psutil
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from deep_translator import GoogleTranslator
from Backend.helper.metadata import metadata

import os

# ---------------- CONFIG ----------------
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
stop_event = asyncio.Event()
DOWNLOAD_DIR = "/"

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
MONGO_URL = db_urls[1] if len(db_urls) > 1 else db_urls[0]

client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]
movie_col = db["movie"]
series_col = db["tv"]

bot_start_time = time.time()

# ---------------- UTILS ----------------
def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {min(percent,100):.2f}%"

def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"
    total_seconds = int(total_seconds)
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text(
            "⛔ İşlem **iptal edildi**!",
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# ---------------- GLOBAL FLAGS ----------------
is_running = False
stop_event = asyncio.Event()

# ---------------- TRANSLATE WORKER ----------------
def translate_batch_worker(batch_docs):
    """
    Batch olarak gelen belgeleri çevirir.
    Her doc {'_id': ..., 'title': ..., 'description': ..., 'seasons': [...]} yapısında olmalıdır.
    """
    CACHE = {}
    results = []
    errors = []

    for doc in batch_docs:
        _id = doc.get("_id")
        upd = {}
        title_main = doc.get("title") or doc.get("name") or "İsim yok"

        try:
            # Film description çevirisi
            if "description" in doc and doc["description"]:
                upd["description"] = translate_text_safe(doc["description"], CACHE)

            # Dizi sezonları ve bölümleri
            seasons = doc.get("seasons")
            if seasons:
                for s in seasons:
                    for ep in s.get("episodes", []):
                        if not ep.get("cevrildi", False):
                            if ep.get("title"):
                                ep["title"] = translate_text_safe(ep["title"], CACHE)
                            if ep.get("overview"):
                                ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                            ep["cevrildi"] = True
                upd["seasons"] = seasons

            upd["cevrildi"] = True
            results.append((_id, upd))
        except Exception as e:
            errors.append(f"ID: {_id} | Film/Dizi: {title_main} | Hata: {str(e)}")

    return results, errors


# ---------------- /cevir ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    global stop_event, is_running

    if is_running:
        await message.reply_text("⛔ Zaten devam eden bir işlem var.")
        return

    is_running = True
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
    )

    start_time = time.time()

    # ---------------- TOPLAM HESAPLAMA ----------------
    movies_to_translate = movie_col.count_documents({})
    episodes_to_translate = 0
    for doc in series_col.find({}, {"seasons.episodes": 1}):
        for season in doc.get("seasons", []):
            episodes_to_translate += len(season.get("episodes", []))

    total_to_translate = movies_to_translate + episodes_to_translate
    translated_movies = 0
    translated_episodes = 0
    error_count = 0

    collections = [
        {"col": movie_col, "type": "film", "translated": 0, "errors_list": []},
        {"col": series_col, "type": "episode", "translated": 0, "errors_list": []},
    ]

    batch_size = 50
    workers = 4
    pool = ThreadPoolExecutor(max_workers=workers)
    loop = asyncio.get_event_loop()
    last_update = time.time()
    update_interval = 10

    try:
        for c in collections:
            col = c["col"]
            docs_cursor = col.find({}, {"_id": 1})
            ids = [d["_id"] for d in docs_cursor]
            idx = 0

            while idx < len(ids):
                if not is_running: # Eğer kullanıcı /durdur dediyse döngüden çık
                    await start_msg.edit_text("⛔ İşlem kullanıcı tarafından durduruldu.")
                    return # İşlemi sonlandır
                    
                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))

                # Worker çağrısı
                results, errors = await loop.run_in_executor(pool, translate_batch_worker, batch_docs)

                for _id, upd in results:
                    try:
                        col.update_one({"_id": _id}, {"$set": upd})
                        if c["type"] == "film":
                            translated_movies += 1
                        else:
                            seasons = upd.get("seasons", [])
                            ep_count = sum(len(s.get("episodes", [])) for s in seasons)
                            translated_episodes += ep_count
                    except:
                        errors.append(f"ID: {_id} | DB Güncelleme Hatası")

                error_count += len(errors)
                c["errors_list"].extend(errors)
                idx += len(batch_ids)

                # ---------------- CPU / RAM ----------------
                cpu = psutil.cpu_percent(interval=None)
                ram = psutil.virtual_memory().percent

                # ---------------- SÜRE HESAPLAMA ----------------
                elapsed = int(time.time() - start_time)
                h, rem = divmod(elapsed, 3600)
                m, s = divmod(rem, 60)
                elapsed_str = f"{h}s{m}d{s}s"

                remaining = (movies_to_translate - translated_movies) + (episodes_to_translate - translated_episodes)
                eta_str = "hesaplanıyor"
                if translated_movies + translated_episodes > 0:
                    avg = elapsed / (translated_movies + translated_episodes)
                    eta_sec = int(avg * remaining)
                    eh, er = divmod(eta_sec, 3600)
                    em, es = divmod(er, 60)
                    eta_str = f"{eh}s{em}d{es}s"

                if time.time() - last_update >= update_interval or idx >= len(ids):
                    last_update = time.time()
                    try:
                        await start_msg.edit_text(
                            (
                                f"🇹🇷 Türkçe çeviri yapılıyor.\n\n"
                                f"Toplam: {total_to_translate} (Film {movies_to_translate} | Bölüm {episodes_to_translate})\n"
                                f"Çevrilen: Film {translated_movies} | Bölüm {translated_episodes}\n"
                                f"Kalan: Film {movies_to_translate - translated_movies} | Bölüm {episodes_to_translate - translated_episodes}\n"
                                f"Hatalı: {error_count}\n"
                                f"{progress_bar(translated_movies + translated_episodes, total_to_translate)}\n\n"
                                f"Süre: `{elapsed_str}` (`{eta_str}`)\n\n"
                                f"┟ CPU → {cpu}%\n"
                                f"┖ RAM → {ram}%\n\n"
                                f"🛑 İşlemi durdurmak için: /durdur" # Yeni eklenen satır
                            ),
                            parse_mode=enums.ParseMode.MARKDOWN,
                        )
                    except:
                        pass
    finally:
        pool.shutdown(wait=False)
        is_running = False

    # ---------------- FINAL ÖZET ----------------
    total_duration = int(time.time() - start_time)
    h, rem = divmod(total_duration, 3600)
    m, s = divmod(rem, 60)
    duration_str = f"{h}s{m}d{s}s"

    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory().percent

    await start_msg.edit_text(
        (
            "📊 **Genel Özet**\n\n"
            f"Toplam: {total_to_translate} (Film {movies_to_translate} | Bölüm {episodes_to_translate})\n"
            f"Çevrilen: Film {translated_movies} | Bölüm {translated_episodes}\n"
            f"Kalan: Film {movies_to_translate - translated_movies} | Bölüm {episodes_to_translate - translated_episodes}\n"
            f"Hatalı: {error_count}\n"
            f"Süre: {duration_str}\n\n"
            f"CPU: {cpu}% | RAM: {ram}%"
        ),
        parse_mode=enums.ParseMode.MARKDOWN
    )

    # -------- HATA DOSYASI --------
    hata_icerigi = []
    for c in collections:
        if c["errors_list"]:
            hata_icerigi.append(f"*** {c['col'].name} Hataları ***")
            hata_icerigi.extend(c["errors_list"])
            hata_icerigi.append("")

    if hata_icerigi:
        log_path = "cevirhatalari.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(hata_icerigi))
        try:
            await client.send_document(
                chat_id=OWNER_ID,
                document=log_path,
                caption="⛔ Çeviri sırasında hatalar oluştu"
            )
        except:
            pass

# ---------------- /TUR ----------------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_komutu(client: Client, message: Message):
    start_msg = await message.reply_text("🔄 Tür güncellemesi başlatıldı…")

    genre_map = {
        "Action": "Aksiyon", "Film-Noir": "Kara Film", "Game-Show": "Oyun Gösterisi", "Short": "Kısa",
        "Sci-Fi": "Bilim Kurgu", "Sport": "Spor", "Adventure": "Macera", "Animation": "Animasyon",
        "Biography": "Biyografi", "Comedy": "Komedi", "Crime": "Suç", "Documentary": "Belgesel",
        "Drama": "Dram", "Family": "Aile", "News": "Haberler", "Fantasy": "Fantastik",
        "History": "Tarih", "Horror": "Korku", "Music": "Müzik", "Musical": "Müzikal",
        "Mystery": "Gizem", "Romance": "Romantik", "Science Fiction": "Bilim Kurgu",
        "TV Movie": "TV Filmi", "Thriller": "Gerilim", "War": "Savaş", "Western": "Vahşi Batı",
        "Action & Adventure": "Aksiyon ve Macera", "Kids": "Çocuklar", "Reality": "Gerçeklik",
        "Reality-TV": "Gerçeklik", "Sci-Fi & Fantasy": "Bilim Kurgu ve Fantazi", "Soap": "Pembe Dizi",
        "War & Politics": "Savaş ve Politika", "Bilim-Kurgu": "Bilim Kurgu",
        "Aksiyon & Macera": "Aksiyon ve Macera", "Savaş & Politik": "Savaş ve Politika",
        "Bilim Kurgu & Fantazi": "Bilim Kurgu ve Fantazi", "Talk": "Talk-Show"
    }

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0

    for col, name in collections:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1})
        bulk_ops = []

        for doc in docs_cursor:
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": new_genres}}))
                total_fixed += 1

        if bulk_ops:
            col.bulk_write(bulk_ops)

    await start_msg.edit_text(f"✅ Tür güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ---------------- /ISTATISTIK ----------------
@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def istatistik(client: Client, message: Message):
    total_movies = movie_col.count_documents({})
    total_series = series_col.count_documents({})

    def count_links_qualities(collection, is_series=False):
        link_set = set()
        telegram_set = set()
        quality_count = defaultdict(lambda: {"Link": 0, "Telegram": 0})

        if is_series:
            for doc in collection.find({}, {"seasons.episodes.telegram": 1}):
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        for t in ep.get("telegram", []):
                            _id = t.get("id", "")
                            q = t.get("quality", "Unknown")
                            if _id.startswith("http://") or _id.startswith("https://"):
                                if _id not in link_set:
                                    link_set.add(_id)
                                    quality_count[q]["Link"] += 1
                            else:
                                if _id not in telegram_set:
                                    telegram_set.add(_id)
                                    quality_count[q]["Telegram"] += 1
        else:
            for doc in collection.find({}, {"telegram": 1}):
                for t in doc.get("telegram", []):
                    _id = t.get("id", "")
                    q = t.get("quality", "Unknown")
                    if _id.startswith("http://") or _id.startswith("https://"):
                        if _id not in link_set:
                            link_set.add(_id)
                            quality_count[q]["Link"] += 1
                    else:
                        if _id not in telegram_set:
                            telegram_set.add(_id)
                            quality_count[q]["Telegram"] += 1
        return len(link_set), len(telegram_set), dict(quality_count)

    movie_link, movie_tg, movie_quality_counts = count_links_qualities(movie_col)
    series_link, series_tg, series_quality_counts = count_links_qualities(series_col, is_series=True)

    def format_quality_stats(q_dict):
        order = ["2160p", "1920p", "1440p", "1080p", "720p", "576p", "480p"]
        sorted_items = sorted(
            q_dict.items(),
            key=lambda x: (order.index(x[0]) if x[0] in order else len(order), x[0])
        )
        return "\n".join(
            f"   ┠ {q} → Link: {c['Link']} | Telegram: {c['Telegram']}"
            for q, c in sorted_items
        )

    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage("/")
    free_disk_gb = round(disk.free / (1024**3), 2)
    free_percent = disk.percent

    # -------- BOT UPTIME + YENİ GÖSTERİM KURALLARI --------
    uptime_seconds = int(time.time() - bot_start_time)
    days, rem = divmod(uptime_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    if days >= 1:
        uptime_str = f"{days}g{hours}s{minutes}d{seconds}s"
    elif hours >= 1:
        uptime_str = f"{hours}s{minutes}d{seconds}s"
    else:
        uptime_str = f"{minutes}d{seconds}s"
    # -----------------------------------------------------

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    storage_percent = round((storage_mb / 512) * 100, 1)

    genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})
    for d in movie_col.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[d["_id"]]["film"] = d["count"]
    for d in series_col.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[d["_id"]]["dizi"] = d["count"]

    genre_text = "\n".join(
        f"{g:<14} | Film: {c['film']:<4} | Dizi: {c['dizi']:<4}"
        for g, c in sorted(genre_stats.items())
    )

    text = (
        f"⌬ <b>İstatistik</b>\n\n"
        f"┠ Filmler : {total_movies}\n"
        f"┃  ┠ Link     : {movie_link}\n"
        f"┃  ┖ Telegram : {movie_tg}\n"
        f"{format_quality_stats(movie_quality_counts)}\n\n"
        f"┠ Diziler : {total_series}\n"
        f"┃  ┠ Link     : {series_link}\n"
        f"┃  ┖ Telegram : {series_tg}\n"
        f"{format_quality_stats(series_quality_counts)}\n\n"
        f"┖ Depolama: {storage_mb} MB (%{storage_percent})\n\n"
        f"<b>Tür Dağılımı</b>\n<pre>{genre_text}</pre>\n\n"
        f"┟ CPU → {cpu}% | Boş → {free_disk_gb}GB [{free_percent}%]\n"
        f"┖ RAM → {ram}% | Süre → {uptime_str}"
    )

    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# ---------- benzerleri sil ----------
@Client.on_message(filters.command("aynivideolarisil") & filters.private & filters.user(OWNER_ID))
async def benzerleri_sil(client: Client, message: Message):
    status = await message.reply_text("🔍 Arşiv taranıyor...")

    total_docs = 0
    total_removed = 0
    log_lines = []

    collections = [
        (movie_col, "movie"),
        (series_col, "tv")
    ]

    for col, col_name in collections:
        cursor = col.find({}, {"telegram": 1, "seasons": 1, "title": 1, "tmdb_id": 1, "imdb_id": 1})

        for doc in cursor:
            doc_updated = False

            # ---------- FILM ----------
            if col_name == "movie" and "telegram" in doc:
                telegram = doc.get("telegram", [])
                grouped = {}

                for idx, t in enumerate(telegram):
                    key = (t.get("name"), t.get("size"))
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append((idx, t))

                new_telegram = []

                for (name, size), items in grouped.items():
                    non_http_items = []
                    for i, t in items:
                        tid = str(t.get("id", "")).lower()
                        if not (tid.startswith("http://") or tid.startswith("https://")):
                            non_http_items.append((i, t))

                    if non_http_items:
                        keep_i, keep_t = max(non_http_items, key=lambda x: x[0])
                    else:
                        keep_i, keep_t = max(items, key=lambda x: x[0])

                    new_telegram.append(keep_t)

                    for i, t in items:
                        if t is not keep_t:
                            total_removed += 1
                            doc_updated = True
                            log_lines.append(
                                f"[Koleksiyon] movie\n"
                                f"ID: {doc.get('tmdb_id')}\n"
                                f"Başlık: {doc.get('title')}\n"
                                f"Name: {t.get('name')}\n"
                                f"Size: {t.get('size')}\n"
                                f"id: {t.get('id')}\n"
                                f"{'-'*50}"
                            )

                if doc_updated:
                    col.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"telegram": new_telegram}}
                    )
                    total_docs += 1

            # ---------- DİZİ / BÖLÜM ----------
            if col_name == "tv":
                seasons = doc.get("seasons", [])

                for season in seasons:
                    season_no = season.get("season_number")
                    episodes = season.get("episodes", [])

                    for ep in episodes:
                        if "telegram" not in ep:
                            continue

                        telegram = ep.get("telegram", [])
                        grouped = {}

                        for idx, t in enumerate(telegram):
                            key = (t.get("name"), t.get("size"))
                            if key not in grouped:
                                grouped[key] = []
                            grouped[key].append((idx, t))

                        new_telegram = []

                        for (name, size), items in grouped.items():
                            non_http_items = []
                            for i, t in items:
                                tid = str(t.get("id", "")).lower()
                                if not (tid.startswith("http://") or tid.startswith("https://")):
                                    non_http_items.append((i, t))

                            if non_http_items:
                                keep_i, keep_t = max(non_http_items, key=lambda x: x[0])
                            else:
                                keep_i, keep_t = max(items, key=lambda x: x[0])

                            new_telegram.append(keep_t)

                            for i, t in items:
                                if t is not keep_t:
                                    total_removed += 1
                                    doc_updated = True
                                    log_lines.append(
                                        f"[Koleksiyon] tv\n"
                                        f"ID: {doc.get('imdb_id')}\n"
                                        f"Dizi: {doc.get('title')}\n"
                                        f"Sezon: {season_no} | Bölüm: {ep.get('episode_number')}\n"
                                        f"Name: {t.get('name')}\n"
                                        f"Size: {t.get('size')}\n"
                                        f"id: {t.get('id')}\n"
                                        f"{'-'*50}"
                                    )

                        if doc_updated:
                            ep["telegram"] = new_telegram

                if doc_updated:
                    col.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"seasons": seasons}}
                    )
                    total_docs += 1

    # ---------- LOG DOSYASI ----------
    if log_lines:
        log_path = "silinenler.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines))

        await client.send_document(
            chat_id=OWNER_ID,
            document=log_path,
            caption="🗑️ Silinen videolar"
        )

    await status.edit_text(
        f"✅ İşlem tamamlandı\n\n"
        f"📄 Etkilenen kayıt: {total_docs}\n"
        f"🗑️ Silinen videolar: {total_removed}"
    )


# ---------- linkleri sil ----------
@Client.on_message(filters.command("linklerisil") & filters.private & filters.user(OWNER_ID))
async def linklerisil(client: Client, message: Message):
    status = await message.reply_text("🔄 Link kayıtları temizleniyor...")
    total_removed = 0
    total_docs = 0

    def is_valid_id(tid):
        return not (str(tid).startswith("http://") or str(tid).startswith("https://"))

    # ---------- FILMLER ----------
    for doc in movie_col.find({}, {"_id": 1, "telegram": 1, "title": 1, "tmdb_id":1}):
        telegram = doc.get("telegram", [])
        new_telegram = [t for t in telegram if is_valid_id(t.get("id", ""))]
        removed_count = len(telegram) - len(new_telegram)
        if removed_count > 0:
            total_removed += removed_count
            if new_telegram:
                movie_col.update_one({"_id": doc["_id"]}, {"$set": {"telegram": new_telegram}})
            else:
                movie_col.delete_one({"_id": doc["_id"]})
            total_docs += 1

    # ---------- DİZİLER ----------
    for doc in series_col.find({}, {"_id": 1, "seasons": 1, "title":1, "imdb_id":1}):
        seasons = doc.get("seasons", [])
        doc_updated = False
        for season in seasons:
            episodes = season.get("episodes", [])
            new_episodes = []
            for ep in episodes:
                telegram = ep.get("telegram", [])
                new_telegram = [t for t in telegram if is_valid_id(t.get("id", ""))]
                removed_count = len(telegram) - len(new_telegram)
                if removed_count > 0:
                    total_removed += removed_count
                if new_telegram:
                    ep["telegram"] = new_telegram
                    new_episodes.append(ep)
                else:
                    doc_updated = True  # bölüm silindi
            season["episodes"] = new_episodes
        # Sezonlar güncellendikten sonra hiçbir bölüm kalmamışsa dizi silinecek
        remaining_eps = sum(len(s.get("episodes", [])) for s in seasons)
        if remaining_eps > 0:
            series_col.update_one({"_id": doc["_id"]}, {"$set": {"seasons": seasons}})
            if doc_updated:
                total_docs += 1
        else:
            series_col.delete_one({"_id": doc["_id"]})
            total_docs += 1

    await status.edit_text(f"✅ İşlem tamamlandı\n\n📄 Etkilenen kayıt: {total_docs}\n🗑️ Silinen tekrar: {total_removed}")

@Client.on_message(filters.command("durdur") & filters.private & filters.user(OWNER_ID))
async def durdur_komutu(client: Client, message: Message):
    global is_running
    if is_running:
        is_running = False
        await message.reply_text("⛔ İşlem durduruluyor... Lütfen bekleyin.")
    else:
        await message.reply_text("⚠️ Şu an çalışan bir işlem yok.")



