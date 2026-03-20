import asyncio
import traceback
import PTN
import re
from re import compile, IGNORECASE
from Backend.helper.imdb import get_detail, get_season, search_title
from themoviedb import aioTMDb
from Backend.config import Telegram
import Backend
from Backend.logger import LOGGER
from Backend.helper.encrypt import encode_string
from deep_translator import GoogleTranslator

# ----------------- Configuration -----------------
DELAY = 0
tmdb = aioTMDb(key=Telegram.TMDB_API, language="tr-TR", region="TR")

# Cache dictionaries (per run)
IMDB_CACHE: dict = {}
TMDB_SEARCH_CACHE: dict = {}
TMDB_DETAILS_CACHE: dict = {}
EPISODE_CACHE: dict = {}
TRANSLATE_CACHE: dict = {}

GENRE_TUR_ALIASES = {
  "action": "Aksiyon",
  "sci-fi": "Bilim Kurgu",
  "science fiction": "Bilim Kurgu",
  "film-noir": "Kara Film",
  "game-show": "Oyun Gösterisi",
  "short": "Kısa",
  "sport": "Spor",
  "adventure": "Macera",
  "animation": "Animasyon",
  "biography": "Biyografi",
  "comedy": "Komedi",
  "crime": "Suç",
  "documentary": "Belgesel",
  "drama": "Dram",
  "family": "Aile",
  "news": "Haberler",
  "fantasy": "Fantastik",
  "history": "Tarih",
  "horror": "Korku",
  "music": "Müzik",
  "musical": "Müzikal",
  "mystery": "Gizem",
  "romance": "Romantik",
  "tv movie": "TV Filmi",
  "thriller": "Gerilim",
  "war": "Savaş",
  "western": "Vahşi Batı",
  "action & adventure": "Aksiyon ve Macera",
  "kids": "Çocuklar",
  "reality": "Gerçeklik",
  "reality-tv": "Gerçeklik",
  "sci-fi & fantasy": "Bilim Kurgu ve Fantazi",
  "soap": "Pembe Dizi",
  "war & politics": "Savaş ve Politika",
  "talk": "Talk-Show",
}

# Concurrency semaphore for external API calls
API_SEMAPHORE = asyncio.Semaphore(12)

# ----------------- Helpers -----------------
def format_tmdb_image(path: str, size="w300") -> str:
    if not path:
        return ""
    return f"https://image.tmdb.org/t/p/{size}{path}"

def get_tmdb_logo(images) -> str:
    if not images:
        return ""
    logos = getattr(images, "logos", None)
    if not logos:
        return ""
    for logo in logos:
        iso_lang = getattr(logo, "iso_639_1", None)
        file_path = getattr(logo, "file_path", None)
        if iso_lang == "en" and file_path:
            return format_tmdb_image(file_path, "w300")
    for logo in logos:
        file_path = getattr(logo, "file_path", None)
        if file_path:
            return format_tmdb_image(file_path, "w300")
    return ""
    

def format_imdb_images(imdb_id: str) -> dict:
    if not imdb_id:
        return {"poster": "", "backdrop": "", "logo": ""}
    return {
        "poster": f"https://images.metahub.space/poster/small/{imdb_id}/img",
        "backdrop": f"https://images.metahub.space/background/medium/{imdb_id}/img",
        "logo": f"https://images.metahub.space/logo/medium/{imdb_id}/img",
    }

def extract_default_id(url: str) -> str | None:
    # IMDb
    imdb_match = re.search(r'/title/(tt\d+)', url)
    if imdb_match:
        return imdb_match.group(1)

    # TMDb movie or TV
    tmdb_match = re.search(r'/((movie|tv))/(\d+)', url)
    if tmdb_match:
        return tmdb_match.group(3)

    return None

async def safe_imdb_search(title: str, type_: str) -> str | None:
    key = f"imdb::{type_}::{title}"
    if key in IMDB_CACHE:
        return IMDB_CACHE[key]
    try:
        async with API_SEMAPHORE:
            result = await search_title(query=title, type=type_)
        imdb_id = result["id"] if result else None
        IMDB_CACHE[key] = imdb_id
        return imdb_id
    except Exception as e:
        LOGGER.warning(f"IMDb search failed for '{title}' [{type_}]: {e}")
        return None

async def safe_tmdb_search(title: str, type_: str, year=None):
    key = f"tmdb_search::{type_}::{title}::{year}"
    if key in TMDB_SEARCH_CACHE:
        return TMDB_SEARCH_CACHE[key]
    try:
        async with API_SEMAPHORE:
            if type_ == "movie":
                results = await tmdb.search().movies(query=title, year=year) if year else await tmdb.search().movies(query=title)
            else:
                results = await tmdb.search().tv(query=title)
        res = results[0] if results else None
        TMDB_SEARCH_CACHE[key] = res
        return res
    except Exception as e:
        LOGGER.error(f"TMDb search failed for '{title}' [{type_}]: {e}")
        TMDB_SEARCH_CACHE[key] = None
        return None

async def _tmdb_movie_details(movie_id):
    if movie_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[movie_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.movie(movie_id).details(
                append_to_response="external_ids,credits"
            )
            images = await tmdb.movie(movie_id).images()
            details.images = images

        TMDB_DETAILS_CACHE[movie_id] = details
        return details
    except Exception as e:
        LOGGER.warning(f"TMDb movie details fetch failed for id={movie_id}: {e}")
        TMDB_DETAILS_CACHE[movie_id] = None
        return None


async def _tmdb_tv_details(tv_id):
    if tv_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[tv_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.tv(tv_id).details(
                append_to_response="external_ids,credits"
            )
            images = await tmdb.tv(tv_id).images()
            details.images = images
        TMDB_DETAILS_CACHE[tv_id] = details
        return details
    except Exception as e:
        LOGGER.warning(f"TMDb tv details fetch failed for id={tv_id}: {e}")
        TMDB_DETAILS_CACHE[tv_id] = None
        return None


async def _tmdb_episode_details(tv_id, season, episode):
    key = (tv_id, season, episode)
    if key in EPISODE_CACHE:
        return EPISODE_CACHE[key]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.episode(tv_id, season, episode).details()
        EPISODE_CACHE[key] = details
        return details
    except Exception:
        EPISODE_CACHE[key] = None
        return None

def translate_text_safe(text: str) -> str:
    if not text:
        return ""

    text = str(text).strip()

    if len(text) < 3:
        return text

    if text in TRANSLATE_CACHE:
        return TRANSLATE_CACHE[text]

    try:
        translated = GoogleTranslator(source="auto", target="tr").translate(text)
    except Exception:
        translated = text

    TRANSLATE_CACHE[text] = translated
    return translated


# 🔽 BURAYA EKLE
def is_turkish(text: str) -> bool:
    if not text:
        return False
    tr_chars = "çğıöşüÇĞİÖŞÜ"
    return any(c in text for c in tr_chars)
def tur_genre_normalize(genres):
    if not genres:
        return []

    out = []

    for g in genres:
        key = g.lower().replace("-", " ").strip()
        out.append(GENRE_TUR_ALIASES.get(key, g))

    return out

# ----------------- Main Metadata -----------------
async def metadata(filename: str, channel: int, msg_id, override_id: str = None) -> dict | None:
    try:
        filename = re.sub(r'\bm(1080p|720p|2160p|480p)\b', r'\1', filename, flags=re.IGNORECASE)
        parsed = PTN.parse(filename)
    except Exception as e:
        LOGGER.error(f"PTN parsing failed for {filename}: {e}\n{traceback.format_exc()}")
        return None

    # Skip combined/invalid files
    if "excess" in parsed and any("combined" in item.lower() for item in parsed["excess"]):
        LOGGER.info(f"Skipping {filename}: contains 'combined'")
        return None

    # Skip split/multipart files
    # if Telegram.SKIP_MULTIPART:
    multipart_pattern = compile(r'(?:part|cd|disc|disk)[s._-]*\d+(?=\.\w+$)', IGNORECASE)
    if multipart_pattern.search(filename):
        LOGGER.info(f"Skipping {filename}: seems to be a split/multipart file")
        return None

    title = parsed.get("title")
    season = parsed.get("season")
    episode = parsed.get("episode")
    year = parsed.get("year")
    quality = parsed.get("resolution")
    if not quality:
        # Çözünürlük yoksa dosya adında dvdrip veya .avi ara
        if re.search(r'dvdrip|\.avi', filename, re.IGNORECASE):
            quality = "576p"
        else:
            # Hiçbir şey bulunamazsa varsayılan 1080p
            quality = "1080p"
    if isinstance(season, list) or isinstance(episode, list):
        LOGGER.warning(f"Invalid season/episode format for {filename}: {parsed}")
        return None
    if season and not episode:
        LOGGER.warning(f"Missing episode in {filename}: {parsed}")
        return None
    if not quality:
        LOGGER.warning(f"Skipping {filename}: No resolution (parsed={parsed})")
        return None
    if not title:
        LOGGER.info(f"No title parsed from: {filename} (parsed={parsed})")
        return None


    default_id = None
    if override_id:
        try:
            default_id = extract_default_id(override_id) or override_id
        except Exception:
            pass
            
    if not default_id:
        try:
            default_id = extract_default_id(Backend.USE_DEFAULT_ID)
        except Exception:
            pass
            
    if not default_id:
        try:
            default_id = extract_default_id(filename)
        except Exception:
            pass

    data = {"chat_id": channel, "msg_id": msg_id}
    try:
        encoded_string = await encode_string(data)
    except Exception:
        encoded_string = None

    try:
        if season and episode:
            LOGGER.info(f"Fetching TV metadata: {title} S{season}E{episode}")
            return await fetch_tv_metadata(title, season, episode, encoded_string, year, quality, default_id)
        else:
            LOGGER.info(f"Fetching Movie metadata: {title} ({year})")
            return await fetch_movie_metadata(title, encoded_string, year, quality, default_id)
    except Exception as e:
        LOGGER.error(f"Error while fetching metadata for {filename}: {e}\n{traceback.format_exc()}")
        return None

# ----------------- TV Metadata -----------------
async def fetch_tv_metadata(title, season, episode, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    tv = None
    ep = None

    # -------------------------------------------------------
    # 1. ID Tespiti (Varsayılan ID Kontrolü)
    # -------------------------------------------------------
    if default_id:
        default_id = str(default_id).strip()
        if default_id.startswith("tt"):
            imdb_id = default_id
        elif default_id.isdigit():
            tmdb_id = int(default_id)

    # -------------------------------------------------------
    # 2. TMDB Öncelikli Veri Arama ve Çekme
    # -------------------------------------------------------
    # Eğer elimizde sadece IMDb ID varsa, TMDb ID'sini bulmaya çalış
    if not tmdb_id and imdb_id:
        try:
            async with API_SEMAPHORE:
                find_res = await tmdb.find(imdb_id, source="imdb_id")
                if find_res and find_res.tv_results:
                    tmdb_id = find_res.tv_results[0].id
        except Exception as e:
            LOGGER.warning(f"TMDb find failed for {imdb_id}: {e}")

    # Eğer hala ID yoksa, başlık ile TMDb'de ara
    if not tmdb_id:
        tmdb_search = await safe_tmdb_search(title, "tv", year)
        if tmdb_search:
            tmdb_id = tmdb_search.id

    # TMDb ID bulunduysa detayları çek
    if tmdb_id:
        tv = await _tmdb_tv_details(tmdb_id)
        if tv:
            ep = await _tmdb_episode_details(tmdb_id, season, episode)

            # Cast listesi
            credits = getattr(tv, "credits", None) or {}
            cast_arr = getattr(credits, "cast", []) or []
            cast = [
                getattr(c, "name", None) or getattr(c, "original_name", None)
                for c in cast_arr
            ]

            # Süre (Bölüm süresi -> Dizi genel süresi)
            ep_runtime = getattr(ep, "runtime", None) if ep else None
            series_runtime = (
                tv.episode_run_time[0] if getattr(tv, "episode_run_time", None) else None
            )
            runtime_val = ep_runtime or series_runtime
            runtime = f"{runtime_val} min" if runtime_val else ""

            # Başlık ve Açıklama Çeviri Kontrolü
            tmdb_title = tv.name or tv.original_name or title
            if not is_turkish(tmdb_title):
                tmdb_title = translate_text_safe(tmdb_title)

            overview = tv.overview or ""
            if overview and not is_turkish(overview):
                overview = translate_text_safe(overview)

            # Görseller ve Metahub Fallback
            tmdb_imdb_ref = getattr(getattr(tv, "external_ids", None), "imdb_id", None) or imdb_id
            images = format_imdb_images(tmdb_imdb_ref)

            poster = format_tmdb_image(tv.poster_path) or images["poster"]
            backdrop = format_tmdb_image(tv.backdrop_path, "original") or images["backdrop"]
            
            # Bölüm Başlığı ve Özeti Çeviri Kontrolü
            ep_title = getattr(ep, "name", f"S{season}E{episode}") if ep else f"S{season}E{episode}"
            if ep_title and not is_turkish(ep_title):
                ep_title = translate_text_safe(ep_title)
            
            ep_overview = getattr(ep, "overview", "") if ep else ""
            if ep_overview and not is_turkish(ep_overview):
                ep_overview = translate_text_safe(ep_overview)

            return {
                "tmdb_id": tv.id,
                "imdb_id": tmdb_imdb_ref,
                "title": tmdb_title,
                "year": getattr(tv.first_air_date, "year", 0) if getattr(tv, "first_air_date", None) else 0,
                "rate": getattr(tv, "vote_average", 0) or 0,
                "description": overview,
                "poster": poster,
                "backdrop": backdrop,
                "logo": get_tmdb_logo(getattr(tv, "images", None)) or images["logo"],
                "genres": tur_genre_normalize([g.name for g in (tv.genres or [])]),
                "media_type": "tv",
                "cast": cast,
                "runtime": str(runtime),
                "season_number": season,
                "episode_number": episode,
                "episode_title": ep_title,
                "episode_backdrop": format_tmdb_image(getattr(ep, "still_path", None), "original") if ep else "",
                "episode_overview": ep_overview,
                "episode_released": (
                    ep.air_date.strftime("%Y-%m-%dT05:00:00.000Z")
                    if getattr(ep, "air_date", None)
                    else ""
                ),
                "quality": quality,
                "encoded_string": encoded_string,
            }

    # -------------------------------------------------------
    # 3. IMDb Fallback (TMDb Verisi Bulunamazsa)
    # -------------------------------------------------------
    if not imdb_id:
        imdb_id = await safe_imdb_search(title, "tvSeries")

    if imdb_id:
        try:
            # Seri detayları
            if imdb_id in IMDB_CACHE:
                imdb_tv = IMDB_CACHE[imdb_id]
            else:
                async with API_SEMAPHORE:
                    imdb_tv = await get_detail(imdb_id=imdb_id, media_type="tvSeries")
                IMDB_CACHE[imdb_id] = imdb_tv

            # Bölüm detayları
            ep_key = f"{imdb_id}::{season}::{episode}"
            if ep_key in EPISODE_CACHE:
                imdb_ep = EPISODE_CACHE[ep_key]
            else:
                async with API_SEMAPHORE:
                    imdb_ep = await get_season(imdb_id=imdb_id, season_id=season, episode_id=episode)
                EPISODE_CACHE[ep_key] = imdb_ep

            if imdb_tv:
                imdb = imdb_tv or {}
                ep_data = imdb_ep or {}
                images = format_imdb_images(imdb_id)

                ep_title_imdb = ep_data.get("name") if ep_data else f"S{season}E{episode}"
                if ep_title_imdb and not is_turkish(ep_title_imdb):
                    ep_title_imdb = translate_text_safe(ep_title_imdb)

                return {
                    "tmdb_id": imdb.get("moviedb_id") or imdb_id.replace("tt", ""),
                    "imdb_id": imdb_id,
                    "title": title or imdb.get("title"),
                    "year": imdb.get("releaseDetailed", {}).get("year", 0),
                    "rate": imdb.get("rating", {}).get("star", 0),
                    "description": translate_text_safe(imdb.get("plot", "")),
                    "poster": images["poster"],
                    "backdrop": images["backdrop"],
                    "logo": images["logo"],
                    "cast": imdb.get("cast", []),
                    "runtime": str(imdb.get("runtime") or ""),          
                    "genres": tur_genre_normalize(imdb.get("genre", [])),
                    "media_type": "tv",
                    "season_number": season,
                    "episode_number": episode,
                    "episode_title": ep_title_imdb,
                    "episode_backdrop": ep_data.get("image", ""),
                    "episode_overview": translate_text_safe(ep_data.get("plot", "")),
                    "episode_released": str(ep_data.get("released", "")),
                    "quality": quality,
                    "encoded_string": encoded_string,
                }
        except Exception as e:
            LOGGER.error(f"IMDb TV fallback fetch failed for {title}: {e}")

    return None

# ----------------- Movie Metadata -----------------
async def fetch_movie_metadata(title, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    movie = None

    # 1. ADIM: Kimlik (ID) Tespiti
    if default_id:
        default_id = str(default_id).strip()
        if default_id.startswith("tt"):
            imdb_id = default_id
        elif default_id.isdigit():
            tmdb_id = int(default_id)

    # 2. ADIM: Cross-Check (IMDb ID varsa TMDb ID'sini bul)
    if not tmdb_id and imdb_id:
        try:
            async with API_SEMAPHORE:
                find_res = await tmdb.find(imdb_id, source="imdb_id")
                if find_res and find_res.movie_results:
                    tmdb_id = find_res.movie_results[0].id
        except Exception as e:
            LOGGER.warning(f"TMDb find failed for {imdb_id}: {e}")

    # 3. ADIM: TMDb Search (ID yoksa isimle ara)
    if not tmdb_id:
        tmdb_result = await safe_tmdb_search(title, "movie", year)
        if tmdb_result:
            tmdb_id = tmdb_result.id

    # 4. ADIM: TMDb Veri Çekme ve İşleme (Öncelikli Mod)
    if tmdb_id:
        movie = await _tmdb_movie_details(tmdb_id)
        if movie:
            # Oyuncu kadrosu
            credits = getattr(movie, "credits", None) or {}
            cast_arr = getattr(credits, "cast", []) or []
            cast_names = [getattr(c, "name", None) or getattr(c, "original_name", None) for c in cast_arr]

            # Süre bilgisi
            runtime_val = getattr(movie, "runtime", None)
            runtime = f"{runtime_val} min" if runtime_val else ""

            # Başlık ve Açıklama (Türkçe kontrolü ve çeviri)
            tmdb_title = movie.title or movie.original_title or title
            if not is_turkish(tmdb_title):
                tmdb_title = translate_text_safe(tmdb_title)

            overview = movie.overview or ""
            if overview and not is_turkish(overview):
                overview = translate_text_safe(overview)

            # Görsel ve Logo yönetimi
            tmdb_imdb_ref = getattr(movie.external_ids, "imdb_id", None) or imdb_id
            fallback_images = format_imdb_images(tmdb_imdb_ref)

            poster = format_tmdb_image(movie.poster_path) or fallback_images["poster"]
            backdrop = format_tmdb_image(movie.backdrop_path, "original") or fallback_images["backdrop"]
            logo = get_tmdb_logo(getattr(movie, "images", None)) or fallback_images["logo"]

            return {
                "tmdb_id": movie.id,
                "imdb_id": tmdb_imdb_ref,
                "title": tmdb_title,
                "year": getattr(movie.release_date, "year", 0) if getattr(movie, "release_date", None) else 0,
                "rate": getattr(movie, "vote_average", 0) or 0,
                "description": overview,
                "poster": poster,
                "backdrop": backdrop,
                "logo": logo,
                "cast": cast_names,
                "runtime": str(runtime),
                "media_type": "movie",
                "genres": tur_genre_normalize([g.name for g in (movie.genres or [])]),
                "quality": quality,
                "encoded_string": encoded_string,
            }

    # 5. ADIM: IMDb Fallback (TMDb verisi bulunamazsa)
    if imdb_id:
        if imdb_id not in IMDB_CACHE:
            async with API_SEMAPHORE:
                imdb_details = await get_detail(imdb_id=imdb_id, media_type="movie")
                IMDB_CACHE[imdb_id] = imdb_details
        else:
            imdb_details = IMDB_CACHE[imdb_id]

        if imdb_details:
            images = format_imdb_images(imdb_id)
            plot_text = imdb_details.get("plot", "")
            if plot_text and not is_turkish(plot_text):
                plot_text = translate_text_safe(plot_text)

            return {
                "tmdb_id": imdb_details.get("moviedb_id") or imdb_id.replace("tt", ""),
                "imdb_id": imdb_id,
                "title": imdb_details.get("title") or title,
                "year": imdb_details.get("releaseDetailed", {}).get("year", 0),
                "rate": imdb_details.get("rating", {}).get("star", 0),
                "description": plot_text,
                "poster": images["poster"],
                "backdrop": images["backdrop"],
                "logo": images["logo"],
                "cast": imdb_details.get("cast", []),
                "runtime": str(imdb_details.get("runtime") or ""),
                "media_type": "movie",
                "genres": tur_genre_normalize(imdb_details.get("genre", [])),
                "quality": quality,
                "encoded_string": encoded_string,
            }

    return None
