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
def format_tmdb_image(path: str, size="w500") -> str:
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
                language="tr-TR",
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
                language="tr-TR",
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
            details = await tmdb.episode(tv_id, season, episode).details(language="tr-TR")
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
    imdb_tv = None
    imdb_ep = None
    use_tmdb = False

    # -------------------------------------------------------
    # 1. Handle default ID (IMDb / TMDb)
    # -------------------------------------------------------
    if default_id:
        default_id = str(default_id)
        if default_id.startswith("tt"):
            imdb_id = default_id
            use_tmdb = False
        elif default_id.isdigit():
            tmdb_id = int(default_id)
            use_tmdb = True

    # -------------------------------------------------------
    # 2. If no ID → Try IMDb search first
    # -------------------------------------------------------
    if not imdb_id and not tmdb_id:
        imdb_id = await safe_imdb_search(title, "tvSeries")
        use_tmdb = not bool(imdb_id)

    # -------------------------------------------------------
    # 3. IMDb fetch (series + episode)
    # -------------------------------------------------------
    if imdb_id and not use_tmdb:
        try:
            # ----- series details
            if imdb_id in IMDB_CACHE:
                imdb_tv = IMDB_CACHE[imdb_id]
            else:
                async with API_SEMAPHORE:
                    imdb_tv = await get_detail(imdb_id=imdb_id, media_type="tvSeries")
                IMDB_CACHE[imdb_id] = imdb_tv

            # ----- episode details
            ep_key = f"{imdb_id}::{season}::{episode}"
            if ep_key in EPISODE_CACHE:
                imdb_ep = EPISODE_CACHE[ep_key]
            else:
                async with API_SEMAPHORE:
                    imdb_ep = await get_season(imdb_id=imdb_id, season_id=season, episode_id=episode)
                EPISODE_CACHE[ep_key] = imdb_ep

        except Exception as e:
            LOGGER.warning(f"IMDb TV fetch failed [{imdb_id}] → {e}")
            imdb_tv = None
            imdb_ep = None
            use_tmdb = True

    # -------------------------------------------------------
    # 4. Decide if TMDb required
    # -------------------------------------------------------
    must_use_tmdb = (
        use_tmdb or
        imdb_tv is None or
        imdb_tv == {}
    )

    # =======================================================
    #  5. TMDb MODE
    # =======================================================
    if must_use_tmdb:
        LOGGER.info(f"No valid IMDb TV data for '{title}' → using TMDb")

        # Search TMDb by title
        if not tmdb_id:
            tmdb_search = await safe_tmdb_search(title, "tv", year)
            if not tmdb_search:
                LOGGER.warning(f"No TMDb TV result for '{title}'")
                return None
            tmdb_id = tmdb_search.id

        # Fetch full TV show details
        tv = await _tmdb_tv_details(tmdb_id)
        if not tv:
            LOGGER.warning(f"TMDb TV details failed for id={tmdb_id}")
            return None

        # Fetch episode
        ep = await _tmdb_episode_details(tmdb_id, season, episode)

        # Cast list
        credits = getattr(tv, "credits", None) or {}
        cast_arr = getattr(credits, "cast", []) or []
        cast = [
            getattr(c, "name", None) or getattr(c, "original_name", None)
            for c in cast_arr
        ]

        # Runtime (prefer episode → series → empty)
        ep_runtime = getattr(ep, "runtime", None) if ep else None
        series_runtime = (
            tv.episode_run_time[0] if getattr(tv, "episode_run_time", None) else None
        )
        runtime_val = ep_runtime or series_runtime
        runtime = f"{runtime_val} min" if runtime_val else ""

        # --- TITLE ---
        tmdb_title = tv.name or ""
        original_title = tv.original_name or title

        if not is_turkish(tmdb_title):
            tmdb_title = translate_text_safe(tmdb_title or original_title)

        # --- DESCRIPTION ---
        overview = tv.overview or ""
        if not is_turkish(overview):
            overview = translate_text_safe(overview)

        # --- IMAGE FALLBACK ---
        imdb_ref = getattr(getattr(tv, "external_ids", None), "imdb_id", None)
        images = format_imdb_images(imdb_ref)

        poster = format_tmdb_image(tv.poster_path)
        backdrop = format_tmdb_image(tv.backdrop_path, "original")

        if not poster:
            poster = images["poster"]

        if not backdrop:
            backdrop = images["backdrop"]
      
        return {
            "tmdb_id": tv.id,
            "imdb_id": getattr(getattr(tv, "external_ids", None), "imdb_id", None),
            "title": tmdb_title,
            "year": getattr(tv.first_air_date, "year", 0) if getattr(tv, "first_air_date", None) else 0,
            "rate": getattr(tv, "vote_average", 0) or 0,
            "description": overview,
            "poster": poster,
            "backdrop": backdrop,
            "logo": get_tmdb_logo(getattr(tv, "images", None)),
            "genres": tur_genre_normalize([g.name for g in (tv.genres or [])]),
            "media_type": "tv",
            "cast": cast,
            "runtime": str(runtime),
            "season_number": season,
            "episode_number": episode,
            "episode_title": translate_text_safe(getattr(ep, "name", f"S{season}E{episode}")) if ep else f"S{season}E{episode}",
            "episode_backdrop": format_tmdb_image(getattr(ep, "still_path", None), "original") if ep else "",
            "episode_overview": translate_text_safe(getattr(ep, "overview", "")) if ep else "",
            "episode_released": (
                ep.air_date.strftime("%Y-%m-%dT05:00:00.000Z")
                if getattr(ep, "air_date", None)
                else ""
            ),

            "quality": quality,
            "encoded_string": encoded_string,
        }

    # =======================================================
    
    # =======================================================
    #  6. IMDb MODE
    # =======================================================
    imdb = imdb_tv or {}
    ep = imdb_ep or {}
    images = format_imdb_images(imdb_id)

    # Mantığı sözlük dışına çıkarıyoruz
    ep_title = ep.get("name") if ep else f"S{season}E{episode}"
    if ep_title and not is_turkish(ep_title):
        ep_title = translate_text_safe(ep_title)

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
        "episode_title": ep_title, # Artık temiz değişkeni kullanıyoruz
        "episode_backdrop": ep.get("image", ""),
        "episode_overview": translate_text_safe(ep.get("plot", "")),
        "episode_released": str(ep.get("released", "")),
        "quality": quality,
        "encoded_string": encoded_string,
    }

# ----------------- Movie Metadata -----------------
# ----------------- TV Metadata (TMDb Priority) -----------------
async def fetch_tv_metadata(title, season, episode, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    tv_details = None
    ep_details = None
    use_imdb = False

    # -------------------------------------------------------
    # 1. PROCESS DEFAULT ID (TMDb priority check)
    # -------------------------------------------------------
    if default_id:
        default_id = str(default_id).strip()
        if default_id.isdigit():
            tmdb_id = int(default_id)
            use_imdb = False
        elif default_id.startswith("tt"):
            imdb_id = default_id
            use_imdb = True

    # -------------------------------------------------------
    # 2. TMDb SEARCH & FETCH (Primary)
    # -------------------------------------------------------
    if not use_imdb:
        try:
            # Eğer ID yoksa başlık ile ara
            if not tmdb_id:
                tmdb_search = await safe_tmdb_search(title, "tv", year)
                if tmdb_search:
                    tmdb_id = tmdb_search.id
            
            # Detayları getir
            if tmdb_id:
                tv = await _tmdb_tv_details(tmdb_id)
                if tv:
                    # Bölüm detaylarını çek
                    ep = await _tmdb_episode_details(tmdb_id, season, episode)
                    
                    # Cast extraction
                    credits = getattr(tv, "credits", None) or {}
                    cast_arr = getattr(credits, "cast", []) or []
                    cast = [
                        getattr(c, "name", None) or getattr(c, "original_name", None)
                        for c in cast_arr
                    ]

                    # Runtime (Bölüm -> Dizi -> Boş)
                    ep_runtime = getattr(ep, "runtime", None) if ep else None
                    series_runtime = (
                        tv.episode_run_time[0] if getattr(tv, "episode_run_time", None) else None
                    )
                    runtime_val = ep_runtime or series_runtime
                    runtime = f"{runtime_val} min" if runtime_val else ""

                    # Title & Description Translation logic
                    tmdb_title = tv.name or ""
                    if not is_turkish(tmdb_title):
                        tmdb_title = translate_text_safe(tmdb_title or tv.original_name or title)

                    overview = tv.overview or ""
                    if not is_turkish(overview):
                        overview = translate_text_safe(overview)

                    # Image Fallback
                    imdb_ref = getattr(getattr(tv, "external_ids", None), "imdb_id", None)
                    images = format_imdb_images(imdb_ref)
                    poster = format_tmdb_image(tv.poster_path) or images["poster"]
                    backdrop = format_tmdb_image(tv.backdrop_path, "original") or images["backdrop"]

                    return {
                        "tmdb_id": tv.id,
                        "imdb_id": imdb_ref,
                        "title": tmdb_title,
                        "year": getattr(tv.first_air_date, "year", 0) if getattr(tv, "first_air_date", None) else 0,
                        "rate": getattr(tv, "vote_average", 0) or 0,
                        "description": overview,
                        "poster": poster,
                        "backdrop": backdrop,
                        "logo": get_tmdb_logo(getattr(tv, "images", None)),
                        "genres": tur_genre_normalize([g.name for g in (tv.genres or [])]),
                        "media_type": "tv",
                        "cast": cast,
                        "runtime": str(runtime),
                        "season_number": season,
                        "episode_number": episode,
                        "episode_title": translate_text_safe(getattr(ep, "name", f"S{season}E{episode}")) if ep else f"S{season}E{episode}",
                        "episode_backdrop": format_tmdb_image(getattr(ep, "still_path", None), "original") if ep else "",
                        "episode_overview": translate_text_safe(getattr(ep, "overview", "")) if ep else "",
                        "episode_released": (
                            ep.air_date.strftime("%Y-%m-%dT05:00:00.000Z")
                            if getattr(ep, "air_date", None)
                            else ""
                        ),
                        "quality": quality,
                        "encoded_string": encoded_string,
                    }
                else:
                    use_imdb = True
            else:
                use_imdb = True

        except Exception as e:
            LOGGER.warning(f"TMDb TV fetch failed, falling back to IMDb: {e}")
            use_imdb = True

    # -------------------------------------------------------
    # 3. IMDb MODE (Fallback)
    # -------------------------------------------------------
    if use_imdb:
        if not imdb_id:
            imdb_id = await safe_imdb_search(title, "tvSeries")
        
        if imdb_id:
            try:
                # Dizi Detayı
                if imdb_id in IMDB_CACHE:
                    imdb_tv = IMDB_CACHE[imdb_id]
                else:
                    async with API_SEMAPHORE:
                        imdb_tv = await get_detail(imdb_id=imdb_id, media_type="tvSeries")
                    IMDB_CACHE[imdb_id] = imdb_tv

                # Bölüm Detayı
                ep_key = f"{imdb_id}::{season}::{episode}"
                if ep_key in EPISODE_CACHE:
                    imdb_ep = EPISODE_CACHE[ep_key]
                else:
                    async with API_SEMAPHORE:
                        imdb_ep = await get_season(imdb_id=imdb_id, season_id=season, episode_id=episode)
                    EPISODE_CACHE[ep_key] = imdb_ep

                if imdb_tv:
                    images = format_imdb_images(imdb_id)
                    ep_title = imdb_ep.get("name") if imdb_ep else f"S{season}E{episode}"
                    if ep_title and not is_turkish(ep_title):
                        ep_title = translate_text_safe(ep_title)

                    return {
                        "tmdb_id": imdb_tv.get("moviedb_id") or imdb_id.replace("tt", ""),
                        "imdb_id": imdb_id,
                        "title": title or imdb_tv.get("title"),
                        "year": imdb_tv.get("releaseDetailed", {}).get("year", 0),
                        "rate": imdb_tv.get("rating", {}).get("star", 0),
                        "description": translate_text_safe(imdb_tv.get("plot", "")),
                        "poster": images["poster"],
                        "backdrop": images["backdrop"],
                        "logo": images["logo"],
                        "cast": imdb_tv.get("cast", []),
                        "runtime": str(imdb_tv.get("runtime") or ""),
                        "genres": tur_genre_normalize(imdb_tv.get("genre", [])),
                        "media_type": "tv",
                        "season_number": season,
                        "episode_number": episode,
                        "episode_title": ep_title,
                        "episode_backdrop": imdb_ep.get("image", "") if imdb_ep else "",
                        "episode_overview": translate_text_safe(imdb_ep.get("plot", "")) if imdb_ep else "",
                        "episode_released": str(imdb_ep.get("released", "")) if imdb_ep else "",
                        "quality": quality,
                        "encoded_string": encoded_string,
                    }
            except Exception as e:
                LOGGER.error(f"IMDb TV fallback failed: {e}")

    return None
