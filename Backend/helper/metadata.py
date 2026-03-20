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
        if getattr(logo, "iso_639_1", None) == "en" and getattr(logo, "file_path", None):
            return format_tmdb_image(logo.file_path, "w300")
    for logo in logos:
        if getattr(logo, "file_path", None):
            return format_tmdb_image(logo.file_path, "w300")
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
    if not url:
        return None
    imdb_match = re.search(r'/title/(tt\d+)', url)
    if imdb_match:
        return imdb_match.group(1)
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
    except Exception:
        return None

async def safe_tmdb_search(title: str, type_: str, year=None):
    key = f"tmdb_search::{type_}::{title}::{year}"
    if key in TMDB_SEARCH_CACHE:
        return TMDB_SEARCH_CACHE[key]
    try:
        async with API_SEMAPHORE:
            if type_ == "movie":
                if year:
                    results = await tmdb.search().movies(query=title, year=year)
                else:
                    results = await tmdb.search().movies(query=title)
            else:
                results = await tmdb.search().tv(query=title)
        res = results[0] if results else None
        TMDB_SEARCH_CACHE[key] = res
        return res
    except Exception:
        return None

async def _tmdb_movie_details(movie_id):
    if movie_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[movie_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.movie(movie_id).details(language="tr-TR", append_to_response="external_ids,credits")
            details.images = await tmdb.movie(movie_id).images()
        TMDB_DETAILS_CACHE[movie_id] = details
        return details
    except Exception:
        return None

async def _tmdb_tv_details(tv_id):
    if tv_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[tv_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.tv(tv_id).details(language="tr-TR", append_to_response="external_ids,credits")
            details.images = await tmdb.tv(tv_id).images()
        TMDB_DETAILS_CACHE[tv_id] = details
        return details
    except Exception:
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
        return None

def translate_text_safe(text: str) -> str:
    if not text or len(str(text).strip()) < 3:
        return text or ""
    if text in TRANSLATE_CACHE:
        return TRANSLATE_CACHE[text]
    try:
        translated = GoogleTranslator(source="auto", target="tr").translate(text)
    except Exception:
        translated = text
    TRANSLATE_CACHE[text] = translated
    return translated

def tur_genre_normalize(genres):
    if not genres:
        return []
    return [GENRE_TUR_ALIASES.get(g.lower().replace("-", " ").strip(), g) for g in genres]

# ----------------- Core -----------------

async def metadata(filename: str, channel: int, msg_id, override_id: str = None) -> dict | None:
    try:
        filename = re.sub(r'\bm(1080p|720p|2160p|480p)\b', r'\1', filename, flags=re.IGNORECASE)
        parsed = PTN.parse(filename)
    except Exception:
        return None

    if "excess" in parsed and any("combined" in item.lower() for item in parsed["excess"]):
        return None
    if compile(r'(?:part|cd|disc|disk)[s._-]*\d+(?=\.\w+$)', IGNORECASE).search(filename):
        return None

    title = parsed.get("title")
    season = parsed.get("season")
    episode = parsed.get("episode")
    year = parsed.get("year")
    quality = parsed.get("resolution") or ("576p" if re.search(r'dvdrip|\.avi', filename, re.IGNORECASE) else "1080p")

    if not title:
        return None
    if season and not episode:
        return None
    if isinstance(season, list) or isinstance(episode, list):
        return None

    default_id = extract_default_id(override_id or Backend.USE_DEFAULT_ID or filename)
    data = {"chat_id": channel, "msg_id": msg_id}
    encoded_string = await encode_string(data)

    if season and episode:
        return await fetch_tv_metadata(title, season, episode, encoded_string, year, quality, default_id)
    else:
        return await fetch_movie_metadata(title, encoded_string, year, quality, default_id)

async def fetch_tv_metadata(title, season, episode, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    tmdb_tv = None
    ep = None

    if default_id:
        default_id = str(default_id)
        if default_id.isdigit():
            tmdb_id = int(default_id)
        elif default_id.startswith("tt"):
            imdb_id = default_id

    # 1. ÖNCE TMDB ARAMASI
    if not tmdb_id and not imdb_id:
        tmdb_search = await safe_tmdb_search(title, "tv", year)
        if tmdb_search:
            tmdb_id = tmdb_search.id

    # 2. TMDB'DEN VERİ ÇEK (Öncelikli Kaynak)
    if tmdb_id:
        tmdb_tv = await _tmdb_tv_details(tmdb_id)
        if tmdb_tv:
            ep = await _tmdb_episode_details(tmdb_id, season, episode)
            
            credits = getattr(tmdb_tv, "credits", None) or {}
            cast_arr = getattr(credits, "cast", []) or []
            cast = [getattr(c, "name", None) or getattr(c, "original_name", None) for c in cast_arr]
            
            ep_runtime = getattr(ep, "runtime", None) if ep else None
            series_runtime = tmdb_tv.episode_run_time[0] if getattr(tmdb_tv, "episode_run_time", None) else None
            runtime_val = ep_runtime or series_runtime
            runtime = f"{runtime_val} min" if runtime_val else ""

            return {
                "tmdb_id": tmdb_tv.id,
                "imdb_id": getattr(getattr(tmdb_tv, "external_ids", None), "imdb_id", None),
                "title": tmdb_tv.name or tmdb_tv.original_name or title,
                "year": getattr(tmdb_tv.first_air_date, "year", 0) if getattr(tmdb_tv, "first_air_date", None) else 0,
                "rate": getattr(tmdb_tv, "vote_average", 0) or 0,
                "description": translate_text_safe(tmdb_tv.overview),
                "poster": format_tmdb_image(tmdb_tv.poster_path),
                "backdrop": format_tmdb_image(tmdb_tv.backdrop_path, "original"),
                "logo": get_tmdb_logo(getattr(tmdb_tv, "images", None)),
                "genres": tur_genre_normalize([g.name for g in (tmdb_tv.genres or [])]),
                "media_type": "tv",
                "cast": cast,
                "runtime": str(runtime),
                "season_number": season,
                "episode_number": episode,
                "episode_title": translate_text_safe(getattr(ep, "name", f"S{season}E{episode}")) if ep else f"S{season}E{episode}",
                "episode_backdrop": format_tmdb_image(getattr(ep, "still_path", None), "original") if ep else "",
                "episode_overview": translate_text_safe(getattr(ep, "overview", "")) if ep else "",
                "episode_released": (ep.air_date.strftime("%Y-%m-%dT05:00:00.000Z") if getattr(ep, "air_date", None) else ""),
                "quality": quality,
                "encoded_string": encoded_string,
            }

    # 3. TMDB BOŞSA IMDb'YE DÖN (Eski Kurallar)
    if not imdb_id:
        imdb_id = await safe_imdb_search(title, "tvSeries")
    
    if imdb_id:
        async with API_SEMAPHORE:
            imdb_tv = await get_detail(imdb_id=imdb_id, media_type="tvSeries")
            imdb_ep = await get_season(imdb_id=imdb_id, season_id=season, episode_id=episode)
        
        if imdb_tv:
            images = format_imdb_images(imdb_id)
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
                "episode_title": translate_text_safe(imdb_ep.get("title", f"S{season}E{episode}")),
                "episode_backdrop": imdb_ep.get("image", ""),
                "episode_overview": translate_text_safe(imdb_ep.get("plot", "")),
                "episode_released": str(imdb_ep.get("released", "")),
                "quality": quality,
                "encoded_string": encoded_string,
            }
    return None

async def fetch_movie_metadata(title, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    movie = None

    if default_id:
        default_id = str(default_id).strip()
        if default_id.isdigit():
            tmdb_id = int(default_id)
        elif default_id.startswith("tt"):
            imdb_id = default_id

    # 1. ÖNCE TMDB ARAMASI
    if not tmdb_id and not imdb_id:
        tmdb_result = await safe_tmdb_search(title, "movie", year)
        if tmdb_result:
            tmdb_id = tmdb_result.id

    # 2. TMDB'DEN VERİ ÇEK (Öncelikli Kaynak)
    if tmdb_id:
        movie = await _tmdb_movie_details(tmdb_id)
        if movie:
            credits = getattr(movie, "credits", None) or {}
            cast_arr = getattr(credits, "cast", []) or []
            cast_names = [getattr(c, "name", None) or getattr(c, "original_name", None) for c in cast_arr]
            runtime_val = getattr(movie, "runtime", None)
            runtime = f"{runtime_val} min" if runtime_val else ""

            return {
                "tmdb_id": movie.id,
                "imdb_id": getattr(movie.external_ids, "imdb_id", None),
                "title": movie.title or movie.original_title or title,
                "year": getattr(movie.release_date, "year", 0) if getattr(movie, "release_date", None) else 0,
                "rate": getattr(movie, "vote_average", 0) or 0,
                "description": translate_text_safe(movie.overview),
                "poster": format_tmdb_image(movie.poster_path),
                "backdrop": format_tmdb_image(movie.backdrop_path, "original"),
                "logo": get_tmdb_logo(getattr(movie, "images", None)),
                "cast": cast_names,
                "runtime": str(runtime),
                "media_type": "movie",
                "genres": tur_genre_normalize([g.name for g in (movie.genres or [])]),
                "quality": quality,
                "encoded_string": encoded_string,
            }

    # 3. TMDB BOŞSA IMDb'YE DÖN (Eski Kurallar)
    if not imdb_id:
        imdb_id = await safe_imdb_search(f"{title} {year}" if year else title, "movie")
    
    if imdb_id:
        async with API_SEMAPHORE:
            imdb_details = await get_detail(imdb_id=imdb_id, media_type="movie")
        
        if imdb_details:
            images = format_imdb_images(imdb_id)
            return {
                "tmdb_id": imdb_details.get("moviedb_id") or imdb_id.replace("tt", ""),
                "imdb_id": imdb_id,
                "title": imdb_details.get("title") or title,
                "year": imdb_details.get("releaseDetailed", {}).get("year", 0),
                "rate": imdb_details.get("rating", {}).get("star", 0),
                "description": translate_text_safe(imdb_details.get("plot", "")),
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
