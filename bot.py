# bot2.py
import os
import io
import json
import random
import hashlib
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urlparse
import aiohttp
import discord
from discord.ext import commands, tasks
from collections import deque

try:
    from PIL import Image, ImageSequence
except Exception:
    Image = None

# ---------- Config ----------
TOKEN = os.getenv("TOKEN", "")
WAIFUIM_API_KEY = os.getenv("WAIFUIM_API_KEY", "")
WAIFUIT_API_KEY = os.getenv("WAIFUIT_API_KEY", "")
DANBOORU_USER = os.getenv("DANBOORU_USER", "")
DANBOORU_API_KEY = os.getenv("DANBOORU_API_KEY", "")
GELBOORU_API_KEY = os.getenv("GELBOORU_API_KEY", "")

_DEBUG_RAW = os.getenv("DEBUG_FETCH", "")
DEBUG_FETCH = str(_DEBUG_RAW).strip().lower() in ("1", "true", "yes", "on")
TRUE_RANDOM = str(os.getenv("TRUE_RANDOM", "")).strip().lower() in ("1", "true", "yes")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "14"))
DISCORD_MAX_UPLOAD = int(os.getenv("DISCORD_MAX_UPLOAD", str(8 * 1024 * 1024)))
HEAD_SIZE_LIMIT = DISCORD_MAX_UPLOAD
DATA_FILE = os.getenv("DATA_FILE", "data_nsfw.json")
AUTOSAVE_INTERVAL = 30
FETCH_ATTEMPTS = 40
MAX_USED_GIFS_PER_USER = 1000

logging.basicConfig(level=logging.DEBUG if DEBUG_FETCH else logging.INFO)
logger = logging.getLogger("nsfw-bot")

# ---------- Helpers & Filters ----------
_token_split_re = re.compile(r"[^a-z0-9]+")

ILLEGAL_TAGS = [
    "underage", "minor", "child", "loli", "shota", "young", "agegap",
    "rape", "sexual violence", "bestiality", "zoophilia", "bestial",
    "scat", "fisting", "incest", "pedo", "pedophile", "creampie"
]
FILENAME_BLOCK_KEYWORDS = ["orgy", "creampie", "facial", "scat", "fisting", "bestiality"]

# For NSFW bot we still exclude explicitly illegal tags and explicit minors/bestiality/etc.
EXCLUDE_TAGS = [
    "loli", "shota", "child", "minor", "underage", "young", "schoolgirl", "age_gap",
    "pedo", "pedophile", "bestiality", "zoophilia", "rape", "sexual violence"
]

def _normalize_text(s: str) -> str:
    return "" if not s else re.sub(r'[\s\-_]+', ' ', s.lower())

def _tag_is_disallowed(t: str) -> bool:
    if not t:
        return True
    t = t.lower()
    if any(b in t for b in ILLEGAL_TAGS):
        return True
    if any(ex in t for ex in EXCLUDE_TAGS):
        return True
    return False

def contains_illegal_indicators(text: str) -> bool:
    if not text or not isinstance(text, str):
        return False
    normalized = _normalize_text(text)
    for bad in ILLEGAL_TAGS:
        if bad in normalized:
            return True
    return False

def filename_has_block_keyword(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    return any(kw in low for kw in FILENAME_BLOCK_KEYWORDS)

def _dedupe_preserve_order(lst):
    seen = set()
    out = []
    for x in lst:
        if not isinstance(x, str):
            continue
        nx = x.strip().lower()
        if not nx or nx in seen:
            continue
        seen.add(nx)
        out.append(nx)
    return out

def add_tag_to_gif_tags(tag: str, GIF_TAGS, data_save):
    if not tag or not isinstance(tag, str):
        return False
    t = tag.strip().lower()
    if len(t) < 3 or t in GIF_TAGS or _tag_is_disallowed(t):
        return False
    GIF_TAGS.append(t)
    data_save["gif_tags"] = _dedupe_preserve_order(data_save.get("gif_tags", []) + [t])
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(data_save, f, indent=2)
    except Exception:
        pass
    logger.debug(f"learned tag: {t}")
    return True

def extract_and_add_tags_from_meta(meta_text: str, GIF_TAGS, data_save):
    if not meta_text:
        return
    text = _normalize_text(meta_text)
    tokens = _token_split_re.split(text)
    for tok in tokens:
        tok = tok.strip()
        if not tok or tok.isdigit() or len(tok) < 3:
            continue
        add_tag_to_gif_tags(tok, GIF_TAGS, data_save)

# ---------- Persistence ----------
if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        json.dump({"provider_weights": {}, "sent_history": {}, "gif_tags": []}, f, indent=2)

with open(DATA_FILE, "r") as f:
    data = json.load(f)

data.setdefault("provider_weights", {})
data.setdefault("sent_history", {})
data.setdefault("gif_tags", [])

_seed_gif_tags = [
    "hentai", "ecchi", "porn", "sex", "oral", "anal", "cum", "cumshot", "orgasm", "sex_scene",
    "breasts", "big_breasts", "oppai", "huge_breasts", "milf", "mature", "thick", "thighs",
    "ass", "booty", "lingerie", "panties", "pantyhose", "stockings", "garter",
    "bikini", "swimsuit", "cleavage", "underboob", "sideboob",
    "blowjob", "paizuri", "oral_focus", "kiss", "cuddle", "teasing", "seductive",
    "fanservice", "bdsm", "bondage", "spanking", "wet",
    "waifu", "neko", "maid", "cosplay", "cheerleader", "idol",
    "threesome", "group", "bukkake", "nipples", "strapon", "double_penetration",
    "masturbation", "footjob", "handjob", "fingering", "cum_on_face", "facesitting",
    "pegging", "public", "group_sex", "yuri", "lesbian", "oral_anal", "double_oral"
]

persisted = _dedupe_preserve_order(data.get("gif_tags", []))
seed = _dedupe_preserve_order(_seed_gif_tags)
combined = seed + [t for t in persisted if t not in seed]
GIF_TAGS = [t for t in _dedupe_preserve_order(combined) if not _tag_is_disallowed(t)]
if not GIF_TAGS:
    GIF_TAGS = ["hentai"]

def save_data():
    try:
        data["gif_tags"] = GIF_TAGS
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"save failed: {e}")

@tasks.loop(seconds=AUTOSAVE_INTERVAL)
async def autosave_task():
    try:
        save_data()
    except Exception as e:
        logger.warning(f"Autosave failed: {e}")

# ---------- Provider terms & mapping (NSFW) ----------
PROVIDER_TERMS = {
    "waifu_pics": ["waifu", "neko", "blowjob"],  # trap excluded
    "waifu_im": ["hentai", "ero", "ecchi", "milf", "oral", "oppai", "cum", "anal"],
    "waifu_it": ["waifu", "hentai", "ero", "milf"],
    "nekos_best": ["neko", "waifu", "kiss", "hug"],
    "nekos_life": ["neko", "lewd", "ngif", "blowjob", "cum"],
    "nekos_moe": ["bikini", "breasts", "panties", "thighs", "stockings"],
    "nekoapi": ["waifu", "neko", "oppai", "bikini", "thighs"],
    "otakugifs": ["kiss", "hug", "cuddle", "dance"],
    "animegirls_online": ["waifu", "bikini", "maid", "cosplay"],
    "danbooru": ["hentai", "ecchi", "breasts", "thighs", "panties", "ass", "anal", "oral", "cum"],
    "gelbooru": ["hentai", "ecchi", "panties", "thighs", "ass", "bikini", "cleavage"],
    "konachan": ["bikini", "swimsuit", "cleavage", "thighs", "lingerie", "panties"],
    "rule34": ["hentai", "ecchi", "panties", "thighs", "ass", "big_breasts"],
    "nekos_api": ["waifu", "neko", "oppai", "bikini", "thighs"]
}

def map_tag_for_provider(provider: str, tag: str) -> str:
    t = (tag or "").lower().strip()
    pool = PROVIDER_TERMS.get(provider, [])
    if t:
        for p in pool:
            if p in t:
                return p
    if pool:
        return random.choice(pool)
    return t or "hentai"

# ---------- HTTP helpers ----------
async def _head_url(session, url, timeout=REQUEST_TIMEOUT):
    try:
        async with session.head(url, timeout=timeout, allow_redirects=True) as resp:
            return resp.status, dict(resp.headers)
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"HEAD failed for {url}: {e}")
        return None, {}

async def _download_bytes_with_limit(session, url, size_limit=HEAD_SIZE_LIMIT, timeout=REQUEST_TIMEOUT):
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"GET {url} returned {resp.status}")
                return None, None
            ctype = resp.content_type or ""
            total = 0
            chunks = []
            async for chunk in resp.content.iter_chunked(1024):
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > size_limit:
                    if DEBUG_FETCH:
                        logger.debug(f"download exceeded limit {size_limit} for {url}")
                    return None, ctype
            return b"".join(chunks), ctype
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"GET exception for {url}: {e}")
        return None, None

# ---------- Provider fetchers (NSFW) ----------
async def fetch_from_waifu_pics(session, positive):
    try:
        category = map_tag_for_provider("waifu_pics", positive)
        url = f"https://api.waifu.pics/nsfw/{quote_plus(category)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"waifu_pics nsfw {category} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"waifu_pics_{category}", payload
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_waifu_pics error: {e}")
        return None, None, None

async def fetch_from_waifu_im(session, positive):
    try:
        q = map_tag_for_provider("waifu_im", positive)
        base = "https://api.waifu.im/search"
        params = {"included_tags": q, "is_nsfw": "true", "limit": 8}
        headers = {}
        if WAIFUIM_API_KEY:
            headers["Authorization"] = f"Bearer {WAIFUIM_API_KEY}"
        async with session.get(base, params=params, headers=headers or None, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"waifu.im nsfw search -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            images = payload.get("images") or payload.get("data") or []
            if not images:
                return None, None, None
            img = random.choice(images)
            gif_url = img.get("url") or img.get("image") or img.get("src")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(img) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(str(img.get("tags", "")), GIF_TAGS, data)
            return gif_url, f"waifu_im_{q}", img
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_waifu_im error: {e}")
        return None, None, None

async def fetch_from_waifu_it(session, positive):
    try:
        if not WAIFUIT_API_KEY:
            if DEBUG_FETCH:
                logger.debug("waifu.it skipped: key missing")
            return None, None, None
        q = map_tag_for_provider("waifu_it", positive)
        endpoint = f"https://waifu.it/api/v4/{quote_plus(q)}"
        headers = {"Authorization": WAIFUIT_API_KEY}
        async with session.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"waifu.it {endpoint} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image") or (payload.get("data") and payload["data"].get("url"))
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"waifu_it_{q}", payload
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_waifu_it error: {e}")
        return None, None, None

async def fetch_from_nekos_best(session, positive):
    try:
        q = map_tag_for_provider("nekos_best", positive)
        url = f"https://nekos.best/api/v2/{quote_plus(q)}?amount=1"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"nekos.best {q} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            results = payload.get("results") or []
            if not results:
                return None, None, None
            r = results[0]
            gif_url = r.get("url") or r.get("file") or r.get("image")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(r) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(r), GIF_TAGS, data)
            return gif_url, f"nekos_best_{q}", r
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_nekos_best error: {e}")
        return None, None, None

async def fetch_from_nekos_life(session, positive):
    try:
        q = map_tag_for_provider("nekos_life", positive)
        url = f"https://nekos.life/api/v2/img/{quote_plus(q)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"nekos.life {q} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image") or payload.get("result")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"nekos_life_{q}", payload
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_nekos_life error: {e}")
        return None, None, None

async def fetch_from_nekos_moe(session, positive):
    try:
        q = map_tag_for_provider("nekos_moe", positive)
        url = f"https://nekos.moe/api/v3/gif/random?tag={quote_plus(q)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"nekos.moe -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            images = payload.get("images") or payload.get("data") or []
            if not images:
                return None, None, None
            item = random.choice(images)
            gif_url = item.get("file") or item.get("url") or item.get("original") or item.get("image")
            if not gif_url and item.get("id"):
                gif_url = f"https://nekos.moe/image/{item['id']}.gif"
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(item) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
            return gif_url, f"nekos_moe_{q}", item
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_nekos_moe error: {e}")
        return None, None, None

async def fetch_from_nekos_api(session, positive):
    try:
        q = map_tag_for_provider("nekos_api", positive)
        url = "https://api.nekosapi.com/v4/images/random"
        params = {}
        if q:
            params["categories"] = q
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"nekos_api -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            images = payload.get("data") or payload.get("images") or []
            if not images:
                if isinstance(payload, dict) and payload.get("url"):
                    gif_url = payload.get("url")
                    if filename_has_block_keyword(gif_url):
                        return None, None, None
                    if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")):
                        return None, None, None
                    return gif_url, f"nekos_api_{q}", payload
                return None, None, None
            item = images[0] if isinstance(images, list) else images
            gif_url = item.get("url") or item.get("image_url") or item.get("file")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(item) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
            return gif_url, f"nekos_api_{q}", item
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_nekos_api error: {e}")
        return None, None, None

async def fetch_from_danbooru(session, positive):
    try:
        q = map_tag_for_provider("danbooru", positive)
        tags = f"{q} rating:explicit"
        url = "https://danbooru.donmai.us/posts.json"
        params = {"tags": tags, "limit": 50}
        auth = None
        if DANBOORU_USER and DANBOORU_API_KEY:
            auth = aiohttp.BasicAuth(DANBOORU_USER, DANBOORU_API_KEY)
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT, auth=auth) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"danbooru -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            if not payload:
                return None, None, None
            random.shuffle(payload)
            for item in payload:
                tags_text = item.get("tag_string", "") or item.get("tag_string_general", "")
                if _tag_is_disallowed(tags_text):
                    continue
                gif_url = item.get("file_url") or item.get("large_file_url") or item.get("source")
                if not gif_url or filename_has_block_keyword(gif_url):
                    continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")):
                    continue
                extract_and_add_tags_from_meta(tags_text, GIF_TAGS, data)
                return gif_url, f"danbooru_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_danbooru error: {e}")
        return None, None, None

async def fetch_from_gelbooru(session, positive):
    try:
        q = map_tag_for_provider("gelbooru", positive)
        tags = q
        url = "https://gelbooru.com/index.php"
        params = {"page": "dapi", "s": "post", "q": "index", "json": 1, "limit": 50, "tags": tags}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"gelbooru -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            posts = payload if isinstance(payload, list) else (payload.get("post") or payload.get("posts") or [])
            if not posts:
                return None, None, None
            random.shuffle(posts)
            for item in posts:
                if _tag_is_disallowed(json.dumps(item)):
                    continue
                gif_url = item.get("file_url") or item.get("image") or item.get("preview_url")
                if not gif_url or filename_has_block_keyword(gif_url):
                    continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")):
                    continue
                extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
                return gif_url, f"gelbooru_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_gelbooru error: {e}")
        return None, None, None

async def fetch_from_konachan(session, positive):
    try:
        q = map_tag_for_provider("konachan", positive)
        tags = f"{q} rating:explicit"
        url = "https://konachan.com/post.json"
        params = {"tags": tags, "limit": 50}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"konachan -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            if not payload:
                return None, None, None
            random.shuffle(payload)
            for item in payload:
                if _tag_is_disallowed(json.dumps(item)):
                    continue
                gif_url = item.get("file_url") or item.get("jpeg_url") or item.get("sample_url")
                if not gif_url or filename_has_block_keyword(gif_url):
                    continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")):
                    continue
                extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
                return gif_url, f"konachan_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_konachan error: {e}")
        return None, None, None

async def fetch_from_rule34(session, positive):
    try:
        q = map_tag_for_provider("rule34", positive)
        tags = q
        url = "https://api.rule34.xxx/index.php"
        params = {"page": "dapi", "s": "post", "q": "index", "json": 1, "limit": 50, "tags": tags}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH:
                    logger.debug(f"rule34 -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            if not payload:
                return None, None, None
            random.shuffle(payload)
            for item in payload:
                if _tag_is_disallowed(json.dumps(item)):
                    continue
                gif_url = item.get("file_url") or item.get("image") or item.get("sample_url")
                if not gif_url or filename_has_block_keyword(gif_url):
                    continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")):
                    continue
                extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
                return gif_url, f"rule34_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"fetch_from_rule34 error: {e}")
        return None, None, None

PROVIDER_FETCHERS = {
    "waifu_pics": fetch_from_waifu_pics,
    "waifu_im": fetch_from_waifu_im,
    "waifu_it": fetch_from_waifu_it,
    "nekos_best": fetch_from_nekos_best,
    "nekos_life": fetch_from_nekos_life,
    "nekos_moe": fetch_from_nekos_moe,
    "nekos_api": fetch_from_nekos_api,
    "otakugifs": fetch_from_otakugifs,
    "animegirls_online": fetch_from_animegirls_online,
    "danbooru": fetch_from_danbooru,
    "gelbooru": fetch_from_gelbooru,
    "konachan": fetch_from_konachan,
    "rule34": fetch_from_rule34
}

_provider_cycle_deque = deque()
_last_cycle_refresh = None

def build_provider_pool():
    providers = [p for p in PROVIDER_FETCHERS.keys()]
    available = []
    for p in providers:
        w = int(data.get("provider_weights", {}).get(p, 1) or 1)
        if w <= 0:
            continue
        available.append(p)
    if not available:
        return []
    if TRUE_RANDOM:
        random.shuffle(available)
        return available
    global _provider_cycle_deque, _last_cycle_refresh
    now = datetime.utcnow()
    if not _provider_cycle_deque or (_last_cycle_refresh and (now - _last_cycle_refresh) > timedelta(minutes=15)):
        random.shuffle(available)
        _provider_cycle_deque = deque(available)
        _last_cycle_refresh = now
        if DEBUG_FETCH:
            logger.debug(f"Provider cycle (refreshed): {_provider_cycle_deque}")
    else:
        current = set(_provider_cycle_deque)
        if set(available) != current:
            random.shuffle(available)
            _provider_cycle_deque = deque(available)
            _last_cycle_refresh = now
            if DEBUG_FETCH:
                logger.debug(f"Provider cycle (rebuild): {_provider_cycle_deque}")
    return list(_provider_cycle_deque)

# ---------- Fetching / sending ----------
async def attempt_get_media_bytes(session, gif_url):
    if not gif_url:
        return None, None, "no-url"
    if contains_illegal_indicators(gif_url):
        return None, None, "illegal-indicator-in-url"
    status, headers = await _head_url(session, gif_url)
    if status is None:
        b, ctype = await _download_bytes_with_limit(session, gif_url, size_limit=HEAD_SIZE_LIMIT)
        if b:
            return b, ctype, "downloaded-after-head-failed"
        return None, ctype, "head-failed-get-failed"
    if status not in (200, 301, 302):
        b, ctype = await _download_bytes_with_limit(session, gif_url, size_limit=HEAD_SIZE_LIMIT)
        if b:
            return b, ctype, f"get-after-head-{status}"
        return None, ctype, f"head-{status}-get-failed"
    cl = headers.get("Content-Length") or headers.get("content-length")
    ctype = headers.get("Content-Type") or headers.get("content-type") or ""
    if cl:
        try:
            clv = int(cl)
            if clv > HEAD_SIZE_LIMIT:
                return None, ctype, f"too-large-head-{clv}"
            b, ctype2 = await _download_bytes_with_limit(session, gif_url, size_limit=HEAD_SIZE_LIMIT)
            if b:
                return b, ctype2 or ctype, "downloaded-with-head-size"
            return None, ctype2 or ctype, "head-said-small-but-get-failed"
        except Exception:
            b, ctype2 = await _download_bytes_with_limit(session, gif_url, size_limit=HEAD_SIZE_LIMIT)
            if b:
                return b, ctype2 or ctype, "downloaded-with-head-parse-except"
            return None, ctype2 or ctype, "head-parse-get-failed"
    else:
        b, ctype2 = await _download_bytes_with_limit(session, gif_url, size_limit=HEAD_SIZE_LIMIT)
        if b:
            return b, ctype2 or ctype, "downloaded-unknown-size"
        return None, ctype2 or ctype, "unknown-size-get-failed-or-too-large"

async def fetch_gif(user_id):
    user_key = str(user_id)
    sent_hashes = set(data.get("sent_history", {}).get(user_key, []))
    providers = build_provider_pool()
    if not providers:
        if DEBUG_FETCH:
            logger.debug("No providers available.")
        return None, None, None, None
    async with aiohttp.ClientSession() as session:
        attempt = 0
        while attempt < FETCH_ATTEMPTS:
            attempt += 1
            if TRUE_RANDOM:
                provider = random.choice(providers)
            else:
                global _provider_cycle_deque, _last_cycle_refresh
                if not _provider_cycle_deque:
                    _provider_cycle_deque = deque(build_provider_pool())
                if not _provider_cycle_deque:
                    return None, None, None, None
                provider = _provider_cycle_deque.popleft()
                _provider_cycle_deque.append(provider)
            positive = random.choice(GIF_TAGS)
            if DEBUG_FETCH:
                logger.debug(f"[fetch_gif] attempt {attempt} provider={provider} positive='{positive}'")
            fetcher = PROVIDER_FETCHERS.get(provider)
            if not fetcher:
                continue
            try:
                gif_url, name_hint, meta = await fetcher(session, positive)
            except Exception as e:
                if DEBUG_FETCH:
                    logger.debug(f"fetcher exception for {provider}: {e}")
                continue
            if not gif_url:
                if DEBUG_FETCH:
                    logger.debug(f"{provider} returned no url.")
                continue
            if filename_has_block_keyword(gif_url):
                continue
            if contains_illegal_indicators((gif_url or "") + " " + (str(meta) or "")):
                continue
            if _tag_is_disallowed(str(meta or "")):
                continue
            gif_hash = hashlib.sha1((gif_url or "").encode()).hexdigest()
            if gif_hash in sent_hashes:
                if DEBUG_FETCH:
                    logger.debug("already sent to user; skipping")
                continue
            b, ctype, reason = await attempt_get_media_bytes(session, gif_url)
            if DEBUG_FETCH:
                logger.debug(f"attempt_get_media_bytes -> provider={provider} url={gif_url} reason={reason} bytes_ok={bool(b)} ctype={ctype}")
            # mark as used (persist)
            sent_hashes.add(gif_hash)
            sent_list = data.get("sent_history", {}).get(user_key, [])
            sent_list.append(gif_hash)
            if len(sent_list) > MAX_USED_GIFS_PER_USER:
                del sent_list[:len(sent_list) - MAX_USED_GIFS_PER_USER]
            data["sent_history"][user_key] = sent_list
            try:
                with open(DATA_FILE, "w") as f:
                    json.dump(data, f, indent=2)
            except Exception:
                pass
            ext = ""
            try:
                parsed = urlparse(gif_url)
                ext = os.path.splitext(parsed.path)[1] or ".gif"
                if len(ext) > 6:
                    ext = ".gif"
            except Exception:
                ext = ".gif"
            name = f"{provider}_{hashlib.sha1(gif_url.encode()).hexdigest()[:10]}{ext}"
            return b, name, gif_url, ctype
        if DEBUG_FETCH:
            logger.debug("fetch_gif exhausted attempts.")
        return None, None, None, None

def try_compress_bytes(b, ctype, max_size):
    if not b or not Image:
        return None
    try:
        buf = io.BytesIO(b)
        img = Image.open(buf)
        fmt = img.format or "GIF"
        if fmt.upper() in ("GIF", "WEBP"):
            frames = [f.copy().convert("RGBA") for f in ImageSequence.Iterator(img)]
            w, h = frames[0].size
            for step in range(1, 13):
                scale = 0.95 ** step
                new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
                out = io.BytesIO()
                resized = [fr.resize(new_size, Image.LANCZOS) for fr in frames]
                try:
                    resized[0].save(out, format="GIF", save_all=True, append_images=resized[1:], optimize=True, loop=0)
                except Exception:
                    try:
                        resized[0].save(out, format="GIF", save_all=True, append_images=resized[1:], loop=0)
                    except Exception:
                        out = None
                if out and out.getbuffer().nbytes <= max_size:
                    return out.getvalue()
            return None
        else:
            w, h = img.size
            for step in range(1, 13):
                scale = 0.95 ** step
                new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
                out = io.BytesIO()
                img2 = img.resize(new_size, Image.LANCZOS)
                if fmt.upper() in ("JPEG", "JPG"):
                    img2.save(out, format="JPEG", quality=85, optimize=True)
                else:
                    img2.save(out, format="PNG", optimize=True)
                if out.getbuffer().nbytes <= max_size:
                    return out.getvalue()
            return None
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"compression failed: {e}")
        return None

def make_embed(title, desc, member, kind="nsfw"):
    color = discord.Color.dark_red() if kind == "nsfw" else discord.Color.dark_gray()
    embed = discord.Embed(title=title, description=desc, color=color, timestamp=datetime.utcnow())
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    embed.set_footer(text=f"{member.display_name} • {member.id}")
    return embed

async def record_sent_for_user(member_id, gif_url):
    try:
        if not gif_url:
            return
        user_key = str(member_id)
        gif_hash = hashlib.sha1(gif_url.encode()).hexdigest()
        sent = data.setdefault("sent_history", {}).setdefault(user_key, [])
        if gif_hash in sent:
            return
        sent.append(gif_hash)
        if len(sent) > MAX_USED_GIFS_PER_USER:
            del sent[:len(sent) - MAX_USED_GIFS_PER_USER]
        data["sent_history"][user_key] = sent
        try:
            with open(DATA_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass
    except Exception:
        pass

async def send_embed_with_media(channel, member, embed, gif_bytes, gif_name, gif_url, ctype=None):
    max_upload = DISCORD_MAX_UPLOAD
    sent_success = False
    try:
        if gif_bytes and len(gif_bytes) <= max_upload:
            try:
                file_server = discord.File(io.BytesIO(gif_bytes), filename=gif_name)
                embed.set_image(url=f"attachment://{gif_name}")
                if channel:
                    await channel.send(embed=embed, file=file_server)
                sent_success = True
            except Exception:
                if channel:
                    if gif_url and gif_url not in (embed.description or ""):
                        embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
                    await channel.send(embed=embed)
                    sent_success = True
            try:
                dm_file = discord.File(io.BytesIO(gif_bytes), filename=gif_name)
                await member.send(embed=embed, file=dm_file)
            except Exception:
                try:
                    dm_embed = make_embed(embed.title or "Media", embed.description or "", member)
                    if gif_url and gif_url not in (dm_embed.description or ""):
                        dm_embed.description = (dm_embed.description or "") + f"\n\n[View media here]({gif_url})"
                    await member.send(dm_embed)
                except Exception:
                    pass
        else:
            if gif_bytes:
                compressed = try_compress_bytes(gif_bytes, ctype, max_upload)
                if compressed and len(compressed) <= max_upload:
                    try:
                        file_server = discord.File(io.BytesIO(compressed), filename=gif_name)
                        embed.set_image(url=f"attachment://{gif_name}")
                        if channel:
                            await channel.send(embed=embed, file=file_server)
                        sent_success = True
                    except Exception:
                        if channel:
                            if gif_url and gif_url not in (embed.description or ""):
                                embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
                            await channel.send(embed=embed)
                            sent_success = True
                    try:
                        dm_file = discord.File(io.BytesIO(compressed), filename=gif_name)
                        await member.send(embed=embed, file=dm_file)
                    except Exception:
                        try:
                            dm_embed = make_embed(embed.title or "Media", embed.description or "", member)
                            if gif_url and gif_url not in (dm_embed.description or ""):
                                dm_embed.description = (dm_embed.description or "") + f"\n\n[View media here]({gif_url})"
                            await member.send(dm_embed)
                        except Exception:
                            pass
                    if sent_success:
                        await record_sent_for_user(member.id, gif_url)
                    return
            if gif_url:
                if gif_url not in (embed.description or ""):
                    embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
            if channel:
                await channel.send(embed=embed)
                sent_success = True
            try:
                dm_embed = make_embed(embed.title or "Media", embed.description or "", member)
                if gif_url and gif_url not in (dm_embed.description or ""):
                    dm_embed.description = (dm_embed.description or "") + f"\n\n[View media here]({gif_url})"
                await member.send(dm_embed)
            except Exception:
                pass
    except Exception:
        try:
            if channel:
                await channel.send(embed=embed)
                sent_success = True
            await member.send(embed=embed)
        except Exception:
            pass
    if sent_success and gif_url:
        await record_sent_for_user(member.id, gif_url)

# ---------- Bot setup ----------
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

@bot.event
async def on_ready():
    try:
        autosave_task.start()
    except Exception:
        pass
    logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.command(name="nsfw", aliases=["nude","hentai"])
@commands.cooldown(1, 3, commands.BucketType.user)
async def nsfw(ctx):
    """Send an NSFW image/gif. Only in NSFW channels or DMs."""
    # enforce channel NSFW unless DM
    if ctx.guild and not getattr(ctx.channel, "is_nsfw", lambda: False)():
        await ctx.send("This command can only be used in NSFW channels or in DMs.")
        return
    await ctx.trigger_typing()
    b, name, url, ctype = await fetch_gif(ctx.author.id)
    embed = make_embed("NSFW content", "", ctx.author, kind="nsfw")
    if b:
        await send_embed_with_media(ctx.channel, ctx.author, embed, b, name, url, ctype)
    elif url:
        if url not in (embed.description or ""):
            embed.description = (embed.description or "") + f"\n\n[View media here]({url})"
        await ctx.send(embed=embed)
        await record_sent_for_user(ctx.author.id, url)
    else:
        await ctx.send("Couldn't find NSFW media right now. Try again later.")

@bot.command(name="ntags")
async def ntags(ctx):
    await ctx.send("Available NSFW seed tags: " + ", ".join(GIF_TAGS[:60]))

if __name__ == "__main__":
    if not TOKEN:
        logger.error("TOKEN not set; exiting.")
    else:
        bot.run(TOKEN)
