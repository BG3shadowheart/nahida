# spiciest-final.py
# Final single-file bot (cleaned: removed default headers, fixed provider terms)

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
import asyncio
import discord
from discord.ext import commands, tasks
from collections import deque

# --------- ENV / CONFIG ----------
TOKEN = os.getenv("TOKEN")
WAIFUIM_API_KEY = os.getenv("WAIFUIM_API_KEY", "")
WAIFUIT_API_KEY = os.getenv("WAIFUIT_API_KEY", "")
DANBOORU_USER = os.getenv("DANBOORU_USER", "")
DANBOORU_API_KEY = os.getenv("DANBOORU_API_KEY", "")
GELBOORU_API_KEY = os.getenv("GELBOORU_API_KEY", "")

_DEBUG_RAW = os.getenv("DEBUG_FETCH", "")
DEBUG_FETCH = str(_DEBUG_RAW).strip().lower() in ("1", "true", "yes", "on")
TRUE_RANDOM = str(os.getenv("TRUE_RANDOM", "")).strip().lower() in ("1", "true", "yes")

# Single voice channel to monitor (user requested)
VC_IDS = [1409170559337762980]
# Text channel where join/leave embeds are posted (set to your desired text channel)
VC_CHANNEL_ID = 1371916812903780573

# Persistence / limits
DATA_FILE = "data.json"
AUTOSAVE_INTERVAL = 30  # seconds
MAX_USED_GIFS_PER_USER = 1000
FETCH_ATTEMPTS = 40
REQUEST_TIMEOUT = 14  # seconds

DISCORD_MAX_UPLOAD = int(os.getenv("DISCORD_MAX_UPLOAD", str(8 * 1024 * 1024)))
HEAD_SIZE_LIMIT = DISCORD_MAX_UPLOAD

logging.basicConfig(level=logging.DEBUG if DEBUG_FETCH else logging.INFO)
logger = logging.getLogger("spiciest-final")

# --------- TAGS & BLOCKLISTS ----------
_seed_gif_tags = [
    "hentai","ecchi","porn","sex","oral","anal","cum","cumshot","orgasm","sex_scene",
    "breasts","big breasts","big_boobs","oppai","huge breasts","milf","mommy","mature",
    "thick","thicc","thick thighs","thighs","thigh_focus","jiggle","bounce",
    "ass","booty","big ass","curvy","round booty",
    "lingerie","panties","pantyhose","stockings","garter",
    "bikini","swimsuit","cleavage","underboob","sideboob",
    "blowjob","paizuri","oral_focus","kiss","cuddle","teasing","seductive",
    "fanservice","bdsm","bondage","spanking","tanlines","wet",
    "waifu","neko","maid","cosplay","school_uniform","cheerleader","idol"
]

# Illegal tags (always blocked)
ILLEGAL_TAGS = [
    "underage","minor","child","loli","shota","young","agegap","rape","sexual violence",
    "bestiality","zoophilia","bestial","scat","fisting","incest","pedo","pedophile","creampie"
]

# Filename keywords that indicate deep illegal or strongly unwanted content
FILENAME_BLOCK_KEYWORDS = ["orgy","creampie","facial","scat","fisting","bestiality"]

# Excluded categories per user's request (many variants included)
EXCLUDE_TAGS = [
    # minors & similar
    "loli","shota","child","minor","underage","young","schoolgirl","age_gap",
    # user-specified removals
    "futa","futanari","futanaris","futanary","futan","futanari_",
    "shemale","shemales","she-male","she_male","shemale_",
    "dickgirl","dick_girl","d-girl","dick-girl","dickgirl_",
    "femboy","femboys","femb0y",
    "trap","traps",
    # male-male / yaoi
    "male","males","man","men","boy","boys","yaoi","gay","mm","male-male","male_male","m/m","yaoi_"
]

# --------- UTILITIES ----------
_token_split_re = re.compile(r"[^a-z0-9]+")

def _normalize_text(s: str) -> str:
    return "" if not s else re.sub(r'[\s\-_]+', ' ', s.lower())

def _tag_is_disallowed(t: str) -> bool:
    if not t:
        return True
    t = t.lower()
    if any(ex in t for ex in EXCLUDE_TAGS):
        return True
    if any(b in t for b in ILLEGAL_TAGS):
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
    # persist
    data_save["gif_tags"] = _dedupe_preserve_order(data_save.get("gif_tags", []) + [t])
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(data_save, f, indent=2)
    except Exception:
        pass
    logger.debug(f"learned tag: {t}")
    return True

def extract_and_add_tags_from_meta(meta_text: str, GIF_TAGS, data_save):
    if not meta_text: return
    text = _normalize_text(meta_text)
    tokens = _token_split_re.split(text)
    for tok in tokens:
        tok = tok.strip()
        if not tok or tok.isdigit() or len(tok) < 3:
            continue
        add_tag_to_gif_tags(tok, GIF_TAGS, data_save)

# --------- PERSISTENCE ----------
if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        json.dump({
            "join_counts": {},
            "used_gifs": {},
            "provider_weights": {},
            "sent_history": {},
            "gif_tags": []
        }, f, indent=2)

with open(DATA_FILE, "r") as f:
    data = json.load(f)

data.setdefault("join_counts", {})
data.setdefault("used_gifs", {})
data.setdefault("provider_weights", {})
data.setdefault("sent_history", {})
data.setdefault("gif_tags", [])

persisted = _dedupe_preserve_order(data.get("gif_tags", []))
seed = _dedupe_preserve_order(_seed_gif_tags)
combined = seed + [t for t in persisted if t not in seed]
GIF_TAGS = [t for t in _dedupe_preserve_order(combined) if not _tag_is_disallowed(t)]
if not GIF_TAGS:
    GIF_TAGS = ["waifu"]

def save_data():
    try:
        data["gif_tags"] = GIF_TAGS
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save data: {e}")

@tasks.loop(seconds=AUTOSAVE_INTERVAL)
async def autosave_task():
    try:
        save_data()
    except Exception as e:
        logger.warning(f"Autosave failed: {e}")

# --------- HTTP Helpers (HEAD + download with size limit) ----------
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

# --------- Provider term pools (fallbacks) ----------
PROVIDER_TERMS = {
    "waifu_pics": ["waifu", "neko", "blowjob", "oral", "ecchi"],
    "waifu_im": ["hentai", "ero", "ecchi", "milf", "oral", "oppai"],
    "waifu_it": ["waifu"],
    "nekos_best": ["neko", "waifu", "kiss", "hug"],
    "nekos_life": ["neko", "lewd", "ngif"],
    "nekos_moe": ["bikini", "breasts", "panties", "thighs", "stockings"],
    "nekoapi": ["waifu", "neko", "oppai", "bikini", "panties"],
    "otakugifs": ["kiss", "hug", "cuddle", "dance"],
    "animegirls_online": ["waifu", "bikini", "maid", "cosplay"],
    "danbooru": ["hentai","ecchi","breasts","thighs","panties","ass","anal","oral","cum"],
    "gelbooru": ["hentai","ecchi","panties","thighs","ass","bikini","cleavage"],
    "konachan": ["bikini","swimsuit","cleavage","thighs","lingerie","panties"],
    "rule34": ["hentai","ecchi","panties","thighs","ass","big_breasts"],
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
    return t or "waifu"

# --------- Provider fetchers ----------
# All fetchers follow same return: (gif_url, name_hint, meta) or (None, None, None)

async def fetch_from_waifu_pics(session, positive):
    try:
        category = map_tag_for_provider("waifu_pics", positive)
        url = f"https://api.waifu.pics/nsfw/{quote_plus(category)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"waifu_pics nsfw {category} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"waifu_pics_{category}", payload
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_waifu_pics error: {e}")
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
                if DEBUG_FETCH: logger.debug(f"waifu.im nsfw search -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            images = payload.get("images") or payload.get("data") or []
            if not images: return None, None, None
            img = random.choice(images)
            gif_url = img.get("url") or img.get("image") or img.get("src")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(img) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(str(img.get("tags", "")), GIF_TAGS, data)
            return gif_url, f"waifu_im_{q}", img
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_waifu_im error: {e}")
        return None, None, None

async def fetch_from_waifu_it(session, positive):
    try:
        if not WAIFUIT_API_KEY:
            if DEBUG_FETCH: logger.debug("waifu.it skipped: key missing")
            return None, None, None
        q = map_tag_for_provider("waifu_it", positive)
        endpoint = f"https://waifu.it/api/v4/{quote_plus(q)}"
        headers = {}
        headers["Authorization"] = WAIFUIT_API_KEY
        async with session.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"waifu.it {endpoint} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image") or (payload.get("data") and payload["data"].get("url"))
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"waifu_it_{q}", payload
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_waifu_it error: {e}")
        return None, None, None

async def fetch_from_nekos_best(session, positive):
    try:
        q = map_tag_for_provider("nekos_best", positive)
        url = f"https://nekos.best/api/v2/{quote_plus(q)}?amount=1"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"nekos.best {q} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            results = payload.get("results") or []
            if not results: return None, None, None
            r = results[0]
            gif_url = r.get("url") or r.get("file") or r.get("image")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(r) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(r), GIF_TAGS, data)
            return gif_url, f"nekos_best_{q}", r
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_nekos_best error: {e}")
        return None, None, None

async def fetch_from_nekos_life(session, positive):
    try:
        q = map_tag_for_provider("nekos_life", positive)
        url = f"https://nekos.life/api/v2/img/{quote_plus(q)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"nekos.life {q} -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image") or payload.get("result")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"nekos_life_{q}", payload
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_nekos_life error: {e}")
        return None, None, None

async def fetch_from_nekos_moe(session, positive):
    try:
        q = map_tag_for_provider("nekos_moe", positive)
        url = f"https://nekos.moe/api/v3/gif/random?tag={quote_plus(q)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"nekos.moe -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            images = payload.get("images") or payload.get("data") or []
            if not images: return None, None, None
            item = random.choice(images)
            gif_url = item.get("file") or item.get("url") or item.get("original") or item.get("image")
            if not gif_url and item.get("id"):
                gif_url = f"https://nekos.moe/image/{item['id']}.gif"
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
            return gif_url, f"nekos_moe_{q}", item
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_nekos_moe error: {e}")
        return None, None, None

async def fetch_from_otakugifs(session, positive):
    try:
        q = map_tag_for_provider("otakugifs", positive)
        valid_reactions = ["kiss","hug","slap","punch","wink","dance","cuddle","poke"]
        reaction = "waifu"
        for v in valid_reactions:
            if v in q:
                reaction = v
                break
        url = f"https://otakugifs.xyz/api/gif?reaction={quote_plus(reaction)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"otakugifs -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("gif") or payload.get("file")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"otakugifs_{reaction}", payload
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_otakugifs error: {e}")
        return None, None, None

async def fetch_from_animegirls_online(session, positive):
    try:
        q = map_tag_for_provider("animegirls_online", positive)
        url = f"https://animegirls.online/api/random?tag={quote_plus(q)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"animegirls_online -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"animegirls_online_{q}", payload
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_animegirls_online error: {e}")
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
                if DEBUG_FETCH: logger.debug(f"nekos_api -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            images = payload.get("data") or payload.get("images") or []
            if not images:
                if isinstance(payload, dict) and payload.get("url"):
                    gif_url = payload.get("url")
                    if filename_has_block_keyword(gif_url): return None, None, None
                    if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
                    return gif_url, f"nekos_api_{q}", payload
                return None, None, None
            item = images[0] if isinstance(images, list) else images
            gif_url = item.get("url") or item.get("image_url") or item.get("file")
            if not gif_url: return None, None, None
            if filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
            return gif_url, f"nekos_api_{q}", item
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_nekos_api error: {e}")
        return None, None, None

# Booru & rule34
async def fetch_from_danbooru(session, positive):
    try:
        q = map_tag_for_provider("danbooru", positive)
        tags = f"{q} rating:explicit"
        url = "https://danbooru.donmai.us/posts.json"
        params = {"tags": tags, "limit": 50}
        headers = {}
        auth = None
        if DANBOORU_USER and DANBOORU_API_KEY:
            auth = aiohttp.BasicAuth(DANBOORU_USER, DANBOORU_API_KEY)
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers or None, auth=auth) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"danbooru -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            if not payload:
                return None, None, None
            random.shuffle(payload)
            for item in payload:
                tags_text = item.get("tag_string", "") or item.get("tag_string_general", "")
                if _tag_is_disallowed(tags_text): continue
                gif_url = item.get("file_url") or item.get("large_file_url") or item.get("source")
                if not gif_url: continue
                if filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                extract_and_add_tags_from_meta(tags_text, GIF_TAGS, data)
                return gif_url, f"danbooru_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_danbooru error: {e}")
        return None, None, None

async def fetch_from_gelbooru(session, positive):
    try:
        q = map_tag_for_provider("gelbooru", positive)
        tags = q
        url = "https://gelbooru.com/index.php"
        params = {"page": "dapi", "s": "post", "q": "index", "json": 1, "limit": 50, "tags": tags}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"gelbooru -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            posts = payload if isinstance(payload, list) else (payload.get("post") or payload.get("posts") or [])
            if not posts:
                return None, None, None
            random.shuffle(posts)
            for item in posts:
                if _tag_is_disallowed(json.dumps(item)): continue
                gif_url = item.get("file_url") or item.get("image") or item.get("preview_url")
                if not gif_url: continue
                if filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
                return gif_url, f"gelbooru_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_gelbooru error: {e}")
        return None, None, None

async def fetch_from_konachan(session, positive):
    try:
        q = map_tag_for_provider("konachan", positive)
        tags = f"{q} rating:explicit"
        url = "https://konachan.com/post.json"
        params = {"tags": tags, "limit": 50}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"konachan -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            if not payload:
                return None, None, None
            random.shuffle(payload)
            for item in payload:
                if _tag_is_disallowed(json.dumps(item)): continue
                gif_url = item.get("file_url") or item.get("jpeg_url") or item.get("sample_url")
                if not gif_url: continue
                if filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
                return gif_url, f"konachan_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_konachan error: {e}")
        return None, None, None

async def fetch_from_rule34(session, positive):
    try:
        q = map_tag_for_provider("rule34", positive)
        tags = q
        url = "https://api.rule34.xxx/index.php"
        params = {"page": "dapi", "s": "post", "q": "index", "json": 1, "limit": 50, "tags": tags}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"rule34 -> {resp.status}")
                return None, None, None
            payload = await resp.json()
            if not payload: return None, None, None
            random.shuffle(payload)
            for item in payload:
                if _tag_is_disallowed(json.dumps(item)): continue
                gif_url = item.get("file_url") or item.get("image") or item.get("sample_url")
                if not gif_url: continue
                if filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                extract_and_add_tags_from_meta(json.dumps(item), GIF_TAGS, data)
                return gif_url, f"rule34_{q}", item
            return None, None, None
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"fetch_from_rule34 error: {e}")
        return None, None, None

# Provider registry (equal priority)
PROVIDER_FETCHERS = {
    "waifu_pics": fetch_from_waifu_pics,
    "waifu_im": fetch_from_waifu_im,
    "waifu_it": fetch_from_waifu_it,
    "nekos_best": fetch_from_nekos_best,
    "nekos_life": fetch_from_nekos_life,
    "nekos_moe": fetch_from_nekos_moe,
    "nekoapi": fetch_from_nekos_moe,
    "otakugifs": fetch_from_otakugifs,
    "animegirls_online": fetch_from_animegirls_online,
    "nekos_api": fetch_from_nekos_api,
    "danbooru": fetch_from_danbooru,
    "gelbooru": fetch_from_gelbooru,
    "konachan": fetch_from_konachan,
    "rule34": fetch_from_rule34
}

# Provider pool builder (equal priority; providers disabled by setting provider_weights[p]=0 in data.json)
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
        if DEBUG_FETCH: logger.debug(f"Provider cycle (refreshed): {_provider_cycle_deque}")
    else:
        current = set(_provider_cycle_deque)
        if set(available) != current:
            random.shuffle(available)
            _provider_cycle_deque = deque(available)
            _last_cycle_refresh = now
            if DEBUG_FETCH: logger.debug(f"Provider cycle (rebuild): {_provider_cycle_deque}")
    return list(_provider_cycle_deque)

# Attempt to fetch media bytes (using HEAD to check size then GET)
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
    if status not in (200,301,302):
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

# Fetch gif: round-robin across providers, uniform tag selection
async def fetch_gif(user_id):
    user_key = str(user_id)
    sent = data["sent_history"].setdefault(user_key, [])
    providers = build_provider_pool()
    if not providers:
        if DEBUG_FETCH: logger.debug("No providers available.")
        return None, None, None

    async with aiohttp.ClientSession() as session:
        tried_providers = set()
        attempt = 0
        while attempt < FETCH_ATTEMPTS:
            attempt += 1
            # choose provider
            if TRUE_RANDOM:
                provider = random.choice(providers)
            else:
                global _provider_cycle_deque, _last_cycle_refresh
                if not _provider_cycle_deque:
                    _provider_cycle_deque = deque(build_provider_pool())
                if not _provider_cycle_deque:
                    return None, None, None
                provider = _provider_cycle_deque.popleft()
                _provider_cycle_deque.append(provider)
                if DEBUG_FETCH: logger.debug(f"Round-robin provider chosen: {provider}")

            tried_providers.add(provider)
            fetcher = PROVIDER_FETCHERS.get(provider)
            if not fetcher:
                if DEBUG_FETCH: logger.debug(f"No fetcher for provider {provider}")
                continue

            # TAG selection: uniform across GIF_TAGS
            positive = random.choice(GIF_TAGS)

            if DEBUG_FETCH: logger.debug(f"[fetch_gif] attempt {attempt}/{FETCH_ATTEMPTS} provider={provider} positive='{positive}'")

            try:
                gif_url, name_hint, meta = await fetcher(session, positive)
            except Exception as e:
                if DEBUG_FETCH: logger.debug(f"Fetcher exception for {provider}: {e}")
                continue

            if not gif_url:
                if DEBUG_FETCH: logger.debug(f"{provider} returned no url.")
                if len(tried_providers) >= len(providers):
                    tried_providers.clear()
                continue

            if filename_has_block_keyword(gif_url):
                if DEBUG_FETCH: logger.debug(f"{provider} returned blocked filename keyword in {gif_url}")
                continue
            if contains_illegal_indicators((gif_url or "") + " " + (str(meta) or "")):
                if DEBUG_FETCH: logger.debug(f"{provider} returned illegal indicators in meta/url for {gif_url}")
                continue
            if _tag_is_disallowed(str(meta or "")):
                if DEBUG_FETCH: logger.debug(f"{provider} returned disallowed tags in meta for {gif_url}")
                continue

            gif_hash = hashlib.sha1((gif_url or name_hint or "").encode()).hexdigest()
            if gif_hash in sent:
                if DEBUG_FETCH: logger.debug(f"Already sent gif hash for {gif_url}; skipping.")
                continue

            b, ctype, reason = await attempt_get_media_bytes(session, gif_url)
            if DEBUG_FETCH: logger.debug(f"attempt_get_media_bytes -> provider={provider} url={gif_url} reason={reason} bytes_ok={bool(b)} ctype={ctype}")

            # mark as attempted to avoid repeat attempts even if download fails
            sent.append(gif_hash)
            if len(sent) > MAX_USED_GIFS_PER_USER:
                del sent[:len(sent) - MAX_USED_GIFS_PER_USER]
            data["sent_history"][user_key] = sent
            try:
                with open(DATA_FILE, "w") as f:
                    json.dump(data, f, indent=2)
            except Exception:
                pass

            if b:
                ext = ""
                try:
                    parsed = urlparse(gif_url)
                    ext = os.path.splitext(parsed.path)[1] or ".gif"
                    if len(ext) > 6:
                        ext = ".gif"
                except Exception:
                    ext = ".gif"
                name = f"{provider}_{hashlib.sha1(gif_url.encode()).hexdigest()[:10]}{ext}"
                return b, name, gif_url
            else:
                # return the URL as fallback (download failed)
                return None, None, gif_url

        if DEBUG_FETCH: logger.debug("fetch_gif exhausted attempts.")
        return None, None, None

# --------- Discord embed and send ----------
def make_embed(title, desc, member, kind="join", count=None):
    color = discord.Color.dark_red() if kind == "join" else discord.Color.dark_gray()
    embed = discord.Embed(title=title, description=desc, color=color, timestamp=datetime.utcnow())
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    footer = f"{member.display_name} • {member.id}"
    if count:
        footer += f" • Joins: {count}"
    embed.set_footer(text=footer)
    return embed

async def send_embed_with_media(text_channel, member, embed, gif_bytes, gif_name, gif_url):
    max_upload = DISCORD_MAX_UPLOAD
    try:
        if gif_bytes and len(gif_bytes) <= max_upload:
            try:
                file_server = discord.File(io.BytesIO(gif_bytes), filename=gif_name)
                embed.set_image(url=f"attachment://{gif_name}")
                if text_channel:
                    await text_channel.send(embed=embed, file=file_server)
            except Exception as e:
                logger.debug(f"attach->channel failed: {e}")
                if text_channel:
                    if gif_url:
                        if gif_url not in (embed.description or ""):
                            embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
                    await text_channel.send(embed=embed)
            try:
                file_dm = discord.File(io.BytesIO(gif_bytes), filename=gif_name)
                await member.send(embed=embed, file=file_dm)
            except Exception as e:
                logger.debug(f"attach->DM failed: {e}")
                try:
                    dm_embed = make_embed(embed.title or "Media", embed.description or "", member, kind="join")
                    if gif_url:
                        if gif_url not in (dm_embed.description or ""):
                            dm_embed.description = (dm_embed.description or "") + f"\n\n[View media here]({gif_url})"
                    await member.send(dm_embed)
                except Exception as e2:
                    logger.debug(f"DM link fallback failed: {e2}")
        else:
            if gif_url:
                if gif_url not in (embed.description or ""):
                    embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
            if text_channel:
                await text_channel.send(embed=embed)
            try:
                dm_embed = make_embed(embed.title or "Media", embed.description or "", member, kind="join")
                if gif_url:
                    if gif_url not in (dm_embed.description or ""):
                        dm_embed.description = (dm_embed.description or "") + f"\n\n[View media here]({gif_url})"
                await member.send(dm_embed)
            except Exception as e:
                logger.debug(f"DM link only failed: {e}")
    except Exception as e:
        logger.warning(f"unexpected error in send_embed_with_media: {e}")
        try:
            if text_channel:
                await text_channel.send(embed=embed)
            await member.send(embed=embed)
        except Exception:
            logger.debug("final fallback failed")

# --------- Greetings ----------

JOIN_GREETINGS = [
    "🔥 {display_name} enters — confidence detected.",
    "✨ {display_name} arrived, and attention followed.",
    "😈 {display_name} joined — bold move.",
    "👀 {display_name} just stepped in. Not unnoticed.",
    "🖤 {display_name} is here. Behave.",
    "💋 {display_name} joined — interesting choice.",
    "🕶️ {display_name} walks in like they own it.",
    "🌒 {display_name} entered quietly. Dangerous.",
    "⚡ {display_name} arrived with presence.",
    "🥀 {display_name} joined — don’t disappoint.",
    "🧠 {display_name} stepped in. I’m watching.",
    "🗝️ {display_name} unlocked the room.",
    "🔥 {display_name} joined — heat follows.",
    "👑 {display_name} arrived. Act accordingly.",
    "🌑 {display_name} stepped into my space.",
    "💎 {display_name} joined — rare energy.",
    "🩸 {display_name} arrived. Brave.",
    "🖤 {display_name} is here. Stay sharp.",
    "🕯️ {display_name} joined — slow and confident.",
    "🐍 {display_name} slid in smoothly.",
    "🌙 {display_name} arrived under quiet watch.",
    "🧿 {display_name} joined. I see you.",
    "🔮 {display_name} appeared — expected.",
    "🪶 {display_name} stepped in lightly.",
    "🎭 {display_name} arrived. Masks on.",
    "🩶 {display_name} joined — calm energy.",
    "🔥 {display_name} entered. Control yourself.",
    "🗝️ {display_name} found the door.",
    "👁️ {display_name} joined — focus locked.",
    "🌫️ {display_name} drifted in smoothly.",
    "🧊 {display_name} arrived cool and composed.",
    "🖤 {display_name} joined — noticed immediately.",
    "⚖️ {display_name} entered. Balance shifts.",
    "🐺 {display_name} joined alone. Respect.",
    "🌘 {display_name} arrived quietly.",
    "💼 {display_name} stepped in professionally.",
    "🕸️ {display_name} entered the web.",
    "🔥 {display_name} joined — tension rises.",
    "🪞 {display_name} arrived. Look sharp.",
    "🧠 {display_name} joined — think carefully.",
    "🖤 {display_name} entered. Eyes on you.",
    "🩸 {display_name} joined — bold timing.",
    "🌑 {display_name} stepped inside.",
    "💋 {display_name} arrived — tempting.",
    "🕶️ {display_name} joined with style.",
    "🔥 {display_name} entered — don’t blink.",
    "👑 {display_name} joined. Hold yourself well.",
    "🌙 {display_name} arrived under watchful eyes.",
    "🖤 {display_name} stepped in confidently.",
    "⚡ {display_name} joined — energy felt.",
    "🗝️ {display_name} crossed the threshold.",
    "😈 {display_name} arrived — curious choice.",
    "🧿 {display_name} joined. Observed.",
    "🔥 {display_name} entered — composure tested.",
    "🩶 {display_name} joined quietly.",
    "👀 {display_name} arrived. I noticed.",
    "🌒 {display_name} stepped in — interesting.",
    "🖤 {display_name} joined. Stay aware."
]
while len(JOIN_GREETINGS) < 60:
    JOIN_GREETINGS.append(random.choice(JOIN_GREETINGS))

LEAVE_GREETINGS = [
    "🌙 {display_name} slips away — silence lingers.",
    "🖤 {display_name} left. I noticed.",
    "🌑 {display_name} disappeared quietly.",
    "👀 {display_name} is gone. Remembered.",
    "🕯️ {display_name} exited — calm choice.",
    "😈 {display_name} left already?",
    "🌫️ {display_name} drifted out.",
    "🧠 {display_name} stepped away. Thinking?",
    "🖤 {display_name} vanished smoothly.",
    "🌒 {display_name} left under watch.",
    "🗝️ {display_name} closed the door.",
    "🩶 {display_name} exited calmly.",
    "🕶️ {display_name} slipped out unnoticed.",
    "🌙 {display_name} faded into the night.",
    "🔥 {display_name} left — heat cools.",
    "🧿 {display_name} exited. Observed.",
    "🖤 {display_name} stepped away.",
    "🕸️ {display_name} escaped the web.",
    "👑 {display_name} left with composure.",
    "🌑 {display_name} disappeared.",
    "💎 {display_name} exited — rare move.",
    "🩸 {display_name} left boldly.",
    "🧠 {display_name} walked away quietly.",
    "🌫️ {display_name} slipped into silence.",
    "🖤 {display_name} is gone for now.",
    "🌘 {display_name} left without a sound.",
    "⚖️ {display_name} exited — balance restored.",
    "🕯️ {display_name} stepped out.",
    "👁️ {display_name} left. Not forgotten.",
    "🌙 {display_name} vanished softly.",
    "🖤 {display_name} exited confidently.",
    "🔥 {display_name} left — tension fades.",
    "🧊 {display_name} stepped away coolly.",
    "🕶️ {display_name} left with style.",
    "🧿 {display_name} exited. Noted.",
    "🌑 {display_name} slipped out quietly.",
    "🩶 {display_name} walked away calmly.",
    "🕸️ {display_name} escaped.",
    "👀 {display_name} left — watched.",
    "🖤 {display_name} disappeared smoothly.",
    "🌒 {display_name} stepped away.",
    "🔥 {display_name} exited — control remains.",
    "🧠 {display_name} left thoughtfully.",
    "🕯️ {display_name} faded out.",
    "🌙 {display_name} slipped into the dark.",
    "🖤 {display_name} left. Silence follows.",
    "🧿 {display_name} exited cleanly.",
    "🩸 {display_name} walked away.",
    "🌑 {display_name} vanished again.",
    "🕶️ {display_name} exited quietly.",
    "👑 {display_name} left with grace.",
    "🖤 {display_name} stepped out calmly.",
    "🌫️ {display_name} dissolved into quiet.",
    "🔥 {display_name} left — eyes linger.",
    "🧠 {display_name} stepped away.",
    "🌙 {display_name} exited softly.",
    "🖤 {display_name} gone — remembered.",
    "👀 {display_name} left. Not ignored."
]
while len(LEAVE_GREETINGS) < 60:
    LEAVE_GREETINGS.append(random.choice(LEAVE_GREETINGS))

# --------- Bot setup ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Keep the bot connected to VC 24/7 (reconnect / move if needed)
@tasks.loop(seconds=60)
async def ensure_connected_task():
    try:
        if not VC_IDS:
            return
        vc_id = VC_IDS[0]
        channel = bot.get_channel(vc_id)
        if not channel:
            for g in bot.guilds:
                ch = g.get_channel(vc_id)
                if ch:
                    channel = ch
                    break
        if not channel:
            return
        vc = discord.utils.get(bot.voice_clients, guild=channel.guild)
        if not vc:
            try:
                await channel.connect(reconnect=True)
                if DEBUG_FETCH: logger.debug(f"ensure_connected_task: connected to VC {vc_id}")
            except Exception as e:
                if DEBUG_FETCH: logger.debug(f"ensure_connected_task: failed to connect to VC {vc_id}: {e}")
        else:
            if vc.channel.id != channel.id:
                try:
                    await vc.move_to(channel)
                except Exception as e:
                    if DEBUG_FETCH: logger.debug(f"ensure_connected_task: failed to move VC to {vc_id}: {e}")
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"ensure_connected_task unexpected: {e}")

@bot.event
async def on_ready():
    try:
        autosave_task.start()
    except RuntimeError:
        pass
    try:
        ensure_connected_task.start()
    except RuntimeError:
        pass
    available = []
    for p in PROVIDER_FETCHERS.keys():
        key_ok = True
        if p == "waifu_it" and not WAIFUIT_API_KEY:
            key_ok = False
        available.append((p, key_ok, data.get("provider_weights", {}).get(p, 1)))
    logger.info("Provider availability (provider, api_key_hint, stored_weight):")
    for t in available:
        logger.info(t)
    logger.info(f"Logged in as {bot.user} (id={bot.user.id})")

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return
    text_channel = bot.get_channel(VC_CHANNEL_ID)
    # joined a monitored VC
    if after.channel and (after.channel.id in VC_IDS) and (before.channel != after.channel):
        try:
            vc = discord.utils.get(bot.voice_clients, guild=member.guild)
            if vc:
                if vc.channel.id != after.channel.id:
                    await vc.move_to(after.channel)
            else:
                await after.channel.connect()
        except Exception as e:
            logger.warning(f"VC join error: {e}")

        raw = random.choice(JOIN_GREETINGS)
        msg = raw.format(display_name=member.display_name)
        data["join_counts"][str(member.id)] = data["join_counts"].get(str(member.id), 0) + 1
        embed = make_embed("Welcome!", msg, member, "join", data["join_counts"][str(member.id)])
        gif_bytes, gif_name, gif_url = await fetch_gif(member.id)
        await send_embed_with_media(text_channel, member, embed, gif_bytes, gif_name, gif_url)

    # left monitored VC
    if before.channel and (before.channel.id in VC_IDS) and (after.channel != before.channel):
        raw = random.choice(LEAVE_GREETINGS)
        msg = raw.format(display_name=member.display_name)
        embed = make_embed("Goodbye!", msg, member, "leave")
        gif_bytes, gif_name, gif_url = await fetch_gif(member.id)
        await send_embed_with_media(text_channel, member, embed, gif_bytes, gif_name, gif_url)
        # DO NOT disconnect when channel empties — bot stays 24/7

# --------- Run ----------
if __name__ == "__main__":
    if not TOKEN:
        logger.error("TOKEN missing. Set TOKEN and restart.")
    else:
        bot.run(TOKEN)
