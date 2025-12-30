# NSFW bot.py
import os
import io
import json
import random
import hashlib
import logging
import re
import asyncio
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus, urlparse

import aiohttp
import discord
from discord.ext import commands, tasks
from collections import deque

try:
    from PIL import Image, ImageSequence
except Exception:
    Image = None

# ====== Config / ENV KEYS ======
TOKEN = os.getenv("TOKEN", "")
WAIFUIM_API_KEY = os.getenv("WAIFUIM_API_KEY", "")
WAIFUIT_API_KEY = os.getenv("WAIFUIT_API_KEY", "")
DANBOORU_USER = os.getenv("DANBOORU_USER", "")
DANBOORU_API_KEY = os.getenv("DANBOORU_API_KEY", "")
GELBOORU_API_KEY = os.getenv("GELBOORU_API_KEY", "")
GELBOORU_USER = os.getenv("GELBOORU_USER", "")

_DEBUG_RAW = os.getenv("DEBUG_FETCH", "")
DEBUG_FETCH = str(_DEBUG_RAW).strip().lower() in ("1", "true", "yes", "on")
TRUE_RANDOM = str(os.getenv("TRUE_RANDOM", "")).strip().lower() in ("1", "true", "yes")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "14"))
DISCORD_MAX_UPLOAD = int(os.getenv("DISCORD_MAX_UPLOAD", str(8 * 1024 * 1024)))
HEAD_SIZE_LIMIT = DISCORD_MAX_UPLOAD
DATA_FILE = os.getenv("DATA_FILE", "data_nsfw.json")
AUTOSAVE_INTERVAL = int(os.getenv("AUTOSAVE_INTERVAL", "30"))
FETCH_ATTEMPTS = int(os.getenv("FETCH_ATTEMPTS", "40"))
MAX_USED_GIFS_PER_USER = int(os.getenv("MAX_USED_GIFS_PER_USER", "1000"))

VC_IDS = [
    int(os.getenv("VC_ID_1", "1409170559337762980")),
]
VC_CHANNEL_ID = int(os.getenv("VC_CHANNEL_ID", "1371916812903780573"))

logging.basicConfig(level=logging.DEBUG if DEBUG_FETCH else logging.INFO)
logger = logging.getLogger("spiciest-nsfw")

# ====== Helpers & Filters ======
_token_split_re = re.compile(r"[^a-z0-9]+")

ILLEGAL_TAGS = [
    "underage", "minor", "child", "loli", "shota", "young", "agegap",
    "bestiality", "zoophilia", "bestial",
    "scat", "fisting", "incest", "pedo", "pedophile"
]

BLOCKED_TAGS = [
    "futanari", "futa", "dickgirl", "shemale", "transgender", "newhalf",
    "yaoi", "gay", "male", "femboy", "trap", "otoko_no_ko", "crossdressing",
    "penis", "bara", "3d", "real", "photo", "cosplay", "irl"
]

FILENAME_BLOCK_KEYWORDS = ["scat", "fisting", "bestiality"]

EXCLUDE_TAGS = [
    "loli", "shota", "child", "minor", "underage", "young", "schoolgirl", "age_gap",
    "pedo", "pedophile", "bestiality", "zoophilia"
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
    if any(bl in t for bl in BLOCKED_TAGS):
        return True
    return False

def contains_illegal_indicators(text: str) -> bool:
    if not text or not isinstance(text, str):
        return False
    normalized = _normalize_text(text)
    for bad in ILLEGAL_TAGS:
        if bad in normalized:
            return True
    for blocked in BLOCKED_TAGS:
        if blocked in normalized:
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

# ====== Persistent data (load/create) ======
if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        json.dump({"provider_weights": {}, "sent_history": {}, "gif_tags": [], "vc_state": {}}, f, indent=2)

with open(DATA_FILE, "r") as f:
    data = json.load(f)

data.setdefault("provider_weights", {})
data.setdefault("sent_history", {})
data.setdefault("gif_tags", [])
data.setdefault("vc_state", {})

_seed_gif_tags = [
    "hentai", "ecchi", "sex", "oral", "anal", "cum", "cumshot", "orgasm",
    "hardcore", "milf", "mature", "oppai", "ass", "thighs", "blowjob",
    "pussy", "nude", "lingerie", "stockings", "underboob", "sideboob", "nsfw"
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

# ====== Provider mapping and tag mapping (NSFW) ======
PROVIDER_TERMS = {
    "waifu_pics": ["waifu", "neko", "blowjob"],
    "waifu_im": ["ero", "hentai", "ass", "hmidriff", "oppai", "hthigh", "paizuri", "ecchi", "selfies"],
    "hmtai": ["hentai", "anal", "ass", "bdsm", "cum", "boobs", "thighs", "pussy", "ahegao", "tentacles"],
    "nekobot": ["hentai", "hentai_anal", "hass", "hboobs", "hthigh", "paizuri", "tentacle", "pgif"],
    "nekos_moe": ["hentai", "ecchi", "ero", "oppai", "yuri"],
    "danbooru": ["hentai", "ecchi", "breasts", "thighs", "panties", "ass", "anal", "oral", "cum"],
    "gelbooru": ["hentai", "ecchi", "panties", "thighs", "ass", "bikini", "cleavage"],
    "rule34": ["hentai", "ecchi", "panties", "thighs", "ass", "big_breasts"],
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

# ====== Network helper to download with size limit ======
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

# ====== Provider fetch implementations (NSFW providers) ======
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
    except Exception:
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
                return None, None, None
            payload = await resp.json()
            images = payload.get("images", [])
            if not images:
                return None, None, None
            img = random.choice(images)
            gif_url = img.get("url")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(img) + " " + (q or "")):
                return None, None, None
            extract_and_add_tags_from_meta(str(img.get("tags", "")), GIF_TAGS, data)
            return gif_url, f"waifu_im_{q}", img
    except Exception:
        return None, None, None

async def fetch_from_hmtai(session, positive):
    try:
        category = map_tag_for_provider("hmtai", positive)
        url = f"https://hmtai.hatsunia.cfd/v2/nsfw/{category}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")):
                return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"hmtai_{category}", payload
    except Exception:
        return None, None, None

async def fetch_from_nekobot(session, positive):
    try:
        category = map_tag_for_provider("nekobot", positive)
        url = f"https://nekobot.xyz/api/image?type={quote_plus(category)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            if not payload.get("success"):
                return None, None, None
            gif_url = payload.get("message")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")):
                return None, None, None
            extract_and_add_tags_from_meta(category, GIF_TAGS, data)
            return gif_url, f"nekobot_{category}", payload
    except Exception:
        return None, None, None

async def fetch_from_nekos_moe(session, positive):
    try:
        url = "https://nekos.moe/api/v1/random/image?nsfw=true&count=1"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            images = payload.get("images", [])
            if not images:
                return None, None, None
            img = random.choice(images)
            img_id = img.get("id")
            if not img_id:
                return None, None, None
            gif_url = f"https://nekos.moe/image/{img_id}.jpg"
            if contains_illegal_indicators(json.dumps(img)):
                return None, None, None
            extract_and_add_tags_from_meta(" ".join(img.get("tags", [])), GIF_TAGS, data)
            return gif_url, "nekos_moe", img
    except Exception:
        return None, None, None

async def fetch_from_danbooru(session, positive):
    try:
        tags = f"{positive} rating:explicit"
        base = "https://danbooru.donmai.us/posts.json"
        params = {"tags": tags, "limit": 20, "random": "true"}
        headers = {}
        if DANBOORU_USER and DANBOORU_API_KEY:
            import base64
            credentials = base64.b64encode(f"{DANBOORU_USER}:{DANBOORU_API_KEY}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"
        async with session.get(base, params=params, headers=headers or None, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            posts = await resp.json()
            if not posts:
                return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url") or post.get("large_file_url")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(post)):
                return None, None, None
            extract_and_add_tags_from_meta(str(post.get("tag_string", "")), GIF_TAGS, data)
            return gif_url, f"danbooru_{positive}", post
    except Exception:
        return None, None, None

async def fetch_from_gelbooru(session, positive):
    try:
        blocked_str = " ".join([f"-{b}" for b in BLOCKED_TAGS])
        tags = f"{positive} rating:explicit {blocked_str} 1girl".strip()
        base = "https://gelbooru.com/index.php"
        params = {
            "page": "dapi",
            "s": "post",
            "q": "index",
            "json": "1",
            "tags": tags,
            "limit": 20
        }
        if GELBOORU_API_KEY and GELBOORU_USER:
            params["api_key"] = GELBOORU_API_KEY
            params["user_id"] = GELBOORU_USER
        async with session.get(base, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            posts = payload.get("post", [])
            if not posts:
                return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(post)):
                return None, None, None
            extract_and_add_tags_from_meta(post.get("tags", ""), GIF_TAGS, data)
            return gif_url, f"gelbooru_{positive}", post
    except Exception:
        return None, None, None

async def fetch_from_rule34(session, positive):
    try:
        blocked_str = " ".join([f"-{b}" for b in BLOCKED_TAGS])
        tags = f"{positive} rating:explicit {blocked_str} 1girl".strip()
        base = "https://api.rule34.xxx/index.php"
        params = {
            "page": "dapi",
            "s": "post",
            "q": "index",
            "json": "1",
            "tags": tags,
            "limit": 100
        }
        async with session.get(base, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            posts = await resp.json()
            if not posts or not isinstance(posts, list):
                return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or filename_has_block_keyword(gif_url):
                return None, None, None
            if contains_illegal_indicators(json.dumps(post)):
                return None, None, None
            extract_and_add_tags_from_meta(post.get("tags", ""), GIF_TAGS, data)
            return gif_url, f"rule34_{positive}", post
    except Exception:
        return None, None, None

# ====== Providers list (NSFW) ======
PROVIDERS = [
    ("hmtai", fetch_from_hmtai, 25),
    ("nekobot", fetch_from_nekobot, 25),
    ("rule34", fetch_from_rule34, 20),
    ("waifu_im", fetch_from_waifu_im, 15),
    ("nekos_moe", fetch_from_nekos_moe, 10),
    ("danbooru", fetch_from_danbooru, 10),
    ("gelbooru", fetch_from_gelbooru, 10),
    ("waifu_pics", fetch_from_waifu_pics, 5),
]

def _hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def _choose_random_provider():
    if TRUE_RANDOM:
        return random.choice(PROVIDERS)
    else:
        weights = [w for _, _, w in PROVIDERS]
        return random.choices(PROVIDERS, weights=weights, k=1)[0]

# ====== Main fetch/selection logic ======
async def _fetch_one_gif(session, user_id=None, used_hashes=None):
    if used_hashes is None:
        used_hashes = set()

    tag = random.choice(GIF_TAGS)
    name, fetch_func, weight = _choose_random_provider()

    try:
        url, source, meta = await fetch_func(session, tag)
        if url:
            h = _hash_url(url)
            if h not in used_hashes:
                return url, source, meta, h
    except Exception as e:
        if DEBUG_FETCH:
            logger.debug(f"{name} fail: {e}")

    return None, None, None, None

async def fetch_random_gif(session, user_id=None):
    user_id_str = str(user_id) if user_id else "global"
    user_history = data["sent_history"].setdefault(user_id_str, [])
    used_hashes = set(user_history)

    for attempt in range(FETCH_ATTEMPTS):
        url, source, meta, url_hash = await _fetch_one_gif(session, user_id, used_hashes)
        if url:
            user_history.append(url_hash)
            if len(user_history) > MAX_USED_GIFS_PER_USER:
                user_history.pop(0)
            data["sent_history"][user_id_str] = user_history
            logger.info(f"Attempt {attempt+1}: Fetched from {source}")
            return url, source, meta

    logger.warning(f"Failed to fetch after {FETCH_ATTEMPTS} attempts")
    return None, None, None

# ====== Utility: compress images if too large ======
async def compress_image(image_bytes, target_size=DISCORD_MAX_UPLOAD):
    if not Image:
        return image_bytes
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.format == "GIF":
            return image_bytes
        output = io.BytesIO()
        quality = 95
        while quality > 10:
            output.seek(0)
            output.truncate()
            img.save(output, format=img.format or "JPEG", quality=quality, optimize=True)
            if output.tell() <= target_size:
                return output.getvalue()
            quality -= 10
        return output.getvalue()
    except Exception as e:
        logger.error(f"Compression failed: {e}")
        return image_bytes

# ====== Embeds + send greeting helper ======
JOIN_GREETINGS = [
    "💋 {display_name} slips in like a slow caress — the room just warmed up.",
    "🔥 {display_name} arrived, tracing heat across the air; someone hold the temperature.",
    "✨ {display_name} joins — all eyes and soft smiles. Dare to stir trouble?",
    "😈 {display_name} steps through the door with a dangerous smile and a hungry look.",
    "👀 {display_name} appeared — sudden quiet, then the world leans in.",
    "🖤 {display_name} joined, breath shallow, pulse audible — tempting, isn’t it?",
    "🌙 {display_name} glides in as if they own the moment — claim it or be claimed.",
    "🕯️ {display_name} arrives wrapped in dusk and whispering promises.",
    "🍷 {display_name} joined — like a warm pour, smooth and slow.",
    "🥀 {display_name} walked in with a smile that asked for trouble.",
    "🕶️ {display_name} stepped in cool, but the air around them is anything but.",
    "💎 {display_name} joined — rare, polished, and distractingly beautiful.",
    "👑 {display_name} arrived; treat them like royalty or lose the crown.",
    "🌫️ {display_name} drifted in; the air tastes sweeter already.",
    "🪞 {display_name} joined — catch their reflection if you dare.",
    "⚡ {display_name} joined and the electricity in the room changed lanes.",
    "🧠 {display_name} arrived with a mind on fire — play smart, play dangerous.",
    "💋 {display_name} slipped in with a grin; the night just leaned forward.",
    "🩸 {display_name} joined — bold, a little wicked, entirely noticed.",
    "🐍 {display_name} slithered in, sly and sure. Watch your step.",
    "🌒 {display_name} arrived quietly — but the silence hums with intent.",
    "🧿 {display_name} joined; the air says stay, the body says closer.",
    "🎭 {display_name} entered with a mischievous tilt — masks are optional.",
    "🪶 {display_name} stepped in with featherlight steps and heavy intent.",
    "🩶 {display_name} joined, calm on the outside, simmering on the inside.",
    "👁️ {display_name} arrived; one look and the night got complicated.",
    "🕸️ {display_name} stepped into the web — enjoy getting tangled.",
    "🌘 {display_name} joined — shadow-soft and dangerously inviting.",
    "🧊 {display_name} arrived cool, but their presence melts the room.",
    "⚖️ {display_name} walked in — the balance shifted toward desire.",
    "🪄 {display_name} joined and something magical tightened in the chest.",
    "🌺 {display_name} arrived like a slow bloom — intoxicating.",
    "🫦 {display_name} joined — lips curved, promise implied.",
    "🎶 {display_name} arrived on a private rhythm; follow if you want to sway.",
    "🌪️ {display_name} joined — whirlwinds look calm until they hit.",
    "🖤 {display_name} slipped in, hush and hunger wrapped together.",
    "💼 {display_name} entered composed — look closer, there’s mischief under the suit.",
    "💫 {display_name} joined and the room took a breathless pause.",
    "🩸 {display_name} enters — the room tightens like it knows what's coming.",
    "🖤 {display_name} joined. Lock your thoughts, not your doors.",
    "🌑 {display_name} stepped in — eyes linger longer than they should.",
    "😈 {display_name} arrived with intent. Pretend you don't feel it.",
    "🕷️ {display_name} entered — something just wrapped around your focus.",
    "🔥 {display_name} joined. Heat climbs. Control slips.",
    "👁️ {display_name} is here — watched before watching back.",
    "🖤 {display_name} arrived. Breathe slow. This one doesn't rush.",
    "🌒 {display_name} slipped in — confidence sharp enough to cut.",
    "🩶 {display_name} joined quietly. Dangerous people don’t announce themselves.",
    "😼 {display_name} joined with a look that asks permission from no one.",
    "🕯️ {display_name} arrived — slow burn, no mercy.",
    "🐍 {display_name} slid in — smooth, patient, inevitable.",
    "🧿 {display_name} joined — attention captured, consent assumed.",
    "🕶️ {display_name} arrived — unreadable, unbothered, unresisted.",
    "🎯 {display_name} joined — precise, unavoidable, magnetic.",
    "🔒 {display_name} arrived — doors close a little tighter.",
    "🗝️ {display_name} unlocked the room; keys aren't always literal.",
    "🧨 {display_name} entered — contained chaos with an inviting grin.",
    "🌌 {display_name} joined — vast, dark, and impossible to ignore.",
    "🖤 {display_name} slips in — the shadows made room for them.",
    "🌑 {display_name} arrived; the air tightened at their name.",
    "🩸 {display_name} walked in with an intent that hums.",
    "🔥 {display_name} entered — eyes sharpen, breaths slow.",
    "😈 {display_name} came — dangerous grace, measured steps.",
    "🕯️ {display_name} arrives, dusk trailing like a promise.",
    "👁️ {display_name} joined — every light found them first.",
    "🐍 {display_name} slipped through; patience wrapped around them.",
    "⚡ {display_name} stepped in and the dark learned to behave.",
    "🗝️ {display_name} unlocked eyes; rooms rearranged themselves.",
    "🖤 {display_name} arrived — presence heavy, attention willing.",
    "🌘 {display_name} joined; the hush leaned forward.",
    "🕶️ {display_name} walked in — unreadable, owning the pause.",
    "🔒 {display_name} arrived; the world closed in tighter.",
    "🧿 {display_name} came — watched and watching back.",
    "🩶 {display_name} stepped through — calm, collected, inevitable.",
    "🌫️ {display_name} appears like smoke — hard to push away.",
    "🐺 {display_name} joined alone; the pack noticed.",
    "🪞 {display_name} arrived — reflection trembles when they move.",
    "🎯 {display_name} entered — precise, unavoidable.",
    "🧨 {display_name} slipped in; contained trouble with a smile.",
    "🪄 {display_name} arrived — small magic, large consequence.",
    "🕷️ {display_name} stepped into the web — enjoy the pull.",
    "🌪️ {display_name} entered — calm before the pleasant storm.",
    "💎 {display_name} joined — cold, beautiful, demanding regard.",
    "🫦 {display_name} arrived; lips quiet, intentions loud.",
    "🎭 {display_name} came with a hidden grin — masks not required.",
    "🍷 {display_name} entered like a slow pour; taste lingers.",
    "🐉 {display_name} arrived — ancient hush follows new steps.",
    "🧠 {display_name} joined; thinking people are deliciously dangerous.",
    "💫 {display_name} arrives and the room holds its breath.",
    "🌺 {display_name} stepped in — beauty that commands.",
    "🕯️ {display_name} came — slow flame, sharp heat.",
    "🪶 {display_name} drifted in; soft steps, heavy intent.",
    "⚖️ {display_name} joined — balance subtly tipped.",
    "🌌 {display_name} arrived — vast, dark, magnetic.",
    "🔮 {display_name} entered; futures leaned toward them.",
    "🧊 {display_name} came cool, but the air betrayed them.",
    "🖤 {display_name} arrived — possession in every glance.",
    "🌒 {display_name} stepped in and the night acknowledged them.",
    "👑 {display_name} joined; crowns fit easily on slow smiles.",
    "🔥 {display_name} slipped in — embers trailed their footsteps.",
    "🕶️ {display_name} arrived — no one dares stare too long.",
    "🩸 {display_name} entered; the room kept a small, sharp memory.",
    "🧿 {display_name} joined — attention harvested like harvests.",
    "🗝️ {display_name} stepped through; doors sighed closed behind them.",
    "🐍 {display_name} arrived — elegant, patient, inevitable.",
    "🌘 {display_name} joined; darkness welcomed a familiar shape.",
    "🎶 {display_name} entered on a low, dangerous rhythm.",
    "🪞 {display_name} came — mirrors preferred their reflection tonight.",
    "🥀 {display_name} arrives — wilted petals, potent scent.",
    "🕸️ {display_name} joined; the web tightened delightfully.",
    "💼 {display_name} stepped in — composed, with a hidden edge.",
    "🧨 {display_name} arrived — quiet fuse, loud results.",
    "🫀 {display_name} came — heartbeats answered them.",
    "🪙 {display_name} arrived — coin dropped, choices made.",
    "🐾 {display_name} joined — footsteps marking territory.",
    "🖤 {display_name} entered and the room learned to follow their lead.",
    "⚡ {display_name} arrived; sparks were not accidental.",
    "🌑 {display_name} joined — welcome to the darker side of curiosity."
]
while len(JOIN_GREETINGS) < 120:
    JOIN_GREETINGS.append(random.choice(JOIN_GREETINGS))

LEAVE_GREETINGS = [
    "🌙 {display_name} slips away — the afterglow lingers.",
    "🖤 {display_name} left; the room exhales and remembers the warmth.",
    "🌑 {display_name} drifted out, leaving charged silence in their wake.",
    "👀 {display_name} left — eyes still searching the doorway.",
    "🕯️ {display_name} exited; the candle burned a little brighter while they were here.",
    "😈 {display_name} disappeared — mischief properly recorded.",
    "🌫️ {display_name} faded into the night; whispers followed.",
    "🧠 {display_name} walked away smiling — plotting, no doubt.",
    "🕶️ {display_name} slipped out unnoticed — or cleverly unnoticed.",
    "🔥 {display_name} left — the heat remains, the body remembers.",
    "🩸 {display_name} exited with a confident step; tension released slowly.",
    "🧿 {display_name} left — observed, appreciated, missed already.",
    "🌙 {display_name} vanished into shadow; the room feels unfinished.",
    "🕸️ {display_name} untangled themselves and left; good luck finding them again.",
    "💎 {display_name} left — rare presence, quickly missed.",
    "👑 {display_name} departed with grace; crowns do sit well on them.",
    "🪞 {display_name} left after leaving reflection and wishful looks behind.",
    "🩶 {display_name} stepped away; calm returned but with an echo.",
    "🕯️ {display_name} faded out like smoke — beautiful and fleeting.",
    "🌫️ {display_name} slipped into silence, leaving a sweet ache.",
    "👁️ {display_name} left; pockets of attention where they walked remain.",
    "🖤 {display_name} walked off; the night kept a small, bright fragment of them.",
    "🌘 {display_name} departed under the moon — teasing the dark.",
    "🧊 {display_name} left coolly; hearts are still thawing.",
    "⚖️ {display_name} exited — balance restored but tempting memories stayed.",
    "🪄 {display_name} vanished like last night's spell.",
    "🌺 {display_name} walked away — a scent and a memory in their wake.",
    "🎶 {display_name} left as their rhythm faded; the silence hums their tune.",
    "🫦 {display_name} stepped out leaving lips and longing behind.",
    "🔥 {display_name} left — the embers keep the story alive.",
    "🕶️ {display_name} disappeared with a wink; the room's temperature dipped.",
    "🌫️ {display_name} walked away, the hush heavy with possibility.",
    "👀 {display_name} exited; the doorway still expecting their return.",
    "🖤 {display_name} left — nothing ended, only paused.",
    "🌒 {display_name} drifted off into the night; the aftertaste is electric.",
    "🧿 {display_name} vanished — marked, noted, and quietly missed.",
    "🪞 {display_name} stepped away; mirrors kept the memory.",
    "🩸 {display_name} left — tension released too late.",
    "🌑 {display_name} disappeared. The room feels exposed.",
    "😈 {display_name} left. Trouble paused, not finished.",
    "🕯️ {display_name} slipped out — the glow fades slowly.",
    "👁️ {display_name} gone. Eyes still searching shadows.",
    "🌫️ {display_name} vanished like smoke — hard to forget.",
    "🖤 {display_name} walked away. Control returned. Barely.",
    "🐍 {display_name} exited — shed skin left behind.",
    "🔥 {display_name} left. Embers remain.",
    "🌒 {display_name} slipped out — unfinished business hangs heavy.",
    "🩶 {display_name} gone quiet. The echo stays loud.",
    "🕶️ {display_name} disappeared smoothly. Suspiciously smooth.",
    "🩸 {display_name} stepped away — hunger postponed.",
    "🌑 {display_name} left. Darkness adjusted.",
    "🖤 {display_name} exited — presence missed immediately.",
    "⚡ {display_name} gone. Static lingers.",
    "🌘 {display_name} vanished into night. Expected.",
    "👀 {display_name} left. Someone exhaled.",
    "🔥 {display_name} walked out — control reclaimed, barely.",
    "🕷️ {display_name} escaped the web. For now.",
    "🖤 {display_name} gone — attention withdrawn slowly.",
    "🌫️ {display_name} faded out. The silence feels intentional.",
    "🩸 {display_name} exited. The night remembers.",
    "🌙 {display_name} slips away — the darkness keeps a taste of them.",
    "🖤 {display_name} left; the room feels slightly unbalanced.",
    "🌑 {display_name} vanished — a hollow heat remains.",
    "🩸 {display_name} departed; the air still remembers their weight.",
    "🔥 {display_name} exited — embers smolder in their wake.",
    "😈 {display_name} left. Trouble folded into the night.",
    "🕯️ {display_name} went; the candle burns down slower now.",
    "👁️ {display_name} left — eyes still search their shadow.",
    "🐍 {display_name} slipped out; the web keeps its secrets.",
    "⚡ {display_name} departed — static hangs in the hair.",
    "🧿 {display_name} left; marked and quietly missed.",
    "🪞 {display_name} exited; reflections linger where they stood.",
    "🌫️ {display_name} faded away — whispers filled the gap.",
    "🕶️ {display_name} left unseen, deliberately unseen.",
    "💎 {display_name} departed — rarity wrapped in dusk.",
    "👑 {display_name} left; a crown's shadow trails after them.",
    "🪄 {display_name} vanished like a small, cruel spell.",
    "🩶 {display_name} left — calm returned with an echo.",
    "🌘 {display_name} stepped away; night kept one extra secret.",
    "🧠 {display_name} exited — minds like theirs leave prints.",
    "🎶 {display_name} left; the rhythm slows to a tempting ache.",
    "🫦 {display_name} slipped out — lips and memory remained.",
    "🕸️ {display_name} escaped the web; someone lost their tangle.",
    "🌺 {display_name} walked away; scent followed like a claim.",
    "🧊 {display_name} left coolly, leaving warm questions.",
    "🔒 {display_name} departed — doors clicked closed with intention.",
    "🗝️ {display_name} left; keys left echoing in locked rooms.",
    "🐺 {display_name} walked off; the pack paused.",
    "🪞 {display_name} exited — mirrors kept whispering their name.",
    "⚖️ {display_name} left; balance returned but not unchanged.",
    "🩸 {display_name} left — hunger postponed, remembered.",
    "🔮 {display_name} departed; futures rearranged themselves.",
    "🪨 {display_name} slipped away — weight of their absence noticeable.",
    "🌪️ {display_name} left; the calm after them felt fragile.",
    "🕯️ {display_name} faded into shadow; the light still flickers.",
    "👀 {display_name} left — glances linger on the doorway.",
    "🖤 {display_name} walked out; the atmosphere sighed.",
    "🧨 {display_name} exited — quiet aftershocks remain.",
    "🌒 {display_name} left; the moon kept their silhouette.",
    "🩶 {display_name} disappeared — echoes of composure stayed behind.",
    "🔥 {display_name} walked away; the warmth presses against memory.",
    "🕶️ {display_name} exited with a calm that's now suspicious.",
    "🩸 {display_name} faded — the room folded around their absence.",
    "🧿 {display_name} vanished — a mark left in the air.",
    "🐍 {display_name} left smoothly; the tension unspooled.",
    "🌫️ {display_name} stepped away; silence filled with possibility.",
    "🧠 {display_name} departed — cleverness left traces.",
    "🪞 {display_name} left; shadows kept a copy of their smile.",
    "🪙 {display_name} exited — choices clinked on the floor.",
    "🎯 {display_name} slipped away; aim unforgotten.",
    "🕷️ {display_name} escaped; the web hummed lonely.",
    "🌑 {display_name} left — darkness adjusted to the loss.",
    "🔮 {display_name} walked off; predictions now tempered.",
    "🪄 {display_name} vanished; the spell lingers in the chest.",
    "🩶 {display_name} left quietly; the room's breath slowed.",
    "💼 {display_name} stepped out — composure left like a promise.",
    "🫀 {display_name} departed; heartbeats counted themselves again.",
    "🌌 {display_name} walked away; the cosmos kept a secret orbit.",
    "⚡ {display_name} exited — sparks died but marks remained.",
    "🕯️ {display_name} disappeared into the dark; the night waits."
]
while len(LEAVE_GREETINGS) < 60:
    LEAVE_GREETINGS.append(random.choice(LEAVE_GREETINGS))
    
async def send_greeting_with_image_embed(channel, session, greeting_text, image_url, member, send_to_dm=None):
    try:
        image_bytes, content_type = await _download_bytes_with_limit(session, image_url)
        if not image_bytes or len(image_bytes) > DISCORD_MAX_UPLOAD:
            if image_bytes and len(image_bytes) > DISCORD_MAX_UPLOAD:
                image_bytes = await compress_image(image_bytes)
            if not image_bytes or len(image_bytes) > DISCORD_MAX_UPLOAD:
                await channel.send(greeting_text)
                return

        ext = ".jpg"
        if "gif" in image_url.lower() or (content_type and "gif" in content_type):
            ext = ".gif"
        elif "png" in image_url.lower() or (content_type and "png" in content_type):
            ext = ".png"
        elif "webp" in image_url.lower() or (content_type and "webp" in content_type):
            ext = ".webp"

        filename = f"nsfw{ext}"
        file = discord.File(io.BytesIO(image_bytes), filename=filename)

        embed = discord.Embed(
            description=greeting_text,
            color=discord.Color.from_rgb(220, 53, 69)
        )
        embed.set_author(name=member.display_name, icon_url=getattr(member.display_avatar, "url", None))
        embed.set_image(url=f"attachment://{filename}")
        embed.set_footer(text="NSFW Bot")

        await channel.send(embed=embed, file=file)

        if send_to_dm:
            try:
                dm_file = discord.File(io.BytesIO(image_bytes), filename=filename)
                dm_embed = discord.Embed(
                    description=greeting_text,
                    color=discord.Color.from_rgb(46, 204, 113)
                )
                dm_embed.set_author(name=member.display_name, icon_url=getattr(member.display_avatar, "url", None))
                dm_embed.set_image(url=f"attachment://{filename}")
                dm_embed.set_footer(text="NSFW Bot")
                await send_to_dm.send(embed=dm_embed, file=dm_file)
            except Exception as e:
                logger.warning(f"Could not DM: {e}")

    except Exception as e:
        logger.error(f"Failed to send greeting embed: {e}")
        try:
            await channel.send(greeting_text)
        except Exception:
            pass

# ====== Voice-channel logic & helper functions ======
def get_all_vcs_with_users(guild):
    out = []
    for vc_id in VC_IDS:
        vc = guild.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel):
            users = [m for m in vc.members if not m.bot]
            if users:
                out.append((vc, users))
    return out

def check_all_vcs_empty(guild):
    for vc_id in VC_IDS:
        vc = guild.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel):
            users = [m for m in vc.members if not m.bot]
            if len(users) > 0:
                return False
    return True

async def update_bot_vc_position(guild, target_channel=None):
    voice_client = guild.voice_client

    if target_channel and target_channel.id in VC_IDS:
        users_in_target = [m for m in target_channel.members if not m.bot]
        if users_in_target:
            if voice_client and voice_client.is_connected():
                if voice_client.channel.id != target_channel.id:
                    try:
                        await voice_client.move_to(target_channel)
                        logger.info(f"Bot moved to VC: {target_channel.name}")
                    except Exception as e:
                        logger.error(f"Failed to move to VC: {e}")
            else:
                try:
                    await target_channel.connect()
                    logger.info(f"Bot joined VC: {target_channel.name}")
                except Exception as e:
                    logger.error(f"Failed to join VC: {e}")
            return target_channel

    vcs_with_users = get_all_vcs_with_users(guild)

    if not vcs_with_users:
        if voice_client and voice_client.is_connected():
            try:
                await voice_client.disconnect()
                logger.info("Bot disconnected - all monitored VCs are empty")
            except Exception as e:
                logger.error(f"Failed to disconnect: {e}")
        return None

    target_vc = vcs_with_users[0][0]

    if voice_client and voice_client.is_connected():
        if voice_client.channel.id == target_vc.id:
            return target_vc
        try:
            await voice_client.move_to(target_vc)
            logger.info(f"Bot moved to VC: {target_vc.name}")
            return target_vc
        except Exception as e:
            logger.error(f"Failed to move to VC: {e}")
            return None
    else:
        try:
            await target_vc.connect()
            logger.info(f"Bot joined VC: {target_vc.name}")
            return target_vc
        except Exception as e:
            logger.error(f"Failed to join VC: {e}")
            return None

# ====== Bot setup and events ======
intents = discord.Intents.default()
intents.voice_states = True
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user}")
    autosave_task.start()
    check_vc.start()
    await join_voice_channel()

async def join_voice_channel():
    await bot.wait_until_ready()
    for vc_id in VC_IDS:
        vc = bot.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel):
            try:
                if vc.guild.voice_client is None:
                    await vc.connect()
                    logger.info(f"Bot joined voice channel: {vc.name}")
                else:
                    logger.info(f"Bot already in voice channel: {vc.name}")
            except Exception as e:
                logger.error(f"Failed to join VC: {e}")

@tasks.loop(seconds=300)
async def check_vc_connection():
    for vc_id in VC_IDS:
        vc = bot.get_channel(vc_id)
        if vc and isinstance(vc, discord.VoiceChannel):
            if vc.guild.voice_client is None or not vc.guild.voice_client.is_connected():
                try:
                    await vc.connect()
                    logger.info(f"Reconnected to VC: {vc.name}")
                except Exception as e:
                    logger.error(f"Failed to reconnect to VC: {e}")

@bot.event
async def on_voice_state_update(member, before, after):
    if member.id == bot.user.id:
        return

    if before.channel is None and after.channel is not None:
        if after.channel.id in VC_IDS:
            channel = bot.get_channel(VC_CHANNEL_ID)
            if channel:
                try:
                    greeting = random.choice(JOIN_GREETINGS).format(display_name=member.display_name)
                    async with aiohttp.ClientSession() as session:
                        gif_url, source, meta = await fetch_random_gif(session, member.id)
                        if gif_url:
                            await send_greeting_with_image_embed(channel, session, greeting, gif_url, member, send_to_dm=member)
                        else:
                            await channel.send(greeting)
                except Exception as e:
                    logger.error(f"Failed to send join greeting: {e}")

    elif before.channel is not None and after.channel is None:
        if before.channel.id in VC_IDS:
            channel = bot.get_channel(VC_CHANNEL_ID)
            if channel:
                try:
                    leave_msg = random.choice(LEAVE_GREETINGS).format(display_name=member.display_name)
                    async with aiohttp.ClientSession() as session:
                        gif_url, source, meta = await fetch_random_gif(session, member.id)
                        if gif_url:
                            await send_greeting_with_image_embed(channel, session, leave_msg, gif_url, member, send_to_dm=member)
                        else:
                            await channel.send(leave_msg)
                except Exception as e:
                    logger.error(f"Failed to send leave greeting: {e}")

@tasks.loop(seconds=120)
async def check_vc():
    for vc_id in VC_IDS:
        vc = bot.get_channel(vc_id)
        if not vc or not isinstance(vc, discord.VoiceChannel):
            continue

        if len(vc.members) > 1:
            channel = bot.get_channel(VC_CHANNEL_ID)
            if channel:
                try:
                    async with aiohttp.ClientSession() as session:
                        gif_url, source, meta = await fetch_random_gif(session)
                        if gif_url:
                            image_bytes, content_type = await _download_bytes_with_limit(session, gif_url)
                            if image_bytes:
                                if len(image_bytes) > DISCORD_MAX_UPLOAD:
                                    image_bytes = await compress_image(image_bytes)
                                if image_bytes and len(image_bytes) <= DISCORD_MAX_UPLOAD:
                                    ext = ".jpg"
                                    if "gif" in gif_url.lower() or (content_type and "gif" in content_type):
                                        ext = ".gif"
                                    elif "png" in gif_url.lower() or (content_type and "png" in content_type):
                                        ext = ".png"
                                    filename = f"nsfw{ext}"
                                    file = discord.File(io.BytesIO(image_bytes), filename=filename)
                                    await channel.send(file=file)
                except Exception as e:
                    logger.error(f"Failed to send in VC check: {e}")

# ====== Commands ======
@bot.command()
async def nsfw(ctx):
    async with aiohttp.ClientSession() as session:
        gif_url, source, meta = await fetch_random_gif(session, ctx.author.id)
        if gif_url:
            try:
                image_bytes, content_type = await _download_bytes_with_limit(session, gif_url)
                if image_bytes:
                    if len(image_bytes) > DISCORD_MAX_UPLOAD:
                        image_bytes = await compress_image(image_bytes)
                    if image_bytes and len(image_bytes) <= DISCORD_MAX_UPLOAD:
                        ext = ".jpg"
                        if "gif" in gif_url.lower() or (content_type and "gif" in content_type):
                            ext = ".gif"
                        elif "png" in gif_url.lower() or (content_type and "png" in content_type):
                            ext = ".png"
                        filename = f"nsfw{ext}"
                        file = discord.File(io.BytesIO(image_bytes), filename=filename)
                        await ctx.send(file=file)
                        return
            except Exception:
                pass
        await ctx.send("Failed to fetch NSFW content. Try again.")

# ====== Run ======
if not TOKEN:
    logger.error("No TOKEN env var set. Exiting.")
else:
    bot.run(TOKEN)
