import os
import io
import json
import random
import hashlib
import logging
import re
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
AUTOSAVE_INTERVAL = int(os.getenv("AUTOSAVE_INTERVAL", "30"))
FETCH_ATTEMPTS = int(os.getenv("FETCH_ATTEMPTS", "40"))
MAX_USED_GIFS_PER_USER = int(os.getenv("MAX_USED_GIFS_PER_USER", "1000"))

VC_IDS = [
    int(os.getenv("VC_ID_1", "1409170559337762980")),
]
VC_CHANNEL_ID = int(os.getenv("VC_CHANNEL_ID", "1371916812903780573"))
GREETING_MESSAGE = os.getenv("GREETING_MESSAGE", "🔥 Welcome to the NSFW zone! Enjoy the content 🔥")

logging.basicConfig(level=logging.DEBUG if DEBUG_FETCH else logging.INFO)
logger = logging.getLogger("spiciest-nsfw")

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
    "hardcore", "milf", "mature", "big_breasts", "huge_breasts", "gigantic_breasts",
    "oppai", "ass", "booty", "big_ass", "huge_ass", "thick_thighs", "thicc",
    "blowjob", "fellatio", "paizuri", "titjob", "boobjob", "pussy", "vagina",
    "breasts", "tits", "boobs", "nipples", "nude", "naked", "completely_nude",
    "lingerie", "panties", "stockings", "thighhighs", "garter_belt", "bikini",
    "cleavage", "underboob", "sideboob", "nsfw", "explicit", "xxx",
    "threesome", "group_sex", "gangbang", "orgy", "bukkake", "creampie",
    "nakadashi", "internal_cumshot", "ahegao", "orgasm_face", "pleasure_face",
    "bdsm", "bondage", "rope", "bound", "restrained", "shibari",
    "masturbation", "fingering", "self_pleasure", "dildo", "vibrator", "sex_toy",
    "footjob", "handjob", "boobjob", "assjob", "thighjob", "hotdogging",
    "facesitting", "sitting_on_face", "cunnilingus", "pussy_licking",
    "yuri", "lesbian", "girl_on_girl", "female_only", "girls_only",
    "double_penetration", "dp", "anal_penetration", "vaginal_penetration",
    "facial", "cum_on_face", "cum_on_body", "cum_on_breasts", "cum_on_ass",
    "wet", "sweaty", "oiled", "shiny_skin", "body_oil",
    "neko", "catgirl", "cat_ears", "animal_ears", "tail",
    "maid", "nurse", "teacher", "secretary", "office_lady",
    "swimsuit", "one_piece_swimsuit", "string_bikini", "micro_bikini",
    "thong", "g_string", "lace", "see_through", "transparent",
    "spread_legs", "open_legs", "legs_spread", "m_legs",
    "bent_over", "all_fours", "doggy_style", "from_behind",
    "cowgirl", "girl_on_top", "reverse_cowgirl", "riding",
    "missionary", "mating_press", "legs_up", "piledriver",
    "standing_sex", "against_wall", "pinned", "held_down",
    "squirting", "female_ejaculation", "pussy_juice", "love_juice",
    "saliva", "drool", "tongue", "tongue_out", "licking",
    "kissing", "french_kiss", "deep_kiss", "saliva_trail",
    "spanking", "slap", "grabbing", "groping", "breast_grab", "ass_grab",
    "nipple_play", "nipple_tweak", "breast_sucking", "lactation",
    "public", "exhibitionism", "outside", "outdoor", "public_nudity",
    "school_uniform", "sailor_uniform", "serafuku", "gym_uniform",
    "torn_clothes", "clothes_rip", "wardrobe_malfunction",
    "no_bra", "no_panties", "commando", "bottomless", "topless",
    "shower", "bath", "bathing", "onsen", "hot_spring", "pool",
    "bedroom", "bed", "lying", "on_back", "on_stomach",
    "armpits", "armpit_focus", "navel", "midriff", "stomach",
    "clitoris", "pussy_focus", "ass_focus", "breast_focus",
    "pov", "first_person", "viewer", "looking_at_viewer",
    "seductive", "seductive_smile", "bedroom_eyes", "inviting",
    "aroused", "horny", "lustful", "desire", "passionate",
    "moaning", "panting", "blushing", "embarrassed", "shy",
    "multiple_girls", "2girls", "3girls", "4girls", "5girls",
    "large_insertion", "excessive_cum", "cum_overflow", "stomach_bulge",
    "x_ray", "internal", "cross_section", "uterus",
    "censored", "uncensored", "mosaic_censoring", "convenient_censoring",
    "after_sex", "afterglow", "exhausted", "satisfied",
    "hair_pull", "hair_grab", "neck_grab", "choking",
    "collar", "leash", "pet_play", "slave", "submissive", "dominant",
    "blindfold", "gag", "ball_gag", "tape_gag",
    "office", "desk", "chair_sex", "table_sex",
    "panty_pull", "bra_pull", "shirt_lift", "skirt_lift",
    "strip", "stripping", "undressing", "revealing",
    "monster_girl", "demon_girl", "succubus", "angel", "elf",
    "dark_skin", "tan", "gyaru", "tanned", "dark_elf",
    "blonde", "brunette", "redhead", "pink_hair", "purple_hair",
    "twintails", "ponytail", "long_hair", "short_hair",
    "glasses", "megane", "teacher_glasses",
    "pregnant", "impregnation", "breeding", "fertilization",
    "mind_break", "mind_control", "hypnosis", "drugged",
    "rape", "forced", "non_consensual", "sexual_assault",
    "tentacle", "tentacles", "tentacle_sex", "monster",
    "slime", "slime_girl", "wet_and_messy",
    "inflation", "belly_inflation", "cum_inflation",
    "piercing", "nipple_piercing", "navel_piercing", "tongue_piercing",
    "tattoo", "body_writing", "womb_tattoo", "crest",
    "wings", "horns", "demon_horns", "halo",
    "fishnet", "fishnet_stockings", "fishnet_top",
    "high_heels", "stiletto", "boots", "thigh_boots",
    "gloves", "elbow_gloves", "latex", "leather",
    "naked_apron", "naked_towel", "towel_around_body",
    "morning_sex", "night_sex", "sleeping", "wake_up_sex",
    "69", "sixty_nine", "mutual_oral",
    "tribadism", "scissoring", "clit_rubbing",
    "fingering_partner", "finger_in_pussy", "finger_in_ass",
    "strap_on", "pegging", "double_dildo",
    "squatting", "squatting_cowgirl", "crouching",
    "waifu", "fanservice", "teasing"
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

PROVIDER_TERMS = {
    "waifu_pics": ["waifu", "neko", "blowjob"],
    "waifu_im": ["ero", "hentai", "ass", "hass", "hmidriff", "oppai", "hthigh", "paizuri", "ecchi", "selfies"],
    "hmtai": ["hentai", "anal", "ass", "bdsm", "cum", "boobs", "thighs", "pussy", "ahegao", "uniform", "gangbang", "tentacles", "gif", "nsfwNeko", "ero", "yuri", "panties", "zettaiRyouiki"],
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

async def _head_url(session, url, timeout=REQUEST_TIMEOUT):
    try:
        async with session.head(url, timeout=timeout, allow_redirects=True) as resp:
            return resp.status, dict(resp.headers)
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"HEAD failed for {url}: {e}")
        return None, {}

async def _download_bytes_with_limit(session, url, size_limit=HEAD_SIZE_LIMIT, timeout=REQUEST_TIMEOUT):
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            if resp.status != 200:
                if DEBUG_FETCH: logger.debug(f"GET {url} returned {resp.status}")
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
                    if DEBUG_FETCH: logger.debug(f"download exceeded limit {size_limit} for {url}")
                    return None, ctype
            return b"".join(chunks), ctype
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"GET exception for {url}: {e}")
        return None, None

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
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")): return None, None, None
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
            images = payload.get("images") or payload.get("data") or []
            if not images: return None, None, None
            img = random.choice(images)
            gif_url = img.get("url") or img.get("image") or img.get("src")
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(img) + " " + (q or "")): return None, None, None
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
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")): return None, None, None
            extract_and_add_tags_from_meta(json.dumps(payload), GIF_TAGS, data)
            return gif_url, f"hmtai_{category}", payload
    except Exception:
        return None, None, None

async def fetch_from_danbooru(session, positive):
    try:
        blocked_str = " ".join([f"-{b}" for b in BLOCKED_TAGS])
        tags = f"{positive} rating:explicit {blocked_str} 1girl".strip()
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
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url") or post.get("large_file_url")
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(post)): return None, None, None
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
        if GELBOORU_API_KEY:
            params["api_key"] = GELBOORU_API_KEY
        async with session.get(base, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            posts = payload.get("post", [])
            if not posts: return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(post)): return None, None, None
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
            if not posts or not isinstance(posts, list): return None, None, None
            post = random.choice(posts)
            gif_url = post.get("file_url")
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(post)): return None, None, None
            extract_and_add_tags_from_meta(post.get("tags", ""), GIF_TAGS, data)
            return gif_url, f"rule34_{positive}", post
    except Exception:
        return None, None, None

PROVIDERS = [
    ("hmtai", fetch_from_hmtai, 30),
    ("rule34", fetch_from_rule34, 25),
    ("waifu_im", fetch_from_waifu_im, 20),
    ("danbooru", fetch_from_danbooru, 15),
    ("gelbooru", fetch_from_gelbooru, 15),
    ("waifu_pics", fetch_from_waifu_pics, 10),
]

def _hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def _choose_random_provider():
    if TRUE_RANDOM:
        return random.choice(PROVIDERS)
    else:
        weights = [w for _, _, w in PROVIDERS]
        return random.choices(PROVIDERS, weights=weights, k=1)[0]

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

intents = discord.Intents.default()
intents.voice_states = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user}")
    autosave_task.start()
    check_vc.start()

@bot.event
async def on_voice_state_update(member, before, after):
    if before.channel is None and after.channel is not None:
        if after.channel.id in VC_IDS:
            channel = bot.get_channel(VC_CHANNEL_ID)
            if channel:
                try:
                    await channel.send(f"{GREETING_MESSAGE} {member.mention}")
                    logger.info(f"Sent greeting to {member.name}")
                    
                    async with aiohttp.ClientSession() as session:
                        gif_url, source, meta = await fetch_random_gif(session, member.id)
                        if gif_url:
                            await channel.send(gif_url)
                            logger.info(f"Sent welcome NSFW content from {source}")
                except Exception as e:
                    logger.error(f"Failed to send greeting: {e}")

@tasks.loop(seconds=60)
async def check_vc():
    for vc_id in VC_IDS:
        vc = bot.get_channel(vc_id)
        if not vc or not isinstance(vc, discord.VoiceChannel):
            continue
        
        if len(vc.members) > 0:
            channel = bot.get_channel(VC_CHANNEL_ID)
            if channel:
                try:
                    async with aiohttp.ClientSession() as session:
                        gif_url, source, meta = await fetch_random_gif(session)
                        if gif_url:
                            await channel.send(gif_url)
                            logger.info(f"Sent NSFW content from {source}")
                        else:
                            logger.warning("Failed to fetch GIF for VC check")
                except Exception as e:
                    logger.error(f"Failed to send in VC check: {e}")

@bot.command()
async def nsfw(ctx):
    async with aiohttp.ClientSession() as session:
        gif_url, source, meta = await fetch_random_gif(session, ctx.author.id)
        if gif_url:
            await ctx.send(gif_url)
        else:
            await ctx.send("Failed to fetch NSFW content. Try again.")

bot.run(TOKEN)
