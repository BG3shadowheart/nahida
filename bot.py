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
    "🥀 {display_name} joined — don't disappoint.",
    "🧠 {display_name} stepped in. I'm watching.",
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
    "🔥 {display_name} entered — don't blink.",
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
            images = payload.get("images", [])
            if not images: return None, None, None
            img = random.choice(images)
            gif_url = img.get("url")
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
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (category or "")): return None, None, None
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
            if not images: return None, None, None
            img = images[0]
            img_id = img.get("id")
            if not img_id: return None, None, None
            gif_url = f"https://nekos.moe/image/{img_id}.jpg"
            if contains_illegal_indicators(json.dumps(img)): return None, None, None
            extract_and_add_tags_from_meta(" ".join(img.get("tags", [])), GIF_TAGS, data)
            return gif_url, "nekos_moe", img
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

async def send_image_as_file(channel, session, image_url, send_to_dm=None):
    try:
        image_bytes, content_type = await _download_bytes_with_limit(session, image_url)
        if not image_bytes or len(image_bytes) > DISCORD_MAX_UPLOAD:
            if image_bytes and len(image_bytes) > DISCORD_MAX_UPLOAD:
                image_bytes = await compress_image(image_bytes)
            if not image_bytes or len(image_bytes) > DISCORD_MAX_UPLOAD:
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
        await channel.send(file=file)
        
        if send_to_dm:
            try:
                file2 = discord.File(io.BytesIO(image_bytes), filename=filename)
                await send_to_dm.send(file=file2)
                logger.info(f"Sent DM to {send_to_dm.name}")
            except Exception as e:
                logger.warning(f"Could not DM: {e}")
        
        logger.info(f"Sent image as file: {filename}")
    except Exception as e:
        logger.error(f"Failed to send as file: {e}")

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
                    embed = discord.Embed(description=f"**{greeting}**", color=discord.Color.dark_purple())
                    embed.set_thumbnail(url=member.display_avatar.url)
                    await channel.send(embed=embed)
                    logger.info(f"Sent greeting to {member.name}")
                    
                    async with aiohttp.ClientSession() as session:
                        gif_url, source, meta = await fetch_random_gif(session, member.id)
                        if gif_url:
                            await send_image_as_file(channel, session, gif_url, send_to_dm=member)
                            logger.info(f"Sent welcome NSFW content from {source}")
                except Exception as e:
                    logger.error(f"Failed to send greeting: {e}")
    
    elif before.channel is not None and after.channel is None:
        if before.channel.id in VC_IDS:
            channel = bot.get_channel(VC_CHANNEL_ID)
            if channel:
                try:
                    leave_msg = random.choice(LEAVE_GREETINGS).format(display_name=member.display_name)
                    await channel.send(leave_msg)
                    logger.info(f"Sent leave message for {member.name}")
                except Exception as e:
                    logger.error(f"Failed to send leave message: {e}")

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
                            await send_image_as_file(channel, session, gif_url)
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
            await send_image_as_file(ctx.channel, session, gif_url)
        else:
            await ctx.send("Failed to fetch NSFW content. Try again.")

bot.run(TOKEN)
