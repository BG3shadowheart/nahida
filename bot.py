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

DEBUG_FETCH = str(os.getenv("DEBUG_FETCH", "")).strip().lower() in ("1", "true", "yes", "on")
TRUE_RANDOM = str(os.getenv("TRUE_RANDOM", "")).strip().lower() in ("1", "true", "yes")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "14"))
DISCORD_MAX_UPLOAD = int(os.getenv("DISCORD_MAX_UPLOAD", str(8 * 1024 * 1024)))
HEAD_SIZE_LIMIT = DISCORD_MAX_UPLOAD
DATA_FILE = os.getenv("DATA_FILE", "data_nsfw.json")
AUTOSAVE_INTERVAL = int(os.getenv("AUTOSAVE_INTERVAL", "30"))
FETCH_ATTEMPTS = int(os.getenv("FETCH_ATTEMPTS", "40"))
MAX_USED_GIFS_PER_USER = int(os.getenv("MAX_USED_GIFS_PER_USER", "1000"))

VC_IDS = [int(os.getenv("VC_ID_1", "1409170559337762980"))]
VC_CHANNEL_ID = int(os.getenv("VC_CHANNEL_ID", "1371916812903780573"))

logging.basicConfig(level=logging.DEBUG if DEBUG_FETCH else logging.INFO)
logger = logging.getLogger("spiciest-nsfw")

_token_split_re = re.compile(r"[^a-z0-9]+")

ILLEGAL_TAGS = [
    "underage", "minor", "child", "loli", "shota", "young", "agegap",
    "bestiality", "zoophilia", "bestial",
    "scat", "fisting", "incest", "pedo", "pedophile"
]
FILENAME_BLOCK_KEYWORDS = ["orgy", "scat", "fisting", "bestiality"]

EXCLUDE_TAGS = [
    "loli", "shota", "child", "minor", "underage", "young", "schoolgirl", "age_gap",
    "pedo", "pedophile", "bestiality", "zoophilia", "incest",
    "futa","futanari","futanaris","futanary","futan","futanari_",
    "shemale","shemales","she-male","she_male","shemale_",
    "dickgirl","dick_girl","d-girl","dick-girl","dickgirl_",
    "femboy","femboys","femb0y",
    "trap","traps",
    "male","males","man","men","boy","boys","yaoi","gay","mm","male-male","male_male","m/m","yaoi_"
]

def _normalize_text(s: str) -> str:
    return "" if not s else re.sub(r'[\s\-_]+', ' ', s.lower())

def _tag_is_disallowed(t: str) -> bool:
    if not t:
        return True
    t = t.lower()
    for b in ILLEGAL_TAGS:
        if b in t:
            return True
    for ex in EXCLUDE_TAGS:
        if ex in t:
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

if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        json.dump({"provider_weights": {}, "sent_history": {}, "gif_tags": []}, f, indent=2)

with open(DATA_FILE, "r") as f:
    data = json.load(f)

data.setdefault("provider_weights", {})
data.setdefault("sent_history", {})
data.setdefault("gif_tags", [])

_seed_gif_tags = [
    "hentai", "porn", "sex", "oral", "anal", "cum", "cumshot", "orgasm",
    "hardcore", "milf", "big breasts", "big_breasts", "huge_breasts", "mature",
    "thick", "thighs", "ass", "booty", "panties", "stockings", "garter",
    "cleavage", "underboob", "sideboob",
    "blowjob", "paizuri", "oral_focus", "teasing", "bdsm", "bondage", "spanking",
    "wet", "oppai", "oppai_focus", "waifu", "neko", "maid", "cosplay",
    "threesome", "group", "bukkake", "nipples", "strapon", "double_penetration",
    "masturbation", "footjob", "handjob", "fingering", "cum_on_face", "facesitting",
    "pegging", "public", "group_sex", "yuri", "lesbian", "facial", "creampie"
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
    "waifu_pics": ["hentai", "blowjob", "cum", "anal", "oral"],
    "waifu_im": ["hentai", "ero", "anal", "oral", "cum", "milf"],
    "waifu_it": ["hentai", "ero", "anal", "oral", "cum"],
    "danbooru": ["hentai", "anal", "oral", "cum", "milf", "bukkake", "yuri", "lesbian"],
    "gelbooru": ["hentai", "anal", "cum", "ass", "boobs", "cleavage"],
    "konachan": ["panties", "lingerie", "thighs", "cleavage"],
    "rule34": ["hentai", "anal", "cum", "ass", "boobs"],
    "nekobot": ["hentai", "pussy", "ass", "boobs", "hentai_gif"]
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
            return gif_url, f"waifu_im_{q}", img
    except Exception:
        return None, None, None

async def fetch_from_waifu_it(session, positive):
    try:
        if not WAIFUIT_API_KEY:
            return None, None, None
        q = map_tag_for_provider("waifu_it", positive)
        endpoint = f"https://waifu.it/api/v4/{quote_plus(q)}"
        headers = {"Authorization": WAIFUIT_API_KEY}
        async with session.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("url") or payload.get("image") or (payload.get("data") and payload["data"].get("url"))
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
            return gif_url, f"waifu_it_{q}", payload
    except Exception:
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
                return None, None, None
            payload = await resp.json()
            if not payload: return None, None, None
            random.shuffle(payload)
            for item in payload:
                tags_text = item.get("tag_string", "") or item.get("tag_string_general", "")
                if _tag_is_disallowed(tags_text): continue
                gif_url = item.get("file_url") or item.get("large_file_url") or item.get("source")
                if not gif_url or filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                return gif_url, f"danbooru_{q}", item
            return None, None, None
    except Exception:
        return None, None, None

async def fetch_from_gelbooru(session, positive):
    try:
        q = map_tag_for_provider("gelbooru", positive)
        tags = f"{q} rating:explicit"
        url = "https://gelbooru.com/index.php"
        params = {"page": "dapi", "s": "post", "q": "index", "json": 1, "limit": 50, "tags": tags}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            posts = payload if isinstance(payload, list) else (payload.get("post") or payload.get("posts") or [])
            if not posts: return None, None, None
            random.shuffle(posts)
            for item in posts:
                if _tag_is_disallowed(json.dumps(item)): continue
                gif_url = item.get("file_url") or item.get("image") or item.get("preview_url")
                if not gif_url or filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                return gif_url, f"gelbooru_{q}", item
            return None, None, None
    except Exception:
        return None, None, None

async def fetch_from_konachan(session, positive):
    try:
        q = map_tag_for_provider("konachan", positive)
        tags = f"{q} rating:explicit"
        url = "https://konachan.com/post.json"
        params = {"tags": tags, "limit": 50}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            if not payload: return None, None, None
            random.shuffle(payload)
            for item in payload:
                if _tag_is_disallowed(json.dumps(item)): continue
                gif_url = item.get("file_url") or item.get("jpeg_url") or item.get("sample_url")
                if not gif_url or filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                return gif_url, f"konachan_{q}", item
            return None, None, None
    except Exception:
        return None, None, None

async def fetch_from_rule34(session, positive):
    try:
        q = map_tag_for_provider("rule34", positive)
        tags = q
        url = "https://api.rule34.xxx/index.php"
        params = {"page": "dapi", "s": "post", "q": "index", "json": 1, "limit": 50, "tags": tags}
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            if not payload: return None, None, None
            random.shuffle(payload)
            for item in payload:
                if _tag_is_disallowed(json.dumps(item)): continue
                gif_url = item.get("file_url") or item.get("image") or item.get("sample_url")
                if not gif_url or filename_has_block_keyword(gif_url): continue
                if contains_illegal_indicators(json.dumps(item) + " " + (q or "")): continue
                return gif_url, f"rule34_{q}", item
            return None, None, None
    except Exception:
        return None, None, None

async def fetch_from_nekobot(session, positive):
    try:
        q = map_tag_for_provider("nekobot", positive)
        # nekobot types: hentai, pussy, ass, boobs, hentai_gif, etc.
        url = f"https://nekobot.xyz/api/image?type={quote_plus(q)}"
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None, None, None
            payload = await resp.json()
            gif_url = payload.get("message") or payload.get("url")
            if not gif_url or filename_has_block_keyword(gif_url): return None, None, None
            if contains_illegal_indicators(json.dumps(payload) + " " + (q or "")): return None, None, None
            return gif_url, f"nekobot_{q}", payload
    except Exception:
        return None, None, None

PROVIDER_FETCHERS = {
    "waifu_pics": fetch_from_waifu_pics,
    "waifu_im": fetch_from_waifu_im,
    "waifu_it": fetch_from_waifu_it,
    "nekobot": fetch_from_nekobot,
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
    now = datetime.now(timezone.utc)
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
    providers = build_provider_pool()
    if not providers:
        if DEBUG_FETCH: logger.debug("No providers available.")
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
                if DEBUG_FETCH: logger.debug(f"fetcher exception for {provider}: {e}")
                continue
            if not gif_url:
                if DEBUG_FETCH: logger.debug(f"{provider} returned no url.")
                continue
            if filename_has_block_keyword(gif_url): continue
            if contains_illegal_indicators((gif_url or "") + " " + (str(meta) or "")): continue
            if _tag_is_disallowed(str(meta or "")): continue
            b, ctype, reason = await attempt_get_media_bytes(session, gif_url)
            if DEBUG_FETCH:
                logger.debug(f"attempt_get_media_bytes -> provider={provider} url={gif_url} reason={reason} bytes_ok={bool(b)} ctype={ctype}")
            ext = ""
            try:
                parsed = urlparse(gif_url)
                ext = os.path.splitext(parsed.path)[1] or ".gif"
                if len(ext) > 6: ext = ".gif"
            except Exception:
                ext = ".gif"
            name = f"{provider}_{hashlib.sha1(gif_url.encode()).hexdigest()[:10]}{ext}"
            return b, name, gif_url, ctype
        if DEBUG_FETCH: logger.debug("fetch_gif exhausted attempts.")
        return None, None, None, None

def try_compress_bytes(b, ctype, max_size):
    if not b or not Image:
        return None
    try:
        buf = io.BytesIO(b)
        img = Image.open(buf)
        fmt = img.format or "GIF"
        if fmt.upper() in ("GIF", "WEBP"):
            frames = [frame.copy().convert("RGBA") for frame in ImageSequence.Iterator(img)]
            w, h = frames[0].size
            for pct in [0.95 ** i for i in range(1, 13)]:
                out = io.BytesIO()
                new_size = (max(1, int(w * pct)), max(1, int(h * pct)))
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
            for pct in [0.95 ** i for i in range(1, 13)]:
                out = io.BytesIO()
                new_size = (max(1, int(w * pct)), max(1, int(h * pct)))
                img2 = img.resize(new_size, Image.LANCZOS)
                if fmt.upper() in ("JPEG", "JPG"):
                    img2.save(out, format="JPEG", quality=85, optimize=True)
                else:
                    img2.save(out, format="PNG", optimize=True)
                if out.getbuffer().nbytes <= max_size:
                    return out.getvalue()
            return None
    except Exception:
        return None

def make_embed(title, desc, member, kind="nsfw", count=None):
    color = discord.Color.dark_red() if kind == "nsfw" else discord.Color.dark_gray()
    embed = discord.Embed(title=title, description=desc, color=color, timestamp=datetime.now(timezone.utc))
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    footer = f"{member.display_name} • {member.id}"
    if count:
        footer += f" • Joins: {count}"
    embed.set_footer(text=footer)
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

async def send_embed_with_media(text_channel, member, embed, gif_bytes, gif_name, gif_url, ctype=None):
    max_upload = DISCORD_MAX_UPLOAD
    sent_success = False
    try:
        if gif_bytes and len(gif_bytes) <= max_upload:
            try:
                file_server = discord.File(io.BytesIO(gif_bytes), filename=gif_name)
                embed.set_image(url=f"attachment://{gif_name}")
                if text_channel:
                    await text_channel.send(embed=embed, file=file_server)
                sent_success = True
            except Exception:
                if text_channel:
                    if gif_url and gif_url not in (embed.description or ""):
                        embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
                    await text_channel.send(embed=embed)
                    sent_success = True
            try:
                dm_file = discord.File(io.BytesIO(gif_bytes), filename=gif_name)
                await member.send(embed=embed, file=dm_file)
            except Exception:
                try:
                    dm_embed = make_embed(embed.title or "Media", embed.description or "", member, kind="nsfw")
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
                        if text_channel:
                            await text_channel.send(embed=embed, file=file_server)
                        sent_success = True
                    except Exception:
                        if text_channel:
                            if gif_url and gif_url not in (embed.description or ""):
                                embed.description = (embed.description or "") + f"\n\n[View media here]({gif_url})"
                            await text_channel.send(embed=embed)
                            sent_success = True
                    try:
                        dm_file = discord.File(io.BytesIO(compressed), filename=gif_name)
                        await member.send(embed=embed, file=dm_file)
                    except Exception:
                        try:
                            dm_embed = make_embed(embed.title or "Media", embed.description or "", member, kind="nsfw")
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
            if text_channel:
                await text_channel.send(embed=embed)
                sent_success = True
            try:
                dm_embed = make_embed(embed.title or "Media", embed.description or "", member, kind="nsfw")
                if gif_url and gif_url not in (dm_embed.description or ""):
                    dm_embed.description = (dm_embed.description or "") + f"\n\n[View media here]({gif_url})"
                await member.send(dm_embed)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"unexpected error in send_embed_with_media: {e}")
        try:
            if text_channel:
                await text_channel.send(embed=embed)
                sent_success = True
            await member.send(embed=embed)
        except Exception:
            pass
    if sent_success and gif_url:
        await record_sent_for_user(member.id, gif_url)

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

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

@tasks.loop(seconds=30)
async def ensure_connected_task():
    try:
        if not VC_IDS:
            return
        target_vc_id = VC_IDS[0]
        for guild in bot.guilds:
            target_channel = guild.get_channel(target_vc_id)
            if not target_channel:
                continue
            vc = discord.utils.get(bot.voice_clients, guild=guild)
            if vc:
                if vc.channel and vc.channel.id == target_vc_id:
                    continue
                else:
                    try:
                        await vc.move_to(target_channel)
                    except Exception:
                        try:
                            await vc.disconnect()
                        except Exception:
                            pass
                        try:
                            await target_channel.connect(reconnect=True)
                        except Exception:
                            if DEBUG_FETCH: logger.debug(f"failed to reconnect after move failure")
            else:
                try:
                    await target_channel.connect(reconnect=True)
                except Exception:
                    if DEBUG_FETCH: logger.debug(f"connect attempt failed")
    except Exception as e:
        if DEBUG_FETCH: logger.debug(f"ensure_connected_task unexpected: {e}")

@bot.event
async def on_ready():
    try:
        autosave_task.start()
    except RuntimeError:
        pass
    try:
        if not ensure_connected_task.is_running():
            ensure_connected_task.start()
    except RuntimeError:
        pass
    available = []
    for p in PROVIDER_FETCHERS.keys():
        key_ok = True
        if p == "waifu_it" and not WAIFUIT_API_KEY:
            key_ok = False
        if p == "danbooru" and (not DANBOORU_API_KEY or not DANBOORU_USER):
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

    if after.channel and (after.channel.id in VC_IDS) and (before.channel != after.channel):
        try:
            vc = discord.utils.get(bot.voice_clients, guild=member.guild)
            if vc:
                if vc.channel.id != after.channel.id:
                    try:
                        await vc.move_to(after.channel)
                    except Exception:
                        pass
            else:
                try:
                    await after.channel.connect()
                except Exception:
                    pass
        except Exception:
            pass

        raw = random.choice(JOIN_GREETINGS)
        msg = raw.format(display_name=member.display_name)
        data["join_counts"] = data.get("join_counts", {})
        data["join_counts"][str(member.id)] = data["join_counts"].get(str(member.id), 0) + 1
        embed = make_embed("Welcome!", msg, member, "nsfw", data["join_counts"][str(member.id)])
        gif_bytes, gif_name, gif_url, ctype = await fetch_gif(member.id)
        await send_embed_with_media(text_channel, member, embed, gif_bytes, gif_name, gif_url, ctype)

    if before.channel and (before.channel.id in VC_IDS) and (after.channel != before.channel):
        raw = random.choice(LEAVE_GREETINGS)
        msg = raw.format(display_name=member.display_name)
        embed = make_embed("Goodbye!", msg, member, "nsfw")
        gif_bytes, gif_name, gif_url, ctype = await fetch_gif(member.id)
        await send_embed_with_media(text_channel, member, embed, gif_bytes, gif_name, gif_url, ctype)

@bot.command(name="nsfw", aliases=["nude","hentai"])
@commands.cooldown(1, 3, commands.BucketType.user)
async def nsfw(ctx):
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
    await ctx.send("Available NSFW seed tags: " + ", ".join(GIF_TAGS[:80]))

if __name__ == "__main__":
    if not TOKEN:
        logger.error("TOKEN missing. Set TOKEN and restart.")
    else:
        bot.run(TOKEN)
