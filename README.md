# Anime Welcome Bot (ECCHI-only FallBacks) — Discord Bot

A Discord bot that posts anime-styled welcome & goodbye embeds with animated GIFs.
Searches Tenor and Giphy first (if API keys provided), then falls back to multiple public booru endpoints (Danbooru, Konachan, Yande.re, Gelbooru, Rule34, OtakuGIFs where available). **Configured to only request `rating:questionable` (ecchi / lewd but NOT full nudity)** and explicitly excludes tags like `loli`, `shota`, `child`.

## Features
- Multi-VC support (list VC IDs in `VC_IDS`)
- Welcome + Goodbye embeds, DM fallback
- Tenor + Giphy (API-key support) + multiple no-key booru fallbacks
- Per-user GIF history to avoid repeats
- Data saved to `data.json`

## Setup (local)
1. Copy files to a repo or folder.
2. Create a virtualenv and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
