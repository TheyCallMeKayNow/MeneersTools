import os
import io
import json
import asyncio
import datetime
import aiohttp
import discord
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()

# ── Optional AI features (grammar fix + summarize) ───────────────────────────
try:
    from google import genai
    _ai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    AI_AVAILABLE = True
except Exception:
    _ai_client = None
    AI_AVAILABLE = False

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing from .env")

intents = discord.Intents.none()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ── Persistent notes store: {user_id: [...]} ─────────────────────────────────
_notes: dict[int, list[dict]] = {}

# ── LCPDFR config & state ─────────────────────────────────────────────────────
LCPDFR_CONFIG_FILE = "lcpdfr_config.json"
LCPDFR_STATE_FILE  = "lcpdfr_state.json"
LCPDFR_API         = "https://www.lcpdfr.com/applications/downloadsng/interface/api.php"

def _load_json(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    except Exception as e:
        print(f"[LCPDFR] Failed to load {path}: {e}")
    return default

def _save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

_lcpdfr_config: dict = _load_json(LCPDFR_CONFIG_FILE, {
    "channel_id":        None,
    "check_interval_s":  600,
    "plugins":           []   # [{"id": "7792", "name": "LSPDFR"}, ...]
})
_lcpdfr_state: dict = _load_json(LCPDFR_STATE_FILE, {"versions": {}})
_poll_task: asyncio.Task | None = None

# ── GitHub Releases config & state ────────────────────────────────────────────
GH_CONFIG_FILE = "gh_config.json"
GH_STATE_FILE  = "gh_state.json"

_gh_config: dict = _load_json(GH_CONFIG_FILE, {
    "repos": []  # [{"owner": "owner", "repo": "repo", "name": "Friendly Name"}, ...]
})
_gh_state: dict = _load_json(GH_STATE_FILE, {"releases": {}})  # {"owner/repo": "v1.2.3"}
_gh_poll_task: asyncio.Task | None = None

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _snowflake_to_dt(snowflake_id: int) -> datetime.datetime:
    discord_epoch = 1420070400000
    ms = (snowflake_id >> 22) + discord_epoch
    return datetime.datetime.utcfromtimestamp(ms / 1000).replace(
        tzinfo=datetime.timezone.utc
    )

def _human_delta(dt: datetime.datetime) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - dt
    years, remainder = divmod(delta.days, 365)
    months = remainder // 30
    days = remainder % 30
    parts = []
    if years:
        parts.append(f"{years} year{'s' if years != 1 else ''}")
    if months:
        parts.append(f"{months} month{'s' if months != 1 else ''}")
    if not parts:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    return ", ".join(parts)

def _badge_names(flags: discord.PublicUserFlags) -> list[str]:
    mapping = {
        "staff": "Discord Staff",
        "partner": "Partnered Server Owner",
        "hypesquad": "HypeSquad Events",
        "bug_hunter": "Bug Hunter (Level 1)",
        "hypesquad_bravery": "HypeSquad Bravery",
        "hypesquad_brilliance": "HypeSquad Brilliance",
        "hypesquad_balance": "HypeSquad Balance",
        "early_supporter": "Early Supporter",
        "bug_hunter_level_2": "Bug Hunter (Level 2)",
        "verified_bot_developer": "Early Verified Bot Developer",
        "discord_certified_moderator": "Certified Moderator",
        "active_developer": "Active Developer",
    }
    return [label for attr, label in mapping.items() if getattr(flags, attr, False)]

async def _ai_call(system: str, user_content: str) -> str:
    if not AI_AVAILABLE:
        return "⚠️ AI features are unavailable (GEMINI_API_KEY not set)."
    loop = asyncio.get_event_loop()
    def _sync():
        response = _ai_client.models.generate_content(
            model="gemini-3.6-flash",
            contents=f"{system}\n\n{user_content}",
        )
        return response.text
    return await loop.run_in_executor(None, _sync)

# ─────────────────────────────────────────────────────────────────────────────
# Discord interaction helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True) -> bool:
    """
    Defer an interaction with retries for temporary Discord 5xx errors.
    Returns True when the interaction was successfully deferred.
    """
    for attempt in range(3):
        try:
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except discord.DiscordServerError as e:
            print(
                f"[Discord] Failed to defer interaction "
                f"(attempt {attempt + 1}/3): {e}"
            )
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
        except (discord.HTTPException, aiohttp.ClientError) as e:
            print(
                f"[Discord] HTTP/network error while deferring interaction "
                f"(attempt {attempt + 1}/3): {e}"
            )
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))

    return False


# ─────────────────────────────────────────────────────────────────────────────
# LCPDFR helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_plugin(session: aiohttp.ClientSession, file_id: str) -> dict | None:
    """Fetch version + metadata for a plugin from LCPDFR. Returns dict or None."""
    headers = {"User-Agent": "LCPDFR-Discord-Bot/2.0"}
    try:
        # The checkForUpdates endpoint returns JSON: {"error": false, "version": "x.y.z", "pageUrl": "..."}
        ver_url = f"{LCPDFR_API}?do=checkForUpdates&fileId={file_id}"
        async with session.get(ver_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            raw = (await r.text()).strip()
            try:
                ver_data = json.loads(raw)
            except Exception:
                ver_data = {}

            # API may return JSON with a version field, or a bare version string
            if isinstance(ver_data, dict):
                if ver_data.get("error"):
                    print(f"[LCPDFR] API error for {file_id}: {ver_data}")
                    return None
                version = ver_data.get("version", "").strip()
                page_url = ver_data.get("pageUrl", "").strip()
            else:
                # Fallback: bare version string
                version = raw
                page_url = ""

        if not version:
            return None

        # Derive a clean URL: prefer pageUrl from the API, fall back to standard pattern
        plugin_url = page_url or f"https://www.lcpdfr.com/files/file/{file_id}/"

        return {
            "version":     version,
            "title":       f"Plugin #{file_id}",   # pageUrl has no title; name comes from /watch
            "url":         plugin_url,
            "description": None,
            "author":      None,
            "thumbnail":   None,
        }
    except Exception as e:
        print(f"[LCPDFR] Error fetching plugin {file_id}: {e}")
        return None

def _build_update_embed(plugin: dict, data: dict, previous_version: str) -> discord.Embed:
    label = plugin.get("name") or data["title"] or plugin["id"]

    description = f"[Download Page]({data['url']})"
    if data.get("description"):
        body = re.sub(r"<[^>]+>", "", data["description"]).strip()
        if len(body) > 300:
            body = body[:300] + "…"
        if body:
            description += f"\n\n{body}"

    embed = discord.Embed(
        title=label,
        colour=0x00AE86,
        description=description,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )
    embed.add_field(name="New Version",      value=f"`{data['version']}`",                inline=True)
    embed.add_field(name="Previous Version", value=f"`{previous_version or 'Unknown'}`",  inline=True)
    if data.get("author"):
        embed.add_field(name="Author", value=data["author"], inline=True)
    if data.get("thumbnail"):
        embed.set_thumbnail(url=data["thumbnail"])
    embed.set_footer(text="LCPDFR Update Tracker")
    return embed

import re  # needed for strip tags above — imported here to keep near usage

async def _run_update_check(notify: bool = True) -> int:
    """
    Poll all watched plugins. If notify=True and a change is found, post to channel.
    Returns the number of updates found.
    """
    plugins = _lcpdfr_config.get("plugins", [])
    if not plugins:
        return 0

    updates = 0
    channel_id = _lcpdfr_config.get("channel_id")
    channel = None
    if notify and channel_id:
        try:
            channel = await client.fetch_channel(int(channel_id))
        except Exception as e:
            print(f"[LCPDFR] Could not fetch notification channel {channel_id}: {e}")

    async with aiohttp.ClientSession() as session:
        for plugin in plugins:
            file_id = plugin["id"]
            data = await _fetch_plugin(session, file_id)
            if not data or not data["version"]:
                await asyncio.sleep(1)
                continue

            previous = _lcpdfr_state["versions"].get(file_id)

            if previous is None:
                print(f"[LCPDFR] {plugin.get('name', file_id)}: initial version {data['version']}")
                _lcpdfr_state["versions"][file_id] = data["version"]
            elif data["version"] != previous:
                print(f"[LCPDFR] {plugin.get('name', file_id)}: {previous} → {data['version']}")
                updates += 1
                _lcpdfr_state["versions"][file_id] = data["version"]
                if notify and channel:
                    try:
                        embed = _build_update_embed(plugin, data, previous)
                        await channel.send(embed=embed)
                    except Exception as e:
                        print(f"[LCPDFR] Failed to send update embed: {e}")
                elif notify and not channel:
                    print(f"[LCPDFR] Update detected but channel is None — not set or unreachable")
            else:
                print(f"[LCPDFR] {plugin.get('name', file_id)}: up-to-date ({data['version']})")

            await asyncio.sleep(1.5)

    _save_json(LCPDFR_STATE_FILE, _lcpdfr_state)
    return updates

async def _poll_loop():
    """Background task: check for updates on the configured interval."""
    await client.wait_until_ready()
    while not client.is_closed():
        interval = _lcpdfr_config.get("check_interval_s", 600)
        print(f"[LCPDFR] Next check in {interval}s…")
        await asyncio.sleep(interval)
        print(f"[LCPDFR] Running scheduled check…")
        await _run_update_check(notify=True)

def _restart_poll_loop():
    global _poll_task
    if _poll_task and not _poll_task.done():
        _poll_task.cancel()
    _poll_task = client.loop.create_task(_poll_loop())

# ─────────────────────────────────────────────────────────────────────────────
# GitHub Releases helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_latest_gh_release(session: aiohttp.ClientSession, owner: str, repo: str) -> dict | None:
    """Fetch the latest GitHub release for owner/repo. Returns dict or None."""
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "LCPDFR-Discord-Bot/2.0",
    }
    gh_token = os.getenv("GITHUB_TOKEN")
    if gh_token:
        headers["Authorization"] = f"Bearer {gh_token}"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 404:
                return None
            if r.status == 403:
                print(f"[GitHub] Rate-limited fetching {owner}/{repo}")
                return None
            if r.status != 200:
                print(f"[GitHub] HTTP {r.status} fetching {owner}/{repo}")
                return None
            data = await r.json()
    except Exception as e:
        print(f"[GitHub] Error fetching {owner}/{repo}: {e}")
        return None

    tag = data.get("tag_name", "").strip()
    if not tag:
        return None

    return {
        "tag":        tag,
        "name":       data.get("name") or tag,
        "url":        data.get("html_url", f"https://github.com/{owner}/{repo}/releases/latest"),
        "body":       (data.get("body") or "").strip()[:300],
        "prerelease": data.get("prerelease", False),
        "published":  data.get("published_at", ""),
    }


def _build_gh_embed(entry: dict, release: dict, previous_tag: str) -> discord.Embed:
    label = entry.get("name") or f"{entry['owner']}/{entry['repo']}"
    release_name = release.get("name") or release["tag"]

    # Build description: "New Release" hyperlink, then body text beneath
    hyperlink = f"[New Release]({release['url']})"
    if release.get("prerelease"):
        hyperlink += " ⚠️ Pre-release"

    body = (release.get("body") or "").strip()
    if len(body) > 300:
        body = body[:300] + "…"

    description = hyperlink
    if body:
        description += f"\n\n{body}"

    embed = discord.Embed(
        title=label,
        colour=0x238636,
        description=description,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )
    embed.add_field(name="New Tag",      value=f"`{release['tag']}`",                inline=True)
    embed.add_field(name="Previous Tag", value=f"`{previous_tag or 'Unknown'}`",     inline=True)
    if release_name != release["tag"]:
        embed.add_field(name="Release Name", value=release_name, inline=True)
    embed.set_footer(text=f"GitHub • {entry['owner']}/{entry['repo']}")
    return embed


async def _run_gh_update_check(notify: bool = True) -> int:
    """Poll all watched GitHub repos. Returns the number of new releases found."""
    repos = _gh_config.get("repos", [])
    if not repos:
        return 0

    updates = 0
    channel_id = _lcpdfr_config.get("channel_id")
    channel = None
    if notify and channel_id:
        try:
            channel = await client.fetch_channel(int(channel_id))
        except Exception as e:
            print(f"[GitHub] Could not fetch notification channel {channel_id}: {e}")

    async with aiohttp.ClientSession() as session:
        for entry in repos:
            owner, repo = entry["owner"], entry["repo"]
            key = f"{owner}/{repo}"
            release = await _fetch_latest_gh_release(session, owner, repo)
            if not release:
                await asyncio.sleep(1)
                continue

            previous = _gh_state["releases"].get(key)

            if previous is None:
                print(f"[GitHub] {key}: initial tag {release['tag']}")
                _gh_state["releases"][key] = release["tag"]
            elif release["tag"] != previous:
                print(f"[GitHub] {key}: {previous} → {release['tag']}")
                updates += 1
                _gh_state["releases"][key] = release["tag"]
                if notify and channel:
                    try:
                        embed = _build_gh_embed(entry, release, previous)
                        await channel.send(embed=embed)
                    except Exception as e:
                        print(f"[GitHub] Failed to send update embed: {e}")
                elif notify and not channel:
                    print(f"[GitHub] Update detected but channel is None — not set or unreachable")
            else:
                print(f"[GitHub] {key}: up-to-date ({release['tag']})")

            await asyncio.sleep(1.5)

    _save_json(GH_STATE_FILE, _gh_state)
    return updates


async def _gh_poll_loop():
    """Background task: check GitHub releases on the same interval as LCPDFR."""
    await client.wait_until_ready()
    while not client.is_closed():
        interval = _lcpdfr_config.get("check_interval_s", 600)
        print(f"[GitHub] Next check in {interval}s…")
        await asyncio.sleep(interval)
        print(f"[GitHub] Running scheduled check…")
        await _run_gh_update_check(notify=True)


def _restart_gh_poll_loop():
    global _gh_poll_task
    if _gh_poll_task and not _gh_poll_task.done():
        _gh_poll_task.cancel()
    _gh_poll_task = client.loop.create_task(_gh_poll_loop())


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE CONTEXT MENUS
# ─────────────────────────────────────────────────────────────────────────────

@tree.context_menu(name="Get Raw Text")
async def get_raw_text(interaction: discord.Interaction, message: discord.Message):
    content = message.content or ""
    if not content:
        content = "[This message has no text content.]"
    if len(content) <= 1900:
        await interaction.response.send_message(f"```text\n{content}\n```", ephemeral=True)
        return
    data = content.encode("utf-8")
    file = discord.File(io.BytesIO(data), filename="raw_message.txt")
    await interaction.response.send_message("Raw message text:", file=file, ephemeral=True)


@tree.context_menu(name="Fix Grammar / Spelling")
async def fix_grammar(interaction: discord.Interaction, message: discord.Message):
    content = message.content or ""
    if not content.strip():
        await interaction.response.send_message(
            "⚠️ This message has no text to fix.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    result = await _ai_call(
        system=(
            "You are a grammar and spelling corrector. "
            "Return ONLY the corrected text — no explanations, no preamble, no quotes. "
            "Preserve the original tone, formatting, and language."
        ),
        user_content=content,
    )
    embed = discord.Embed(title="✏️ Grammar Fix", colour=0x5865F2)
    embed.add_field(name="Original",  value=content[:1024], inline=False)
    embed.add_field(name="Corrected", value=result[:1024],  inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.context_menu(name="Summarize Message")
async def summarize_message(interaction: discord.Interaction, message: discord.Message):
    content = message.content or ""
    if not content.strip():
        await interaction.response.send_message(
            "⚠️ This message has no text to summarize.", ephemeral=True
        )
        return
    if len(content) < 100:
        await interaction.response.send_message(
            "⚠️ Message is too short to summarize.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    result = await _ai_call(
        system=(
            "You are a concise summarizer. "
            "Summarize the user's message in 1–3 short sentences. "
            "Return ONLY the summary — no preamble."
        ),
        user_content=content,
    )
    embed = discord.Embed(title="📋 Summary", description=result, colour=0x57F287)
    embed.set_footer(text=f"Original was {len(content)} characters")
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.context_menu(name="Was This Edited?")
async def was_edited(interaction: discord.Interaction, message: discord.Message):
    sent_at   = message.created_at.replace(tzinfo=datetime.timezone.utc)
    edited_at = message.edited_at

    embed = discord.Embed(title="🕵️ Edit History", colour=0xFEE75C)
    embed.add_field(
        name="Sent at",
        value=f"<t:{int(sent_at.timestamp())}:F>",
        inline=True,
    )
    if edited_at:
        edited_at = edited_at.replace(tzinfo=datetime.timezone.utc)
        embed.add_field(
            name="Last edited at",
            value=f"<t:{int(edited_at.timestamp())}:F>",
            inline=True,
        )
        delta        = edited_at - sent_at
        total_secs   = int(delta.total_seconds())
        mins, secs   = divmod(total_secs, 60)
        hours, mins  = divmod(mins, 60)
        if hours:
            diff_str = f"{hours}h {mins}m {secs}s after sending"
        elif mins:
            diff_str = f"{mins}m {secs}s after sending"
        else:
            diff_str = f"{secs}s after sending"
        embed.add_field(name="Edited", value=diff_str, inline=False)
        embed.colour = 0xEB459E
    else:
        embed.add_field(
            name="Edited?", value="No — this message has never been edited.", inline=False
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.context_menu(name="Save to Notes")
async def save_to_notes(interaction: discord.Interaction, message: discord.Message):
    content = message.content or ""
    if not content.strip() and not message.attachments:
        await interaction.response.send_message(
            "⚠️ Nothing to save — this message has no text or attachments.", ephemeral=True
        )
        return

    user_id = interaction.user.id
    if user_id not in _notes:
        _notes[user_id] = []

    entry = {
        "content":     content[:500] if content else "[No text]",
        "jump_url":    message.jump_url,
        "author":      str(message.author),
        "saved_at":    datetime.datetime.now(datetime.timezone.utc),
        "attachments": [a.url for a in message.attachments],
    }
    _notes[user_id].append(entry)

    total = len(_notes[user_id])
    embed = discord.Embed(title="📌 Saved to Notes", colour=0x57F287)
    embed.add_field(name="From",             value=entry["author"],   inline=True)
    embed.add_field(name="Note #",           value=str(total),        inline=True)
    embed.add_field(name="Content",          value=entry["content"],  inline=False)
    embed.add_field(name="Jump to message",  value=entry["jump_url"], inline=False)
    embed.set_footer(text="Use /notes to view all your saved notes.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ─────────────────────────────────────────────────────────────────────────────
# USER CONTEXT MENUS
# ─────────────────────────────────────────────────────────────────────────────

@tree.context_menu(name="View User Info")
async def view_user_info(interaction: discord.Interaction, user: discord.User):
    created_at = user.created_at.replace(tzinfo=datetime.timezone.utc)
    badges     = _badge_names(user.public_flags)

    embed = discord.Embed(
        title=f"{'🤖 ' if user.bot else ''}@{user.name}",
        colour=user.accent_colour or 0x5865F2,
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    embed.add_field(name="User ID",       value=str(user.id),         inline=True)
    embed.add_field(name="Display name",  value=user.display_name,    inline=True)
    embed.add_field(name="Bot?",          value="Yes" if user.bot else "No", inline=True)
    embed.add_field(
        name="Account created",
        value=f"<t:{int(created_at.timestamp())}:F>\n({_human_delta(created_at)} ago)",
        inline=False,
    )
    embed.add_field(
        name="Badges",
        value="\n".join(f"• {b}" for b in badges) if badges else "None",
        inline=False,
    )
    try:
        full_user = await client.fetch_user(user.id)
        if full_user.banner:
            embed.set_image(url=full_user.banner.url)
            embed.add_field(name="Banner", value="(shown above)", inline=False)
    except Exception:
        pass
    embed.add_field(
        name="Avatar URL",
        value=f"[Open full size]({user.display_avatar.url})",
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.context_menu(name="Account Age")
async def account_age(interaction: discord.Interaction, user: discord.User):
    created_at = user.created_at.replace(tzinfo=datetime.timezone.utc)
    age        = _human_delta(created_at)

    embed = discord.Embed(title="📅 Account Age", colour=0x5865F2)
    embed.set_author(name=str(user), icon_url=user.display_avatar.url)
    embed.add_field(name="Age",             value=age,                                       inline=True)
    embed.add_field(name="Created on",      value=f"<t:{int(created_at.timestamp())}:D>",   inline=True)
    embed.add_field(name="Exact date & time", value=f"<t:{int(created_at.timestamp())}:F>", inline=False)

    now = datetime.datetime.now(datetime.timezone.utc)
    if (now - created_at).days < 30:
        embed.set_footer(text="⚠️ Account is less than 30 days old.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.context_menu(name="Compare Join Dates")
async def compare_join_dates(interaction: discord.Interaction, user: discord.User):
    me           = interaction.user
    me_created   = me.created_at.replace(tzinfo=datetime.timezone.utc)
    them_created = user.created_at.replace(tzinfo=datetime.timezone.utc)

    embed = discord.Embed(title="📊 Account Creation Comparison", colour=0x5865F2)
    embed.add_field(
        name=f"You ({me.display_name})",
        value=f"<t:{int(me_created.timestamp())}:D>\n({_human_delta(me_created)} ago)",
        inline=True,
    )
    embed.add_field(
        name=f"Them ({user.display_name})",
        value=f"<t:{int(them_created.timestamp())}:D>\n({_human_delta(them_created)} ago)",
        inline=True,
    )
    delta        = abs((me_created - them_created).days)
    years, rem   = divmod(delta, 365)
    months       = rem // 30
    days         = rem % 30
    parts        = []
    if years:  parts.append(f"{years}y")
    if months: parts.append(f"{months}mo")
    parts.append(f"{days}d")
    diff_str = " ".join(parts) if parts else "same day"

    if me_created < them_created:
        verdict = f"Your account is **{diff_str} older** than theirs."
    elif them_created < me_created:
        verdict = f"Their account is **{diff_str} older** than yours."
    else:
        verdict = "You both created your accounts on the same day!"

    embed.add_field(name="Difference", value=verdict, inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.context_menu(name="Mention Snippet")
async def mention_snippet(interaction: discord.Interaction, user: discord.User):
    mention = f"<@{user.id}>"
    embed   = discord.Embed(title="🔖 Mention Snippet", colour=0x5865F2)
    embed.set_author(name=str(user), icon_url=user.display_avatar.url)
    embed.add_field(name="Mention",       value=f"`{mention}`",          inline=False)
    embed.add_field(name="Plain username", value=f"`{user.name}`",       inline=False)
    if user.display_name != user.name:
        embed.add_field(name="Display name", value=f"`{user.display_name}`", inline=False)
    embed.set_footer(text="Copy the mention above and paste it anywhere.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ─────────────────────────────────────────────────────────────────────────────
# SLASH COMMANDS — notes
# ─────────────────────────────────────────────────────────────────────────────

@tree.command(name="notes", description="View all your saved notes.")
async def view_notes(interaction: discord.Interaction):
    user_id = interaction.user.id
    notes   = _notes.get(user_id, [])

    if not notes:
        await interaction.response.send_message(
            "You have no saved notes yet. Right-click any message → Apps → **Save to Notes**.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"📌 Your Notes ({len(notes)} total)",
        colour=0x57F287,
    )
    for i, note in enumerate(notes[-10:], start=max(1, len(notes) - 9)):
        ts = int(note["saved_at"].timestamp())
        embed.add_field(
            name=f"#{i} — {note['author']} (<t:{ts}:d>)",
            value=f"{note['content'][:200]}\n[Jump]({note['jump_url']})",
            inline=False,
        )
    if len(notes) > 10:
        embed.set_footer(text=f"Showing last 10 of {len(notes)} notes.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="clearnotes", description="Delete all your saved notes.")
async def clear_notes(interaction: discord.Interaction):
    user_id = interaction.user.id
    count   = len(_notes.pop(user_id, []))
    await interaction.response.send_message(
        f"🗑️ Cleared {count} note{'s' if count != 1 else ''}.", ephemeral=True
    )


# ─────────────────────────────────────────────────────────────────────────────
# SLASH COMMANDS — LCPDFR tracker
# ─────────────────────────────────────────────────────────────────────────────

@tree.command(name="watch", description="Start watching an LCPDFR plugin for updates.")
@app_commands.describe(
    id="LCPDFR file ID — the number in the URL: /files/file/ID/",
    name="Friendly label for this plugin (optional)",
)
async def lcpdfr_watch(interaction: discord.Interaction, id: str, name: str | None = None):
    file_id = id.strip()
    label   = name or f"Plugin #{file_id}"

    if any(p["id"] == file_id for p in _lcpdfr_config["plugins"]):
        await interaction.response.send_message(
            f"⚠️ Plugin `{file_id}` is already being watched.", ephemeral=True
        )
        return

    if not await _safe_defer(interaction, ephemeral=True):
        print(f"[LCPDFR] Could not defer /watch interaction after 3 attempts.")
        return

    async with aiohttp.ClientSession() as session:
        data = await _fetch_plugin(session, file_id)

    if not data or not data["version"]:
        await interaction.followup.send(
            f"❌ Could not find a plugin with ID `{file_id}` on LCPDFR.\n"
            "Double-check the file ID from the URL (e.g. `7792` from `/files/file/7792-lspdfr/`).",
            ephemeral=True,
        )
        return

    _lcpdfr_config["plugins"].append({"id": file_id, "name": label})
    _lcpdfr_state["versions"][file_id] = data["version"]   # no notification on first add
    _save_json(LCPDFR_CONFIG_FILE, _lcpdfr_config)
    _save_json(LCPDFR_STATE_FILE,  _lcpdfr_state)

    await interaction.followup.send(
        f"✅ Now watching **{label}** (ID: `{file_id}`)\n"
        f"Current version: `{data['version']}`",
        ephemeral=True,
    )


@tree.command(name="unwatch", description="Stop watching an LCPDFR plugin.")
@app_commands.describe(id="LCPDFR file ID to remove")
async def lcpdfr_unwatch(interaction: discord.Interaction, id: str):
    file_id = id.strip()
    before  = len(_lcpdfr_config["plugins"])
    _lcpdfr_config["plugins"] = [p for p in _lcpdfr_config["plugins"] if p["id"] != file_id]

    if len(_lcpdfr_config["plugins"]) == before:
        await interaction.response.send_message(
            f"⚠️ Plugin `{file_id}` was not in the watch list.", ephemeral=True
        )
        return

    _lcpdfr_state["versions"].pop(file_id, None)
    _save_json(LCPDFR_CONFIG_FILE, _lcpdfr_config)
    _save_json(LCPDFR_STATE_FILE,  _lcpdfr_state)
    await interaction.response.send_message(
        f"🗑️ Removed plugin `{file_id}` from the watch list.", ephemeral=True
    )


@tree.command(name="watchlist", description="List all watched LCPDFR plugins and GitHub repos.")
async def lcpdfr_list(interaction: discord.Interaction):
    plugins = _lcpdfr_config.get("plugins", [])
    repos   = _gh_config.get("repos", [])

    if not plugins and not repos:
        await interaction.response.send_message(
            "📋 Nothing is being watched yet. Use `/watch` to add LCPDFR plugins or `/ghwatch` to add GitHub repos.",
            ephemeral=True,
        )
        return

    interval_min = _lcpdfr_config["check_interval_s"] // 60
    channel_id   = _lcpdfr_config.get("channel_id")

    embed = discord.Embed(title="📋 Watch List", colour=0x5865F2)

    if plugins:
        lcpdfr_lines = "\n".join(
            f"• **{p.get('name', p['id'])}** (ID: `{p['id']}`) — "
            f"version `{_lcpdfr_state['versions'].get(p['id'], 'unknown')}`"
            for p in plugins
        )
        embed.add_field(name=f"🔵 LCPDFR Plugins ({len(plugins)})", value=lcpdfr_lines, inline=False)

    if repos:
        gh_lines = "\n".join(
            f"• **{e.get('name') or e['owner'] + '/' + e['repo']}** (`{e['owner']}/{e['repo']}`) — "
            f"tag `{_gh_state['releases'].get(e['owner'] + '/' + e['repo'], 'unknown')}`"
            for e in repos
        )
        embed.add_field(name=f"⚫ GitHub Repos ({len(repos)})", value=gh_lines, inline=False)

    embed.add_field(
        name="Notification channel",
        value=f"<#{channel_id}>" if channel_id else "Not set — use `/setchannel`",
        inline=True,
    )
    embed.add_field(name="Check interval", value=f"Every {interval_min} minute(s)", inline=True)
    embed.set_footer(text="Update Tracker")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="checkupdates", description="Manually trigger an LCPDFR update check right now.")
async def lcpdfr_check(interaction: discord.Interaction):
    plugins = _lcpdfr_config.get("plugins", [])
    if not plugins:
        await interaction.response.send_message(
            "📋 No plugins are being watched. Use `/watch` to add some.", ephemeral=True
        )
        return
    await interaction.response.send_message("🔍 Running update check…", ephemeral=True)
    updates = await _run_update_check(notify=True)
    await interaction.edit_original_response(
        content=f"✅ Update check complete — **{updates}** update(s) found and posted."
    )


@tree.command(name="setchannel", description="Set the channel where LCPDFR update notifications are sent.")
@app_commands.describe(channel_id="Right-click a channel → Copy Channel ID and paste it here")
async def lcpdfr_setchannel(interaction: discord.Interaction, channel_id: str):
    channel_id = channel_id.strip()
    if not channel_id.isdigit():
        await interaction.response.send_message(
            "⚠️ That doesn't look like a valid channel ID. Right-click a channel → **Copy Channel ID** and paste the number here.\n"
            "(Developer Mode must be on in Discord Settings → Advanced.)",
            ephemeral=True,
        )
        return

    # Verify the channel is actually reachable
    try:
        channel = await client.fetch_channel(int(channel_id))
    except discord.NotFound:
        await interaction.response.send_message(
            f"❌ Channel `{channel_id}` not found. Make sure the bot has access to that channel.", ephemeral=True
        )
        return
    except discord.Forbidden:
        await interaction.response.send_message(
            f"❌ I don't have permission to view channel `{channel_id}`.", ephemeral=True
        )
        return

    _lcpdfr_config["channel_id"] = channel_id
    _save_json(LCPDFR_CONFIG_FILE, _lcpdfr_config)
    await interaction.response.send_message(
        f"✅ LCPDFR update notifications will now go to <#{channel_id}>.", ephemeral=True
    )


@tree.command(name="setinterval", description="Set how often LCPDFR is checked for updates.")
@app_commands.describe(minutes="Minutes between checks (minimum 5)")
async def lcpdfr_setinterval(interaction: discord.Interaction, minutes: int):
    if minutes < 5:
        await interaction.response.send_message(
            "⚠️ Minimum interval is 5 minutes.", ephemeral=True
        )
        return
    _lcpdfr_config["check_interval_s"] = minutes * 60
    _save_json(LCPDFR_CONFIG_FILE, _lcpdfr_config)
    _restart_poll_loop()
    await interaction.response.send_message(
        f"✅ LCPDFR check interval updated to every **{minutes} minute(s)**.", ephemeral=True
    )


# ─────────────────────────────────────────────────────────────────────────────
# SLASH COMMANDS — GitHub release tracker
# ─────────────────────────────────────────────────────────────────────────────

@tree.command(name="ghwatch", description="Watch a GitHub repository for new releases.")
@app_commands.describe(
    repo="Repository in owner/repo format — e.g. microsoft/vscode",
    name="Friendly label for this repo (optional)",
)
async def gh_watch(interaction: discord.Interaction, repo: str, name: str | None = None):
    repo = repo.strip().rstrip("/")

    # Validate owner/repo format
    parts = repo.split("/")
    if len(parts) != 2 or not all(parts):
        await interaction.response.send_message(
            "⚠️ Please provide the repository in **owner/repo** format — e.g. `microsoft/vscode`.",
            ephemeral=True,
        )
        return

    owner, repo_name = parts[0], parts[1]
    key = f"{owner}/{repo_name}"
    label = name or key

    if any(f"{e['owner']}/{e['repo']}" == key for e in _gh_config["repos"]):
        await interaction.response.send_message(
            f"⚠️ `{key}` is already being watched.", ephemeral=True
        )
        return

    if not await _safe_defer(interaction, ephemeral=True):
        print(f"[GitHub] Could not defer /ghwatch interaction after 3 attempts.")
        return

    async with aiohttp.ClientSession() as session:
        release = await _fetch_latest_gh_release(session, owner, repo_name)

    if not release:
        await interaction.followup.send(
            f"❌ Could not find a public GitHub repository at `{key}`.\n"
            "Make sure the repo exists, is public (or set `GITHUB_TOKEN`), and has at least one release.",
            ephemeral=True,
        )
        return

    _gh_config["repos"].append({"owner": owner, "repo": repo_name, "name": label})
    _gh_state["releases"][key] = release["tag"]   # record silently; no first-run notification
    _save_json(GH_CONFIG_FILE, _gh_config)
    _save_json(GH_STATE_FILE,  _gh_state)

    channel_id = _lcpdfr_config.get("channel_id")
    channel_hint = f"Notifications → <#{channel_id}>" if channel_id else "⚠️ No notification channel set — use `/setchannel` to configure one."

    await interaction.followup.send(
        f"✅ Now watching **{label}** (`{key}`)\n"
        f"Current release: `{release['tag']}`\n"
        f"{channel_hint}",
        ephemeral=True,
    )


@tree.command(name="ghunwatch", description="Stop watching a GitHub repository for new releases.")
@app_commands.describe(repo="Repository in owner/repo format — e.g. microsoft/vscode")
async def gh_unwatch(interaction: discord.Interaction, repo: str):
    repo = repo.strip().rstrip("/")
    parts = repo.split("/")
    if len(parts) != 2 or not all(parts):
        await interaction.response.send_message(
            "⚠️ Please provide the repository in **owner/repo** format — e.g. `microsoft/vscode`.",
            ephemeral=True,
        )
        return

    owner, repo_name = parts[0], parts[1]
    key = f"{owner}/{repo_name}"
    before = len(_gh_config["repos"])
    _gh_config["repos"] = [
        e for e in _gh_config["repos"]
        if f"{e['owner']}/{e['repo']}" != key
    ]

    if len(_gh_config["repos"]) == before:
        await interaction.response.send_message(
            f"⚠️ `{key}` was not in the GitHub watch list.", ephemeral=True
        )
        return

    _gh_state["releases"].pop(key, None)
    _save_json(GH_CONFIG_FILE, _gh_config)
    _save_json(GH_STATE_FILE,  _gh_state)
    await interaction.response.send_message(
        f"🗑️ Removed `{key}` from the GitHub watch list.", ephemeral=True
    )


# ─────────────────────────────────────────────────────────────────────────────
# on_ready
# ─────────────────────────────────────────────────────────────────────────────

@client.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {client.user} ({client.user.id})")
    print("AI features:", "enabled (Gemini 3.6 Flash)" if AI_AVAILABLE else "disabled (no GEMINI_API_KEY)")
    print()
    print("── Message context menus ──────────────────")
    print("  Get Raw Text")
    print("  Fix Grammar / Spelling  (AI)")
    print("  Summarize Message       (AI)")
    print("  Was This Edited?")
    print("  Save to Notes")
    print()
    print("── User context menus ─────────────────────")
    print("  View User Info")
    print("  Account Age")
    print("  Compare Join Dates")
    print("  Mention Snippet")
    print()
    print("── Slash commands ─────────────────────────")
    print("  /notes          — view saved notes")
    print("  /clearnotes     — clear saved notes")
    print("  /watch          — watch an LCPDFR plugin")
    print("  /unwatch        — stop watching a plugin")
    print("  /watchlist      — list watched plugins + versions")
    print("  /checkupdates   — manually trigger update check")
    print("  /setchannel     — set notification channel")
    print("  /setinterval    — set polling interval")
    print("  /ghwatch        — watch a GitHub repo for releases")
    print("  /ghunwatch      — stop watching a GitHub repo")
    print()
    plugin_count = len(_lcpdfr_config.get("plugins", []))
    print(f"── LCPDFR Tracker ─────────────────────────")
    print(f"  Watching {plugin_count} plugin(s)")
    print(f"  Interval: {_lcpdfr_config['check_interval_s'] // 60} minute(s)")
    channel_id = _lcpdfr_config.get("channel_id")
    print(f"  Channel:  {'#' + str(channel_id) if channel_id else 'not set'}")

    gh_count = len(_gh_config.get("repos", []))
    print(f"── GitHub Release Tracker ─────────────────")
    print(f"  Watching {gh_count} repo(s)")
    print(f"  Interval: {_lcpdfr_config['check_interval_s'] // 60} minute(s) (shared)")
    print(f"  Channel:  {'#' + str(channel_id) if channel_id else 'not set (shared with LCPDFR)'}")

    # Start background pollers
    _restart_poll_loop()
    _restart_gh_poll_loop()

    # Initial checks (records versions without notifying on first run)
    print("[LCPDFR] Running initial version scan…")
    await _run_update_check(notify=False)
    print("[GitHub] Running initial release scan…")
    await _run_gh_update_check(notify=False)

client.run(TOKEN)