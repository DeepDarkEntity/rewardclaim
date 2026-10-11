"""
Discord Reward Claim Website (self-hosted)
One process = Flask website + discord.py bot (background thread).

Setup:  python -m pip install -r requirements.txt
        copy .env.example -> .env and fill it in
        python app.py
rewardscategory.json and stocks.json are re-read on every request, so you can edit them live.

Bot permissions needed in your server: Manage Server (to read invites), Manage Channels,
View Channels, Send Messages, Embed Links, Read Message History.
Invite the bot with BOTH scopes: bot + applications.commands.
Developer Portal -> Bot -> turn ON "Server Members Intent" and "Message Content Intent".

Commands:  /rewarder123 invitessett user:@x invites:N   (staff)   |   r-invitecheck   (everyone)
"""
import asyncio
import concurrent.futures
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
import time
from contextlib import closing
from urllib.parse import urlencode

import discord
import requests
from discord import app_commands
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, session
from werkzeug.middleware.proxy_fix import ProxyFix

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rewards")
BASE = os.path.dirname(os.path.abspath(__file__))


def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and not v:
        raise SystemExit(f"Missing {name} in .env")
    return v


TOKEN = env("DISCORD_BOT_TOKEN", required=True)
CLIENT_ID = env("DISCORD_CLIENT_ID", required=True)
CLIENT_SECRET = env("DISCORD_CLIENT_SECRET", required=True)
REDIRECT_URI = env("REDIRECT_URI", required=True)
GUILD_ID = int(env("GUILD_ID", required=True))
TICKET_CATEGORY_ID = int(env("TICKET_CATEGORY_ID", required=True))
STAFF_ROLE_ID = int(env("STAFF_ROLE_ID", "0") or 0)
COMMAND_ROLE_ID = int(env("COMMAND_ROLE_ID", "0") or 0)
SERVER_INVITE_URL = env("SERVER_INVITE_URL", "")
SECRET_KEY = env("SECRET_KEY", required=True)
SITE_NAME = env("SITE_NAME", "Reward Center")
HOST, PORT = env("HOST", "0.0.0.0"), int(env("PORT", "5000"))
BEHIND_PROXY = env("BEHIND_PROXY", "false").lower() == "true"
CODE_TTL = int(env("CODE_TTL_HOURS", "48")) * 3600
DEDUCT_INVITES = env("DEDUCT_INVITES", "true").lower() == "true"
TICKET_DELETE_AFTER = int(env("TICKET_DELETE_AFTER_SECONDS", "0"))

SITE_URL = REDIRECT_URI.rsplit("/callback", 1)[0]
CATS_FILE = os.path.join(BASE, "rewardscategory.json")
STOCKS_FILE = os.path.join(BASE, "stocks.json")
DB_PATH = os.path.join(BASE, "claims.db")
LOCK = threading.Lock()  # one claim/ticket operation at a time -> no double delivery

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=REDIRECT_URI.startswith("https"),
)
if BEHIND_PROXY:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)


# ───────────────────────── storage helpers ─────────────────────────
def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)  # atomic: stocks file is never half-written


def q(sql, args=(), one=False):
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute(sql, args)
        res = cur.fetchone() if one else cur.fetchall()
        con.commit()
        return res


# codes.tier = the invite budget the user had when the code was made
q("CREATE TABLE IF NOT EXISTS codes (code TEXT PRIMARY KEY, user_id TEXT, tier INTEGER, "
  "channel_id TEXT, created REAL, status TEXT DEFAULT 'open', category TEXT)")
q("CREATE TABLE IF NOT EXISTS spent (user_id TEXT PRIMARY KEY, amount INTEGER DEFAULT 0)")
q("CREATE TABLE IF NOT EXISTS bonus (user_id TEXT PRIMARY KEY, amount INTEGER DEFAULT 0)")
q("CREATE TABLE IF NOT EXISTS joins (member_id TEXT PRIMARY KEY, inviter_id TEXT, name TEXT, joined REAL)")

ALPHA = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no 0/O/1/I


def gen_code():
    return "".join(secrets.choice(ALPHA) for _ in range(12))


def fmt(code):
    return f"{code[:4]}-{code[4:8]}-{code[8:]}"


def norm(s):
    return re.sub(r"[^A-Z0-9]", "", str(s or "").upper())


def get_valid_code(raw):
    code = norm(raw)
    if len(code) != 12:
        return None
    row = q("SELECT * FROM codes WHERE code=? AND status='open'", (code,), one=True)
    return row if row and time.time() - row["created"] < CODE_TTL else None


def ch_url(channel_id):
    return f"https://discord.com/channels/{GUILD_ID}/{channel_id}"


_hits = {}


def rate_limited(key, limit=10, window=60):
    now = time.time()
    hits = [t for t in _hits.get(key, []) if now - t < window] + [now]
    _hits[key] = hits
    return len(hits) > limit


# ───────────────────────── discord bot ─────────────────────────
class BotError(Exception):
    """Message is safe to show to visitors."""


intents = discord.Intents.default()
intents.members = True          # detect who joined (Server Members Intent)
intents.message_content = True  # read the r-invitecheck prefix command (Message Content Intent)
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)
bot_loop = None
bot_ready = threading.Event()
_tasks = set()


@bot.event
async def on_ready():
    bot_ready.set()
    log.info("Bot online as %s", bot.user)
    await snapshot_invites()


def run_bot():
    async def main():
        global bot_loop
        bot_loop = asyncio.get_running_loop()
        async with bot:
            await bot.start(TOKEN)
    try:
        asyncio.run(main())
    except Exception:
        bot_ready.clear()
        log.exception("Bot stopped")


def call_bot(coro, timeout=25):
    if not bot_ready.is_set() or bot_loop is None:
        coro.close()
        raise BotError("The bot is still starting up. Please try again in a few seconds.")
    fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
    try:
        return fut.result(timeout)
    except concurrent.futures.TimeoutError:
        fut.cancel()
        raise


def _guild():
    g = bot.get_guild(GUILD_ID)
    if not g:
        raise RuntimeError("Bot is not in the configured guild (check GUILD_ID / invite the bot).")
    return g


async def bot_channel_exists(channel_id):
    return bot.get_channel(channel_id) is not None


async def bot_count_invites(uid):
    """Returns (is_member, total uses of the invites this user created)."""
    g = _guild()
    try:
        await g.fetch_member(uid)
    except discord.NotFound:
        return False, 0
    invites = await g.invites()
    return True, sum(i.uses or 0 for i in invites if i.inviter and i.inviter.id == uid)


async def bot_create_ticket(uid, budget, code):
    g = _guild()
    member = await g.fetch_member(uid)
    cat = g.get_channel(TICKET_CATEGORY_ID)
    if not isinstance(cat, discord.CategoryChannel):
        raise RuntimeError("TICKET_CATEGORY_ID is not a valid category in this guild.")
    view = dict(view_channel=True, send_messages=True, read_message_history=True)
    overwrites = {
        g.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(**view),
        g.me: discord.PermissionOverwrite(**view, embed_links=True, manage_channels=True),
    }
    if STAFF_ROLE_ID and (role := g.get_role(STAFF_ROLE_ID)):
        overwrites[role] = discord.PermissionOverwrite(**view)
    ch = await g.create_text_channel(f"reward-{member.name}"[:90], category=cat,
                                     overwrites=overwrites, reason=f"Reward ticket for {member}")
    embed = discord.Embed(
        title="🎁 Your reward code", color=0x5865F2,
        description=f"Hey {member.mention}! You have **{budget}** invites to spend.\n"
                    "Enter this code in **Step 2** on the website to pick your reward:")
    embed.add_field(name="Code", value=f"```{fmt(code)}```", inline=False)
    embed.add_field(name="Website", value=SITE_URL, inline=False)
    embed.set_footer(text=f"Valid for {CODE_TTL // 3600}h • One use • Don't share it")
    await ch.send(content=member.mention, embed=embed)
    return ch.id


async def _delete_later(ch, seconds):
    await asyncio.sleep(seconds)
    try:
        await ch.delete(reason="Reward claimed")
    except discord.HTTPException:
        pass


async def bot_send_reward(channel_id, uid, cat, reward):
    ch = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
    safe = str(reward).replace("```", "'''")
    embed = discord.Embed(title=f"🎉 {cat['name']}", description=f"```\n{safe}\n```", color=0x57F287)
    foot = "Keep this safe — it will not be sent again."
    if TICKET_DELETE_AFTER > 0:
        foot += f" This ticket closes in {max(1, TICKET_DELETE_AFTER // 60)} min."
    embed.set_footer(text=foot)
    await ch.send(content=f"<@{uid}>", embed=embed)
    if TICKET_DELETE_AFTER > 0:
        t = asyncio.create_task(_delete_later(ch, TICKET_DELETE_AFTER))
        _tasks.add(t)
        t.add_done_callback(_tasks.discard)


# ───────────────────────── staff slash command ─────────────────────────
rewarder = app_commands.Group(name="rewarder123", description="Reward system staff commands")


def is_staff(member):
    p = member.guild_permissions
    return bool(p.administrator or p.manage_guild
                or (COMMAND_ROLE_ID and any(r.id == COMMAND_ROLE_ID for r in member.roles)))


@rewarder.command(name="invitessett", description="Set how many invites a member has for rewards")
@app_commands.describe(user="Member to update", invites="Invites they should have available")
async def invitessett(interaction: discord.Interaction, user: discord.Member,
                      invites: app_commands.Range[int, 0, 100000]):
    if not is_staff(interaction.user):
        return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    uid = str(user.id)
    _, total = await bot_count_invites(user.id)
    spent = q("SELECT amount FROM spent WHERE user_id=?", (uid,), one=True)
    bonus = invites - (total - (spent["amount"] if spent else 0))
    q("INSERT INTO bonus (user_id, amount) VALUES (?, ?) ON CONFLICT(user_id) "
      "DO UPDATE SET amount = excluded.amount", (uid, bonus))
    await interaction.followup.send(f"✅ {user.mention} now has **{invites}** available invites.", ephemeral=True)


async def _setup_commands():
    g = discord.Object(id=GUILD_ID)
    tree.add_command(rewarder, guild=g)
    try:
        await tree.sync(guild=g)
        log.info("Slash commands synced")
    except discord.HTTPException:
        log.exception("Slash sync failed - re-invite the bot with the applications.commands scope")


bot.setup_hook = _setup_commands


# ───────────────────── invite tracking + r-invitecheck ─────────────────────
PREFIX = "r-"  # change the prefix here
invite_cache = {}  # code -> (uses, max_uses, inviter_id)


def _inv(i):
    return (i.uses or 0, i.max_uses or 0, i.inviter.id if i.inviter else None)


async def snapshot_invites():
    try:
        invite_cache.clear()
        invite_cache.update({i.code: _inv(i) for i in await _guild().invites()})
    except Exception:
        log.exception("Could not read invites (bot needs Manage Server)")


@bot.event
async def on_invite_create(invite):
    if invite.guild and invite.guild.id == GUILD_ID:
        invite_cache[invite.code] = _inv(invite)


@bot.event
async def on_member_join(member):
    if member.guild.id != GUILD_ID or member.bot:
        return
    try:
        current = {i.code: _inv(i) for i in await member.guild.invites()}
    except discord.HTTPException:
        return
    used = None
    for code, (uses, _mx, inviter) in current.items():  # an invite's use count went up
        if uses > invite_cache.get(code, (0,))[0]:
            used = inviter
            break
    if used is None:  # a limited invite that vanished because its last use was just spent
        for code, (uses, mx, inviter) in invite_cache.items():
            if code not in current and mx and uses + 1 >= mx:
                used = inviter
                break
    invite_cache.clear()
    invite_cache.update(current)
    if used:
        q("INSERT OR REPLACE INTO joins VALUES (?,?,?,?)", (str(member.id), str(used), member.name, time.time()))


async def build_invitecheck(user):
    uid = str(user.id)
    try:
        _, total = await bot_count_invites(user.id)
        g = _guild()
    except Exception:
        log.exception("invitecheck failed")
        return discord.Embed(description="❌ Couldn't read invites right now.", color=0xED4245)
    spent = q("SELECT amount FROM spent WHERE user_id=?", (uid,), one=True)
    bonus = q("SELECT amount FROM bonus WHERE user_id=?", (uid,), one=True)
    s, b = (spent["amount"] if spent else 0), (bonus["amount"] if bonus else 0)
    available = max(0, total - s + b)

    rows = q("SELECT member_id, name FROM joins WHERE inviter_id=? ORDER BY joined DESC", (uid,))
    lines = []
    for r in rows[:25]:
        m = g.get_member(int(r["member_id"]))
        lines.append(f"✅ {discord.utils.escape_markdown(m.display_name)}" if m
                     else f"🚪 {discord.utils.escape_markdown(r['name'])} (left)")
    if len(rows) > 25:
        lines.append(f"…and {len(rows) - 25} more")

    embed = discord.Embed(title=f"📨 Invites for {user.display_name}", color=0x5865F2,
                          description=f"**People you invited ({len(rows)} tracked)**\n"
                                      + ("\n".join(lines) if lines else "None tracked yet."))
    embed.add_field(name="Available to spend", value=f"**{available}**")
    embed.add_field(name="Total invite uses", value=str(total))
    embed.add_field(name="Spent on rewards", value=str(s))
    if b:
        embed.add_field(name="Staff adjustment", value=f"{b:+d}")
    embed.set_footer(text="✅ in server • 🚪 left • Names are tracked from the moment tracking started")
    return embed


@bot.event
async def on_message(message):
    if message.author.bot or not message.guild or message.guild.id != GUILD_ID:
        return
    cmd = message.content.strip().lower()
    if cmd == f"{PREFIX}invitecheck" or cmd.startswith(f"{PREFIX}invitecheck "):
        async with message.channel.typing():
            embed = await build_invitecheck(message.author)
        await message.reply(embed=embed, mention_author=False)


# ───────────────────────── claim flow (panel 1) ─────────────────────────
def fail(msg, link="", label=""):
    return {"ok": False, "message": msg, "link": link, "link_label": label}


def start_flow(uid):
    uid = str(uid)
    row = q("SELECT * FROM codes WHERE user_id=? AND status='open' ORDER BY created DESC", (uid,), one=True)
    if row:
        if time.time() - row["created"] < CODE_TTL and call_bot(bot_channel_exists(int(row["channel_id"]))):
            return {"ok": True, "message": "You already have an open ticket — your code is waiting there.",
                    "link": ch_url(row["channel_id"]), "link_label": "Open ticket"}
        q("UPDATE codes SET status='expired' WHERE code=?", (row["code"],))

    in_server, total = call_bot(bot_count_invites(int(uid)))
    if not in_server:
        return fail("Join our server first, then click Claim Reward again.", SERVER_INVITE_URL, "Join server")

    spent = q("SELECT amount FROM spent WHERE user_id=?", (uid,), one=True)
    bonus = q("SELECT amount FROM bonus WHERE user_id=?", (uid,), one=True)
    available = max(0, total - (spent["amount"] if spent else 0) + (bonus["amount"] if bonus else 0))
    cats, stocks = load_json(CATS_FILE, []), load_json(STOCKS_FILE, {})
    tiers = sorted({int(c["invites"]) for c in cats if stocks.get(c["id"])})
    if not tiers:
        return fail("Everything is out of stock right now. Check back later!")
    if not [t for t in tiers if t <= available]:
        return fail(f"You have {available} available invite(s). The lowest reward needs {tiers[0]}.")

    code = gen_code()
    cid = call_bot(bot_create_ticket(int(uid), available, code))
    q("INSERT INTO codes (code, user_id, tier, channel_id, created) VALUES (?,?,?,?,?)",
      (code, uid, available, str(cid), time.time()))  # "tier" column stores their invite budget
    return {"ok": True, "message": f"Ticket created! You have {available} available invites to spend on a reward.",
            "link": ch_url(cid), "link_label": "Open ticket"}


# ───────────────────────── routes ─────────────────────────
@app.get("/")
def index():
    tiers = {}
    for c in load_json(CATS_FILE, []):
        tiers.setdefault(int(c["invites"]), []).append(c)
    return render_template("index.html", site=SITE_NAME, tiers=sorted(tiers.items()),
                           result=session.pop("result", None))


@app.get("/login")
def login():
    session["state"] = secrets.token_urlsafe(16)
    return redirect("https://discord.com/oauth2/authorize?" + urlencode({
        "client_id": CLIENT_ID, "redirect_uri": REDIRECT_URI,
        "response_type": "code", "scope": "identify", "state": session["state"]}))


@app.get("/callback")
def callback():
    state = session.pop("state", None)
    if request.args.get("error"):
        session["result"] = fail("Authorization was cancelled.")
        return redirect("/")
    if not state or state != request.args.get("state") or not request.args.get("code"):
        session["result"] = fail("Login expired. Please click Claim Reward again.")
        return redirect("/")
    try:
        t = requests.post("https://discord.com/api/v10/oauth2/token", timeout=15, data={
            "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "grant_type": "authorization_code",
            "code": request.args["code"], "redirect_uri": REDIRECT_URI})
        t.raise_for_status()
        me = requests.get("https://discord.com/api/v10/users/@me", timeout=15,
                          headers={"Authorization": f"Bearer {t.json()['access_token']}"})
        me.raise_for_status()
        with LOCK:
            result = start_flow(me.json()["id"])
    except BotError as e:
        result = fail(str(e))
    except Exception:
        log.exception("start_flow failed")
        result = fail("Something went wrong on our side. Please try again later.")
    session["result"] = result
    return redirect("/")


@app.post("/api/verify")
def api_verify():
    if rate_limited(request.remote_addr):
        return jsonify(ok=False, error="Too many attempts. Wait a minute."), 429
    row = get_valid_code((request.get_json(silent=True) or {}).get("code"))
    if not row:
        return jsonify(ok=False, error="Invalid, used or expired code."), 400
    stocks = load_json(STOCKS_FILE, {})
    cats = sorted(({"id": c["id"], "name": c["name"], "emoji": c.get("emoji", ""),
                    "description": c.get("description", ""), "invites": int(c["invites"]),
                    "stock": len(stocks.get(c["id"], []))}
                   for c in load_json(CATS_FILE, []) if int(c["invites"]) <= row["tier"]),
                  key=lambda c: c["invites"])
    return jsonify(ok=True, tier=row["tier"], categories=cats)


@app.post("/api/claim")
def api_claim():
    if rate_limited(request.remote_addr, limit=15):
        return jsonify(ok=False, error="Too many attempts. Wait a minute."), 429
    d = request.get_json(silent=True) or {}
    cat_id = str(d.get("category", ""))
    try:
        with LOCK:
            row = get_valid_code(d.get("code"))
            if not row:
                return jsonify(ok=False, error="Invalid, used or expired code."), 400
            cat = next((c for c in load_json(CATS_FILE, [])
                        if c["id"] == cat_id and int(c["invites"]) <= row["tier"]), None)
            if not cat:
                return jsonify(ok=False, error="That category isn't available for your code."), 400
            stocks = load_json(STOCKS_FILE, {})
            items = stocks.get(cat_id, [])
            if not items:
                return jsonify(ok=False, error="That category is out of stock. Pick another one."), 409

            call_bot(bot_send_reward(int(row["channel_id"]), row["user_id"], cat, items[0]))
            # Sent successfully -> NOW remove it from stock, burn the code, spend the invites
            stocks[cat_id] = items[1:]
            save_json(STOCKS_FILE, stocks)
            q("UPDATE codes SET status='used', category=? WHERE code=?", (cat_id, row["code"]))
            if DEDUCT_INVITES:
                q("INSERT INTO spent (user_id, amount) VALUES (?, ?) ON CONFLICT(user_id) "
                  "DO UPDATE SET amount = amount + excluded.amount", (row["user_id"], int(cat["invites"])))
        return jsonify(ok=True, link=ch_url(row["channel_id"]))
    except BotError as e:
        return jsonify(ok=False, error=str(e)), 503
    except Exception:
        log.exception("claim failed")
        return jsonify(ok=False, error="Couldn't deliver the reward (nothing was used). Try again."), 502


if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    log.info("Website running on http://%s:%s", HOST, PORT)
    from waitress import serve
    serve(app, host=HOST, port=PORT, threads=8)  # keep this single-process (bot lives in it)
