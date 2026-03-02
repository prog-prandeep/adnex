# ══════════════════════════════════════════════════════════════════════
#  ProAdBot v2.2  –  Production-Ready Multi-User Ad Forwarding System
# ══════════════════════════════════════════════════════════════════════
#
#  QUICK START:
#    1.  pip install python-telegram-bot telethon
#    2.  Set BOT_TOKEN and ADMIN_USER_ID at the bottom of this file
#    3.  python adbot.py
#
#  SLASH COMMANDS:
#    /start  /menu         – Main menu
#    /selfbots             – Manage selfbots
#    /groups               – Manage groups
#    /status               – Live forwarding status
#    /sync                 – Sync groups from selfbot dialogs
#    /start_all            – Start forwarding on all selfbots
#    /stop_all             – Stop forwarding on all selfbots
#    /cancel               – Cancel active input flow
#    /admin  (admin only)  – Admin panel
#
#  PLAN LIMITS:
#    basic  – 1 selfbot,  5  custom groups
#    pro    – 2 selfbots, 10 custom groups
#    elite  – 3 selfbots, 50 custom groups
#  Global groups (admin-added) are always free, never counted.
#
#  NEW in v2.2:
#    - /sync command: scans selfbot dialogs → paginated list to toggle
#      groups on/off up to plan limit
#    - Full slash-command set for every major action
#    - Cleaner selfbot menu: recent log + progress inline, fewer taps
#    - Groups menu has sync button built in
#    - Inline status badges 🟢▶️ / 🟡⏸ / 🟢⏹ / 🔴
# ══════════════════════════════════════════════════════════════════════

import asyncio, io, json, logging, os, random, re
import sqlite3, sys
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters,
)
from telegram.error import BadRequest

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError,
    ChatWriteForbiddenError, ChatAdminRequiredError,
    UserBannedInChannelError, ChannelPrivateError,
    PhoneCodeInvalidError,
)
from telethon.tl.functions.messages import ForwardMessagesRequest, GetForumTopicsRequest
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest
from telethon.tl.types import ForumTopic, Channel, Chat

# ══════════════════════════════════════════════════════════════════════
#  PLAN CONFIG
# ══════════════════════════════════════════════════════════════════════

PLANS: Dict[str, Dict] = {
    "basic": {
        "label":        "⚡ Basic",
        "selfbots":     1,
        "extra_groups": 5,
        "description":  "1 selfbot · 5 custom groups",
    },
    "pro": {
        "label":        "🚀 Pro",
        "selfbots":     2,
        "extra_groups": 10,
        "description":  "2 selfbots · 10 custom groups",
    },
    "elite": {
        "label":        "👑 Elite",
        "selfbots":     3,
        "extra_groups": 50,
        "description":  "3 selfbots · 50 custom groups",
    },
}

CREDENTIAL_POOL = [
    # Slot 0 filled from config (your own credentials)
    {"api_id": 2040,    "api_hash": "b18441a1ff607e10a989891a5462e627"},  # TelegramDesktop
    {"api_id": 6,       "api_hash": "eb06d4abfb49dc3eeb1aeb98ae0f581e"},  # Android
    {"api_id": 17349,   "api_hash": "344583e45741c457fe1862106095a5eb"},  # iOS
    {"api_id": 1176661, "api_hash": "df05523b63c97ccd9ecd7a51e55abf50"},  # Telegram X
    {"api_id": 4,       "api_hash": "014b35b6184100b085b0d0572f9b5103"},  # MacOS
]

MIN_DELAY    = 45
MAX_DELAY    = 90
HOLD_MINUTES = 30
SESSIONS_DIR = Path("sessions")
DB_PATH      = "adbot.db"
PAGE_SIZE    = 8   # groups per page in sync view

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
logging.getLogger().addHandler(_handler)
logging.getLogger().setLevel(logging.WARNING)
logger = logging.getLogger("adbot")
logger.setLevel(logging.DEBUG)
logger.propagate = False
logger.addHandler(_handler)
SESSIONS_DIR.mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════

class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        con = sqlite3.connect(self.path, timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        con.commit(); con.close()
        self._migrate()

    def _con(self):
        c = sqlite3.connect(self.path, timeout=30)
        c.row_factory = sqlite3.Row
        return c

    def q(self, sql, params=()):
        with self._con() as c: return c.execute(sql, params).fetchall()
    def run(self, sql, params=()):
        with self._con() as c: c.execute(sql, params); c.commit()
    def one(self, sql, params=()):
        r = self.q(sql, params); return r[0] if r else None

    def _migrate(self):
        with self._con() as c:
            c.executescript("""
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY, plan TEXT NOT NULL DEFAULT 'basic',
    granted_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT, is_active INTEGER NOT NULL DEFAULT 1, note TEXT
);
CREATE TABLE IF NOT EXISTS selfbots (
    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
    slot INTEGER NOT NULL, api_id INTEGER, api_hash TEXT, phone TEXT,
    session_file TEXT, is_active INTEGER DEFAULT 0, last_seen TEXT,
    display_name TEXT, ad_message TEXT, fwd_running INTEGER DEFAULT 0,
    fwd_paused INTEGER DEFAULT 0, fwd_cycle_idx INTEGER DEFAULT 0,
    fwd_phase TEXT DEFAULT 'forwarding', fwd_phase_ts TEXT,
    UNIQUE(user_id, slot)
);
CREATE TABLE IF NOT EXISTS global_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER UNIQUE NOT NULL,
    title TEXT, username TEXT, is_forum INTEGER DEFAULT 0,
    added_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS user_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL, title TEXT, username TEXT,
    is_forum INTEGER DEFAULT 0, is_enabled INTEGER DEFAULT 1,
    added_at TEXT DEFAULT (datetime('now')), UNIQUE(user_id, chat_id)
);
CREATE TABLE IF NOT EXISTS selfbot_topics (
    selfbot_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL, topic_title TEXT,
    PRIMARY KEY (selfbot_id, chat_id, topic_id)
);
CREATE TABLE IF NOT EXISTS fwd_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, selfbot_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL, group_title TEXT, success INTEGER NOT NULL,
    error TEXT, sent_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS flow_state (user_id INTEGER PRIMARY KEY, data TEXT);
""")
            c.commit()

    def get_setting(self, key, default=""): r = self.one("SELECT value FROM settings WHERE key=?", (key,)); return r["value"] if r else default
    def set_setting(self, key, value): self.run("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
    def get_user(self, uid): return self.one("SELECT * FROM users WHERE user_id=?", (uid,))
    def user_authorized(self, uid):
        r = self.get_user(uid)
        if not r or not r["is_active"]: return False
        if r["expires_at"] and datetime.now() > datetime.fromisoformat(r["expires_at"]): return False
        return True
    def get_plan(self, uid): r = self.get_user(uid); return PLANS.get(r["plan"]) if r else None
    def get_selfbots(self, uid): return self.q("SELECT * FROM selfbots WHERE user_id=? ORDER BY slot", (uid,))
    def get_selfbot(self, sbid): return self.one("SELECT * FROM selfbots WHERE id=?", (sbid,))
    def get_sb_slot(self, uid, slot): return self.one("SELECT * FROM selfbots WHERE user_id=? AND slot=?", (uid, slot))
    def ensure_sb_slot(self, uid, slot):
        self.run("INSERT OR IGNORE INTO selfbots(user_id,slot) VALUES(?,?)", (uid, slot))
        return self.get_sb_slot(uid, slot)["id"]
    def groups_for_user(self, uid):
        out, seen = [], set()
        for r in self.q("SELECT * FROM global_groups"): out.append(dict(r)); seen.add(r["chat_id"])
        for r in self.q("SELECT * FROM user_groups WHERE user_id=? AND is_enabled=1", (uid,)):
            if r["chat_id"] not in seen: out.append(dict(r))
        return out
    def custom_group_count(self, uid): r = self.one("SELECT COUNT(*) as n FROM user_groups WHERE user_id=?", (uid,)); return r["n"] if r else 0
    def enabled_group_count(self, uid): r = self.one("SELECT COUNT(*) as n FROM user_groups WHERE user_id=? AND is_enabled=1", (uid,)); return r["n"] if r else 0
    def get_topics(self, sbid, chat_id): return self.q("SELECT * FROM selfbot_topics WHERE selfbot_id=? AND chat_id=?", (sbid, chat_id))
    def set_topics(self, sbid, chat_id, pairs):
        self.run("DELETE FROM selfbot_topics WHERE selfbot_id=? AND chat_id=?", (sbid, chat_id))
        for tid, ttitle in pairs: self.run("INSERT OR REPLACE INTO selfbot_topics VALUES(?,?,?,?)", (sbid, chat_id, tid, ttitle))
    def get_flow(self, uid): r = self.one("SELECT data FROM flow_state WHERE user_id=?", (uid,)); return json.loads(r["data"]) if r else None
    def set_flow(self, uid, data): self.run("INSERT OR REPLACE INTO flow_state(user_id,data) VALUES(?,?)", (uid, json.dumps(data)))
    def clear_flow(self, uid): self.run("DELETE FROM flow_state WHERE user_id=?", (uid,))


# ══════════════════════════════════════════════════════════════════════
#  MTPROTO FORWARDER
# ══════════════════════════════════════════════════════════════════════

class MTProtoForwarder:
    def __init__(self, client: TelegramClient):
        self.client = client
        self._cache: Dict[int, Any] = {}

    async def _peer(self, chat_id):
        if chat_id in self._cache: return self._cache[chat_id]
        entity = await self.client.get_entity(chat_id)
        peer   = await self.client.get_input_entity(entity)
        self._cache[chat_id] = peer
        return peer

    async def warm_entity(self, chat_id) -> Tuple[bool, str]:
        try: await self._peer(chat_id); return True, ""
        except Exception as e:
            try:
                async for _ in self.client.iter_messages(chat_id, limit=1): break
                await self._peer(chat_id); return True, ""
            except Exception as e2: return False, str(e2)

    async def forward(self, src_chat, src_msgid, to_chat,
                      topic_id=None, drop_author=False) -> Tuple[bool, str]:
        try:
            await self.client(ForwardMessagesRequest(
                from_peer=await self._peer(src_chat), id=[src_msgid],
                to_peer=await self._peer(to_chat), top_msg_id=topic_id,
                random_id=[random.randint(1, 2**63-1)], silent=False,
                drop_author=drop_author, noforwards=False,
                schedule_date=None, send_as=None))
            return True, ""
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5); return False, f"FloodWait {e.seconds}s"
        except (ChatWriteForbiddenError, ChatAdminRequiredError,
                UserBannedInChannelError, ChannelPrivateError) as e:
            return False, f"No permission: {type(e).__name__}"
        except Exception as e:
            err = str(e)
            if topic_id is not None and ("top_msg_id" in err or "TOPIC" in err.upper()):
                return await self.forward(src_chat, src_msgid, to_chat,
                                          topic_id=None, drop_author=drop_author)
            return False, err

    async def send_copy(self, src_chat, src_msgid, to_chat, topic_id=None) -> Tuple[bool, str]:
        try:
            orig = await self.client.get_messages(src_chat, ids=src_msgid)
            if orig is None: return False, "Original not found"
            kw = {"entity": to_chat}
            if topic_id is not None: kw["reply_to"] = topic_id
            if getattr(orig, "media", None):
                await self.client.send_file(file=orig.media, caption=orig.message or "", **kw)
            else:
                await self.client.send_message(message=orig.message or orig.text or "", **kw)
            return True, ""
        except FloodWaitError as e: await asyncio.sleep(e.seconds+5); return False, f"FloodWait {e.seconds}s"
        except (ChatWriteForbiddenError, ChatAdminRequiredError,
                UserBannedInChannelError, ChannelPrivateError) as e:
            return False, f"No permission: {type(e).__name__}"
        except Exception as e: return False, str(e)


# ══════════════════════════════════════════════════════════════════════
#  SELFBOT INSTANCE
# ══════════════════════════════════════════════════════════════════════

class SelfbotInstance:
    def __init__(self, db: Database, sbid: int):
        self.db = db; self.sbid = sbid
        self.client: Optional[TelegramClient] = None
        self.is_auth = False
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self.fwd: Optional[MTProtoForwarder] = None

    async def request_code(self, phone) -> Tuple[bool, str]:
        pool = [c for c in CREDENTIAL_POOL if c.get("api_id", 0) != 0]
        if not pool: return False, "❌ No credentials configured."
        last_err = ""
        for i, cred in enumerate(pool):
            try:
                sf = str(SESSIONS_DIR / f"sb_{self.sbid}")
                if self.client:
                    with suppress(Exception): await self.client.disconnect()
                self.client = TelegramClient(sf, cred["api_id"], cred["api_hash"],
                    device_model="Samsung Galaxy S23", system_version="Android 14",
                    app_version="10.3.2")
                await self.client.connect()
                await self.client.send_code_request(phone)
                self.db.run("UPDATE selfbots SET api_id=?,api_hash=?,phone=?,session_file=? WHERE id=?",
                            (cred["api_id"], cred["api_hash"], phone, sf, self.sbid))
                return True, "✅ OTP sent!"
            except Exception as e: last_err = str(e); continue
        return False, f"❌ Failed: {last_err}"

    async def verify_code(self, phone, code, password="") -> Tuple[bool, str]:
        if not self.client: return False, "No session."
        try:
            try: await self.client.sign_in(phone, code)
            except SessionPasswordNeededError:
                if not password: return False, "2FA_REQUIRED"
                await self.client.sign_in(password=password)
            if await self.client.is_user_authorized():
                self.is_auth = True
                self.fwd     = MTProtoForwarder(self.client)
                self.db.run("UPDATE selfbots SET is_active=1,last_seen=? WHERE id=?",
                            (datetime.now().isoformat(), self.sbid))
                asyncio.create_task(self._warm_dialogs())
                return True, "✅ Selfbot connected!"
            return False, "Auth failed."
        except PhoneCodeInvalidError: return False, "❌ Invalid OTP."
        except Exception as e: return False, f"❌ {e}"

    async def restore(self) -> bool:
        r = self.db.get_selfbot(self.sbid)
        if not r or not r["session_file"] or not r["api_id"]: return False
        try:
            self.client = TelegramClient(r["session_file"], r["api_id"], r["api_hash"])
            await self.client.connect()
            if await self.client.is_user_authorized():
                self.is_auth = True; self.fwd = MTProtoForwarder(self.client)
                asyncio.create_task(self._warm_dialogs()); return True
        except Exception as e: logger.warning("restore sb%s: %s", self.sbid, e)
        return False

    async def _warm_dialogs(self):
        try:
            logger.debug("warming dialogs sb%s", self.sbid)
            await self.client.get_dialogs(limit=200)
            logger.debug("dialogs warm done sb%s", self.sbid)
        except Exception as e: logger.warning("_warm_dialogs sb%s: %s", self.sbid, e)

    async def warm_source_entity(self, src_chat) -> Tuple[bool, str]:
        if not self.fwd: return False, "Not connected"
        return await self.fwd.warm_entity(src_chat)

    async def disconnect(self):
        self.is_auth = False; self.fwd = None
        self.stop_forwarding()
        if self.client:
            with suppress(Exception): await self.client.disconnect()
            self.client = None
        self.db.run("UPDATE selfbots SET is_active=0 WHERE id=?", (self.sbid,))

    async def update_profile(self, name=None, bio=None) -> Tuple[bool, str]:
        if not self.is_auth: return False, "Not connected."
        try:
            kw = {}
            if name: parts = (name+" ").split(" ",1); kw["first_name"]=parts[0].strip(); kw["last_name"]=parts[1].strip()
            if bio is not None: kw["about"] = bio
            if kw: await self.client(UpdateProfileRequest(**kw))
            return True, "✅ Profile updated."
        except Exception as e: return False, f"❌ {e}"

    async def update_photo(self, data: bytes) -> Tuple[bool, str]:
        if not self.is_auth: return False, "Not connected."
        try:
            f = await self.client.upload_file(data, file_name="photo.jpg")
            await self.client(UploadProfilePhotoRequest(file=f))
            return True, "✅ Photo updated."
        except Exception as e: return False, f"❌ {e}"

    async def get_all_groups(self) -> List[Dict]:
        """
        Returns all groups/supergroups the selfbot is a member of.
        Excludes broadcast-only channels (no megagroup flag).
        """
        if not self.is_auth: return []
        results = []
        try:
            dialogs = await self.client.get_dialogs(limit=500)
            for dlg in dialogs:
                entity = dlg.entity
                if not isinstance(entity, (Channel, Chat)): continue
                # Skip pure broadcast channels
                if (isinstance(entity, Channel) and
                        getattr(entity, "broadcast", False) and
                        not getattr(entity, "megagroup", False)):
                    continue
                chat_id  = entity.id
                title    = getattr(entity, "title", str(chat_id)) or str(chat_id)
                username = getattr(entity, "username", None)
                is_forum = (isinstance(entity, Channel) and
                            getattr(entity, "megagroup", False) and
                            getattr(entity, "forum", False))
                results.append({"chat_id": chat_id, "title": title,
                                 "username": username, "is_forum": is_forum})
        except Exception as e: logger.warning("get_all_groups sb%s: %s", self.sbid, e)
        return results

    async def fetch_topics(self, chat_id, keyword="") -> List[Tuple[int, str]]:
        if not self.is_auth: return []
        out = []
        try:
            entity       = await self.client.get_entity(chat_id)
            input_entity = await self.client.get_input_entity(entity)
            res = await self.client(GetForumTopicsRequest(
                input_entity, offset_date=0, offset_id=0, offset_topic=0, limit=100))
            for t in res.topics:
                if not isinstance(t, ForumTopic): continue
                if getattr(t, "closed", False) or getattr(t, "hidden", False): continue
                title = t.title or ""
                if keyword and keyword.lower() not in title.lower(): continue
                out.append((t.id, title))
        except Exception as e:
            if "frozen" in str(e).lower(): logger.warning("fetch_topics sb%s: frozen", self.sbid)
            else: logger.warning("fetch_topics %s: %s", chat_id, e)
        return out

    async def start_forwarding(self) -> Tuple[bool, str]:
        r = self.db.get_selfbot(self.sbid)
        if not r:               return False, "Not found."
        if not self.is_auth:    return False, "❌ Not connected."
        if not r["ad_message"]: return False, "❌ No ad message set."
        if not self.db.groups_for_user(r["user_id"]): return False, "❌ No groups configured."
        if self._task and not self._task.done(): return False, "Already running."
        try:
            ad = json.loads(r["ad_message"]); src = ad.get("src_chat")
            if src:
                ok, err = await self.warm_source_entity(src)
                if not ok: return False, f"❌ Cannot resolve source channel.\n{err}\n\nSelfbot must be a member."
        except Exception as e: logger.warning("start_fwd warm sb%s: %s", self.sbid, e)
        self._stop.clear()
        self.db.run("UPDATE selfbots SET fwd_running=1,fwd_paused=0,fwd_phase='forwarding',fwd_phase_ts=? WHERE id=?",
                    (datetime.now().isoformat(), self.sbid))
        self._task = asyncio.create_task(self._loop())
        return True, "▶️ Forwarding started."

    def stop_forwarding(self):
        self._stop.set()
        if self._task: self._task.cancel()
        self.db.run("UPDATE selfbots SET fwd_running=0,fwd_paused=0 WHERE id=?", (self.sbid,))

    def pause_forwarding(self): self.db.run("UPDATE selfbots SET fwd_paused=1 WHERE id=?", (self.sbid,))
    def resume_forwarding(self): self.db.run("UPDATE selfbots SET fwd_paused=0 WHERE id=?", (self.sbid,))

    async def _loop(self):
        idx = self.db.get_selfbot(self.sbid)["fwd_cycle_idx"] or 0
        try:
            while not self._stop.is_set():
                r = self.db.get_selfbot(self.sbid)
                if not r or not r["fwd_running"]: break
                if r["fwd_paused"]: await asyncio.sleep(3); continue
                if r["fwd_phase"] == "holding":
                    ts = datetime.fromisoformat(r["fwd_phase_ts"])
                    if (datetime.now()-ts).total_seconds() < HOLD_MINUTES*60:
                        await asyncio.sleep(10); continue
                    self.db.run("UPDATE selfbots SET fwd_phase='forwarding',fwd_phase_ts=?,fwd_cycle_idx=0 WHERE id=?",
                                (datetime.now().isoformat(), self.sbid))
                    idx = 0; continue
                groups = self.db.groups_for_user(r["user_id"])
                if not groups: await asyncio.sleep(30); continue
                r = self.db.get_selfbot(self.sbid)
                if not r["ad_message"]: await asyncio.sleep(15); continue
                if idx >= len(groups):
                    self.db.run("UPDATE selfbots SET fwd_phase='holding',fwd_phase_ts=?,fwd_cycle_idx=0 WHERE id=?",
                                (datetime.now().isoformat(), self.sbid))
                    idx = 0; continue
                group = groups[idx]; chat_id = group["chat_id"]
                idx += 1
                self.db.run("UPDATE selfbots SET fwd_cycle_idx=? WHERE id=?", (idx, self.sbid))
                try:
                    ad = json.loads(r["ad_message"])
                    ok, err = await self._send(ad, chat_id, group.get("is_forum", 0))
                    self.db.run("INSERT INTO fwd_log(selfbot_id,chat_id,group_title,success,error) VALUES(?,?,?,?,?)",
                                (self.sbid, chat_id, group.get("title",""), 1 if ok else 0, err or None))
                except asyncio.CancelledError: raise
                except Exception as e: logger.warning("send sb%s→%s: %s", self.sbid, chat_id, e)
                delay = random.randint(MIN_DELAY, MAX_DELAY)
                try: await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError: pass
        except asyncio.CancelledError: pass
        except Exception as e: logger.error("loop sb%s: %s", self.sbid, e)
        finally: self.db.run("UPDATE selfbots SET fwd_running=0,fwd_paused=0 WHERE id=?", (self.sbid,))

    async def _send(self, ad, chat_id, is_forum) -> Tuple[bool, str]:
        if not self.fwd: return False, "Not connected"
        mode = ad.get("mode","forward"); src = ad.get("src_chat"); msgid = ad.get("src_msgid")
        if not src or not msgid: return False, "Invalid ad data"
        tids = ([r["topic_id"] for r in self.db.get_topics(self.sbid, chat_id)] or [None]) if is_forum else [None]
        any_ok, last_err = False, ""
        for tid in tids:
            ok, err = (await self.fwd.forward(src, msgid, chat_id, topic_id=tid)
                       if mode == "forward"
                       else await self.fwd.send_copy(src, msgid, chat_id, topic_id=tid))
            if ok: any_ok = True; last_err = ""
            else:
                last_err = err
                logger.warning("fail sb%s→%s tid=%s: %s", self.sbid, chat_id, tid, err)
                if "FloodWait" in err: return False, err
        return (True, "") if any_ok else (False, last_err)


# ══════════════════════════════════════════════════════════════════════
#  REGISTRY
# ══════════════════════════════════════════════════════════════════════

class Registry:
    def __init__(self): self._p: Dict[int, SelfbotInstance] = {}
    def get(self, sbid): return self._p.get(sbid)
    def get_or_create(self, db, sbid):
        if sbid not in self._p: self._p[sbid] = SelfbotInstance(db, sbid)
        return self._p[sbid]
    async def restore_all(self, db):
        for r in db.q("SELECT id FROM selfbots WHERE is_active=1"):
            inst = self.get_or_create(db, r["id"])
            ok   = await inst.restore()
            if ok:
                row = db.get_selfbot(r["id"])
                if row and row["fwd_running"]: await inst.start_forwarding()


# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════

def fmt_exp(exp):
    if not exp: return "♾ Never"
    dt = datetime.fromisoformat(exp)
    if dt < datetime.now(): return "⛔ Expired"
    d = dt - datetime.now(); h, rem = divmod(int(d.total_seconds()), 3600)
    return f"{h//24}d {h%24}h {rem//60}m left"

async def edit(q, text, kb=None):
    kw = {"parse_mode": ParseMode.MARKDOWN}
    if kb: kw["reply_markup"] = kb
    try: await q.edit_message_text(text, **kw)
    except BadRequest as e:
        if "not modified" in str(e).lower() or "not found" in str(e).lower(): pass
        else: logger.error("edit failed: %s", e); raise
    except Exception as e: logger.error("edit err: %s", e); raise

async def answer(q):
    try: await q.answer()
    except Exception as e: logger.warning("answer failed: %s", e)


# ══════════════════════════════════════════════════════════════════════
#  BOT
# ══════════════════════════════════════════════════════════════════════

class AdBot:
    def __init__(self, token, admin_id):
        self.token = token; self.admin_id = admin_id
        self.db = Database(); self.reg = Registry(); self.app = None

    def _any_auth(self):
        for sb in self.db.q("SELECT id FROM selfbots WHERE is_active=1"):
            inst = self.reg.get_or_create(self.db, sb["id"])
            if inst.is_auth: return inst
        return None

    def _user_auth(self, uid):
        for sb in self.db.get_selfbots(uid):
            inst = self.reg.get_or_create(self.db, sb["id"])
            if inst.is_auth: return inst
        return None

    def _is_admin(self, uid): return uid == self.admin_id
    def _is_authed(self, uid): return self._is_admin(uid) or self.db.user_authorized(uid)

    async def _reply(self, msg, text, **kw):
        for i in range(3):
            try: await msg.reply_text(text, **kw); return True
            except Exception as e:
                if "timed out" in str(e).lower(): await asyncio.sleep(2**i)
                else: logger.error("reply err: %s", e); return False
        return False

    def run(self, proxy=None):
        builder = Application.builder().token(self.token)
        if proxy:
            from telegram.request import HTTPXRequest
            builder = builder.request(HTTPXRequest(proxy=proxy))
            print(f"🔀  Proxy: {proxy}")
        self.app = builder.build()

        for cmd, fn in [
            ("start",     self._cmd_start),
            ("menu",      self._cmd_start),
            ("cancel",    self._cmd_cancel),
            ("selfbots",  self._cmd_selfbots),
            ("groups",    self._cmd_groups),
            ("status",    self._cmd_status),
            ("sync",      self._cmd_sync),
            ("start_all", self._cmd_start_all),
            ("stop_all",  self._cmd_stop_all),
            ("admin",     self._cmd_admin),
        ]:
            self.app.add_handler(CommandHandler(cmd, fn))

        self.app.add_handler(CallbackQueryHandler(self._dispatch))
        self.app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self._on_msg))
        self.app.add_error_handler(self._on_err)
        self.app.post_init = self._post_init
        print("✅  ProAdBot v2.2 starting…")
        self.app.run_polling(drop_pending_updates=True)

    async def _post_init(self, app):
        await app.bot.set_my_commands([
            BotCommand("start",     "Main menu"),
            BotCommand("selfbots",  "Manage selfbots"),
            BotCommand("groups",    "Manage groups"),
            BotCommand("status",    "Forwarding status"),
            BotCommand("sync",      "Sync groups from selfbot"),
            BotCommand("start_all", "Start all selfbots"),
            BotCommand("stop_all",  "Stop all selfbots"),
            BotCommand("cancel",    "Cancel current input"),
        ])
        await self.reg.restore_all(self.db)

    async def _on_err(self, update, ctx):
        import traceback
        tb = "".join(traceback.format_exception(type(ctx.error), ctx.error, ctx.error.__traceback__))
        logger.error("UNHANDLED\n%s", tb)
        if update and update.callback_query:
            with suppress(Exception): await update.callback_query.answer("⚠️ Error. Check console.", show_alert=True)

    # ══════════════════════════════════════════════════
    #  SLASH COMMANDS
    # ══════════════════════════════════════════════════

    async def _guard(self, update) -> bool:
        uid = update.effective_user.id
        if not self._is_authed(uid):
            await update.message.reply_text("❌ No access. Contact @YourUsername.")
            return False
        return True

    async def _cmd_start(self, update: Update, ctx):
        uid = update.effective_user.id
        flow = self.db.get_flow(uid)
        if flow:
            await update.message.reply_text(
                f"⚠️ Active step: `{flow.get('step','?')}`\n\nSend input or /cancel.",
                parse_mode=ParseMode.MARKDOWN); return
        if self._is_admin(uid):    await self._m_admin(update, ctx)
        elif self._is_authed(uid): await self._m_main(update, ctx)
        else:                      await self._m_public(update, ctx)

    async def _cmd_cancel(self, update: Update, ctx):
        uid = update.effective_user.id; flow = self.db.get_flow(uid)
        if flow: self.db.clear_flow(uid); await update.message.reply_text(f"❌ Cancelled `{flow.get('step','?')}`.", parse_mode=ParseMode.MARKDOWN)
        else: await update.message.reply_text("Nothing to cancel.")

    async def _cmd_selfbots(self, u, c):
        if not await self._guard(u): return
        await self._m_selfbots(u, c)

    async def _cmd_groups(self, u, c):
        if not await self._guard(u): return
        await self._m_groups(u, c)

    async def _cmd_status(self, u, c):
        if not await self._guard(u): return
        await self._m_status(u, c)

    async def _cmd_admin(self, u, c):
        if not self._is_admin(u.effective_user.id): await u.message.reply_text("❌ Admin only."); return
        await self._m_admin(u, c)

    async def _cmd_start_all(self, update: Update, ctx):
        if not await self._guard(update): return
        uid = update.effective_user.id; results = []
        for sb in self.db.get_selfbots(uid):
            inst = self.reg.get_or_create(self.db, sb["id"])
            ok, msg = await inst.start_forwarding()
            results.append(f"Selfbot {sb['slot']}: {msg}")
        await update.message.reply_text("\n".join(results) or "No selfbots found.")

    async def _cmd_stop_all(self, update: Update, ctx):
        if not await self._guard(update): return
        uid = update.effective_user.id; n = 0
        for sb in self.db.get_selfbots(uid):
            self.reg.get_or_create(self.db, sb["id"]).stop_forwarding(); n += 1
        await update.message.reply_text(f"⏹ Stopped {n} selfbot(s).")

    async def _cmd_sync(self, update: Update, ctx):
        """Scan selfbot dialogs and show group list to toggle on/off."""
        if not await self._guard(update): return
        uid  = update.effective_user.id
        inst = self._user_auth(uid)
        if not inst:
            await update.message.reply_text("❌ No connected selfbot. Use /selfbots to connect one.")
            return
        msg = await update.message.reply_text("🔄 Scanning selfbot's dialogs…")
        groups = await inst.get_all_groups()
        if not groups:
            await msg.edit_text("❌ No groups found in selfbot's dialogs.")
            return
        plan  = self.db.get_plan(uid)
        limit = plan["extra_groups"] if plan else 5
        self.db.set_flow(uid, {"step": "sync_view", "groups": groups, "page": 0, "limit": limit})
        await msg.delete()
        await self._show_sync_page(update, uid, page=0)

    # ══════════════════════════════════════════════════
    #  DISPATCHER
    # ══════════════════════════════════════════════════

    async def _dispatch(self, update: Update, ctx):
        q = update.callback_query; uid = update.effective_user.id; d = q.data or ""
        await answer(q)
        logger.debug("CB uid=%s d=%s", uid, d)
        try: await self._route(update, ctx, uid, d)
        except Exception as e:
            import traceback
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            logger.error("DISPATCH uid=%s d=%s\n%s", uid, d, tb)
            with suppress(Exception): await q.edit_message_text(f"⚠️ Error: {e}")

    async def _route(self, update, ctx, uid, d):
        q = update.callback_query
        if d == "pub_contact": return await self._m_public(update, ctx)
        if not self._is_authed(uid): return await q.edit_message_text("❌ No access.")

        # — admin —
        if d == "admin_menu":             return await self._m_admin(update, ctx)
        if d == "admin_users":            return await self._adm_users(update, ctx)
        if d == "admin_add_user":         return await self._adm_add_user_p(update, ctx)
        if d.startswith("admin_revoke_"): return await self._adm_revoke(update, ctx, d)
        if d.startswith("admin_eu_"):     return await self._adm_edit_user(update, ctx, d)
        if d == "admin_gg":               return await self._adm_gg(update, ctx)
        if d == "admin_add_gg":           return await self._adm_add_gg_p(update, ctx)
        if d.startswith("admin_del_gg_"): return await self._adm_del_gg(update, ctx, d)
        if d == "admin_broadcast":        return await self._adm_bc_p(update, ctx)
        if d == "admin_stats":            return await self._adm_stats(update, ctx)
        # — main —
        if d == "main_menu":              return await self._m_main(update, ctx)
        if d == "m_selfbots":             return await self._m_selfbots(update, ctx)
        if d == "m_groups":               return await self._m_groups(update, ctx)
        if d == "m_status":               return await self._m_status(update, ctx)
        if d == "m_sync":                 return await self._cb_sync(update, ctx, uid)
        # — selfbot —
        if d.startswith("sb_sel_"):       return await self._sb_select(update, ctx, d)
        if d.startswith("sb_men_"):       return await self._sb_menu_id(update, ctx, int(d[7:]))
        if d.startswith("sb_conn_"):      return await self._sb_conn_p(update, ctx, d)
        if d.startswith("sb_disc_"):      return await self._sb_disc(update, ctx, d)
        if d.startswith("sb_seta_"):      return await self._sb_seta_p(update, ctx, d)
        if d.startswith("sb_clra_"):      return await self._sb_clra(update, ctx, d)
        if d.startswith("sb_pfp_"):       return await self._sb_pfp(update, ctx, d)
        if d.startswith("sb_ename_"):     return await self._sb_ename_p(update, ctx, d)
        if d.startswith("sb_ebio_"):      return await self._sb_ebio_p(update, ctx, d)
        if d.startswith("sb_ephoto_"):    return await self._sb_ephoto_p(update, ctx, d)
        if d.startswith("sb_fstart_"):    return await self._sb_fstart(update, ctx, d)
        if d.startswith("sb_fstop_"):     return await self._sb_fstop(update, ctx, d)
        if d.startswith("sb_fpause_"):    return await self._sb_fpause(update, ctx, d)
        if d.startswith("sb_fresume_"):   return await self._sb_fresume(update, ctx, d)
        if d.startswith("sb_topics_"):    return await self._sb_topics(update, ctx, d)
        if d.startswith("sbt_g_"):        return await self._sb_topics_grp(update, ctx, d)
        if d.startswith("sbt_add_"):      return await self._sb_topic_add(update, ctx, d)
        if d.startswith("sbt_rm_"):       return await self._sb_topic_rm(update, ctx, d)
        if d.startswith("sbt_kw_"):       return await self._sb_topic_kw_p(update, ctx, d)
        # — groups —
        if d == "g_global":               return await self._g_global(update, ctx)
        if d == "g_mycustom":             return await self._g_custom(update, ctx)
        if d == "g_add":                  return await self._g_add_p(update, ctx)
        if d.startswith("g_tog_"):        return await self._g_toggle(update, ctx, d)
        if d.startswith("g_del_"):        return await self._g_delete(update, ctx, d)
        # — sync —
        if d.startswith("sync_pg_"):      return await self._cb_sync_page(update, ctx, uid, d)
        if d.startswith("sync_tog_"):     return await self._cb_sync_tog(update, ctx, uid, d)
        if d == "sync_done":              return await self._cb_sync_done(update, ctx, uid)
        if d == "noop":                   return  # pagination label button
        await q.edit_message_text("⚠️ Unknown action.")

    # ══════════════════════════════════════════════════
    #  PUBLIC
    # ══════════════════════════════════════════════════

    async def _m_public(self, update, ctx):
        text = "👋 *ProAdBot v2.2*\n\nPrivate Telegram ad forwarding system.\n\nContact @YourUsername for access."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Get Access", url="https://t.me/YourUsername")]])
        if update.callback_query: await edit(update.callback_query, text, kb)
        else: await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    # ══════════════════════════════════════════════════
    #  ADMIN
    # ══════════════════════════════════════════════════

    async def _m_admin(self, update, ctx):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Users",         callback_data="admin_users"),
             InlineKeyboardButton("📊 Stats",         callback_data="admin_stats")],
            [InlineKeyboardButton("➕ Add User",       callback_data="admin_add_user")],
            [InlineKeyboardButton("🌐 Global Groups",  callback_data="admin_gg")],
            [InlineKeyboardButton("📢 Broadcast",      callback_data="admin_broadcast")],
            [InlineKeyboardButton("🏠 My Panel",       callback_data="main_menu")],
        ])
        if update.callback_query: await edit(update.callback_query, "🔐 *Admin Panel*", kb)
        else: await update.message.reply_text("🔐 *Admin Panel*", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _adm_users(self, update, ctx):
        rows = self.db.q("SELECT * FROM users ORDER BY granted_at DESC")
        lines = ["📋 *Users*\n"]; btns = []
        for r in rows:
            ok = self.db.user_authorized(r["user_id"])
            pl = PLANS.get(r["plan"], {}).get("label", r["plan"])
            lines.append(f"{'✅' if ok else '❌'} `{r['user_id']}` — {pl} — {fmt_exp(r['expires_at'])}")
            btns.append([InlineKeyboardButton(f"✏️ {r['user_id']}", callback_data=f"admin_eu_{r['user_id']}"),
                         InlineKeyboardButton("🗑 Revoke",           callback_data=f"admin_revoke_{r['user_id']}")])
        if not rows: lines.append("_No users yet._")
        btns.append([InlineKeyboardButton("🔙 Back", callback_data="admin_menu")])
        await edit(update.callback_query, "\n".join(lines), InlineKeyboardMarkup(btns))

    async def _adm_add_user_p(self, update, ctx):
        uid = update.effective_user.id; self.db.set_flow(uid, {"step": "adm_add_user"})
        await edit(update.callback_query,
                   "➕ *Add User*\n\nFormat: `<user_id> <plan> [days]`\nPlans: `basic` `pro` `elite`\n\nExample: `123456789 pro 30`",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]]))

    async def _adm_edit_user(self, update, ctx, d):
        target = int(d[9:]); r = self.db.get_user(target)
        if not r: return await edit(update.callback_query, "User not found.")
        uid = update.effective_user.id; self.db.set_flow(uid, {"step": "adm_edit_user", "target": target})
        pl = PLANS.get(r["plan"],{}).get("label",r["plan"])
        await edit(update.callback_query,
                   f"✏️ *Edit* `{target}`\nPlan: {pl}  ·  Expires: {fmt_exp(r['expires_at'])}\n\nSend: `<plan> [days]`",
                   InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]))

    async def _adm_revoke(self, update, ctx, d):
        target = int(d[13:]); self.db.run("UPDATE users SET is_active=0 WHERE user_id=?", (target,))
        for sb in self.db.get_selfbots(target):
            inst = self.reg.get(sb["id"]);
            if inst: inst.stop_forwarding()
        await edit(update.callback_query, f"✅ User `{target}` revoked.",
                   InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]))

    async def _adm_gg(self, update, ctx):
        rows = self.db.q("SELECT * FROM global_groups ORDER BY title")
        lines = ["🌐 *Global Groups*\n"]; btns = []
        for r in rows:
            tag = "📋" if r["is_forum"] else "👥"
            lines.append(f"{tag} {r['title']} (`{r['chat_id']}`)")
            btns.append([InlineKeyboardButton(f"🗑 {r['title'][:28]}", callback_data=f"admin_del_gg_{r['id']}")])
        if not rows: lines.append("_None yet._")
        btns.append([InlineKeyboardButton("➕ Add", callback_data="admin_add_gg"),
                     InlineKeyboardButton("🔙 Back", callback_data="admin_menu")])
        await edit(update.callback_query, "\n".join(lines), InlineKeyboardMarkup(btns))

    async def _adm_add_gg_p(self, update, ctx):
        uid = update.effective_user.id; self.db.set_flow(uid, {"step": "adm_add_gg"})
        await edit(update.callback_query,
                   "➕ *Add Global Group*\n\nSend username, chat ID, or multiple (one per line).",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_gg")]]))

    async def _adm_del_gg(self, update, ctx, d):
        self.db.run("DELETE FROM global_groups WHERE id=?", (int(d[13:]),))
        await self._adm_gg(update, ctx)

    async def _adm_bc_p(self, update, ctx):
        uid = update.effective_user.id; self.db.set_flow(uid, {"step": "adm_broadcast"})
        await edit(update.callback_query, "📢 *Broadcast*\n\nSend any message.",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]]))

    async def _adm_stats(self, update, ctx):
        tu   = self.db.one("SELECT COUNT(*) as n FROM users")["n"]
        au   = sum(1 for r in self.db.q("SELECT user_id FROM users") if self.db.user_authorized(r["user_id"]))
        tsb  = self.db.one("SELECT COUNT(*) as n FROM selfbots WHERE is_active=1")["n"]
        rsb  = self.db.one("SELECT COUNT(*) as n FROM selfbots WHERE fwd_running=1")["n"]
        tfwd = self.db.one("SELECT COUNT(*) as n FROM fwd_log WHERE success=1")["n"]
        tgg  = self.db.one("SELECT COUNT(*) as n FROM global_groups")["n"]
        await edit(update.callback_query,
                   f"📊 *Stats*\n\n👥 Users: `{tu}` (active: `{au}`)\n🤖 Selfbots: `{tsb}` (fwd: `{rsb}`)\n📤 Sent: `{tfwd}`\n🌐 Global: `{tgg}`",
                   InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_menu")]]))

    # ══════════════════════════════════════════════════
    #  MAIN MENU
    # ══════════════════════════════════════════════════

    async def _m_main(self, update, ctx):
        uid  = update.effective_user.id
        user = self.db.get_user(uid)
        pl   = PLANS.get(user["plan"], {}) if user else {}
        exp  = fmt_exp(user["expires_at"]) if user else "—"
        sbs  = self.db.get_selfbots(uid)
        n_run = sum(1 for s in sbs if s["fwd_running"])
        n_con = sum(1 for s in sbs if self.reg.get(s["id"]) and self.reg.get(s["id"]).is_auth)
        g_tot = len(self.db.groups_for_user(uid))
        text = (f"👋 *ProAdBot v2.2*\n\n"
                f"Plan: {pl.get('label','—')}  ·  {exp}\n"
                f"Selfbots: {n_con} connected  ·  {n_run} forwarding\n"
                f"Groups: {g_tot} active\n\n"
                "_Use buttons or slash commands_")
        rows = [
            [InlineKeyboardButton("🤖 Selfbots",  callback_data="m_selfbots"),
             InlineKeyboardButton("👥 Groups",     callback_data="m_groups")],
            [InlineKeyboardButton("📊 Status",     callback_data="m_status"),
             InlineKeyboardButton("🔄 Sync Groups",callback_data="m_sync")],
        ]
        if self._is_admin(uid):
            rows.append([InlineKeyboardButton("🔐 Admin", callback_data="admin_menu")])
        kb = InlineKeyboardMarkup(rows)
        if update.callback_query: await edit(update.callback_query, text, kb)
        else: await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    # ══════════════════════════════════════════════════
    #  SELFBOTS
    # ══════════════════════════════════════════════════

    async def _m_selfbots(self, update, ctx):
        uid  = update.effective_user.id
        plan = self.db.get_plan(uid)
        if not plan:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]])
            if update.callback_query: return await edit(update.callback_query, "❌ No plan assigned.", kb)
            return await update.message.reply_text("❌ No plan assigned.", reply_markup=kb)
        slots = plan["selfbots"]; rows_map = {r["slot"]: r for r in self.db.get_selfbots(uid)}
        lines = ["🤖 *Selfbots*\n"]; btns = []
        for s in range(1, slots+1):
            r = rows_map.get(s)
            if r:
                inst = self.reg.get(r["id"]); auth = inst and inst.is_auth
                if r["fwd_running"] and not r["fwd_paused"]: ico = "🟢▶️"
                elif r["fwd_paused"]: ico = "🟡⏸"
                elif auth: ico = "🟢⏹"
                else: ico = "🔴"
                lbl = f"{ico} Slot {s} — {r['display_name'] or r['phone'] or '—'}"
            else:
                lbl = f"⬜ Slot {s} — tap to setup"
            lines.append(f"• {lbl}")
            btns.append([InlineKeyboardButton(lbl, callback_data=f"sb_sel_{s}")])
        btns.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        kb = InlineKeyboardMarkup(btns)
        if update.callback_query: await edit(update.callback_query, "\n".join(lines), kb)
        else: await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _sb_select(self, update, ctx, d):
        slot = int(d[7:]); uid = update.effective_user.id
        sbid = self.db.ensure_sb_slot(uid, slot)
        await self._sb_menu_id(update, ctx, sbid)

    async def _sb_menu_id(self, update, ctx, sbid):
        r    = self.db.get_selfbot(sbid)
        inst = self.reg.get_or_create(self.db, sbid)
        auth = inst.is_auth
        if r["fwd_running"] and not r["fwd_paused"]: fst = "▶️ Running"
        elif r["fwd_paused"]: fst = "⏸ Paused"
        else: fst = "⏹ Stopped"
        ph_i = ""
        if r["fwd_phase_ts"] and r["fwd_running"]:
            ts = datetime.fromisoformat(r["fwd_phase_ts"]); el = (datetime.now()-ts).total_seconds()
            if r["fwd_phase"] == "holding":
                left = max(0, HOLD_MINUTES*60-el)
                ph_i = f" _(hold {int(left//60)}m left)_"
            else:
                tot = len(self.db.groups_for_user(r["user_id"]))
                ph_i = f" _{r['fwd_cycle_idx']}/{tot}_"
        recent = self.db.q("SELECT group_title,success,sent_at FROM fwd_log WHERE selfbot_id=? ORDER BY id DESC LIMIT 3", (r["id"],))
        log_txt = ""
        if recent:
            log_txt = "\n\n*Recent:*\n" + "\n".join(
                f"{'✅' if lg['success'] else '❌'} {lg['group_title'] or '?'}" for lg in recent)
        nm   = r["display_name"] or r["phone"] or f"Slot {r['slot']}"
        text = (f"🤖 *Selfbot {r['slot']}* — {nm}\n\n"
                f"{'🟢 Connected' if auth else '🔴 Not connected'}  ·  {fst}{ph_i}\n"
                f"Ad: {'✅ Set' if r['ad_message'] else '❌ Not set'}"
                f"{log_txt}")
        conn_btn = (InlineKeyboardButton("🔌 Disconnect", callback_data=f"sb_disc_{sbid}")
                    if auth else InlineKeyboardButton("🔗 Connect", callback_data=f"sb_conn_{sbid}"))
        if r["fwd_running"] and not r["fwd_paused"]:
            fwd_row = [InlineKeyboardButton("⏸ Pause", callback_data=f"sb_fpause_{sbid}"),
                       InlineKeyboardButton("⏹ Stop",  callback_data=f"sb_fstop_{sbid}")]
        elif r["fwd_paused"]:
            fwd_row = [InlineKeyboardButton("▶️ Resume", callback_data=f"sb_fresume_{sbid}"),
                       InlineKeyboardButton("⏹ Stop",    callback_data=f"sb_fstop_{sbid}")]
        else:
            fwd_row = [InlineKeyboardButton("▶️ Start Forwarding", callback_data=f"sb_fstart_{sbid}")]
        kb = InlineKeyboardMarkup([
            [conn_btn, InlineKeyboardButton("📝 Set Ad", callback_data=f"sb_seta_{sbid}")],
            fwd_row,
            [InlineKeyboardButton("👤 Profile",  callback_data=f"sb_pfp_{sbid}"),
             InlineKeyboardButton("🎯 Topics",   callback_data=f"sb_topics_{sbid}"),
             InlineKeyboardButton("🗑 Clear Ad", callback_data=f"sb_clra_{sbid}")],
            [InlineKeyboardButton("🔙 Back",     callback_data="m_selfbots"),
             InlineKeyboardButton("🔄 Refresh",  callback_data=f"sb_men_{sbid}")],
        ])
        await edit(update.callback_query, text, kb)

    async def _sb_conn_p(self, update, ctx, d):
        sbid = int(d[8:]); uid = update.effective_user.id
        self.db.set_flow(uid, {"step": "sb_phone", "sbid": sbid})
        await edit(update.callback_query,
                   "🔗 *Connect Selfbot*\n\nSend phone number with country code:\n`+447911123456`",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_disc(self, update, ctx, d):
        sbid = int(d[8:]); inst = self.reg.get_or_create(self.db, sbid)
        await inst.disconnect()
        for f in SESSIONS_DIR.glob(f"sb_{sbid}*"):
            with suppress(Exception): f.unlink()
        self.db.run("UPDATE selfbots SET api_id=NULL,api_hash=NULL,phone=NULL,session_file=NULL,is_active=0 WHERE id=?", (sbid,))
        await edit(update.callback_query, "✅ Selfbot disconnected.",
                   InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_seta_p(self, update, ctx, d):
        sbid = int(d[8:]); uid = update.effective_user.id
        self.db.set_flow(uid, {"step": "sb_set_ad", "sbid": sbid})
        slot = self.db.get_selfbot(sbid)["slot"]
        await edit(update.callback_query,
                   f"📝 *Set Ad — Selfbot {slot}*\n\n"
                   "**A)** Forward a message from a channel → native forward\n"
                   "**B)** Type a message directly → sent as new\n\n"
                   "⚠️ For A, selfbot must be in the source channel.",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_clra(self, update, ctx, d):
        sbid = int(d[8:]); self.db.run("UPDATE selfbots SET ad_message=NULL WHERE id=?", (sbid,))
        await edit(update.callback_query, "✅ Ad cleared.",
                   InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_fstart(self, update, ctx, d):
        sbid = int(d[10:]); inst = self.reg.get_or_create(self.db, sbid)
        ok, msg = await inst.start_forwarding()
        await edit(update.callback_query, msg, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_fstop(self, update, ctx, d):
        sbid = int(d[9:]); self.reg.get_or_create(self.db, sbid).stop_forwarding()
        await edit(update.callback_query, "⏹ Stopped.", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_fpause(self, update, ctx, d):
        sbid = int(d[10:]); self.reg.get_or_create(self.db, sbid).pause_forwarding()
        await edit(update.callback_query, "⏸ Paused.", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_fresume(self, update, ctx, d):
        sbid = int(d[11:]); self.reg.get_or_create(self.db, sbid).resume_forwarding()
        await edit(update.callback_query, "▶️ Resumed.", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))

    async def _sb_pfp(self, update, ctx, d):
        sbid = int(d[7:]); r = self.db.get_selfbot(sbid)
        await edit(update.callback_query, f"👤 *Profile — Selfbot {r['slot']}*",
                   InlineKeyboardMarkup([
                       [InlineKeyboardButton("✏️ Name", callback_data=f"sb_ename_{sbid}"),
                        InlineKeyboardButton("📝 Bio",  callback_data=f"sb_ebio_{sbid}")],
                       [InlineKeyboardButton("🖼 Photo",callback_data=f"sb_ephoto_{sbid}")],
                       [InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")],
                   ]))

    async def _sb_ename_p(self, update, ctx, d):
        sbid = int(d[9:]); uid = update.effective_user.id; self.db.set_flow(uid, {"step": "sb_ename", "sbid": sbid})
        await edit(update.callback_query, "✏️ Send new name:", InlineKeyboardMarkup([[InlineKeyboardButton("❌", callback_data=f"sb_pfp_{sbid}")]]))

    async def _sb_ebio_p(self, update, ctx, d):
        sbid = int(d[8:]); uid = update.effective_user.id; self.db.set_flow(uid, {"step": "sb_ebio", "sbid": sbid})
        await edit(update.callback_query, "📝 Send new bio:", InlineKeyboardMarkup([[InlineKeyboardButton("❌", callback_data=f"sb_pfp_{sbid}")]]))

    async def _sb_ephoto_p(self, update, ctx, d):
        sbid = int(d[10:]); uid = update.effective_user.id; self.db.set_flow(uid, {"step": "sb_ephoto", "sbid": sbid})
        await edit(update.callback_query, "🖼 Send new photo:", InlineKeyboardMarkup([[InlineKeyboardButton("❌", callback_data=f"sb_pfp_{sbid}")]]))

    async def _sb_topics(self, update, ctx, d):
        sbid   = int(d[10:]); r = self.db.get_selfbot(sbid)
        forums = [g for g in self.db.groups_for_user(r["user_id"]) if g.get("is_forum")]
        if not forums:
            return await edit(update.callback_query, "ℹ️ No forum supergroups in your group list.",
                              InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")]]))
        btns = []
        for g in forums:
            saved = self.db.get_topics(sbid, g["chat_id"])
            tag   = f" ({len(saved)})" if saved else ""
            btns.append([InlineKeyboardButton(f"📋 {g['title'][:32]}{tag}", callback_data=f"sbt_g_{sbid}_{g['chat_id']}")])
        btns.append([InlineKeyboardButton("🔙 Back", callback_data=f"sb_men_{sbid}")])
        await edit(update.callback_query, f"🎯 *Topics — Selfbot {r['slot']}*\n\nSelect group:",
                   InlineKeyboardMarkup(btns))

    async def _sb_topics_grp(self, update, ctx, d):
        parts = d.split("_"); sbid = int(parts[2]); chat_id = int(parts[3])
        uid = update.effective_user.id; inst = self.reg.get_or_create(self.db, sbid)
        if not inst.is_auth:
            return await edit(update.callback_query, "❌ Connect selfbot first.",
                              InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data=f"sb_topics_{sbid}")]]))
        topics = await inst.fetch_topics(chat_id)
        if not topics:
            return await edit(update.callback_query, "⚠️ No open topics found.",
                              InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"sb_topics_{sbid}")]]))
        saved_ids = {r["topic_id"] for r in self.db.get_topics(sbid, chat_id)}
        g = (self.db.one("SELECT title FROM global_groups WHERE chat_id=?", (chat_id,)) or
             self.db.one("SELECT title FROM user_groups WHERE chat_id=?", (chat_id,)))
        gt = g["title"] if g else str(chat_id)
        self.db.set_flow(uid, {"step": "sbt_view", "sbid": sbid, "chat_id": chat_id,
                               "topics": [[t[0], t[1]] for t in topics]})
        btns = []
        for tid, ttitle in topics:
            tick = "✅" if tid in saved_ids else "☑️"
            cb   = f"sbt_rm_{sbid}_{chat_id}_{tid}" if tid in saved_ids else f"sbt_add_{sbid}_{chat_id}_{tid}"
            btns.append([InlineKeyboardButton(f"{tick} {ttitle}", callback_data=cb)])
        btns.append([InlineKeyboardButton("🔍 Filter", callback_data=f"sbt_kw_{sbid}_{chat_id}"),
                     InlineKeyboardButton("🔙 Back",   callback_data=f"sb_topics_{sbid}")])
        r = self.db.get_selfbot(sbid)
        await edit(update.callback_query, f"🎯 *{gt}* — Selfbot {r['slot']}\n\n✅ = will forward to topic.",
                   InlineKeyboardMarkup(btns))

    async def _sb_topic_add(self, update, ctx, d):
        parts = d.split("_"); sbid, chat_id, tid = int(parts[2]), int(parts[3]), int(parts[4])
        uid = update.effective_user.id; flow = self.db.get_flow(uid) or {}
        title = next((t[1] for t in flow.get("topics", []) if t[0] == tid), str(tid))
        saved = self.db.get_topics(sbid, chat_id)
        pairs = [(r["topic_id"], r["topic_title"]) for r in saved]
        if not any(p[0] == tid for p in pairs): pairs.append((tid, title))
        self.db.set_topics(sbid, chat_id, pairs)
        await self._sb_topics_grp(update, ctx, f"sbt_g_{sbid}_{chat_id}")

    async def _sb_topic_rm(self, update, ctx, d):
        parts = d.split("_"); sbid, chat_id, tid = int(parts[2]), int(parts[3]), int(parts[4])
        pairs = [(r["topic_id"], r["topic_title"]) for r in self.db.get_topics(sbid, chat_id) if r["topic_id"] != tid]
        self.db.set_topics(sbid, chat_id, pairs)
        await self._sb_topics_grp(update, ctx, f"sbt_g_{sbid}_{chat_id}")

    async def _sb_topic_kw_p(self, update, ctx, d):
        parts = d.split("_"); sbid, chat_id = int(parts[2]), int(parts[3])
        uid = update.effective_user.id; flow = self.db.get_flow(uid) or {}
        flow.update({"step": "sbt_keyword", "sbid": sbid, "chat_id": chat_id}); self.db.set_flow(uid, flow)
        await edit(update.callback_query, "🔍 Send keyword to filter:",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"sbt_g_{sbid}_{chat_id}")]]))

    # ══════════════════════════════════════════════════
    #  GROUPS
    # ══════════════════════════════════════════════════

    async def _m_groups(self, update, ctx):
        uid   = update.effective_user.id; plan = self.db.get_plan(uid)
        limit = plan["extra_groups"] if plan else 0
        total = self.db.custom_group_count(uid); en = self.db.enabled_group_count(uid)
        gg    = len(self.db.q("SELECT id FROM global_groups"))
        text  = (f"👥 *Groups*\n\n"
                 f"🌐 Global: {gg} (always active)\n"
                 f"📋 Custom: {en} enabled · {total} saved · {limit} max")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 View Global", callback_data="g_global"),
             InlineKeyboardButton("📋 My Groups",   callback_data="g_mycustom")],
            [InlineKeyboardButton("➕ Add Manual",  callback_data="g_add"),
             InlineKeyboardButton("🔄 Sync",        callback_data="m_sync")],
            [InlineKeyboardButton("🔙 Back",        callback_data="main_menu")],
        ])
        if update.callback_query: await edit(update.callback_query, text, kb)
        else: await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _g_global(self, update, ctx):
        rows = self.db.q("SELECT * FROM global_groups ORDER BY title")
        lines = ["🌐 *Global Groups*\n"]
        for r in rows: lines.append(f"{'📋' if r['is_forum'] else '👥'} {r['title']}")
        if len(lines) == 1: lines.append("_None yet._")
        await edit(update.callback_query, "\n".join(lines),
                   InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="m_groups")]]))

    async def _g_custom(self, update, ctx):
        uid = update.effective_user.id
        rows = self.db.q("SELECT * FROM user_groups WHERE user_id=? ORDER BY title", (uid,))
        lines = ["📋 *Your Groups*\n"]; btns = []
        for r in rows:
            en = "✅" if r["is_enabled"] else "❌"; tag = "📋" if r["is_forum"] else "👥"
            lines.append(f"{en} {tag} {r['title']}")
            btns.append([InlineKeyboardButton(f"{'🔕' if r['is_enabled'] else '🔔'} {r['title'][:24]}",
                                              callback_data=f"g_tog_{r['id']}"),
                         InlineKeyboardButton("🗑", callback_data=f"g_del_{r['id']}")])
        if not rows: lines.append("_No groups yet. Use ➕ Add or 🔄 Sync._")
        btns.append([InlineKeyboardButton("🔙 Back", callback_data="m_groups")])
        await edit(update.callback_query, "\n".join(lines), InlineKeyboardMarkup(btns))

    async def _g_add_p(self, update, ctx):
        uid = update.effective_user.id; plan = self.db.get_plan(uid)
        if not plan: return await edit(update.callback_query, "❌ No plan found.")
        limit = plan["extra_groups"]; count = self.db.custom_group_count(uid)
        if count >= limit:
            return await edit(update.callback_query,
                              f"❌ Limit reached ({count}/{limit}). Upgrade or use 🔄 Sync.",
                              InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="m_groups")]]))
        self.db.set_flow(uid, {"step": "g_add"})
        await edit(update.callback_query,
                   "➕ *Add Group Manually*\n\nOne per line:\n`@MyGroup`  `https://t.me/X`  `-1001234567890`\n\n_Selfbot must be a member._",
                   InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m_groups")]]))

    async def _g_toggle(self, update, ctx, d):
        gid = int(d[6:]); r = self.db.one("SELECT is_enabled FROM user_groups WHERE id=?", (gid,))
        if r: self.db.run("UPDATE user_groups SET is_enabled=? WHERE id=?", (0 if r["is_enabled"] else 1, gid))
        await self._g_custom(update, ctx)

    async def _g_delete(self, update, ctx, d):
        self.db.run("DELETE FROM user_groups WHERE id=?", (int(d[6:]),))
        await self._g_custom(update, ctx)

    # ══════════════════════════════════════════════════
    #  SYNC GROUPS
    # ══════════════════════════════════════════════════

    async def _cb_sync(self, update, ctx, uid):
        inst = self._user_auth(uid)
        if not inst:
            return await edit(update.callback_query,
                              "❌ No connected selfbot.\nConnect one via 🤖 Selfbots first.",
                              InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="m_groups")]]))
        await edit(update.callback_query, "🔄 Scanning selfbot's dialogs…")
        groups = await inst.get_all_groups()
        if not groups:
            return await edit(update.callback_query, "❌ No groups found.",
                              InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="m_groups")]]))
        plan  = self.db.get_plan(uid)
        limit = plan["extra_groups"] if plan else 5
        self.db.set_flow(uid, {"step": "sync_view", "groups": groups, "page": 0, "limit": limit})
        await self._show_sync_page(update, uid, page=0)

    async def _show_sync_page(self, update, uid, page):
        flow = self.db.get_flow(uid)
        if not flow or flow.get("step") != "sync_view": return
        groups = flow["groups"]; limit = flow["limit"]
        total  = len(groups); pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page   = max(0, min(page, pages-1))
        flow["page"] = page; self.db.set_flow(uid, flow)
        chunk = groups[page*PAGE_SIZE: (page+1)*PAGE_SIZE]
        enabled_count = self.db.enabled_group_count(uid)
        btns = []
        for g in chunk:
            cid = g["chat_id"]
            in_db  = self.db.one("SELECT id,is_enabled FROM user_groups WHERE user_id=? AND chat_id=?", (uid, cid))
            enabled = bool(in_db and in_db["is_enabled"])
            tick    = "✅" if enabled else "☑️"
            tag     = "📋" if g["is_forum"] else "👥"
            btns.append([InlineKeyboardButton(f"{tick} {tag} {g['title'][:30]}", callback_data=f"sync_tog_{cid}")])
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"sync_pg_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("▶️", callback_data=f"sync_pg_{page+1}"))
        if nav: btns.append(nav)
        btns.append([InlineKeyboardButton("✅ Done", callback_data="sync_done"),
                     InlineKeyboardButton("🔙 Back", callback_data="m_groups")])
        text = (f"🔄 *Sync Groups* — {total} found\n\n"
                f"Enabled: {enabled_count} / {limit}  _(plan limit)_\n\n"
                "✅ = active  ·  Tap to toggle")
        if update.callback_query: await edit(update.callback_query, text, InlineKeyboardMarkup(btns))
        else: await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=InlineKeyboardMarkup(btns))

    async def _cb_sync_page(self, update, ctx, uid, d):
        await self._show_sync_page(update, uid, page=int(d[8:]))

    async def _cb_sync_tog(self, update, ctx, uid, d):
        cid = int(d[9:]); flow = self.db.get_flow(uid)
        if not flow: return
        limit  = flow.get("limit", 5)
        g_info = next((g for g in flow.get("groups", []) if g["chat_id"] == cid), None)
        if not g_info: return
        in_db = self.db.one("SELECT id,is_enabled FROM user_groups WHERE user_id=? AND chat_id=?", (uid, cid))
        if in_db:
            new_state = 0 if in_db["is_enabled"] else 1
            if new_state == 1 and self.db.enabled_group_count(uid) >= limit:
                await update.callback_query.answer(f"⚠️ Limit {limit} reached. Disable another first.", show_alert=True); return
            self.db.run("UPDATE user_groups SET is_enabled=? WHERE id=?", (new_state, in_db["id"]))
        else:
            if self.db.enabled_group_count(uid) >= limit:
                await update.callback_query.answer(f"⚠️ Limit {limit} reached. Disable another first.", show_alert=True); return
            self.db.run("INSERT OR IGNORE INTO user_groups(user_id,chat_id,title,username,is_forum,is_enabled) VALUES(?,?,?,?,?,1)",
                        (uid, cid, g_info["title"], g_info.get("username"), int(g_info["is_forum"])))
        await self._show_sync_page(update, uid, page=flow.get("page", 0))

    async def _cb_sync_done(self, update, ctx, uid):
        self.db.clear_flow(uid)
        en = self.db.enabled_group_count(uid); total = self.db.custom_group_count(uid)
        await edit(update.callback_query,
                   f"✅ *Sync complete!*\n\n{en} enabled / {total} saved groups.\n\nReady for forwarding.",
                   InlineKeyboardMarkup([
                       [InlineKeyboardButton("📋 My Groups", callback_data="g_mycustom"),
                        InlineKeyboardButton("🏠 Menu",      callback_data="main_menu")]]))

    # ══════════════════════════════════════════════════
    #  STATUS
    # ══════════════════════════════════════════════════

    async def _m_status(self, update, ctx):
        uid = update.effective_user.id; rows = self.db.get_selfbots(uid)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="m_status"),
                                    InlineKeyboardButton("🔙 Back",    callback_data="main_menu")]])
        if not rows:
            if update.callback_query: return await edit(update.callback_query, "No selfbots configured.", kb)
            return await update.message.reply_text("No selfbots configured.", reply_markup=kb)
        lines = ["📊 *Status*\n"]
        for r in rows:
            inst = self.reg.get(r["id"]); auth = "🟢" if (inst and inst.is_auth) else "🔴"
            fwd  = "▶️" if r["fwd_running"] else ("⏸" if r["fwd_paused"] else "⏹")
            ph   = r["fwd_phase"] or "—"; idx = r["fwd_cycle_idx"] or 0; tot = len(self.db.groups_for_user(uid))
            ph_i = ""
            if r["fwd_phase_ts"] and r["fwd_running"]:
                ts = datetime.fromisoformat(r["fwd_phase_ts"]); el = (datetime.now()-ts).total_seconds()
                ph_i = f"  _(hold {int(max(0,HOLD_MINUTES*60-el)//60)}m left)_" if ph == "holding" else f"  _{idx}/{tot}_"
            lines.append(f"{auth} *Selfbot {r['slot']}*  {fwd} {ph.title()}{ph_i}")
            recent = self.db.q("SELECT group_title,success,sent_at FROM fwd_log WHERE selfbot_id=? ORDER BY id DESC LIMIT 3", (r["id"],))
            for lg in recent: lines.append(f"  {'✅' if lg['success'] else '❌'} {lg['group_title'] or '?'} {lg['sent_at'][-8:-3]}")
            lines.append("")
        tf = self.db.one(f"SELECT COUNT(*) as n FROM fwd_log WHERE success=1 AND selfbot_id IN (SELECT id FROM selfbots WHERE user_id={uid})")["n"]
        lines.append(f"📤 Total sent: `{tf}`")
        if update.callback_query: await edit(update.callback_query, "\n".join(lines), kb)
        else: await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    # ══════════════════════════════════════════════════
    #  MESSAGE HANDLER
    # ══════════════════════════════════════════════════

    async def _on_msg(self, update: Update, ctx):
        msg = update.message; uid = update.effective_user.id
        flow = self.db.get_flow(uid)
        if not flow: return
        step = flow.get("step", "")
        logger.debug("MSG uid=%s step=%s", uid, step)
        try:
            if   step == "adm_add_user":    await self._f_adm_add(uid, msg, flow)
            elif step == "adm_edit_user":   await self._f_adm_edit(uid, msg, flow)
            elif step == "adm_add_gg":      await self._f_adm_add_gg(uid, msg, flow)
            elif step == "adm_broadcast":   await self._f_adm_bc(uid, msg, flow)
            elif step == "sb_phone":        await self._f_phone(uid, msg, flow)
            elif step == "sb_code":         await self._f_code(uid, msg, flow)
            elif step == "sb_2fa":          await self._f_2fa(uid, msg, flow)
            elif step == "sb_set_ad":       await self._f_set_ad(uid, msg, flow)
            elif step == "sb_ename":        await self._f_ename(uid, msg, flow)
            elif step == "sb_ebio":         await self._f_ebio(uid, msg, flow)
            elif step == "sb_ephoto":       await self._f_ephoto(uid, msg, flow)
            elif step == "sbt_keyword":     await self._f_sbt_keyword(uid, msg, flow)
            elif step == "g_add":           await self._f_g_add(uid, msg, flow)
        except Exception as e:
            logger.error("MSG err uid=%s step=%s: %s", uid, step, e, exc_info=True)
            with suppress(Exception): await msg.reply_text(f"⚠️ Error: {e}")

    # ── admin flows ───────────────────────────────────

    async def _f_adm_add(self, uid, msg, flow):
        parts = (msg.text or "").strip().split()
        if len(parts) < 2: return await msg.reply_text("❌ Format: `<user_id> <plan> [days]`", parse_mode=ParseMode.MARKDOWN)
        try: target=int(parts[0]); pk=parts[1].lower(); days=int(parts[2]) if len(parts)>=3 else None
        except ValueError: return await msg.reply_text("❌ Invalid input.")
        if pk not in PLANS: return await msg.reply_text(f"❌ Plans: {', '.join(PLANS)}")
        exp = (datetime.now()+timedelta(days=days)).isoformat() if days else None
        self.db.run("INSERT OR REPLACE INTO users(user_id,plan,granted_at,expires_at,is_active) VALUES(?,?,?,?,1)",
                    (target, pk, datetime.now().isoformat(), exp))
        self.db.clear_flow(uid)
        await msg.reply_text(f"✅ `{target}` — {PLANS[pk]['label']} — {fmt_exp(exp)}", parse_mode=ParseMode.MARKDOWN)

    async def _f_adm_edit(self, uid, msg, flow):
        parts = (msg.text or "").strip().split()
        if not parts: return await msg.reply_text("❌ Format: `<plan> [days]`", parse_mode=ParseMode.MARKDOWN)
        pk = parts[0].lower(); days = int(parts[1]) if len(parts)>=2 else None
        if pk not in PLANS: return await msg.reply_text(f"❌ Plans: {', '.join(PLANS)}")
        exp = (datetime.now()+timedelta(days=days)).isoformat() if days else None
        self.db.run("UPDATE users SET plan=?,expires_at=?,is_active=1 WHERE user_id=?", (pk, exp, flow["target"]))
        self.db.clear_flow(uid)
        await msg.reply_text(f"✅ Updated to {PLANS[pk]['label']}.", parse_mode=ParseMode.MARKDOWN)

    async def _resolve_entity(self, inst, raw) -> Optional[Tuple]:
        raw = raw.strip().rstrip("/")
        if not raw: return None
        if raw.lstrip("-").isdigit():
            chat_id = int(raw); title, username, is_forum = str(chat_id), None, False
            if inst:
                try:
                    e = await inst.client.get_entity(chat_id)
                    title=getattr(e,"title",str(chat_id)); username=getattr(e,"username",None)
                    is_forum=(getattr(e,"megagroup",False) and getattr(e,"forum",False))
                except Exception as ex: logger.warning("resolve %s: %s", chat_id, ex)
            return chat_id, title, username, is_forum
        if inst is None: return None
        try:
            if "t.me/+" in raw or "joinchat" in raw:
                ih = raw.split("/")[-1].lstrip("+"); e = await inst.client.get_entity(f"https://t.me/joinchat/{ih}")
            elif "t.me/" in raw:
                slug = raw.split("t.me/")[-1].split("?")[0].split("/")[0]; e = await inst.client.get_entity(f"@{slug}")
            else: e = await inst.client.get_entity(f"@{raw.lstrip('@')}")
            return (e.id, getattr(e,"title",str(e.id)), getattr(e,"username",None),
                    getattr(e,"megagroup",False) and getattr(e,"forum",False))
        except Exception as ex: logger.error("resolve '%s': %s", raw, ex); return None

    async def _f_adm_add_gg(self, uid, msg, flow):
        raw_text = (msg.text or "").strip(); self.db.clear_flow(uid)
        inst = self._any_auth(); added, failed = [], []
        for raw in [l.strip() for l in raw_text.splitlines() if l.strip()]:
            r = await self._resolve_entity(inst, raw)
            if r is None: failed.append(f"`{raw}` — {'no selfbot' if inst is None else 'failed'}"); continue
            chat_id, title, username, is_forum = r
            self.db.run("INSERT OR REPLACE INTO global_groups(chat_id,title,username,is_forum) VALUES(?,?,?,?)",
                        (chat_id, title, username, int(is_forum)))
            added.append(f"{'📋' if is_forum else '👥'} *{title}*")
        parts = []
        if added: parts.append("✅ Added:\n"+"\n".join(added))
        if failed: parts.append("❌ Failed:\n"+"\n".join(failed))
        await msg.reply_text("\n\n".join(parts or ["Nothing added."]), parse_mode=ParseMode.MARKDOWN)

    async def _f_adm_bc(self, uid, msg, flow):
        self.db.clear_flow(uid); users = self.db.q("SELECT user_id FROM users WHERE is_active=1"); sent = 0
        for r in users:
            with suppress(Exception): await msg.copy_to(r["user_id"]); sent += 1; await asyncio.sleep(0.05)
        await msg.reply_text(f"✅ Sent to {sent} users.")

    async def _f_phone(self, uid, msg, flow):
        phone = (msg.text or "").strip().replace(" ","").replace("-",""); sbid = flow["sbid"]
        if not re.match(r"^\+\d{7,15}$", phone):
            return await msg.reply_text("❌ Invalid format. Use: `+447911123456`", parse_mode=ParseMode.MARKDOWN)
        inst = self.reg.get_or_create(self.db, sbid)
        ok, r = await inst.request_code(phone)
        if not ok: return await msg.reply_text(r)
        self.db.set_flow(uid, {"step": "sb_code", "sbid": sbid, "phone": phone})
        await msg.reply_text("📱 *OTP sent!*\n\nSend the code from Telegram app.", parse_mode=ParseMode.MARKDOWN)

    async def _f_code(self, uid, msg, flow):
        code = (msg.text or "").strip().replace(" ",""); sbid = flow["sbid"]
        inst = self.reg.get_or_create(self.db, sbid)
        ok, r = await inst.verify_code(flow["phone"], code)
        if r == "2FA_REQUIRED":
            self.db.set_flow(uid, {"step": "sb_2fa", "sbid": sbid, "phone": flow["phone"], "code": code})
            return await msg.reply_text("🔐 *2FA Required*\n\nSend your Telegram password:", parse_mode=ParseMode.MARKDOWN)
        self.db.clear_flow(uid)
        if ok:
            with suppress(Exception):
                me = await inst.client.get_me(); name = f"{me.first_name or ''} {me.last_name or ''}".strip()
                if name: self.db.run("UPDATE selfbots SET display_name=? WHERE id=?", (name, sbid))
        await msg.reply_text(r)

    async def _f_2fa(self, uid, msg, flow):
        pw = (msg.text or "").strip(); sbid = flow["sbid"]
        inst = self.reg.get_or_create(self.db, sbid)
        ok, r = await inst.verify_code(flow["phone"], flow.get("code",""), password=pw)
        self.db.clear_flow(uid)
        if ok:
            with suppress(Exception):
                me = await inst.client.get_me(); name = f"{me.first_name or ''} {me.last_name or ''}".strip()
                if name: self.db.run("UPDATE selfbots SET display_name=? WHERE id=?", (name, sbid))
        await msg.reply_text(r)

    async def _f_set_ad(self, uid, msg, flow):
        sbid = flow["sbid"]; self.db.clear_flow(uid)
        origin = getattr(msg, "forward_origin", None)
        fwd_chat_id = fwd_msg_id = None
        if origin is not None:
            fc = getattr(origin, "chat", None); fm = getattr(origin, "message_id", None)
            if fc and fm: fwd_chat_id = fc.id; fwd_msg_id = fm
        if fwd_chat_id and fwd_msg_id:
            ad = json.dumps({"mode": "forward", "src_chat": fwd_chat_id, "src_msgid": fwd_msg_id})
            mode_label = "forwarded message"
        else:
            ad = json.dumps({"mode": "copy", "src_chat": msg.chat_id, "src_msgid": msg.message_id})
            mode_label = "typed message"
        self.db.run("UPDATE selfbots SET ad_message=? WHERE id=?", (ad, sbid))
        inst = self.reg.get_or_create(self.db, sbid); ad_data = json.loads(ad); src = ad_data.get("src_chat")
        warning = ""
        if inst.is_auth and src:
            ok, err = await inst.warm_source_entity(src)
            if not ok: warning = f"\n\n⚠️ Cannot resolve source channel (`{src}`).\n`{err}`\n\nSelfbot must be a member."
        elif not inst.is_auth: warning = "\n\n⚠️ Selfbot not connected — warm-up skipped."
        r = self.db.get_selfbot(sbid)
        note = "\n\n_Live update active._" if r["fwd_running"] else ""
        await msg.reply_text(f"✅ Ad saved (Selfbot {r['slot']}) — _{mode_label}_{note}{warning}",
                             parse_mode=ParseMode.MARKDOWN)

    async def _f_ename(self, uid, msg, flow):
        name = (msg.text or "").strip(); sbid = flow["sbid"]; self.db.clear_flow(uid)
        inst = self.reg.get_or_create(self.db, sbid); ok, r = await inst.update_profile(name=name)
        if ok: self.db.run("UPDATE selfbots SET display_name=? WHERE id=?", (name, sbid))
        await msg.reply_text(r)

    async def _f_ebio(self, uid, msg, flow):
        bio = (msg.text or "").strip(); sbid = flow["sbid"]; self.db.clear_flow(uid)
        inst = self.reg.get_or_create(self.db, sbid); ok, r = await inst.update_profile(bio=bio)
        await msg.reply_text(r)

    async def _f_ephoto(self, uid, msg, flow):
        sbid = flow["sbid"]; self.db.clear_flow(uid)
        if not (msg.photo or msg.document): return await msg.reply_text("❌ Please send a photo.")
        inst = self.reg.get_or_create(self.db, sbid)
        file_ob = msg.photo[-1] if msg.photo else msg.document
        tg_file = await file_ob.get_file(); bio_io = io.BytesIO()
        await tg_file.download_to_memory(bio_io); ok, r = await inst.update_photo(bio_io.getvalue())
        await msg.reply_text(r)

    async def _f_sbt_keyword(self, uid, msg, flow):
        kw = (msg.text or "").strip(); sbid = flow["sbid"]; chat_id = flow["chat_id"]
        inst = self.reg.get_or_create(self.db, sbid); self.db.clear_flow(uid)
        topics = await inst.fetch_topics(chat_id, keyword=kw)
        if not topics: return await msg.reply_text(f"❌ No topics matching `{kw}`.", parse_mode=ParseMode.MARKDOWN)
        saved = self.db.get_topics(sbid, chat_id)
        pairs = [(r["topic_id"], r["topic_title"]) for r in saved]
        for tid, ttitle in topics:
            if not any(p[0]==tid for p in pairs): pairs.append((tid, ttitle))
        self.db.set_topics(sbid, chat_id, pairs)
        await msg.reply_text(f"✅ Added {len(topics)} topic(s): {', '.join(t[1] for t in topics)}",
                             parse_mode=ParseMode.MARKDOWN)

    async def _f_g_add(self, uid, msg, flow):
        raw_text = (msg.text or "").strip(); self.db.clear_flow(uid)
        lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
        inst   = self._user_auth(uid); plan = self.db.get_plan(uid)
        limit  = plan["extra_groups"] if plan else 0; added, failed = [], []
        for raw in lines:
            if self.db.custom_group_count(uid) >= limit: failed.append(f"`{raw}` — limit ({limit})"); continue
            r = await self._resolve_entity(inst, raw)
            if r is None: failed.append(f"`{raw}` — {'no selfbot' if inst is None else 'failed'}"); continue
            chat_id, title, username, is_forum = r
            self.db.run("INSERT OR REPLACE INTO user_groups(user_id,chat_id,title,username,is_forum,is_enabled) VALUES(?,?,?,?,?,1)",
                        (uid, chat_id, title, username, int(is_forum)))
            added.append(f"{'📋' if is_forum else '👥'} *{title}*")
        parts = []
        if added: parts.append("✅ Added:\n"+"\n".join(added))
        if failed: parts.append("❌ Failed:\n"+"\n".join(failed))
        await msg.reply_text("\n\n".join(parts or ["Nothing added."]), parse_mode=ParseMode.MARKDOWN)


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    # ╔══════════════════════════════════════════╗
    # ║         ✏️  CONFIGURATION  ✏️           ║
    # ╚══════════════════════════════════════════╝

    BOT_TOKEN     = "8759285150:AAFRvarCSGc7M_JvBExXrlfS8gKyOIZiZSQ"   # from @BotFather
    ADMIN_USER_ID = 5740574752               # your Telegram user ID

    # https://my.telegram.org → API development tools → Create app
    SHARED_API_ID   = 20999330
    SHARED_API_HASH = "f36dc8ffb55146ec8551b53acb6a1184"

    # "socks5://host:port"  or  "http://host:port"  or  ""
    PROXY = ""

    # ╚══════════════════════════════════════════╝

    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌  Set BOT_TOKEN."); sys.exit(1)
    if ADMIN_USER_ID == 123456789:
        print("❌  Set ADMIN_USER_ID."); sys.exit(1)

    if SHARED_API_ID and SHARED_API_HASH:
        CREDENTIAL_POOL.insert(0, {"api_id": SHARED_API_ID, "api_hash": SHARED_API_HASH})
        print(f"✅  Own credentials loaded (ID: {SHARED_API_ID})")
    else:
        print("⚠️   Using built-in creds (set your own at my.telegram.org for reliability)")

    print("""
╔══════════════════════════════════════════════════════╗
║            ProAdBot  v2.2  –  Starting               ║
║  /selfbots /groups /sync /status /start_all /stop_all║
╚══════════════════════════════════════════════════════╝""")

    AdBot(token=BOT_TOKEN, admin_id=ADMIN_USER_ID).run(proxy=PROXY or None)