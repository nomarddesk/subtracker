"""
SubTrackBot — Telegram channel join/leave tracker with crypto subscriptions.

Single file. Deploy on Railway with a Postgres plugin attached.

Required env:
  BOT_TOKEN        - from @BotFather
  DATABASE_URL     - set automatically by the Railway Postgres plugin
  ADMIN_IDS        - your own Telegram user id(s), comma separated

Optional env (sane defaults below):
  TON_ADDRESS, TRC20_ADDRESS, TONCENTER_API_KEY, TRONGRID_API_KEY,
  PRICE_USD, TRIAL_DAYS, SUB_DAYS, INVOICE_MINUTES, MAX_CHANNELS,
  DIGEST_HOUR, SUPPORT_USERNAME, TON_PRICE_FALLBACK
"""

import asyncio
import io
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN

import asyncpg
import httpx
import qrcode
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("subtrack")
logging.getLogger("aiogram.event").setLevel(logging.WARNING)

# ----------------------------------------------------------------------------
# config
# ----------------------------------------------------------------------------

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_IDS = {
    int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x
}

TON_ADDRESS = os.environ.get(
    "TON_ADDRESS", "UQCXmVhyJI63uMJ067Nr46JxNUhMomdCHoQkHD9NK-igmQVE"
)
TRC20_ADDRESS = os.environ.get(
    "TRC20_ADDRESS", "TQ5AtjJz9tXwHAfYrQg9BUjBmDWkDjq1Zt"
)

TONCENTER_API_KEY = os.environ.get("TONCENTER_API_KEY", "")
TRONGRID_API_KEY = os.environ.get("TRONGRID_API_KEY", "")

PRICE_USD = Decimal(os.environ.get("PRICE_USD", "30"))
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "4"))
SUB_DAYS = int(os.environ.get("SUB_DAYS", "30"))
INVOICE_MINUTES = int(os.environ.get("INVOICE_MINUTES", "60"))
MAX_CHANNELS = int(os.environ.get("MAX_CHANNELS", "3"))
DIGEST_HOUR = int(os.environ.get("DIGEST_HOUR", "9"))  # UTC
SUPPORT_USERNAME = os.environ.get("SUPPORT_USERNAME", "ads2defiCEO").lstrip("@")
TON_PRICE_FALLBACK = Decimal(os.environ.get("TON_PRICE_FALLBACK", "3.00"))

# USDT contract on Tron
USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

# smallest unique step used to tell two invoices apart on a shared address
DUST_STEP = {"ton": Decimal("0.0001"), "trc20": Decimal("0.01")}
# how far UNDER the invoice we still accept, per chain.
# This exists for the common case: someone types "30" instead of "30.07" and
# drops the digits that identify them. It is only ever applied when exactly one
# open invoice fits, so it can never credit the wrong person.
FUZZY_TOL = {"ton": Decimal("0.05"), "trc20": Decimal("1.00")}
# only chase unmatched on-chain payments from the last few hours
ORPHAN_WINDOW = 6 * 3600

POLL_SECONDS = 30
COUNTDOWN_SECONDS = 60

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
pool: asyncpg.Pool | None = None

# ----------------------------------------------------------------------------
# schema
# ----------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id           BIGINT PRIMARY KEY,
    username          TEXT,
    first_name        TEXT,
    trial_started_at  TIMESTAMPTZ,
    trial_ends_at     TIMESTAMPTZ,
    paid_until        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_digest_on    DATE,
    warned_stage      TEXT,
    lapsed_teased_on  DATE
);

CREATE TABLE IF NOT EXISTS channels (
    chat_id     BIGINT PRIMARY KEY,
    owner_id    BIGINT NOT NULL,
    title       TEXT,
    username    TEXT,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    removed_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS channels_owner_idx ON channels (owner_id);

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    owner_id    BIGINT NOT NULL,
    member_id   BIGINT NOT NULL,
    username    TEXT,
    first_name  TEXT,
    action      TEXT NOT NULL,
    at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered   BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS events_owner_at_idx ON events (owner_id, at DESC);

CREATE TABLE IF NOT EXISTS invoices (
    id           TEXT PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    chain        TEXT NOT NULL,
    amount       NUMERIC(24,8) NOT NULL,
    memo         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'open',
    chat_id      BIGINT,
    message_id   BIGINT,
    tx_hash      TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    paid_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS invoices_open_idx ON invoices (status, chain);

CREATE TABLE IF NOT EXISTS seen_tx (
    tx_hash   TEXT PRIMARY KEY,
    chain     TEXT NOT NULL,
    amount    NUMERIC(24,8),
    matched   BOOLEAN NOT NULL DEFAULT FALSE,
    at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def db_init() -> None:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=6)
    async with pool.acquire() as con:
        await con.execute(SCHEMA)
    log.info("database ready")


# ----------------------------------------------------------------------------
# small helpers
# ----------------------------------------------------------------------------


def now() -> datetime:
    return datetime.now(timezone.utc)


def esc(s: str | None) -> str:
    if not s:
        return ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def person(first_name: str | None, username: str | None, uid: int) -> str:
    name = esc(first_name) or "Someone"
    if username:
        return f'<a href="https://t.me/{username}">{name}</a> (@{esc(username)})'
    return f'<a href="tg://user?id={uid}">{name}</a>'


def human_left(until: datetime) -> str:
    secs = int((until - now()).total_seconds())
    if secs <= 0:
        return "expired"
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


async def ensure_user(uid: int, username: str | None, first_name: str | None):
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
              SET username = EXCLUDED.username,
                  first_name = EXCLUDED.first_name
            """,
            uid,
            username,
            first_name,
        )
        return await con.fetchrow("SELECT * FROM users WHERE user_id = $1", uid)


async def get_user(uid: int):
    async with pool.acquire() as con:
        return await con.fetchrow("SELECT * FROM users WHERE user_id = $1", uid)


def access_state(u) -> tuple[str, datetime | None]:
    """Returns (state, until). state is 'trial' | 'paid' | 'expired' | 'none'."""
    if u is None:
        return "none", None
    t = now()
    paid = u["paid_until"]
    if paid and paid > t:
        return "paid", paid
    trial_end = u["trial_ends_at"]
    if trial_end and trial_end > t:
        return "trial", trial_end
    if trial_end or paid:
        return "expired", (paid or trial_end)
    return "none", None


async def has_access(uid: int) -> bool:
    state, _ = access_state(await get_user(uid))
    return state in ("trial", "paid")


async def start_trial_if_new(uid: int) -> bool:
    """
    Starts the one and only trial this user will ever get.

    trial_started_at is written once and never cleared. Removing the bot from a
    channel and adding it back does not touch this row, and neither does adding
    a different channel — that is the whole anti-bypass design.
    """
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            UPDATE users
               SET trial_started_at = now(),
                   trial_ends_at    = now() + ($2 || ' days')::interval
             WHERE user_id = $1
               AND trial_started_at IS NULL
            RETURNING trial_ends_at
            """,
            uid,
            str(TRIAL_DAYS),
        )
        return row is not None


async def dm(uid: int, text: str, markup: InlineKeyboardMarkup | None = None) -> bool:
    try:
        await bot.send_message(
            uid, text, reply_markup=markup, disable_web_page_preview=True
        )
        return True
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        log.warning("dm to %s failed: %s", uid, e)
        return False


# ----------------------------------------------------------------------------
# keyboards
# ----------------------------------------------------------------------------


def kb_main(state: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="➕ Add me to a channel",
                url=f"https://t.me/{BOT_USERNAME}?startchannel&admin=post_messages+invite_users",
            )
        ],
        [InlineKeyboardButton(text="📊 My channels", callback_data="mychannels")],
    ]
    if state != "paid":
        rows.append(
            [InlineKeyboardButton(text="💎 Subscribe — $30/mo", callback_data="sub")]
        )
    else:
        rows.append(
            [InlineKeyboardButton(text="🔁 Extend subscription", callback_data="sub")]
        )
    rows.append(
        [InlineKeyboardButton(text="💬 Support", url=f"https://t.me/{SUPPORT_USERNAME}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_chains() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💵 USDT (TRC-20)", callback_data="pay:trc20")],
            [InlineKeyboardButton(text="💎 TON", callback_data="pay:ton")],
            [InlineKeyboardButton(text="« Back", callback_data="home")],
        ]
    )


def kb_invoice(inv_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ I've paid — check now", callback_data=f"chk:{inv_id}")],
            [InlineKeyboardButton(text="✖️ Cancel", callback_data=f"cxl:{inv_id}")],
        ]
    )


def kb_sub_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Subscribe — $30/mo", callback_data="sub")]
        ]
    )


# ----------------------------------------------------------------------------
# pricing + invoices
# ----------------------------------------------------------------------------

_price_cache: dict[str, tuple[Decimal, float]] = {}


async def ton_usd_price() -> Decimal:
    cached = _price_cache.get("ton")
    if cached and (asyncio.get_running_loop().time() - cached[1]) < 300:
        return cached[0]
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "the-open-network", "vs_currencies": "usd"},
            )
            r.raise_for_status()
            p = Decimal(str(r.json()["the-open-network"]["usd"]))
            if p <= 0:
                raise ValueError("bad price")
            _price_cache["ton"] = (p, asyncio.get_running_loop().time())
            return p
    except Exception as e:
        log.warning("TON price fetch failed (%s), using fallback %s", e, TON_PRICE_FALLBACK)
        return TON_PRICE_FALLBACK


def q(amount: Decimal, chain: str) -> Decimal:
    places = Decimal("0.0001") if chain == "ton" else Decimal("0.01")
    return amount.quantize(places, rounding=ROUND_DOWN)


async def allocate_amount(chain: str) -> Decimal | None:
    """
    Both addresses are shared across every customer, so the amount itself is the
    invoice number. We nudge each new invoice up by one dust step until we find a
    value no other open invoice is using.
    """
    if chain == "trc20":
        base = q(PRICE_USD, chain)
    else:
        base = q(PRICE_USD / await ton_usd_price(), chain)

    step = DUST_STEP[chain]
    async with pool.acquire() as con:
        taken = {
            r["amount"]
            for r in await con.fetch(
                "SELECT amount FROM invoices WHERE status = 'open' AND chain = $1",
                chain,
            )
        }
    for i in range(1, 200):
        cand = q(base + step * i, chain)
        if cand not in taken:
            return cand
    return None


async def create_invoice(uid: int, chain: str):
    amount = await allocate_amount(chain)
    if amount is None:
        return None
    inv_id = secrets.token_hex(4).upper()
    memo = f"SUB{inv_id}"
    expires = now() + timedelta(minutes=INVOICE_MINUTES)
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO invoices (id, user_id, chain, amount, memo, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            inv_id,
            uid,
            chain,
            amount,
            memo,
            expires,
        )
        return await con.fetchrow("SELECT * FROM invoices WHERE id = $1", inv_id)


def qr_png(payload: str) -> bytes:
    img = qrcode.make(payload)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def invoice_payload(inv) -> str:
    if inv["chain"] == "ton":
        nano = int(Decimal(inv["amount"]) * Decimal(10**9))
        return f"ton://transfer/{TON_ADDRESS}?amount={nano}&text={inv['memo']}"
    return TRC20_ADDRESS


def invoice_caption(inv) -> str:
    amount = Decimal(inv["amount"])
    left = human_left(inv["expires_at"])
    if inv["chain"] == "ton":
        head = (
            f"💎 <b>Pay {amount} TON</b>\n\n"
            f"<b>Address</b>\n<code>{TON_ADDRESS}</code>\n\n"
            f"<b>Comment / memo</b>\n<code>{inv['memo']}</code>\n"
        )
        note = (
            "Send the <b>exact</b> amount. The comment helps but the amount is what "
            "identifies you, so do not round it."
        )
    else:
        head = (
            f"💵 <b>Pay {amount} USDT</b>\n"
            f"<i>Network: TRON (TRC-20) only</i>\n\n"
            f"<b>Address</b>\n<code>{TRC20_ADDRESS}</code>\n"
        )
        note = (
            "Send the <b>exact</b> amount, including the last two digits — that is how "
            "the bot knows the payment is yours. TRC-20 only; anything sent on another "
            "network is lost."
        )
    return (
        f"{head}\n{note}\n\n"
        f"⏳ <b>Expires in {left}</b>\n"
        f"Invoice <code>{inv['id']}</code> · detected automatically, usually within a minute."
    )


async def send_invoice(uid: int, inv) -> None:
    png = qr_png(invoice_payload(inv))
    msg = await bot.send_photo(
        uid,
        BufferedInputFile(png, filename=f"invoice_{inv['id']}.png"),
        caption=invoice_caption(inv),
        reply_markup=kb_invoice(inv["id"]),
    )
    async with pool.acquire() as con:
        await con.execute(
            "UPDATE invoices SET chat_id = $1, message_id = $2 WHERE id = $3",
            msg.chat.id,
            msg.message_id,
            inv["id"],
        )


async def credit_payment(inv, tx_hash: str) -> None:
    """Extend the subscription and tell the user. Idempotent on invoice status."""
    async with pool.acquire() as con:
        upd = await con.fetchrow(
            """
            UPDATE invoices
               SET status = 'paid', tx_hash = $2, paid_at = now()
             WHERE id = $1 AND status = 'open'
            RETURNING user_id
            """,
            inv["id"],
            tx_hash,
        )
        if upd is None:
            return
        row = await con.fetchrow(
            """
            UPDATE users
               SET paid_until = GREATEST(COALESCE(paid_until, now()), now())
                                + ($2 || ' days')::interval,
                   warned_stage = NULL,
                   lapsed_teased_on = NULL
             WHERE user_id = $1
            RETURNING paid_until
            """,
            inv["user_id"],
            str(SUB_DAYS),
        )
        await con.execute(
            "UPDATE seen_tx SET matched = TRUE WHERE tx_hash = $1", tx_hash
        )

    until = row["paid_until"]
    if inv["chat_id"] and inv["message_id"]:
        try:
            await bot.edit_message_caption(
                chat_id=inv["chat_id"],
                message_id=inv["message_id"],
                caption=f"✅ <b>Paid</b> · invoice <code>{inv['id']}</code>",
                reply_markup=None,
            )
        except TelegramBadRequest:
            pass

    await dm(
        inv["user_id"],
        "✅ <b>Payment confirmed. You're subscribed.</b>\n\n"
        f"Amount: <b>{Decimal(inv['amount'])} "
        f"{'TON' if inv['chain'] == 'ton' else 'USDT'}</b>\n"
        f"Active until: <b>{until:%d %b %Y}</b> ({SUB_DAYS} days)\n\n"
        "Join and leave alerts are live again on all your channels. "
        "You'll get a daily summary every morning, and a reminder before this runs out.",
        kb_main("paid"),
    )
    log.info("credited %s to user %s (tx %s)", inv["id"], inv["user_id"], tx_hash[:16])


# ----------------------------------------------------------------------------
# chain watchers
# ----------------------------------------------------------------------------


async def fetch_trc20() -> list[dict]:
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    url = f"https://api.trongrid.io/v1/accounts/{TRC20_ADDRESS}/transactions/trc20"
    params = {"limit": 50, "only_to": "true", "contract_address": USDT_TRC20_CONTRACT}
    out = []
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url, params=params, headers=headers)
            r.raise_for_status()
            for t in r.json().get("data", []):
                if t.get("to") != TRC20_ADDRESS or t.get("type") != "Transfer":
                    continue
                dec = int(t.get("token_info", {}).get("decimals", 6))
                out.append(
                    {
                        "hash": t["transaction_id"],
                        "chain": "trc20",
                        "amount": Decimal(t["value"]) / Decimal(10**dec),
                        "memo": "",
                        "ts": int(t.get("block_timestamp", 0)) // 1000,
                    }
                )
    except Exception as e:
        log.warning("trongrid fetch failed: %s", e)
    return out


async def fetch_ton() -> list[dict]:
    params = {"address": TON_ADDRESS, "limit": 50, "archival": "false"}
    if TONCENTER_API_KEY:
        params["api_key"] = TONCENTER_API_KEY
    out = []
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://toncenter.com/api/v2/getTransactions", params=params)
            r.raise_for_status()
            for t in r.json().get("result", []):
                inm = t.get("in_msg") or {}
                val = inm.get("value")
                if not val or int(val) <= 0:
                    continue
                out.append(
                    {
                        "hash": t.get("transaction_id", {}).get("hash", ""),
                        "chain": "ton",
                        "amount": Decimal(val) / Decimal(10**9),
                        "memo": (inm.get("message") or "").strip(),
                        "ts": int(t.get("utime", 0)),
                    }
                )
    except Exception as e:
        log.warning("toncenter fetch failed: %s", e)
    return [t for t in out if t["hash"]]


def fits(paid: Decimal, invoiced: Decimal, chain: str) -> bool:
    """Accept the exact amount, anything above it, or a little under."""
    if paid >= invoiced:
        return True
    return (invoiced - paid) <= FUZZY_TOL[chain]


async def match_payments(txs: list[dict]) -> None:
    if not txs:
        return
    async with pool.acquire() as con:
        for t in txs:
            fresh = await con.fetchrow(
                """
                INSERT INTO seen_tx (tx_hash, chain, amount) VALUES ($1, $2, $3)
                ON CONFLICT (tx_hash) DO NOTHING
                RETURNING tx_hash
                """,
                t["hash"],
                t["chain"],
                t["amount"],
            )
            if fresh is None:
                continue  # already processed on an earlier pass

            open_invs = await con.fetch(
                """
                SELECT * FROM invoices
                 WHERE status = 'open' AND chain = $1 AND expires_at > now() - interval '2 hours'
                 ORDER BY created_at
                """,
                t["chain"],
            )

            # 1. memo match (TON only — TRC-20 transfers carry no memo)
            hit = None
            if t["memo"]:
                for inv in open_invs:
                    if inv["memo"].lower() in t["memo"].lower():
                        hit = inv
                        break

            # 2. exact amount
            if hit is None:
                for inv in open_invs:
                    if t["amount"] == Decimal(inv["amount"]):
                        hit = inv
                        break

            # 3. close enough, but only when exactly one invoice could be meant
            if hit is None:
                cands = [
                    i for i in open_invs
                    if fits(t["amount"], Decimal(i["amount"]), t["chain"])
                ]
                if len(cands) == 1:
                    hit = cands[0]
                elif len(cands) > 1:
                    log.info(
                        "ambiguous payment %s %s — %d candidates, leaving for review",
                        t["amount"],
                        t["chain"],
                        len(cands),
                    )
                    await alert_admins(
                        f"⚠️ Unmatched payment: <b>{t['amount']} {t['chain']}</b>\n"
                        f"tx <code>{t['hash'][:24]}</code>\n"
                        f"{len(cands)} open invoices could match — "
                        f"use /grant &lt;user_id&gt; to credit manually."
                    )

            if hit is not None:
                await credit_payment(hit, t["hash"])
            elif (now().timestamp() - t["ts"]) < ORPHAN_WINDOW:
                await alert_admins(
                    f"⚠️ Payment with no invoice: <b>{t['amount']} {t['chain']}</b>\n"
                    f"tx <code>{t['hash'][:24]}</code>"
                )


async def alert_admins(text: str) -> None:
    for aid in ADMIN_IDS:
        await dm(aid, text)


async def payment_poller() -> None:
    await asyncio.sleep(5)
    while True:
        try:
            trc, ton = await asyncio.gather(fetch_trc20(), fetch_ton())
            await match_payments(trc + ton)
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE invoices SET status = 'expired' "
                    "WHERE status = 'open' AND expires_at < now()"
                )
        except Exception as e:
            log.exception("payment poller error: %s", e)
        await asyncio.sleep(POLL_SECONDS)


async def countdown_updater() -> None:
    """Refresh the 'expires in X' line on every live invoice."""
    while True:
        try:
            async with pool.acquire() as con:
                rows = await con.fetch(
                    "SELECT * FROM invoices WHERE status = 'open' "
                    "AND chat_id IS NOT NULL AND expires_at > now()"
                )
            for inv in rows:
                try:
                    await bot.edit_message_caption(
                        chat_id=inv["chat_id"],
                        message_id=inv["message_id"],
                        caption=invoice_caption(inv),
                        reply_markup=kb_invoice(inv["id"]),
                    )
                except TelegramBadRequest:
                    pass
                await asyncio.sleep(0.2)

            async with pool.acquire() as con:
                dead = await con.fetch(
                    "SELECT * FROM invoices WHERE status = 'expired' "
                    "AND chat_id IS NOT NULL AND message_id IS NOT NULL "
                    "AND paid_at IS NULL AND expires_at > now() - interval '10 minutes'"
                )
            for inv in dead:
                try:
                    await bot.edit_message_caption(
                        chat_id=inv["chat_id"],
                        message_id=inv["message_id"],
                        caption=(
                            f"⌛ <b>Invoice {inv['id']} expired.</b>\n\n"
                            "Nothing was charged. Tap Subscribe to generate a fresh one.\n"
                            "If you already sent the payment, contact support with the "
                            "transaction hash and it will be credited."
                        ),
                        reply_markup=kb_sub_only(),
                    )
                except TelegramBadRequest:
                    pass
        except Exception as e:
            log.exception("countdown updater error: %s", e)
        await asyncio.sleep(COUNTDOWN_SECONDS)


# ----------------------------------------------------------------------------
# commands
# ----------------------------------------------------------------------------

BOT_USERNAME = ""  # filled at startup


def welcome_text(state: str, until: datetime | None) -> str:
    if state == "paid":
        status = f"💎 <b>Subscribed</b> — {human_left(until)} left"
    elif state == "trial":
        status = f"🎁 <b>Free trial</b> — {human_left(until)} left"
    elif state == "expired":
        status = "⌛ <b>Expired</b> — alerts are paused"
    else:
        status = f"🎁 <b>{TRIAL_DAYS} days free</b> when you add your first channel"

    return (
        "🤖 <b>SubTrackBot</b>\n"
        "<i>Know exactly who joins and who leaves your channel.</i>\n\n"
        f"{status}\n\n"
        "<b>What you get</b>\n"
        "• Instant alert the moment someone joins or leaves\n"
        "• Their name and @username, so you can actually follow up\n"
        "• A morning summary of everyone who joined the day before\n"
        "• Up to "
        f"{MAX_CHANNELS} channels on one subscription\n\n"
        "<b>Setup takes one tap</b>\n"
        "Add me to your channel as an admin. That's it — I start tracking "
        "immediately. I only see join and leave events, nothing else.\n\n"
        f"<b>Price:</b> ${PRICE_USD}/month after the free trial. "
        "USDT (TRC-20) or TON."
    )


@dp.message(CommandStart())
async def cmd_start(m: Message):
    u = await ensure_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    state, until = access_state(u)
    await m.answer(welcome_text(state, until), reply_markup=kb_main(state))


@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "<b>Commands</b>\n"
        "/start — main menu\n"
        "/status — subscription and channels\n"
        "/subscribe — pay or extend\n"
        "/today — who joined and left in the last 24h\n"
        "/channels — channels I'm tracking\n\n"
        f"Stuck? Message @{SUPPORT_USERNAME}."
    )


@dp.message(Command("status"))
async def cmd_status(m: Message):
    u = await ensure_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    state, until = access_state(u)
    async with pool.acquire() as con:
        chans = await con.fetch(
            "SELECT * FROM channels WHERE owner_id = $1 AND active ORDER BY added_at",
            m.from_user.id,
        )
        stats = await con.fetchrow(
            """
            SELECT
              COUNT(*) FILTER (WHERE action = 'join') AS j,
              COUNT(*) FILTER (WHERE action = 'leave') AS l
            FROM events WHERE owner_id = $1 AND at > now() - interval '30 days'
            """,
            m.from_user.id,
        )

    if state == "paid":
        line = f"💎 Subscribed · renews needed in <b>{human_left(until)}</b>"
    elif state == "trial":
        line = f"🎁 Free trial · <b>{human_left(until)}</b> left"
    elif state == "expired":
        line = "⌛ <b>Expired.</b> Tracking continues but alerts are paused."
    else:
        line = "No trial started yet — add a channel to begin."

    chan_lines = (
        "\n".join(f"• {esc(c['title'])}" for c in chans) or "<i>none yet</i>"
    )
    await m.answer(
        f"{line}\n\n"
        f"<b>Channels ({len(chans)}/{MAX_CHANNELS})</b>\n{chan_lines}\n\n"
        f"<b>Last 30 days</b>\n"
        f"↗️ {stats['j']} joined · ↘️ {stats['l']} left",
        reply_markup=kb_main(state),
    )


@dp.message(Command("channels"))
async def cmd_channels(m: Message):
    await show_channels(m.from_user.id, m.answer)


async def show_channels(uid: int, send):
    async with pool.acquire() as con:
        chans = await con.fetch(
            "SELECT * FROM channels WHERE owner_id = $1 AND active ORDER BY added_at", uid
        )
    if not chans:
        await send(
            "You haven't added any channels yet.\n\n"
            "Tap the button below, pick your channel, and make sure I'm given admin "
            "rights — I can't see members otherwise.",
            reply_markup=kb_main("none"),
        )
        return
    out = [f"<b>Tracking {len(chans)} channel(s)</b>\n"]
    for c in chans:
        async with pool.acquire() as con:
            s = await con.fetchrow(
                """
                SELECT COUNT(*) FILTER (WHERE action='join') AS j,
                       COUNT(*) FILTER (WHERE action='leave') AS l
                FROM events WHERE chat_id = $1 AND at > now() - interval '7 days'
                """,
                c["chat_id"],
            )
        out.append(f"• <b>{esc(c['title'])}</b>\n  7d: ↗️ {s['j']} · ↘️ {s['l']}")
    await send("\n".join(out))


@dp.message(Command("today"))
async def cmd_today(m: Message):
    if not await has_access(m.from_user.id):
        await m.answer(
            "⌛ Your access has ended, so the details are locked.\n\n"
            "I'm still recording every join and leave — subscribe and the full list "
            "unlocks instantly, including everything from while you were away.",
            reply_markup=kb_sub_only(),
        )
        return
    text = await build_digest(m.from_user.id, hours=24)
    await m.answer(text or "Nothing in the last 24 hours.")


@dp.message(Command("subscribe"))
async def cmd_subscribe(m: Message):
    await ensure_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    await m.answer(pay_intro(), reply_markup=kb_chains())


def pay_intro() -> str:
    return (
        f"💎 <b>${PRICE_USD} — {SUB_DAYS} days</b>\n\n"
        "Covers every channel on your account, alerts in real time, and the daily "
        "summary.\n\n"
        "Pick how you want to pay. You'll get a QR code and an exact amount, and the "
        f"bot confirms on its own — usually inside a minute. The invoice holds for "
        f"{INVOICE_MINUTES} minutes."
    )


# --- admin ------------------------------------------------------------------


@dp.message(Command("grant"))
async def cmd_grant(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Usage: <code>/grant &lt;user_id&gt; [days]</code>")
        return
    try:
        uid = int(parts[1])
        days = int(parts[2]) if len(parts) > 2 else SUB_DAYS
    except ValueError:
        await m.answer("Bad arguments.")
        return
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            UPDATE users
               SET paid_until = GREATEST(COALESCE(paid_until, now()), now())
                                + ($2 || ' days')::interval,
                   warned_stage = NULL, lapsed_teased_on = NULL
             WHERE user_id = $1
            RETURNING paid_until
            """,
            uid,
            str(days),
        )
    if row is None:
        await m.answer("No such user — they must /start the bot first.")
        return
    await m.answer(f"✅ Granted {days} days to {uid}. Active until {row['paid_until']:%d %b %Y}.")
    await dm(
        uid,
        f"✅ <b>Your subscription has been activated.</b>\n"
        f"Active until <b>{row['paid_until']:%d %b %Y}</b>.",
    )


@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    async with pool.acquire() as con:
        s = await con.fetchrow(
            """
            SELECT
              (SELECT COUNT(*) FROM users) AS users,
              (SELECT COUNT(*) FROM users WHERE paid_until > now()) AS paid,
              (SELECT COUNT(*) FROM users WHERE trial_ends_at > now()
                 AND (paid_until IS NULL OR paid_until < now())) AS trialing,
              (SELECT COUNT(*) FROM channels WHERE active) AS channels,
              (SELECT COUNT(*) FROM invoices WHERE status = 'paid') AS paid_invoices,
              (SELECT COUNT(*) FROM events WHERE at > now() - interval '24 hours') AS events24
            """
        )
    mrr = s["paid"] * PRICE_USD
    await m.answer(
        f"<b>Stats</b>\n"
        f"Users: {s['users']}\n"
        f"Paying: <b>{s['paid']}</b> · Trialing: {s['trialing']}\n"
        f"Channels tracked: {s['channels']}\n"
        f"Invoices paid: {s['paid_invoices']}\n"
        f"Events (24h): {s['events24']}\n\n"
        f"MRR ≈ <b>${mrr}</b>"
    )


# ----------------------------------------------------------------------------
# callbacks
# ----------------------------------------------------------------------------


@dp.callback_query(F.data == "home")
async def cb_home(c: CallbackQuery):
    u = await get_user(c.from_user.id)
    state, until = access_state(u)
    try:
        await c.message.edit_text(welcome_text(state, until), reply_markup=kb_main(state))
    except TelegramBadRequest:
        pass
    await c.answer()


@dp.callback_query(F.data == "mychannels")
async def cb_channels(c: CallbackQuery):
    await show_channels(c.from_user.id, c.message.answer)
    await c.answer()


@dp.callback_query(F.data == "sub")
async def cb_sub(c: CallbackQuery):
    await ensure_user(c.from_user.id, c.from_user.username, c.from_user.first_name)
    await c.message.answer(pay_intro(), reply_markup=kb_chains())
    await c.answer()


@dp.callback_query(F.data.startswith("pay:"))
async def cb_pay(c: CallbackQuery):
    chain = c.data.split(":", 1)[1]
    if chain not in ("ton", "trc20"):
        await c.answer("Unknown option.", show_alert=True)
        return
    async with pool.acquire() as con:
        existing = await con.fetchrow(
            "SELECT * FROM invoices WHERE user_id = $1 AND status = 'open' "
            "AND expires_at > now() ORDER BY created_at DESC LIMIT 1",
            c.from_user.id,
        )
    if existing:
        await c.answer("You already have an open invoice — scroll up.", show_alert=True)
        return

    await c.answer("Generating…")
    inv = await create_invoice(c.from_user.id, chain)
    if inv is None:
        await c.message.answer(
            "Couldn't generate an invoice right now. Try again in a minute, or "
            f"message @{SUPPORT_USERNAME}."
        )
        return
    await send_invoice(c.from_user.id, inv)


@dp.callback_query(F.data.startswith("chk:"))
async def cb_check(c: CallbackQuery):
    inv_id = c.data.split(":", 1)[1]
    async with pool.acquire() as con:
        inv = await con.fetchrow("SELECT * FROM invoices WHERE id = $1", inv_id)
    if inv is None:
        await c.answer("Invoice not found.", show_alert=True)
        return
    if inv["status"] == "paid":
        await c.answer("Already confirmed ✅", show_alert=True)
        return

    await c.answer("Checking the chain…")
    txs = await (fetch_ton() if inv["chain"] == "ton" else fetch_trc20())
    await match_payments(txs)

    async with pool.acquire() as con:
        inv2 = await con.fetchrow("SELECT status FROM invoices WHERE id = $1", inv_id)
    if inv2 and inv2["status"] != "paid":
        await c.message.answer(
            "Nothing on-chain for this invoice yet.\n\n"
            "If you've just sent it, give it a minute or two — confirmations take "
            "time and the bot checks every 30 seconds. You don't need to press "
            "anything again; I'll message you the moment it lands."
        )


@dp.callback_query(F.data.startswith("cxl:"))
async def cb_cancel(c: CallbackQuery):
    inv_id = c.data.split(":", 1)[1]
    async with pool.acquire() as con:
        await con.execute(
            "UPDATE invoices SET status = 'cancelled' WHERE id = $1 AND user_id = $2 "
            "AND status = 'open'",
            inv_id,
            c.from_user.id,
        )
    try:
        await c.message.edit_caption(
            caption=f"✖️ Invoice <code>{inv_id}</code> cancelled.",
            reply_markup=kb_sub_only(),
        )
    except TelegramBadRequest:
        pass
    await c.answer("Cancelled.")


# ----------------------------------------------------------------------------
# channel wiring
# ----------------------------------------------------------------------------


@dp.my_chat_member()
async def on_bot_status(ev: ChatMemberUpdated):
    """Bot itself was added to / removed from a chat."""
    if ev.chat.type not in ("channel", "supergroup"):
        return

    new_status = ev.new_chat_member.status
    actor = ev.from_user

    if new_status in ("administrator",):
        await ensure_user(actor.id, actor.username, actor.first_name)

        async with pool.acquire() as con:
            existing = await con.fetchrow(
                "SELECT * FROM channels WHERE chat_id = $1", ev.chat.id
            )
            active_count = await con.fetchval(
                "SELECT COUNT(*) FROM channels WHERE owner_id = $1 AND active "
                "AND chat_id <> $2",
                actor.id,
                ev.chat.id,
            )

        if existing is None and active_count >= MAX_CHANNELS:
            await dm(
                actor.id,
                f"⚠️ <b>Channel limit reached.</b>\n\n"
                f"Your plan covers {MAX_CHANNELS} channels and you already have "
                f"{active_count}. Remove one, or message @{SUPPORT_USERNAME} about a "
                f"bigger plan.",
            )
            try:
                await bot.leave_chat(ev.chat.id)
            except TelegramBadRequest:
                pass
            return

        async with pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO channels (chat_id, owner_id, title, username, active, removed_at)
                VALUES ($1, $2, $3, $4, TRUE, NULL)
                ON CONFLICT (chat_id) DO UPDATE
                  SET active = TRUE, removed_at = NULL,
                      title = EXCLUDED.title, username = EXCLUDED.username
                """,
                ev.chat.id,
                actor.id,
                ev.chat.title,
                ev.chat.username,
            )

        started = await start_trial_if_new(actor.id)
        u = await get_user(actor.id)
        state, until = access_state(u)

        if started:
            body = (
                f"🎁 Your <b>{TRIAL_DAYS}-day free trial</b> starts now and ends "
                f"<b>{until:%d %b, %H:%M} UTC</b>."
            )
        elif state == "paid":
            body = f"💎 Subscription active — {human_left(until)} left."
        elif state == "trial":
            body = f"🎁 Trial still running — {human_left(until)} left."
        else:
            body = (
                "⌛ Your access has ended, so I'll keep recording but hold the alerts.\n"
                f"Subscribe for ${PRICE_USD}/month to switch them back on."
            )

        await dm(
            actor.id,
            f"✅ <b>Now tracking {esc(ev.chat.title)}</b>\n\n{body}\n\n"
            "Test it: have someone join, or leave and rejoin yourself. You should get "
            "an alert within a second or two.\n\n"
            "<i>Heads up — I can only see people who join from now on. Telegram does "
            "not let any bot read the members who were already there.</i>",
            kb_main(state),
        )
        log.info("channel %s registered by %s", ev.chat.id, actor.id)

    elif new_status in ("left", "kicked", "member", "restricted"):
        # demoted or removed — stop tracking but keep the row for good
        async with pool.acquire() as con:
            row = await con.fetchrow(
                "UPDATE channels SET active = FALSE, removed_at = now() "
                "WHERE chat_id = $1 AND active RETURNING owner_id, title",
                ev.chat.id,
            )
        if row:
            await dm(
                row["owner_id"],
                f"⏹ <b>Stopped tracking {esc(row['title'])}</b>\n\n"
                "I was removed or lost admin rights there. Add me back as an admin any "
                "time and tracking resumes.\n\n"
                "<i>Your trial and subscription are tied to your account, not to the "
                "channel — removing and re-adding me does not restart anything.</i>",
            )
            log.info("channel %s deactivated", ev.chat.id)


@dp.chat_member()
async def on_member_change(ev: ChatMemberUpdated):
    """Someone joined or left a tracked channel."""
    async with pool.acquire() as con:
        chan = await con.fetchrow(
            "SELECT * FROM channels WHERE chat_id = $1 AND active", ev.chat.id
        )
    if chan is None:
        return

    old = ev.old_chat_member.status
    new = ev.new_chat_member.status
    inside = ("member", "administrator", "creator", "restricted")
    outside = ("left", "kicked")

    if old in outside and new in inside:
        action = "join"
    elif old in inside and new in outside:
        action = "leave"
    else:
        return

    who = ev.new_chat_member.user
    if who.is_bot:
        return

    owner_id = chan["owner_id"]
    async with pool.acquire() as con:
        event_id = await con.fetchval(
            """
            INSERT INTO events (chat_id, owner_id, member_id, username, first_name, action)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            ev.chat.id,
            owner_id,
            who.id,
            who.username,
            who.first_name,
            action,
        )

    # Recording never stops. Delivery is what the subscription buys.
    if not await has_access(owner_id):
        return

    icon = "🟢" if action == "join" else "🔴"
    verb = "joined" if action == "join" else "left"
    async with pool.acquire() as con:
        today = await con.fetchrow(
            """
            SELECT COUNT(*) FILTER (WHERE action='join') AS j,
                   COUNT(*) FILTER (WHERE action='leave') AS l
            FROM events WHERE chat_id = $1 AND at > date_trunc('day', now())
            """,
            ev.chat.id,
        )

    text = (
        f"{icon} <b>{verb.title()}</b> · {esc(chan['title'])}\n\n"
        f"{person(who.first_name, who.username, who.id)}\n"
        f"<code>{who.id}</code>\n\n"
        f"<i>Today: ↗️ {today['j']} · ↘️ {today['l']}</i>"
    )
    if await dm(owner_id, text):
        async with pool.acquire() as con:
            await con.execute(
                "UPDATE events SET delivered = TRUE WHERE id = $1", event_id
            )


# ----------------------------------------------------------------------------
# digests and reminders
# ----------------------------------------------------------------------------

CHURN_NOTE = (
    "<i>About the leavers — don't read too much into that number. A large share of "
    "channel churn is dormant accounts, throwaways and accounts Telegram purges on "
    "its own, and even genuine people leave for reasons that have nothing to do with "
    "your content. The joins are the number worth working. Reply to a few of them "
    "today.</i>"
)


async def build_digest(uid: int, hours: int = 24) -> str:
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT e.*, c.title
              FROM events e JOIN channels c ON c.chat_id = e.chat_id
             WHERE e.owner_id = $1 AND e.at > now() - ($2 || ' hours')::interval
             ORDER BY e.at DESC
            """,
            uid,
            str(hours),
        )
    if not rows:
        return ""

    joins = [r for r in rows if r["action"] == "join"]
    leaves = [r for r in rows if r["action"] == "leave"]

    out = [
        f"☀️ <b>Daily summary</b> · last {hours}h\n",
        f"↗️ <b>{len(joins)} joined</b>   ↘️ {len(leaves)} left\n",
    ]

    if joins:
        out.append("\n<b>New subscribers — worth a follow-up</b>")
        for r in joins[:40]:
            out.append(
                f"• {person(r['first_name'], r['username'], r['member_id'])} "
                f"— {esc(r['title'])} · {r['at']:%H:%M}"
            )
        if len(joins) > 40:
            out.append(f"<i>…and {len(joins) - 40} more</i>")
        out.append(
            "\n<i>Message the ones with a @username while they still remember why "
            "they joined. That window is about 48 hours.</i>"
        )

    if leaves:
        out.append(f"\n<b>{len(leaves)} left</b>")
        out.append(CHURN_NOTE)

    return "\n".join(out)


async def daily_digest_job() -> None:
    while True:
        try:
            t = now()
            if t.hour == DIGEST_HOUR:
                async with pool.acquire() as con:
                    users = await con.fetch(
                        "SELECT * FROM users WHERE (last_digest_on IS NULL "
                        "OR last_digest_on < $1)",
                        t.date(),
                    )
                for u in users:
                    uid = u["user_id"]
                    state, _ = access_state(u)
                    async with pool.acquire() as con:
                        await con.execute(
                            "UPDATE users SET last_digest_on = $2 WHERE user_id = $1",
                            uid,
                            t.date(),
                        )
                    if state in ("trial", "paid"):
                        text = await build_digest(uid, 24)
                        if text:
                            await dm(uid, text, kb_main(state))
                    await asyncio.sleep(0.1)
        except Exception as e:
            log.exception("digest job error: %s", e)
        await asyncio.sleep(600)


async def expiry_job() -> None:
    """Renewal nudges, then the locked-out teaser once access has lapsed."""
    while True:
        try:
            async with pool.acquire() as con:
                users = await con.fetch(
                    "SELECT * FROM users WHERE trial_started_at IS NOT NULL"
                )
            for u in users:
                uid = u["user_id"]
                state, until = access_state(u)

                if state in ("trial", "paid") and until:
                    hours = (until - now()).total_seconds() / 3600
                    stage = None
                    if hours <= 6:
                        stage = "h6"
                    elif hours <= 24:
                        stage = "h24"
                    elif hours <= 72 and state == "paid":
                        stage = "h72"

                    if stage and u["warned_stage"] != stage:
                        async with pool.acquire() as con:
                            await con.execute(
                                "UPDATE users SET warned_stage = $2 WHERE user_id = $1",
                                uid,
                                stage,
                            )
                        label = "free trial" if state == "trial" else "subscription"
                        await dm(
                            uid,
                            f"⏳ <b>Your {label} ends in {human_left(until)}.</b>\n\n"
                            "When it does, I keep watching your channel but stop sending "
                            "the alerts — so you'd still be gaining and losing "
                            "subscribers, just without knowing who.\n\n"
                            f"${PRICE_USD} keeps it running for another {SUB_DAYS} days.",
                            kb_sub_only(),
                        )

                elif state == "expired":
                    today = now().date()
                    if u["lapsed_teased_on"] == today:
                        continue
                    async with pool.acquire() as con:
                        s = await con.fetchrow(
                            """
                            SELECT COUNT(*) FILTER (WHERE action='join')  AS j,
                                   COUNT(*) FILTER (WHERE action='leave') AS l
                              FROM events
                             WHERE owner_id = $1 AND delivered = FALSE
                               AND at > $2
                            """,
                            uid,
                            until or (now() - timedelta(days=30)),
                        )
                    if (s["j"] or 0) + (s["l"] or 0) == 0:
                        continue
                    async with pool.acquire() as con:
                        await con.execute(
                            "UPDATE users SET lapsed_teased_on = $2 WHERE user_id = $1",
                            uid,
                            today,
                        )
                    await dm(
                        uid,
                        f"🔔 <b>{s['j']} people joined and {s['l']} left</b> since your "
                        "access ended.\n\n"
                        "I've logged every one of them — names, usernames, timestamps. "
                        "Subscribe and the whole list unlocks immediately, including the "
                        "ones you missed.\n\n"
                        f"${PRICE_USD} · {SUB_DAYS} days · USDT or TON",
                        kb_sub_only(),
                    )
                await asyncio.sleep(0.05)
        except Exception as e:
            log.exception("expiry job error: %s", e)
        await asyncio.sleep(1800)


# ----------------------------------------------------------------------------
# boot
# ----------------------------------------------------------------------------


async def main() -> None:
    global BOT_USERNAME
    await db_init()
    me = await bot.get_me()
    BOT_USERNAME = me.username
    log.info("starting as @%s", BOT_USERNAME)

    asyncio.create_task(payment_poller())
    asyncio.create_task(countdown_updater())
    asyncio.create_task(daily_digest_job())
    asyncio.create_task(expiry_job())

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(
        bot,
        allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"],
    )


if __name__ == "__main__":
    asyncio.run(main())
