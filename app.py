# app.py
#
# RFactor v2 — Live Dashboard (FastAPI + KiteTicker)
#
# Local run:
#   export KITE_API_KEY="..."
#   export KITE_ACCESS_TOKEN="..."
#   uvicorn app:app --host 0.0.0.0 --port 5555 --workers 1
#
# Render start command:
#   uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1
#
# Notes:
# - Put theme.css in the SAME folder as this app.py
# - Clicking a STOCK name opens TradingView (NSE) at 5-min timeframe.
# - Clicking a SECTOR bar opens a popup listing sector stocks.

import os
import sys
import time
import threading
import logging
from collections import deque
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from kiteconnect import KiteConnect, KiteTicker

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rfv2")

# =============================================================================
# CONFIG
# =============================================================================
IST = ZoneInfo("Asia/Kolkata")
PORT = int(os.getenv("PORT", os.getenv("RFV2_PORT", "5555")))

API_KEY = os.getenv("KITE_API_KEY", "").strip()
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

LOOKBACK_SESSIONS = int(os.getenv("RFV2_LOOKBACK_DAYS", "20"))
HISTORY_DAYS = int(os.getenv("RFV2_HISTORY_DAYS", "220"))
SEED_PACE = float(os.getenv("RFV2_SEED_PACE", "0.35"))

HOT_SAMPLE_SEC = 5
HOT_MAX_SEC = 60 * 60

# RFactor weights
W_RVOLM = 0.35
W_RANGE = 0.20
W_MOVE = 0.25
W_VOL_ACCEL = 0.15

# Boost ranges
TQ_MIN, TQ_MAX = 0.5, 1.5
PV_MIN, PV_MAX = 0.7, 1.4
REC_MIN, REC_MAX = 0.6, 1.3

# =============================================================================
# UNIVERSE / SECTORS
# =============================================================================
SECTOR_DEFINITIONS = {
    "METAL": [
        "ADANIENT", "APLAPOLLO", "BHARATFORG", "COALINDIA",
        "HINDALCO", "HINDZINC", "JSWSTEEL",
        "JINDALSTEL", "NMDC", "NATIONALUM",
        "SAIL", "TATASTEEL", "VEDL",
    ],
    "REALTY": [
        "PHOENIXLTD", "GODREJPROP", "LODHA",
        "OBEROIRLTY", "DLF", "PRESTIGE",
        "NBCC", "RVNL", "HUDCO",
    ],
    "ENERGY": [
        "RELIANCE", "ONGC", "IOC", "BPCL", "OIL",
        "NTPC", "POWERGRID", "POWERINDIA",
        "TATAPOWER", "TORNTPOWER", "JSWENERGY",
        "ADANIGREEN", "ADANIENSOL",
        "NHPC", "IREDA", "SUZLON", "INOXWIND",
        "WAAREEENER", "PREMIERENE",
        "PETRONET", "GAIL", "HINDPETRO",
    ],
    "AUTO": [
        "BOSCHLTD", "TIINDIA", "HEROMOTOCO",
        "M&M", "EICHERMOT", "EXIDEIND",
        "BAJAJ-AUTO", "ASHOKLEY",
        "MARUTI", "TVSMOTOR",
        "MOTHERSON", "SONACOMS",
        "UNOMINDA", "TMPV",
        "AMBER",
    ],
    "IT": [
        "INFY", "TCS", "HCLTECH", "WIPRO",
        "TECHM", "LTM", "MPHASIS",
        "KPITTECH", "COFORGE", "PERSISTENT",
        "TATAELXSI", "OFSS", "CAMS",
        "TATATECH", "NAUKRI", "KAYNES",
    ],
    "PHARMA": [
        "CIPLA", "ALKEM", "BIOCON", "DRREDDY",
        "MANKIND", "TORNTPHARM", "ZYDUSLIFE",
        "DIVISLAB", "LUPIN", "PPLPHARMA",
        "LAURUSLABS", "FORTIS",
        "AUROPHARMA", "GLENMARK",
        "SUNPHARMA", "SYNGENE",
        "MAXHEALTH", "APOLLOHOSP",
    ],
    "FMCG": [
        "HINDUNILVR", "ITC", "NESTLEIND",
        "BRITANNIA", "DABUR", "MARICO",
        "COLPAL", "GODREJCP",
        "TATACONSUM", "PATANJALI",
        "UNITDSPR",
        "VBL", "DMART", "NYKAA",
        "ETERNAL", "SWIGGY",
        "TITAN", "TRENT",
        "KALYANKJIL", "JUBLFOOD",
        "ASIANPAINT",
    ],
    "CEMENT": [
        "ULTRACEMCO", "SHREECEM",
        "AMBUJACEM", "DALBHARAT",
        "GRASIM", "ASTRAL",
        "PIDILITIND", "SUPREMEIND",
    ],
    "FINSERVICE": [
        "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG",
        "ICICIPRULI", "ICICIGI", "SBILIFE",
        "HDFCLIFE", "LICI", "LICHSGFIN",
        "PNBHOUSING", "MUTHOOTFIN",
        "MANAPPURAM", "CHOLAFIN",
        "PFC", "RECLTD",
        "HDFCAMC", "360ONE",
        "KFINTECH", "NUVAMA",
        "PAYTM", "POLICYBZR",
        "IIFL", "SBICARD",
        "JIOFIN", "SHRIRAMFIN",
        "SAMMAANCAP", "ANGELONE",
        "BSE", "CDSL", "MCX", "IRFC",
    ],
    "BANK": [
        "HDFCBANK", "ICICIBANK", "AXISBANK",
        "KOTAKBANK", "IDFCFIRSTB",
        "FEDERALBNK", "INDUSINDBK",
        "AUBANK", "BANDHANBNK",
        "RBLBANK", "BANKINDIA", "PNB", "INDIANB",
        "SBIN", "UNIONBANK", "BANKBARODA", "CANBK",
    ],
    "TELECOM": [
        "BHARTIARTL", "INDUSTOWER",
        "HAVELLS", "KEI", "POLYCAB",
        "CROMPTON", "VOLTAS",
        "PGEL", "DIXON",
    ],
    "LOGISTICS": [
        "CONCOR", "DELHIVERY", "INDIGO",
        "INDHOTEL", "IRCTC",
        "BLUESTARCO", "GMRAIRPORT",
        "PAGEIND", "UPL",
    ],
    "DEFENCE": [
        "ABB", "BDL", "BEL", "BHEL",
        "CGPOWER", "CUMMINSIND",
        "HAL", "LT", "MAZDOCK",
        "SIEMENS", "SOLARINDS",
    ],
    "NIFTY_50": [
        "ADANIENT", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE",
        "BAJAJFINSV", "BEL", "BHARTIARTL", "BPCL", "CIPLA", "COALINDIA",
        "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
        "HINDALCO", "HINDUNILVR", "ICICIBANK", "INFY", "INDIGO", "ITC",
        "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT", "M&M", "MARUTI",
        "MAXHEALTH", "NESTLEIND", "NTPC", "ONGC", "POWERGRID", "RELIANCE",
        "SBILIFE", "SHRIRAMFIN", "SBIN", "SUNPHARMA", "TCS", "TATACONSUM",
        "TATASTEEL", "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
        "TMPV", "ETERNAL",
    ],
}

ALL_SYMBOLS = sorted(set(sum(SECTOR_DEFINITIONS.values(), [])))


def _sector_of(sym: str) -> str:
    for s, syms in SECTOR_DEFINITIONS.items():
        if s != "NIFTY_50" and sym in syms:
            return s
    return "OTHER"


# =============================================================================
# SHARED STATE
# =============================================================================
LOCK = threading.Lock()

kite: Optional[KiteConnect] = None

symbol_to_token: Dict[str, int] = {}
token_to_symbol: Dict[int, str] = {}
ACTIVE_SYMBOLS: List[str] = []
TOKENS: List[int] = []

LAST_PRICE: Dict[int, float] = {}
DAY_VOL: Dict[int, float] = {}
LAST_OHLC: Dict[int, dict] = {}
HOT_HISTORY: Dict[int, deque] = {}

DAILY_STATS: Dict[int, dict] = {}
EOD_SNAPSHOT: Dict[int, dict] = {}

TOTAL_TICKS = 0
TPS_BUCKETS: deque = deque()
TPS_WINDOW = 2.0

SEED_DONE = False
SEED_STARTED = False
SEED_PROGRESS = {"done": 0, "total": 0, "errors": 0}
SEED_CURRENT_SYMBOL = ""
SEED_START_TIME: Optional[float] = None

WS_CONNECTED = False
WS_CLIENTS: List[WebSocket] = []

INIT_DONE = False
INIT_ERROR: Optional[str] = None

# =============================================================================
# MARKET / EXPECTED VOLUME
# =============================================================================
def market_open(now=None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 15) <= t <= dtime(15, 30)


VOL_CURVE = [
    (0.00, 0.00), (0.08, 0.25), (0.20, 0.40), (0.40, 0.55),
    (0.60, 0.65), (0.80, 0.78), (0.90, 0.85), (1.00, 1.00),
]


def _vol_frac(t: float) -> float:
    if t <= 0:
        return 0.0
    if t >= 1:
        return 1.0
    for i in range(1, len(VOL_CURVE)):
        t1, v1 = VOL_CURVE[i - 1]
        t2, v2 = VOL_CURVE[i]
        if t <= t2:
            f = (t - t1) / (t2 - t1 + 1e-9)
            return v1 + f * (v2 - v1)
    return 1.0


def _expected_vol(avg20: float, now=None) -> float:
    now = now or datetime.now(IST)
    o = now.replace(hour=9, minute=15, second=0, microsecond=0)
    c = now.replace(hour=15, minute=30, second=0, microsecond=0)
    total = (c - o).total_seconds()

    if now <= o:
        t = 0.0
    elif now >= c:
        t = 1.0
    else:
        t = (now - o).total_seconds() / total

    return avg20 * max(_vol_frac(t), 0.01)


# =============================================================================
# TICKS
# =============================================================================
def _record_ticks(count: int):
    global TOTAL_TICKS
    now = time.time()
    TOTAL_TICKS += count
    TPS_BUCKETS.append((now, count))
    cutoff = now - TPS_WINDOW
    while TPS_BUCKETS and TPS_BUCKETS[0][0] < cutoff:
        TPS_BUCKETS.popleft()


def _get_tps() -> float:
    if not TPS_BUCKETS:
        return 0.0
    return sum(c for _, c in TPS_BUCKETS) / TPS_WINDOW


def _hot_push(token: int, epoch: float, ltp: float, vol: Optional[float]):
    dq = HOT_HISTORY.get(token)
    if dq is None:
        dq = deque()
        HOT_HISTORY[token] = dq

    if dq and (epoch - dq[-1][0]) < HOT_SAMPLE_SEC:
        e, _, lv = dq[-1]
        dq[-1] = (e, ltp, vol if vol is not None else lv)
    else:
        dq.append((epoch, ltp, vol))

    cutoff = epoch - HOT_MAX_SEC
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def process_tick(tick: dict):
    token = tick["instrument_token"]
    ltp = tick.get("last_price")
    vol = tick.get("volume_traded")
    ohlc = tick.get("ohlc") or {}

    if ltp is None:
        return

    LAST_PRICE[token] = float(ltp)
    if vol is not None:
        DAY_VOL[token] = float(vol)
    if ohlc:
        LAST_OHLC[token] = ohlc

    _hot_push(token, time.time(), float(ltp), float(vol) if vol is not None else None)


# =============================================================================
# RFACTOR v2 COMPONENTS
# =============================================================================
def _vol_accel(token: int, window_sec: int = 900) -> float:
    dq = HOT_HISTORY.get(token)
    if not dq or len(dq) < 4:
        return 1.0
    now_e = dq[-1][0]
    rc = now_e - window_sec
    pc = rc - window_sec
    recent = [(t, p, v) for t, p, v in dq if t >= rc and v is not None]
    prior = [(t, p, v) for t, p, v in dq if pc <= t < rc and v is not None]
    if len(recent) < 2 or len(prior) < 2:
        return 1.0
    rv = max(0.0, float(recent[-1][2]) - float(recent[0][2]))
    pv = max(0.0, float(prior[-1][2]) - float(prior[0][2]))
    if pv <= 0:
        return 2.0 if rv > 0 else 1.0
    return max(0.1, min(5.0, rv / pv))


def _trend_quality(token: int, window_sec: int = 1800) -> float:
    dq = HOT_HISTORY.get(token)
    if not dq or len(dq) < 5:
        return 0.5
    now_e = dq[-1][0]
    cutoff = now_e - window_sec
    prices = [float(p) for t, p, _ in dq if t >= cutoff and p is not None]
    if len(prices) < 3:
        return 0.5
    net = abs(prices[-1] - prices[0])
    total = sum(abs(prices[i] - prices[i - 1]) for i in range(1, len(prices)))
    if total <= 0:
        return 0.5
    eff = net / total
    up = sum(1 for i in range(1, len(prices)) if prices[i] > prices[i - 1])
    dn = sum(1 for i in range(1, len(prices)) if prices[i] < prices[i - 1])
    tm = up + dn
    mono = max(up, dn) / tm if tm > 0 else 0.5
    return max(0.0, min(1.0, eff * 0.6 + mono * 0.4))


def _pv_confirm(token: int, window_sec: int = 1800) -> float:
    dq = HOT_HISTORY.get(token)
    if not dq or len(dq) < 5:
        return 1.0
    now_e = dq[-1][0]
    cutoff = now_e - window_sec
    pts = [(float(t), float(p), float(v)) for t, p, v in dq
           if t >= cutoff and p is not None and v is not None]
    if len(pts) < 3:
        return 1.0
    overall = 1.0 if pts[-1][1] > pts[0][1] else -1.0
    vw, va = 0.0, 0.0
    for i in range(1, len(pts)):
        pc = pts[i][1] - pts[i - 1][1]
        vd = max(0.0, pts[i][2] - pts[i - 1][2])
        if vd <= 0:
            continue
        if (pc > 0 and overall > 0) or (pc < 0 and overall < 0):
            vw += vd
        else:
            va += vd
    return max(0.1, min(3.0, (vw + 1.0) / (va + 1.0)))


def _recency(token: int, window_sec: int = 900) -> float:
    dq = HOT_HISTORY.get(token)
    if not dq or len(dq) < 4:
        return 0.5
    now_e = dq[-1][0]
    rc = now_e - window_sec
    recent_prices = [float(p) for t, p, _ in dq if t >= rc and p is not None]
    all_prices = [float(p) for _, p, _ in dq if p is not None]
    if len(recent_prices) < 2 or len(all_prices) < 2:
        return 0.5
    rr = max(recent_prices) - min(recent_prices)
    tr = max(all_prices) - min(all_prices)
    if tr <= 0:
        return 0.5
    return max(0.0, min(1.0, rr / tr))


def _signal(rf: float, tq: float, pv: float) -> str:
    c = rf * 0.4 + tq * 20.0 * 0.3 + pv * 5.0 * 0.3
    if c >= 20:
        return "EXTREME"
    if c >= 10:
        return "STRONG"
    if c >= 4:
        return "MODERATE"
    return "WEAK"


# =============================================================================
# RFACTOR v2 COMPUTE
# =============================================================================
def compute_rfv2(token: int) -> Optional[dict]:
    ltp = LAST_PRICE.get(token)
    vol = DAY_VOL.get(token)
    ohlc = LAST_OHLC.get(token) or {}

    if ltp is None or vol is None:
        eod = EOD_SNAPSHOT.get(token)
        if not eod:
            return None
        ltp = float(eod["close"])
        vol = float(eod["volume"])
        ohlc = {"open": eod["open"], "high": eod["high"], "low": eod["low"], "close": eod["prev_close"]}

    eod = EOD_SNAPSHOT.get(token)
    if eod:
        ohlc.setdefault("open", eod["open"])
        ohlc.setdefault("high", eod["high"])
        ohlc.setdefault("low", eod["low"])
        ohlc.setdefault("close", eod["prev_close"])

    prev_close = float(ohlc.get("close") or 0.0)
    day_open = float(ohlc.get("open") or 0.0)
    day_high = float(ohlc.get("high") or ltp)
    day_low = float(ohlc.get("low") or ltp)

    ltp = float(ltp)
    vol = float(vol)

    if prev_close <= 0 or day_open <= 0 or ltp <= 0:
        return None

    st = DAILY_STATS.get(token) or {}
    a_vol = st.get("avg_vol_20")
    a_rng = st.get("avg_range_20")
    a_ret = st.get("avg_abs_oc_ret_20")
    if not a_vol or not a_rng or not a_ret:
        return None

    eps = 1e-9
    pct_open = ((ltp - day_open) / day_open) * 100.0
    rng = max(0.0, day_high - day_low)

    exp_vol = _expected_vol(float(a_vol), datetime.now(IST))
    rvolm = vol / (exp_vol + eps)
    range_f = rng / (float(a_rng) + eps)
    move_f = abs(pct_open) / (float(a_ret) + eps)

    va = _vol_accel(token)
    tq = _trend_quality(token)
    pv = _pv_confirm(token)
    rec = _recency(token)

    core = (
        max(rvolm, eps) ** W_RVOLM *
        max(range_f, eps) ** W_RANGE *
        max(move_f, eps) ** W_MOVE *
        max(va, eps) ** W_VOL_ACCEL
    )

    tb = TQ_MIN + tq * (TQ_MAX - TQ_MIN)
    pvb = PV_MIN + min(pv, 2.0) / 2.0 * (PV_MAX - PV_MIN)
    rb = REC_MIN + rec * (REC_MAX - REC_MIN)

    rf = core * tb * pvb * rb
    dirr = (1.0 if pct_open >= 0 else -1.0) * rf

    sym = token_to_symbol.get(token, "?")
    sector = _sector_of(sym)

    return {
        "symbol": sym,
        "sector": sector,
        "rfactor": round(rf, 4),
        "dirr": round(dirr, 4),
        "rvolm": round(rvolm, 4),
        "ltp": round(ltp, 2),
        "pct_open": round(pct_open, 2),
        "signal": _signal(rf, tq, pv),
    }


def compute_all() -> List[dict]:
    out: List[dict] = []
    for tok in TOKENS:
        r = compute_rfv2(tok)
        if r:
            out.append(r)
    return out


# =============================================================================
# INIT (KITE + INSTRUMENTS)
# =============================================================================
def _init_kite_and_instruments():
    global kite, symbol_to_token, token_to_symbol, ACTIVE_SYMBOLS, TOKENS, INIT_DONE, INIT_ERROR

    if not API_KEY or not ACCESS_TOKEN:
        INIT_ERROR = "Missing KITE_API_KEY / KITE_ACCESS_TOKEN"
        log.error(INIT_ERROR)
        return

    try:
        log.info("Connecting to Kite…")
        k = KiteConnect(api_key=API_KEY)
        k.set_access_token(ACCESS_TOKEN)

        log.info("Loading NSE instruments…")
        ins_df = pd.DataFrame(k.instruments("NSE"))
        ins_df = ins_df[ins_df["tradingsymbol"].isin(ALL_SYMBOLS)].copy()

        s2t: Dict[str, int] = dict(zip(ins_df["tradingsymbol"], ins_df["instrument_token"]))
        t2s: Dict[int, str] = {v: k for k, v in s2t.items()}

        active = sorted(s2t.keys())
        toks = sorted(s2t.values())

        missing = set(ALL_SYMBOLS) - set(active)
        if missing:
            log.warning("Missing symbols (not in NSE instruments): %s", sorted(missing))

        with LOCK:
            kite = k
            symbol_to_token = s2t
            token_to_symbol = t2s
            ACTIVE_SYMBOLS = active
            TOKENS = toks
            SEED_PROGRESS["total"] = len(TOKENS)

        INIT_DONE = True
        log.info("Resolved %d / %d symbols", len(ACTIVE_SYMBOLS), len(ALL_SYMBOLS))

    except Exception as e:
        INIT_ERROR = str(e)
        log.exception("Init failed: %s", e)


# =============================================================================
# DAILY SEED
# =============================================================================
def _seed_daily():
    global SEED_DONE, SEED_STARTED, SEED_CURRENT_SYMBOL, SEED_START_TIME

    while not INIT_DONE and not INIT_ERROR:
        time.sleep(0.25)
    if INIT_ERROR or kite is None:
        return

    SEED_STARTED = True
    SEED_START_TIME = time.time()
    log.info("Seeding %d-day baselines for %d tokens…", LOOKBACK_SESSIONS, len(TOKENS))

    now_ist = datetime.now(IST)
    to_dt = now_ist.date()
    from_dt = to_dt - timedelta(days=HISTORY_DAYS)

    for i, tok in enumerate(TOKENS, 1):
        sym = token_to_symbol.get(tok, str(tok))
        SEED_CURRENT_SYMBOL = sym

        try:
            candles = kite.historical_data(
                instrument_token=tok,
                from_date=from_dt,
                to_date=to_dt,
                interval="day",
                continuous=False,
                oi=False,
            )
        except Exception as e:
            log.debug("Seed %s failed: %s", sym, e)
            SEED_PROGRESS["errors"] += 1
            SEED_PROGRESS["done"] = i
            time.sleep(SEED_PACE)
            continue

        df = pd.DataFrame(candles)
        if df.empty or len(df) < LOOKBACK_SESSIONS + 2:
            SEED_PROGRESS["done"] = i
            time.sleep(SEED_PACE)
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["d"] = df["date"].dt.date
        today = now_ist.date()

        if market_open(now_ist) and df.iloc[-1]["d"] == today:
            df = df.iloc[:-1].copy()

        if len(df) < LOOKBACK_SESSIONS + 1:
            SEED_PROGRESS["done"] = i
            time.sleep(SEED_PACE)
            continue

        last = df.iloc[-1]
        prev = df.iloc[-2]

        eod = {
            "date": last["d"],
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            "volume": float(last["volume"]),
            "prev_close": float(prev["close"]),
        }

        tail = df.tail(LOOKBACK_SESSIONS).copy()
        tail["range"] = (tail["high"] - tail["low"]).astype(float)
        tail["oc_ret"] = ((tail["close"] - tail["open"]) / tail["open"] * 100.0).astype(float)

        with LOCK:
            DAILY_STATS[tok] = {
                "avg_vol_20": float(tail["volume"].mean()),
                "avg_range_20": float(tail["range"].mean()),
                "avg_abs_oc_ret_20": float(tail["oc_ret"].abs().mean()),
            }
            EOD_SNAPSHOT[tok] = eod

        SEED_PROGRESS["done"] = i
        if i % 25 == 0 or i == len(TOKENS):
            log.info("  Seed: %d / %d (err %d)", i, len(TOKENS), SEED_PROGRESS["errors"])

        time.sleep(SEED_PACE)

    SEED_DONE = True
    SEED_CURRENT_SYMBOL = ""
    elapsed = time.time() - (SEED_START_TIME or time.time())
    log.info("Seed complete: %d loaded, %d errors (%.1fs)", len(DAILY_STATS), SEED_PROGRESS["errors"], elapsed)


# =============================================================================
# KITE TICKER
# =============================================================================
_ticker_started = False


def _start_ticker():
    global _ticker_started
    if _ticker_started:
        return
    _ticker_started = True

    def _run():
        global WS_CONNECTED

        while not INIT_DONE and not INIT_ERROR:
            time.sleep(0.25)
        if INIT_ERROR:
            return

        while True:
            try:
                kws = KiteTicker(API_KEY, ACCESS_TOKEN)

                def on_connect(ws, _):
                    global WS_CONNECTED
                    WS_CONNECTED = True
                    log.info("KiteTicker CONNECTED — subscribing %d tokens", len(TOKENS))
                    ws.subscribe(TOKENS)
                    ws.set_mode(ws.MODE_FULL, TOKENS)

                def on_ticks(ws, ticks):
                    try:
                        with LOCK:
                            for t in ticks:
                                process_tick(t)
                            _record_ticks(len(ticks))
                    except Exception:
                        log.exception("on_ticks error")

                def on_close(ws, code, reason):
                    global WS_CONNECTED
                    WS_CONNECTED = False
                    log.warning("KiteTicker CLOSED: %s %s", code, reason)

                def on_error(ws, code, reason):
                    global WS_CONNECTED
                    WS_CONNECTED = False
                    log.error("KiteTicker ERROR: %s %s", code, reason)

                kws.on_connect = on_connect
                kws.on_ticks = on_ticks
                kws.on_close = on_close
                kws.on_error = on_error

                kws.connect(threaded=True)

                last_ok = time.time()
                while True:
                    time.sleep(2)
                    if WS_CONNECTED:
                        last_ok = time.time()
                    if not WS_CONNECTED and (time.time() - last_ok) > 15:
                        try:
                            kws.close()
                        except Exception:
                            pass
                        break

            except Exception:
                WS_CONNECTED = False
                log.exception("Ticker crashed — restarting in 5s")
                time.sleep(5)

    threading.Thread(target=_run, daemon=True).start()


# =============================================================================
# FASTAPI
# =============================================================================
app = FastAPI(title="RFactor v2 Live")


@app.on_event("startup")
async def startup():
    threading.Thread(target=_init_kite_and_instruments, daemon=True).start()
    threading.Thread(target=_seed_daily, daemon=True).start()
    _start_ticker()
    log.info("Dashboard starting on port %d", PORT)


@app.get("/theme.css")
def theme_css():
    path = os.path.join(os.path.dirname(__file__), "theme.css")
    if not os.path.exists(path):
        return JSONResponse({"error": "theme.css not found. Put theme.css next to app.py"}, 404)
    return FileResponse(path, media_type="text/css")


# =============================================================================
# API
# =============================================================================
@app.get("/api/health")
def health():
    with LOCK:
        now_ist = datetime.now(IST)
        elapsed = (time.time() - SEED_START_TIME) if SEED_START_TIME else 0.0
        return {
            "status": "ok",
            "init_done": INIT_DONE,
            "init_error": INIT_ERROR,
            "seed_done": SEED_DONE,
            "seed_started": SEED_STARTED,
            "seed_progress": dict(SEED_PROGRESS),
            "seed_current": SEED_CURRENT_SYMBOL,
            "seed_elapsed_sec": round(elapsed, 1),
            "total_ticks": TOTAL_TICKS,
            "tps": round(_get_tps(), 1),
            "live_tokens": len(LAST_PRICE),
            "stats_loaded": len(DAILY_STATS),
            "hot_tokens": len(HOT_HISTORY),
            "ws_connected": WS_CONNECTED,
            "ws_clients": len(WS_CLIENTS),
            "market_open": market_open(now_ist),
            "time_ist": now_ist.strftime("%H:%M:%S"),
            "symbols_total": len(ACTIVE_SYMBOLS),
        }


@app.get("/api/rfactor/all")
def api_all(sort: str = "rfactor", order: str = "desc", limit: int = 999):
    with LOCK:
        results = compute_all()

    rev = (order == "desc")
    if sort in ("pct_open", "dirr"):
        results.sort(key=lambda x: x.get(sort, 0), reverse=rev)
    else:
        results.sort(key=lambda x: abs(x.get(sort, 0)), reverse=rev)
    return results[:limit]


@app.get("/api/sectors")
def api_sectors():
    with LOCK:
        all_r = compute_all()

    sectors: Dict[str, List[dict]] = {}
    for r in all_r:
        sec = r["sector"]
        if sec in ("OTHER",):
            continue
        sectors.setdefault(sec, []).append(r)

    result = {}
    for sec, stocks in sectors.items():
        gainers = [s for s in stocks if s["pct_open"] > 0]
        losers  = [s for s in stocks if s["pct_open"] < 0]
        result[sec] = {
            "avg_dirr": round(float(np.mean([s["dirr"] for s in stocks])), 4),
            "avg_rfactor": round(float(np.mean([s["rfactor"] for s in stocks])), 4),
            "avg_rvolm": round(float(np.mean([s["rvolm"] for s in stocks])), 4),
            "n": len(stocks),
            "gainers": len(gainers),
            "losers": len(losers),
        }

    # ----- ADD NIFTY 50 AGGREGATE ROW -----
    nifty_syms = set(SECTOR_DEFINITIONS.get("NIFTY_50", []))
    nifty = [r for r in all_r if r["symbol"] in nifty_syms]
    if nifty:
        gainers = [s for s in nifty if s["pct_open"] > 0]
        losers  = [s for s in nifty if s["pct_open"] < 0]
        result["NIFTY 50"] = {
            "avg_dirr": round(float(np.mean([s["dirr"] for s in nifty])), 4),
            "avg_rfactor": round(float(np.mean([s["rfactor"] for s in nifty])), 4),
            "avg_rvolm": round(float(np.mean([s["rvolm"] for s in nifty])), 4),
            "n": len(nifty),
            "gainers": len(gainers),
            "losers": len(losers),
        }

    return result


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    WS_CLIENTS.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in WS_CLIENTS:
            WS_CLIENTS.remove(ws)


# =============================================================================
# HTML
# =============================================================================
HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>RFactor v2 — Live</title>
  <link rel="stylesheet" href="/theme.css?v=3">
</head>
<body>

  <!-- SECTOR MODAL -->
  <div class="modal-bg" id="sec-modal-bg">
    <div class="modal" role="dialog" aria-modal="true">
      <div class="modal-h">
        <div class="modal-t" id="sec-modal-title">SECTOR</div>
        <button class="modal-x" id="sec-modal-close" type="button">×</button>
      </div>
      <div class="modal-b" id="sec-modal-body"></div>
    </div>
  </div>

  <div class="hdr">
    <div class="hdr-top">
      <div class="brand">
        <h1>RFACTOR v2</h1>
        <span class="tag live" id="status-tag">…</span>
      </div>
      <div class="stats" id="hdr-stats"></div>
    </div>

    <div class="seed-bar-wrap" id="seed-bar-wrap">
      <div class="seed-info">
        <span class="seed-label" id="seed-label">Seeding baselines…</span>
        <span class="seed-detail" id="seed-detail"></span>
      </div>
      <div class="seed-track"><div class="seed-fill" id="seed-fill" style="width:0%"></div></div>
    </div>
    <div class="seed-done" id="seed-done">✓ Seed complete</div>
  </div>

  <div class="main">
    <div class="sumrow" id="sum"></div>

    <div class="sec-title">SECTOR RANKINGS (DIRR)</div>
    <div class="sec-chart">
  <div class="sec-y" id="sec-y"></div>

  <div class="sec-plot-wrap">
    <div class="sec-plot" id="sec-plot"></div>
    <div class="sec-tip" id="sec-tip"></div>
  </div>

  <div class="sec-y-pad"></div>
  <div class="sec-xlabels" id="sec-xlabels"></div>
</div>

    <div class="tbl-row">
      <div class="tcard">
        <div class="thead g">TOP 10 GAINERS — RFACTOR v2</div>
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>STOCKS</th>
              <th>LTP</th>
              <th>%CHG</th>
              <th>RFACTOR</th>
              <th>RVOLM</th>
              <th>SIGNAL</th>
            </tr>
          </thead>
          <tbody id="gtb"></tbody>
        </table>
      </div>

      <div class="tcard">
        <div class="thead r">TOP 10 LOSERS — RFACTOR v2</div>
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>STOCKS</th>
              <th>LTP</th>
              <th>%CHG</th>
              <th>RFACTOR</th>
              <th>RVOLM</th>
              <th>SIGNAL</th>
            </tr>
          </thead>
          <tbody id="ltb"></tbody>
        </table>
      </div>
    </div>
  </div>

<script>
const R = 2000;
let seedWasRunning = false;
let allData = [];

function f(v, d=2){ return (v==null ? '—' : Number(v).toFixed(d)); }
function fT(v){
  if(v>=1e6) return (v/1e6).toFixed(2)+'M';
  if(v>=1e3) return (v/1e3).toFixed(1)+'K';
  return String(v||0);
}
function pp(v){
  const c = v>0 ? 'pos' : (v<0 ? 'neg' : 'neu');
  return '<span class="pill '+c+'">'+(v>0?'+':'')+f(v,2)+'%</span>';
}
function sb(s){ return '<span class="bdg '+(s||'weak').toLowerCase()+'">'+(s||'—')+'</span>'; }

function waitRow(msg, sub){
  const x = '<div class="wait-msg"><div class="wait-icon">⏳</div>'+msg+(sub?'<div class="wait-sub">'+sub+'</div>':'')+'</div>';
  return '<tr><td colspan="7">'+x+'</td></tr>';
}

function tvUrl(sym){
  return 'https://www.tradingview.com/chart/?symbol='
    + encodeURIComponent('NSE:' + sym)
    + '&interval=5';
}

function escapeHtml(s){
  return String(s)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function row(r, i){
  const sym = escapeHtml(r.symbol);
  const sec = escapeHtml(r.sector || '');
  const url = tvUrl(r.symbol);

  return '<tr>'+
    '<td class="rk">'+i+'</td>'+
    '<td class="symcell">'+
      '<a class="symlink" href="'+url+'" target="_blank" rel="noopener noreferrer">'+
        '<div class="sym">'+sym+'</div>'+
      '</a>'+
      '<div class="sec-sm">'+sec+'</div>'+
    '</td>'+
    '<td class="num">₹'+f(r.ltp,2)+'</td>'+
    '<td class="num">'+pp(r.pct_open)+'</td>'+
    '<td class="num rf">'+f(r.rfactor, 2)+'</td>'+
    '<td class="num">'+f(r.rvolm, 2)+'x</td>'+
    '<td class="num">'+sb(r.signal)+'</td>'+
  '</tr>';
}

function sc(l, v, cls){
  cls = cls || '';
  return '<div class="scard"><div class="sl">'+l+'</div><div class="sv '+cls+'">'+v+'</div></div>';
}

/* ---------- SECTOR MODAL ---------- */
function openSectorModal(sector){
  const bg = document.getElementById('sec-modal-bg');
  const title = document.getElementById('sec-modal-title');
  const body = document.getElementById('sec-modal-body');
  if(!bg || !title || !body) return;

  title.textContent = (sector || '').toUpperCase() + ' — STOCKS';

  const rows = (allData || [])
    .filter(r => r.sector === sector)
    .sort((a,b) => b.rfactor - a.rfactor);

  if(!rows.length){
    body.innerHTML = '<div class="muted">NO STOCKS FOUND</div>';
    bg.classList.add('show');
    return;
  }

  let h = ''
    + '<table class="mtbl">'
    + '<thead><tr>'
    + '<th>STOCK</th>'
    + '<th>RFACTOR</th>'
    + '<th>RVOLM</th>'
    + '<th>SIGNAL</th>'
    + '</tr></thead><tbody>';

  for(const r of rows){
    const url = tvUrl(r.symbol);
    h += '<tr>'
      + '<td><a class="symlink" href="'+url+'" target="_blank" rel="noopener noreferrer">'+escapeHtml(r.symbol)+'</a></td>'
      + '<td class="rf">' + f(r.rfactor, 2) + '</td>'
      + '<td>' + f(r.rvolm, 2) + 'X</td>'
      + '<td class="sig">' + sb(r.signal) + '</td>'
      + '</tr>';
  }

  h += '</tbody></table>';
  body.innerHTML = h;
  bg.classList.add('show');
}

function closeSectorModal(){
  const bg = document.getElementById('sec-modal-bg');
  if(bg) bg.classList.remove('show');
}

document.addEventListener('DOMContentLoaded', () => {
  const bg = document.getElementById('sec-modal-bg');
  const closeBtn = document.getElementById('sec-modal-close');

  if(closeBtn) closeBtn.addEventListener('click', closeSectorModal);
  if(bg){
    bg.addEventListener('click', (e) => {
      if(e.target && e.target.id === 'sec-modal-bg') closeSectorModal();
    });
  }
  document.addEventListener('keydown', (e) => {
    if(e.key === 'Escape') closeSectorModal();
  });
});

/* ---------- SECTOR CHART ---------- */
function niceStep(range, ticks){
  const rough = range / Math.max(1, ticks);
  if(rough <= 0) return 1;
  const pow = Math.pow(10, Math.floor(Math.log10(rough)));
  const r = rough / pow;
  let step = 10;
  if(r <= 1) step = 1;
  else if(r <= 2) step = 2;
  else if(r <= 5) step = 5;
  return step * pow;
}

function renderSectorChart(secObj){
  const yEl = document.getElementById('sec-y');
  const plot = document.getElementById('sec-plot');
  const tip = document.getElementById('sec-tip');
  const xlabels = document.getElementById('sec-xlabels');
  if(!yEl || !plot || !tip) return;

  const entries = Object.entries(secObj || {}).sort((a,b) => (b[1].avg_dirr||0) - (a[1].avg_dirr||0));
  if(!entries.length){
    yEl.innerHTML = '';
    plot.innerHTML = '<div class="muted">Waiting for sector data…</div>';
    return;
  }

  const values = entries.map(([_,d]) => Number(d.avg_dirr||0));
  let minV = Math.min(...values, 0);
  let maxV = Math.max(...values, 0);

  // Expand a bit if flat
  if(Math.abs(maxV - minV) < 1e-6){
    maxV += 1;
    minV -= 1;
  }

  const height = 320;                 // must match CSS height
  plot.style.height = height + 'px';

  const step = niceStep(maxV - minV, 7);
  const minT = Math.floor(minV / step) * step;
  const maxT = Math.ceil(maxV / step) * step;

  const y = (v) => ((maxT - v) / (maxT - minT)) * height; // 0..height px
  const yZero = y(0);

  // Y axis ticks
  let yHtml = '';
  for(let v = maxT; v >= minT - 1e-9; v -= step){
    const yp = y(v);
    const label = (Math.abs(step) >= 1) ? f(v,0) : f(v,2);
    yHtml += '<div class="sec-tick" style="top:'+yp+'px">'+label+'</div>';
  }
  yEl.innerHTML = yHtml;

  // Plot baseline
  let html = '<div class="sec-zero" style="top:'+yZero+'px"></div>';

  // Bars
  for(const [name, d] of entries){
    const v = Number(d.avg_dirr||0);
    const top = Math.min(y(v), yZero);
    const bot = Math.max(y(v), yZero);
    const h = Math.max(2, bot - top); // keep visible

    const cls = v >= 0 ? 'pos' : 'neg';
    const valTxt = (v>=0?'+':'') + f(v,2);

    html += ''
      + '<div class="sec-col" data-sector="'+encodeURIComponent(name)+'" data-val="'+valTxt+'" data-name="'+escapeHtml(name)+'">'
      +   '<div class="sec-col-box">'
      +     '<div class="sec-bar '+cls+'" style="top:'+top+'px;height:'+h+'px"></div>'
      +   '</div>'
      + '</div>';
  }

  plot.innerHTML = html;
  
  if(xlabels){
  xlabels.innerHTML = entries.map(([name, d]) => {
    return '<div class="sec-xlab" data-sector="'+encodeURIComponent(name)+'">'
      + escapeHtml(name) +
    '</div>';
  }).join('');

  // make labels clickable too
  xlabels.querySelectorAll('.sec-xlab').forEach(lb => {
    lb.addEventListener('click', () => {
      const sector = decodeURIComponent(lb.dataset.sector || '');
      if(sector) openSectorModal(sector);
    });
  });
}

  // Events (hover tooltip + click)
  const cols = plot.querySelectorAll('.sec-col');
  cols.forEach(col => {
    col.addEventListener('mouseenter', (e) => {
      const name = col.dataset.name || '';
      const val = col.dataset.val || '';
      tip.innerHTML = '<div class="sec-tip-t">'+name+'</div><div class="sec-tip-v">'+val+'</div>';
      tip.style.display = 'block';
      col.classList.add('active');
    });

    col.addEventListener('mousemove', (e) => {
      const rect = plot.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      tip.style.left = (x + 14) + 'px';
      tip.style.top = (y - 10) + 'px';
    });

    col.addEventListener('mouseleave', () => {
      tip.style.display = 'none';
      col.classList.remove('active');
    });

    col.addEventListener('click', () => {
      const sector = decodeURIComponent(col.dataset.sector || '');
      if(sector) openSectorModal(sector);
    });
  });
}

async function tick(){
  try{
    const [hR, aR, sR] = await Promise.all([
      fetch('/api/health'),
      fetch('/api/rfactor/all?sort=rfactor&order=desc&limit=999'),
      fetch('/api/sectors')
    ]);
    const h = await hR.json();
    const all = await aR.json();
    const sec = await sR.json();
    allData = all;

    // Status tag
    const st = document.getElementById('status-tag');
    if(h.init_error){
      st.textContent = 'ERROR'; st.className = 'tag off';
    }else if(!h.seed_done && h.seed_started){
      st.textContent = 'SEEDING'; st.className = 'tag seed';
    }else if(h.ws_connected && h.total_ticks > 0){
      st.textContent = 'LIVE'; st.className = 'tag live';
    }else if(h.seed_done && !h.market_open){
      st.textContent = 'CLOSED'; st.className = 'tag off';
    }else{
      st.textContent = 'OFFLINE'; st.className = 'tag off';
    }

    // Header stats
    let hs = '';
    hs += '<div class="chip"><span class="lbl">TICKS</span><span class="val c">'+fT(h.total_ticks)+'</span></div>';
    hs += '<div class="chip"><span class="lbl">TPS</span><span class="val c">'+f(h.tps,1)+'</span></div>';
    hs += '<div class="chip"><span class="lbl">TOKENS</span><span class="val">'+h.live_tokens+'/'+h.symbols_total+'</span></div>';
    hs += '<div class="chip"><span class="lbl">STATS</span><span class="val '+(h.seed_done?'g':'y')+'">'+h.stats_loaded+'/'+h.symbols_total+'</span></div>';
    hs += '<div class="chip"><span class="lbl">MKT</span><span class="val '+(h.market_open?'g':'r')+'">'+(h.market_open?'OPEN':'CLOSED')+'</span></div>';
    hs += '<div class="chip"><span class="val">'+h.time_ist+' IST</span></div>';
    document.getElementById('hdr-stats').innerHTML = hs;

    // Seed bar
    const seedBar = document.getElementById('seed-bar-wrap');
    const seedDone = document.getElementById('seed-done');
    const seedFill = document.getElementById('seed-fill');
    const seedLabel = document.getElementById('seed-label');
    const seedDetail = document.getElementById('seed-detail');

    if(!h.seed_done && h.seed_started){
      seedWasRunning = true;
      seedBar.classList.add('show');
      seedDone.classList.remove('show');

      const pct = h.seed_progress.total>0 ? Math.round((h.seed_progress.done/h.seed_progress.total)*100) : 0;
      seedFill.style.width = pct + '%';
      seedLabel.textContent = 'Seeding baselines… ' + h.seed_progress.done + ' / ' + h.seed_progress.total;

      let det = '';
      if(h.seed_current) det += 'Currently: ' + h.seed_current;
      if(h.seed_progress.errors>0) det += ' · Errors: ' + h.seed_progress.errors;
      if(h.seed_elapsed_sec>0) det += ' · ' + f(h.seed_elapsed_sec,0) + 's elapsed';
      seedDetail.textContent = det;
    }else if(h.seed_done){
      seedBar.classList.remove('show');
      if(seedWasRunning){
        seedDone.classList.add('show');
        seedDone.textContent = '✓ Seed complete — ' + h.stats_loaded + ' stocks loaded'
          + (h.seed_progress.errors>0 ? (' ('+h.seed_progress.errors+' errors)') : '')
          + ' in ' + f(h.seed_elapsed_sec,1) + 's';
        setTimeout(()=>{ seedDone.classList.remove('show'); seedWasRunning=false; }, 8000);
      }
    }

    // Summary
    const g = all.filter(r => r.pct_open > 0);
    const l = all.filter(r => r.pct_open < 0);
    const avgR = all.length ? (all.reduce((s,r)=>s+r.rfactor,0)/all.length) : 0;
    const avgRv = all.length ? (all.reduce((s,r)=>s+r.rvolm,0)/all.length) : 0;

    let sm = '';
    sm += sc('STOCKS', all.length || '—', 'c');
    sm += sc('ADV', g.length || '—', 'g');
    sm += sc('DEC', l.length || '—', 'r');
    sm += sc('AVG RFACTOR', all.length ? f(avgR,2) : '—', 'c');
    sm += sc('AVG RVOLM', all.length ? (f(avgRv,2)+'x') : '—', '');
    document.getElementById('sum').innerHTML = sm;

    // Sector chart
    renderSectorChart(sec);

    // Tables
    if(h.init_error){
      document.getElementById('gtb').innerHTML = waitRow('Init error', escapeHtml(h.init_error));
      document.getElementById('ltb').innerHTML = waitRow('Init error', escapeHtml(h.init_error));
      return;
    }

    if(!h.seed_done){
      const sub = 'Seeding '+h.seed_progress.done+'/'+h.seed_progress.total + (h.seed_current ? (' — '+h.seed_current) : '');
      document.getElementById('gtb').innerHTML = waitRow('Waiting for baseline seed to complete…', sub);
      document.getElementById('ltb').innerHTML = waitRow('Waiting for baseline seed to complete…', sub);
      return;
    }

    if(!all.length){
      document.getElementById('gtb').innerHTML = waitRow('No data yet', 'Waiting for live ticks…');
      document.getElementById('ltb').innerHTML = waitRow('No data yet', 'Waiting for live ticks…');
      return;
    }

    const t10g = g.sort((a,b)=>b.rfactor-a.rfactor).slice(0,10);
    const t10l = l.sort((a,b)=>b.rfactor-a.rfactor).slice(0,10);

    document.getElementById('gtb').innerHTML =
      t10g.length ? t10g.map((r,i)=>row(r,i+1)).join('') :
      '<tr><td colspan="7" class="empty">No gainers</td></tr>';

    document.getElementById('ltb').innerHTML =
      t10l.length ? t10l.map((r,i)=>row(r,i+1)).join('') :
      '<tr><td colspan="7" class="empty">No losers</td></tr>';

  }catch(e){
    console.error(e);
  }
}

tick();
setInterval(tick, R);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTML_PAGE.replace("__PORT__", str(PORT))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, workers=1, reload=False, log_level="info")