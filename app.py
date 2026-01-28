# app.py  (updated: more accurate volume/VWAP + VWAP momentum/slope fields)
import os
import time
import socket
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from kiteconnect import KiteConnect, KiteTicker


# ================= CONFIG =================
API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")
if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("KITE_API_KEY or KITE_ACCESS_TOKEN not set")

IST = ZoneInfo("Asia/Kolkata")
LOOKBACK_DAYS = 20
BUFFER_DAYS = 40

SCRIPT_START = datetime.now(IST)

LIVE_STARTED = False
live_lock = threading.Lock()
ticker: KiteTicker | None = None

ws_dead = threading.Event()

# Metrics
tick_count = 0
ticks_per_sec = 0.0
last_tick_count = 0
last_tick_time = time.time()
metrics_lock = threading.Lock()
tps_window = deque(maxlen=3)

# Data locks
df_lock = threading.Lock()
tick_lock = threading.Lock()

# ================= HELPERS =================
def market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:  # Sat/Sun
        return False
    t = now.time()
    return datetime.strptime("09:15", "%H:%M").time() <= t <= datetime.strptime("15:30", "%H:%M").time()

def internet_up() -> bool:
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=2)
        return True
    except Exception:
        return False


# ================= FASTAPI =================
app = FastAPI(title="Sector Flow Scanner")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= KITE =================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ================= SECTORS =================
SECTOR_DEFINITIONS = {
    "METAL": [
        "ADANIENT", "HINDALCO", "JSWSTEEL", "HINDZINC", "APLAPOLLO",
        "TATASTEEL", "JINDALSTEL", "VEDL", "SAIL", "NATIONALUM", "NMDC"
    ],
    "PSUS": [
        "BANKINDIA", "PNB", "INDIANB", "SBIN", "UNIONBANK",
        "BANKBARODA", "CANBK"
    ],
    "REALTY": [
        "PHOENIXLTD", "GODREJPROP", "LODHA", "OBEROIRLTY",
        "DLF", "PRESTIGE", "NBCC", "NCC"
    ],
    "ENERGY": [
        "CGPOWER", "RELIANCE", "GMRAIRPORT", "JSWENERGY", "ONGC",
        "POWERGRID", "BLUESTARCO", "COALINDIA", "SUZLON", "IREDA",
        "IOC", "IGL", "TATAPOWER", "INOXWIND", "MAZDOCK", "PETRONET",
        "SOLARINDS", "ADANIGREEN", "NTPC", "OIL", "BDL", "BPCL",
        "NHPC", "POWERINDIA", "ADANIENSOL", "HINDPETRO", "TORNTPOWER"
    ],
    "AUTO": [
        "BOSCHLTD", "TIINDIA", "HEROMOTOCO", "M&M", "EICHERMOT",
        "EXIDEIND", "BAJAJ-AUTO", "ASHOKLEY", "MARUTI", "TITAGARH",
        "MOTHERSON", "SONACOMS", "UNOMINDA", "TMPV", "BHARATFORG"
    ],
    "IT": [
        "KAYNES", "TATATECH", "LTIM", "CYIENT", "MPHASIS",
        "TCS", "CAMS", "OFSS", "HFCL", "TECHM",
        "TATAELXSI", "HCLTECH", "WIPRO", "KPITTECH",
        "COFORGE", "PERSISTENT", "INFY"
    ],
    "PHARMA": [
        "CIPLA", "ALKEM", "BIOCON", "DRREDDY", "MANKIND",
        "TORNTPHARM", "ZYDUSLIFE", "DIVISLAB", "LUPIN",
        "PPLPHARMA", "LAURUSLABS", "FORTIS", "AUROPHARMA",
        "GLENMARK", "SUNPHARMA"
    ],
    "FMCG": [
        "ETERNAL", "MARICO", "NYKAA", "NESTLEIND", "VBL",
        "COLPAL", "HINDUNILVR", "PATANJALI", "DMART",
        "DABUR", "GODREJCP", "BRITANNIA", "UNITDSPR",
        "ITC", "TATACONSUM", "KALYANKJIL", "SUPREMEIND"
    ],
    "CEMENT": [
        "SHREECEM", "DALBHARAT", "AMBUJACEM", "ULTRACEMCO"
    ],
    "FINANCE": [
        "PNBHOUSING", "BAJAJFINSV", "ICICIPRULI", "NUVAMA", "HDFCLIFE", "SAMMAANCAP",
        "ANGELONE", "RECLTD", "BAJFINANCE", "BSE", "MAXHEALTH",
        "ICICIGI", "HUDCO", "CHOLAFIN", "PFC", "HDFCAMC", "MUTHOOTFIN",
        "PAYTM", "JIOFIN", "SHRIRAMFIN", "SBICARD", "POLICYBZR",
        "SBILIFE", "LICHSGFIN", "LICI", "MANAPPURAM", "IRFC", "IIFL", "CDSL"
    ],
    "BANK": [
        "IDFCFIRSTB", "FEDERALBNK", "INDUSINDBK",
        "HDFCBANK", "SBIN", "KOTAKBANK", "AUBANK",
        "CANBK", "BANDHANBNK", "RBLBANK",
        "ICICIBANK", "AXISBANK"
    ],
    "NIFTY_50": [
        "ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK",
        "BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BEL","BHARTIARTL",
        "CIPLA","COALINDIA","DRREDDY","EICHERMOT","GRASIM",
        "HCLTECH","HDFCBANK","HDFCLIFE","HINDALCO","HINDUNILVR",
        "ICICIBANK","INFY","INDIGO","ITC","JIOFIN","JSWSTEEL",
        "KOTAKBANK","LT","M&M","MARUTI","MAXHEALTH","NESTLEIND",
        "NTPC","ONGC","POWERGRID","RELIANCE","SBILIFE","SHRIRAMFIN",
        "SBIN","SUNPHARMA","TCS","TATACONSUM","TATASTEEL",
        "TECHM","TITAN","TRENT","ULTRACEMCO","WIPRO",
        "TATAMOTORS","ETERNAL"
    ],
    "MIDCAP": [
        "RVNL", "MPHASIS", "HINDPETRO", "PAGEIND", "POLYCAB",
        "LUPIN", "IDFCFIRSTB", "CONCOR", "CUMMINSIND", "VOLTAS",
        "BHARATFORG", "FEDERALBNK", "INDHOTEL", "COFORGE",
        "ASHOKLEY", "PERSISTENT", "UPL", "GODREJPROP",
        "AUROPHARMA", "AUBANK", "ASTRAL", "HDFCAMC",
        "JUBLFOOD", "PIIND"
    ]
}

ALL_SYMBOLS = sorted(set(sum(SECTOR_DEFINITIONS.values(), [])))

# ================= INSTRUMENT MAP =================
inst = pd.DataFrame(kite.instruments("NSE"))
inst = inst[inst.tradingsymbol.isin(ALL_SYMBOLS)]
TOKEN_TO_SYMBOL = dict(zip(inst.instrument_token, inst.tradingsymbol))
SYMBOL_TO_TOKEN = dict(zip(inst.tradingsymbol, inst.instrument_token))

missing = sorted(set(ALL_SYMBOLS) - set(SYMBOL_TO_TOKEN.keys()))
if missing:
    print("⚠️ Missing symbols not found in instrument dump (ignored):", missing)
    # Remove missing symbols from sectors to avoid KeyError later
    for sec, syms in list(SECTOR_DEFINITIONS.items()):
        SECTOR_DEFINITIONS[sec] = [s for s in syms if s in SYMBOL_TO_TOKEN]
    ALL_SYMBOLS = sorted(set(sum(SECTOR_DEFINITIONS.values(), [])))

# ================= STORAGE =================
# Per-stock minute dataframe built from live ticks
df_1m = {
    s: pd.DataFrame(columns=["ts", "vol", "cum_vol", "cum_turnover", "vwap", "last_price", "vwap_dev"])
    for s in ALL_SYMBOLS
}

# Accurate minute bucketing using cumulative day volume (volume_traded) deltas
# minute_bucket[symbol][minute_ts] = {"vol": int, "turn": float, "last_price": float}
minute_bucket = {
    s: defaultdict(lambda: {"vol": 0, "turn": 0.0, "last_price": 0.0})
    for s in ALL_SYMBOLS
}
last_day_volume = {s: None for s in ALL_SYMBOLS}  # last seen volume_traded


# ================= BASELINE (20D AVG) =================
def load_20d_avg():
    today = datetime.now(IST).date()
    start = today - timedelta(days=BUFFER_DAYS)
    out = {}
    for s, tok in SYMBOL_TO_TOKEN.items():
        try:
            d = pd.DataFrame(kite.historical_data(tok, start, today, "day"))
            out[s] = int(d.tail(LOOKBACK_DAYS)["volume"].mean()) if not d.empty else 1
        except Exception:
            out[s] = 1
    return out

STOCK_20D_AVG = load_20d_avg()


# ================= EOD FALLBACK (minute history after close) =================
def load_eod_intraday():
    today = datetime.now(IST).date()
    fallback_day = today - timedelta(days=1)

    for s, tok in SYMBOL_TO_TOKEN.items():
        try:
            d = pd.DataFrame(kite.historical_data(tok, today, today, "minute"))
            if d.empty:
                d = pd.DataFrame(kite.historical_data(tok, fallback_day, fallback_day, "minute"))
            if d.empty:
                continue

            d["cum_vol"] = d["volume"].cumsum()
            d["cum_turnover"] = (d["volume"] * d["close"]).cumsum()
            d["vwap"] = d["cum_turnover"] / d["cum_vol"]
            d["vwap_dev"] = (d["close"] - d["vwap"]) / d["vwap"] * 100

            df_1m[s] = d.rename(
                columns={"date": "ts", "volume": "vol", "close": "last_price"}
            )[["ts", "vol", "cum_vol", "cum_turnover", "vwap", "last_price", "vwap_dev"]]
        except Exception as e:
            print("EOD load failed:", s, e)


# ================= LIVE TICKS (accurate volume deltas) =================
def _coerce_exchange_ts(tick: dict) -> datetime:
    ts = tick.get("exchange_timestamp")
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=IST)
        return ts.astimezone(IST)
    return datetime.now(IST)

def on_ticks(ws, ticks):
    global tick_count, last_tick_time

    with metrics_lock:
        tick_count += len(ticks)
        last_tick_time = time.time()

    with tick_lock:
        for t in ticks:
            s = TOKEN_TO_SYMBOL.get(t.get("instrument_token"))
            if not s:
                continue

            price = float(t.get("last_price") or 0.0)
            ex_ts = _coerce_exchange_ts(t)
            minute = ex_ts.replace(second=0, microsecond=0)

            vtt = t.get("volume_traded")  # cumulative day volume (best)
            if vtt is not None:
                vtt = int(vtt)
                prev = last_day_volume.get(s)
                delta_vol = max(vtt - prev, 0) if prev is not None else 0
                last_day_volume[s] = vtt
            else:
                # fallback (less accurate) if volume_traded isn't available
                delta_vol = int(t.get("last_traded_quantity") or 0)

            if delta_vol > 0:
                b = minute_bucket[s][minute]
                b["vol"] += delta_vol
                b["turn"] += delta_vol * price

            # keep last price for that minute
            minute_bucket[s][minute]["last_price"] = price

def on_connect(ws, resp):
    print("✅ WebSocket connected / reconnected")
    tokens = list(TOKEN_TO_SYMBOL.keys())
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)  # FULL gives best chance of volume_traded + timestamps

def on_close(ws, code, reason):
    print("❌ WebSocket closed:", code, reason)
    ws_dead.set()

def on_error(ws, code, reason):
    print("⚠️ WS error:", code, reason)
    ws_dead.set()


# ================= DAILY RESET =================
def reset_intraday_state():
    global tick_count, ticks_per_sec, last_tick_count, ticker, LIVE_STARTED, last_tick_time

    print("🧹 DAILY RESET @ 09:00 — clearing intraday state")

    try:
        if ticker:
            ticker.close()
            print("🔌 WebSocket closed for clean session")
    except Exception:
        pass

    with live_lock:
        LIVE_STARTED = False

    with df_lock:
        for s in df_1m:
            df_1m[s].drop(df_1m[s].index, inplace=True)

    with tick_lock:
        for s in minute_bucket:
            minute_bucket[s].clear()
        for s in last_day_volume:
            last_day_volume[s] = None

    with metrics_lock:
        tick_count = 0
        ticks_per_sec = 0.0
        last_tick_count = 0
        last_tick_time = time.time()


def eod_reset_watcher():
    last_reset_date = None
    while True:
        now = datetime.now(IST)
        if now.time() >= datetime.strptime("09:00", "%H:%M").time():
            if last_reset_date != now.date():
                reset_intraday_state()
                last_reset_date = now.date()
        time.sleep(5)


# ================= LIVE MODE CONTROL =================
def start_live_mode():
    global LIVE_STARTED, ticker

    with live_lock:
        if LIVE_STARTED:
            return

        print("🚀 Starting LIVE mode")
        ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
        ticker.on_ticks = on_ticks
        ticker.on_connect = on_connect
        ticker.on_close = on_close
        ticker.on_error = on_error

        ticker.connect(threaded=True)
        LIVE_STARTED = True


def ws_reviver():
    print("🧠 ws_reviver thread started")
    global LIVE_STARTED

    while True:
        ws_dead.wait()

        print("⏳ Waiting for internet to return...")
        while not internet_up():
            time.sleep(3)

        print("🌐 Internet back — restarting WebSocket")
        time.sleep(2)

        with live_lock:
            LIVE_STARTED = False

        start_live_mode()
        ws_dead.clear()


def market_watcher():
    was_open = False
    while True:
        now_open = market_open()
        if now_open and not was_open:
            start_live_mode()
        was_open = now_open
        time.sleep(1)


def ws_watchdog():
    """Restart WS if no ticks are seen for a while during market hours."""
    global LIVE_STARTED, ticker
    while True:
        time.sleep(30)
        if not market_open():
            continue
        with metrics_lock:
            idle = (time.time() - last_tick_time)
        if idle > 45:  # no ticks for 45s while market open
            print(f"🔄 No ticks for {int(idle)}s — forcing WS restart")
            try:
                if ticker:
                    ticker.close()
            except Exception:
                pass
            with live_lock:
                LIVE_STARTED = False
            start_live_mode()


# ================= 1-MIN AGGREGATION (flush completed minutes) =================
def aggregate_1min():
    while True:
        now = datetime.now(IST)
        cutoff = now.replace(second=0, microsecond=0)  # current minute start

        # Determine minutes to flush without holding locks too long
        with tick_lock:
            to_flush = {s: [m for m in minute_bucket[s].keys() if m < cutoff] for s in ALL_SYMBOLS}

        did_any = False
        with df_lock:
            for s, mins in to_flush.items():
                if not mins:
                    continue

                for m in sorted(mins):
                    with tick_lock:
                        b = minute_bucket[s].pop(m, None)

                    if not b:
                        continue

                    vol = int(b["vol"])
                    if vol <= 0:
                        continue

                    turn = float(b["turn"])
                    price = float(b["last_price"] or 0.0)

                    d = df_1m[s]
                    cum_vol = vol if d.empty else int(d.iloc[-1]["cum_vol"]) + vol
                    cum_turn = turn if d.empty else float(d.iloc[-1]["cum_turnover"]) + turn
                    vwap = (cum_turn / cum_vol) if cum_vol else 0.0
                    dev = ((price - vwap) / vwap * 100.0) if vwap else 0.0

                    df_1m[s] = pd.concat(
                        [d, pd.DataFrame([{
                            "ts": m,
                            "vol": vol,
                            "cum_vol": cum_vol,
                            "cum_turnover": cum_turn,
                            "vwap": round(vwap, 2),
                            "last_price": price,
                            "vwap_dev": round(dev, 2),
                        }])],
                        ignore_index=True
                    )
                    did_any = True

        time.sleep(0.25 if did_any else 0.6)


# ================= TPS =================
def tick_rate():
    global ticks_per_sec, last_tick_count
    while True:
        time.sleep(1)
        with metrics_lock:
            delta = tick_count - last_tick_count
            last_tick_count = tick_count
        tps_window.append(delta)
        ticks_per_sec = round(sum(tps_window) / len(tps_window), 1) if tps_window else 0.0


# ================= API =================
@app.get("/status")
def status():
    return {"progress": 100, "message": "LIVE" if market_open() else "Market Closed (EOD)"}

@app.get("/metrics")
def metrics():
    with metrics_lock:
        return {"total_ticks": tick_count, "ticks_per_sec": ticks_per_sec}

@app.get("/live")
def live():
    now = datetime.now(IST)

    # PREOPEN PROTECTION
    if now.time() < datetime.strptime("09:00", "%H:%M").time():
        return {"mode": "PREOPEN", "started_at": SCRIPT_START.strftime("%H:%M:%S"), "data": {}}

    data = {}
    with df_lock:
        for sector, syms in SECTOR_DEFINITIONS.items():
            sl = 0           # sector live volume
            sa = 0           # sector avg (20D) volume sum
            vw_w = 0.0       # for volume-weighted sector VWAP dev
            ab = 0           # breadth count (vwap_dev > 0)
            mom_w = 0.0      # volume-weighted VWAP slope (%/min)
            rows = []

            for s in syms:
                d = df_1m[s]

                lv = int(d.iloc[-1]["cum_vol"]) if not d.empty else 0
                av = int(STOCK_20D_AVG.get(s, 1))
                vd = float(d.iloc[-1]["vwap_dev"]) if not d.empty else 0.0

                # VWAP momentum (5-min) as %/min
                # slope_pct_5 = ((vwap_now - vwap_5min_ago)/vwap_5min_ago) * 100 / 5
                vwap_slope_5_pct = 0.0
                if len(d) >= 6:
                    vwap_now = float(d.iloc[-1]["vwap"])
                    vwap_5 = float(d.iloc[-6]["vwap"])
                    if vwap_5:
                        vwap_slope_5_pct = ((vwap_now - vwap_5) / vwap_5) * (100.0 / 5.0)

                sl += lv
                sa += av
                vw_w += vd * lv
                ab += 1 if vd > 0 else 0
                mom_w += vwap_slope_5_pct * lv

                rows.append({
                    "symbol": s,
                    "live_vol": lv,
                    "avg_20d_vol": av,
                    "vwap_dev": round(vd, 2),
                    "vwap_slope_5_pct": round(vwap_slope_5_pct, 4),  # NEW
                })

            sector_vwap = (vw_w / sl) if sl else 0.0
            sector_vwap_slope_5_pct = (mom_w / sl) if sl else 0.0  # NEW

            vr = (sl / sa) if sa else 0.0
            br = ab / max(len(syms), 1)

            # Keep your original FSS; add momentum as a small extra term (optional, but useful)
            # You can set momentum weight to 0.0 if you don't want it.
            MOM_WT = 0.10
            fss = 0.5 * sector_vwap + 0.3 * vr + 0.2 * br + MOM_WT * sector_vwap_slope_5_pct
            fss = round(fss, 2)

            data[sector] = {
                "stocks": rows,
                "sector_live": int(sl),
                "sector_avg": int(sa),
                "sector_vwap": round(sector_vwap, 2),
                "sector_vwap_slope_5_pct": round(sector_vwap_slope_5_pct, 4),  # NEW
                "fss": fss
            }

    return {
        "mode": "LIVE" if market_open() else "EOD",
        "started_at": SCRIPT_START.strftime("%H:%M:%S"),
        "data": data
    }

@app.get("/")
def index():
    return FileResponse("index.html")


# ================= STARTUP =================
@app.on_event("startup")
def startup_event():
    now = datetime.now(IST)

    # Reset intraday state once after 09:00 to avoid carrying old buffers
    if now.time() >= datetime.strptime("09:00", "%H:%M").time():
        reset_intraday_state()

    # Load EOD minute history only after close
    if now.time() > datetime.strptime("15:30", "%H:%M").time():
        load_eod_intraday()

    threading.Thread(target=tick_rate, daemon=True).start()
    threading.Thread(target=aggregate_1min, daemon=True).start()
    threading.Thread(target=market_watcher, daemon=True).start()
    threading.Thread(target=ws_watchdog, daemon=True).start()
    threading.Thread(target=ws_reviver, daemon=True).start()
    threading.Thread(target=eod_reset_watcher, daemon=True).start()

    if now.time() < datetime.strptime("09:00", "%H:%M").time():
        print("⏳ PREOPEN — keeping intraday state empty")