import time
import threading
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kiteconnect import KiteConnect, KiteTicker
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn

LIVE_STARTED = False
live_lock = threading.Lock()
ticker = None
import socket
ws_dead = threading.Event()



# ================= CONFIG =================
import os

API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("KITE_API_KEY or KITE_ACCESS_TOKEN not set")


IST = ZoneInfo("Asia/Kolkata")
LOOKBACK_DAYS = 20
BUFFER_DAYS = 40

# ================= HELPERS =================
def market_open():
    t = datetime.now(IST).time()
    return t >= datetime.strptime("09:15", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time()

# ================= FASTAPI =================
app = FastAPI(title="Sector Flow Scanner")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
        "SBILIFE", "LICHSGFIN", "LICI", "MANAPPURAM", 'IRFC', "IIFL", "CDSL"
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

# ================= STORAGE =================
df_1m = {s: pd.DataFrame(columns=[
    "ts","vol","cum_vol","cum_turnover","vwap","last_price","vwap_dev"
]) for s in ALL_SYMBOLS}

tick_buffer = {s: [] for s in ALL_SYMBOLS}
df_lock = threading.Lock()

tick_count = 0
ticks_per_sec = 0
last_tick_count = 0
metrics_lock = threading.Lock()

SCRIPT_START = datetime.now(IST)

# ================= BASELINE =================
def load_20d_avg():
    today = datetime.now(IST).date()
    start = today - timedelta(days=BUFFER_DAYS)
    out = {}
    for s, t in SYMBOL_TO_TOKEN.items():
        try:
            d = pd.DataFrame(kite.historical_data(t, start, today, "day"))
            out[s] = int(d.tail(LOOKBACK_DAYS)["volume"].mean())
        except:
            out[s] = 1
    return out

STOCK_20D_AVG = load_20d_avg()

# ================= EOD FALLBACK =================
def load_eod_intraday():
    today = datetime.now(IST).date()
    fallback_day = today - timedelta(days=1)

    for s, t in SYMBOL_TO_TOKEN.items():
        try:
            d = pd.DataFrame(kite.historical_data(t, today, today, "minute"))

            if d.empty:
                d = pd.DataFrame(kite.historical_data(t, fallback_day, fallback_day, "minute"))

            if d.empty:
                continue

            d["cum_vol"] = d["volume"].cumsum()
            d["cum_turnover"] = (d["volume"] * d["close"]).cumsum()
            d["vwap"] = d["cum_turnover"] / d["cum_vol"]
            d["vwap_dev"] = (d["close"] - d["vwap"]) / d["vwap"] * 100

            df_1m[s] = d.rename(columns={
                "date":"ts",
                "volume":"vol",
                "close":"last_price"
            })[["ts","vol","cum_vol","cum_turnover","vwap","last_price","vwap_dev"]]

        except Exception as e:
            print("EOD load failed:", s, e)


# ================= LIVE TICKS =================
def on_ticks(ws, ticks):
    global tick_count
    with metrics_lock:
        tick_count += len(ticks)

    for t in ticks:
        s = TOKEN_TO_SYMBOL.get(t["instrument_token"])
        if s:
            tick_buffer[s].append({
                "ltq": t.get("last_traded_quantity", 0),
                "price": t["last_price"]
            })


def on_connect(ws, resp):
    print("✅ WebSocket connected / reconnected")
    ws.subscribe(list(TOKEN_TO_SYMBOL.keys()))
    ws.set_mode(ws.MODE_QUOTE, list(TOKEN_TO_SYMBOL.keys()))

def on_close(ws, code, reason):
    if not ws_dead.is_set():
        print("❌ WebSocket closed:", code, reason)
        ws_dead.set()


def reset_intraday_state():
    global df_1m, tick_buffer, tick_count, ticks_per_sec, last_tick_count, ticker, LIVE_STARTED

    print("🧹 DAILY RESET @ 09:00 — clearing intraday state")

    try:
        if ticker:
            ticker.close()
            print("🔌 WebSocket closed for clean session")
    except:
        pass

    with live_lock:
        LIVE_STARTED = False

    with df_lock:
        df_1m = {s: pd.DataFrame(columns=[
            "ts","vol","cum_vol","cum_turnover","vwap","last_price","vwap_dev"
        ]) for s in ALL_SYMBOLS}
        tick_buffer = {s: [] for s in ALL_SYMBOLS}

    with metrics_lock:
        tick_count = 0
        ticks_per_sec = 0
        last_tick_count = 0



def eod_reset_watcher():
    last_reset_date = None

    while True:
        now = datetime.now(IST)

        if now.time() >= datetime.strptime("9:00", "%H:%M").time():
            if last_reset_date != now.date():
                reset_intraday_state()
                last_reset_date = now.date()

        time.sleep(5)


def internet_up():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=2)
        return True
    except:
        return False


def start_live_mode():
    global LIVE_STARTED, ticker

    with live_lock:
        if LIVE_STARTED:
            return

        print("🚀 Starting LIVE mode")

        # NO reconnect args at all
        ticker = KiteTicker(API_KEY, ACCESS_TOKEN)

        ticker.on_ticks = on_ticks
        ticker.on_connect = on_connect
        ticker.on_close = on_close
        ticker.on_error = lambda ws, code, reason: print(
            "⚠️ WS error, waiting for reconnect...", code, reason
        )

        ticker.connect(threaded=True)
        LIVE_STARTED = True


def ws_reviver():
    print("🧠 ws_reviver thread started")
    global LIVE_STARTED

    while True:
        ws_dead.wait()  # wait until WS dies

        print("⏳ Waiting for internet to return...")
        while not internet_up():
            time.sleep(3)

        print("🌐 Internet back — restarting WebSocket")
        time.sleep(2)

        with live_lock:
            LIVE_STARTED = False  # allow restart

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
    global LIVE_STARTED, ticker
    while True:
        time.sleep(60)
        if ticks_per_sec == 0 and market_open():
            print("🔄 TPS=0 detected — forcing WS restart")
            try:
                ticker.close()
            except:
                pass
            with live_lock:
                LIVE_STARTED = False
            start_live_mode()


def aggregate_1min():
    last_min = None
    while True:
        now = datetime.now(IST)

        # wait until second == 0 (minute close)
        if now.second != 0:
            time.sleep(0.2)
            continue

        current_min = now.replace(second=0, microsecond=0)

        if last_min == current_min:
            time.sleep(0.2)
            continue

        with df_lock:
            for s, ticks in tick_buffer.items():
                if not ticks:
                    continue

                vol = sum(x["ltq"] for x in ticks)
                turn = sum(x["ltq"] * x["price"] for x in ticks)
                price = ticks[-1]["price"]
                tick_buffer[s] = []

                d = df_1m[s]
                cum_vol = vol if d.empty else d.iloc[-1]["cum_vol"] + vol
                cum_turn = turn if d.empty else d.iloc[-1]["cum_turnover"] + turn
                vwap = cum_turn / cum_vol if cum_vol else 0
                dev = (price - vwap) / vwap * 100 if vwap else 0

                df_1m[s] = pd.concat([d, pd.DataFrame([{
                    "ts": current_min,
                    "vol": vol,
                    "cum_vol": cum_vol,
                    "cum_turnover": cum_turn,
                    "vwap": round(vwap,2),
                    "last_price": price,
                    "vwap_dev": round(dev,2)
                }])], ignore_index=True)

        last_min = current_min
        time.sleep(0.5)


from collections import deque

tps_window = deque(maxlen=3)

def tick_rate():
    global ticks_per_sec, last_tick_count
    while True:
        time.sleep(1)
        with metrics_lock:
            delta = tick_count - last_tick_count
            last_tick_count = tick_count
        tps_window.append(delta)
        ticks_per_sec = round(sum(tps_window) / len(tps_window), 1)


# ================= API =================
@app.get("/status")
def status():
    return {
        "progress": 100,
        "message": "LIVE" if market_open() else "Market Closed (EOD)"
    }

@app.get("/metrics")
def metrics():
    with metrics_lock:
        return {"total_ticks": tick_count, "ticks_per_sec": ticks_per_sec}

@app.get("/live")
def live():
    data = {}
    with df_lock:
        for sector, syms in SECTOR_DEFINITIONS.items():
            sl = sa = vw = ab = 0
            rows = []
            for s in syms:
                d = df_1m[s]
                lv = int(d.iloc[-1]["cum_vol"]) if not d.empty else 0
                av = STOCK_20D_AVG.get(s,1)
                vd = float(d.iloc[-1]["vwap_dev"]) if not d.empty else 0
                sl += lv; sa += av; vw += vd * lv; ab += vd > 0
                rows.append({"symbol":s,"live_vol":lv,"avg_20d_vol":av,"vwap_dev":round(vd,2)})

            sv = vw / sl if sl else 0
            vr = sl / sa if sa else 0
            br = ab / max(len(syms),1)
            fss = round(0.5*sv + 0.3*vr + 0.2*br, 2)

            data[sector] = {
                "stocks": rows,
                "sector_live": sl,
                "sector_avg": sa,
                "sector_vwap": round(sv,2),
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

@app.on_event("startup")
def startup_event():
    now = datetime.now(IST)
    if now.time() >= datetime.strptime("09:00", "%H:%M").time():
        reset_intraday_state()

    if not market_open():
        load_eod_intraday()

    threading.Thread(target=tick_rate, daemon=True).start()
    threading.Thread(target=aggregate_1min, daemon=True).start()
    threading.Thread(target=market_watcher, daemon=True).start()
    threading.Thread(target=ws_watchdog, daemon=True).start()
    threading.Thread(target=ws_reviver, daemon=True).start()
    threading.Thread(target=eod_reset_watcher, daemon=True).start()







