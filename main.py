from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import httpx, os, time, re

app = FastAPI(title="Cryptyos API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
    if os.path.isdir("static/assets"):
        app.mount("/assets", StaticFiles(directory="static/assets"), name="assets")

GECKO        = "https://api.geckoterminal.com/api/v2"
DEXSCREENER  = "https://api.dexscreener.com/latest/dex"
DEXSCREENER_V1 = "https://api.dexscreener.com"
BINANCE      = "https://api.binance.com/api/v3"
LIFI         = "https://li.quest/v1"

@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
async def root():
    if os.path.exists("static/index.html"):
        return HTMLResponse(open("static/index.html").read())
    return HTMLResponse("<h1>zaCrypto API</h1>")

CHAINS = {
    "ethereum": "eth", "bsc": "bsc", "polygon": "polygon_pos",
    "avalanche": "avax", "fantom": "ftm", "arbitrum": "arbitrum",
    "cronos": "cro", "optimism": "optimism", "base": "base", "solana": "solana",
}

# Binance interval mapping
BINANCE_TF = {"minute": "1m", "hour": "1h", "day": "1d"}

# ── TTL cache ───────────────────────────────────────────────────────────────
_cache: dict = {}

def _cache_get(key, ttl):
    e = _cache.get(key)
    return e["data"] if e and time.time() - e["ts"] < ttl else None

def _cache_set(key, data):
    _cache[key] = {"ts": time.time(), "data": data}

# ── HTTP ────────────────────────────────────────────────────────────────────
async def get(url, params=None):
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(url, params=params)
        if r.status_code == 429:
            raise HTTPException(429, "Upstream rate limit — try again shortly")
        if r.status_code != 200:
            raise HTTPException(r.status_code, r.text[:300])
        return r.json()

async def get_cached(url, params=None, ttl=60):
    key = url + str(sorted((params or {}).items()))
    hit = _cache_get(key, ttl)
    if hit is not None:
        return hit
    data = await get(url, params)
    _cache_set(key, data)
    return data

# ── Helpers ─────────────────────────────────────────────────────────────────
def _is_address(s: str) -> bool:
    """True if looks like a contract address (0x… or base58 Solana)."""
    return bool(re.match(r'^0x[0-9a-fA-F]{10,}$', s) or
                (len(s) > 30 and not re.search(r'[^1-9A-HJ-NP-Za-km-z]', s)))

def _binance_symbol(pool_address: str) -> str | None:
    """
    If pool_address is a plain ticker like 'BTCUSDT' or 'ETH' or 'BTC',
    return the Binance symbol. Returns None for contract addresses.
    """
    if _is_address(pool_address):
        return None
    s = pool_address.upper().strip()
    if s.endswith("USDT") or s.endswith("USDC") or s.endswith("BTC") or s.endswith("ETH") or s.endswith("BNB"):
        return s
    # auto-append USDT for bare tickers
    return s + "USDT"

async def _binance_klines(symbol: str, tf: str, limit: int) -> dict:
    """Fetch from Binance and normalise to GeckoTerminal OHLCV shape."""
    interval = BINANCE_TF.get(tf, "1h")
    raw = await get_cached(
        f"{BINANCE}/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
        ttl=30,  # Binance: no rate limit concern, but cache 30s anyway
    )
    # Binance: [openTime, open, high, low, close, volume, ...]
    ohlcv = [[int(c[0]/1000), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in raw]
    return {"data": {"attributes": {"ohlcv_list": ohlcv}}, "source": "binance"}

async def _gecko_klines(network: str, pool: str, tf: str, aggregate: int, limit: int) -> dict:
    """Fetch from GeckoTerminal — cached 5 min to stay under 10 req/min."""
    data = await get_cached(
        f"{GECKO}/networks/{network}/pools/{pool}/ohlcv/{tf}",
        {"aggregate": aggregate, "limit": limit, "currency": "usd"},
        ttl=300,  # 5 min cache — at most 1 hit per pool per 5 min
    )
    data["source"] = "geckoterminal"
    return data

async def _synthetic_klines(chain: str, pool: str, limit: int) -> dict:
    """
    Fallback: build synthetic candles from DexScreener pair data.
    Returns a single 'candle' representing current price as open=close.
    Useful when GeckoTerminal also rate-limits.
    """
    data = await get_cached(f"{DEXSCREENER_V1}/token-pairs/v1/{chain}/{pool}", ttl=20)
    pairs = data if isinstance(data, list) else (data.get("pairs") or [])
    if not pairs:
        raise HTTPException(404, "No pair data found for this pool")
    p = pairs[0]
    price = float(p.get("priceUsd") or 0)
    vol   = float((p.get("volume") or {}).get("h24") or 0)
    ts    = int(time.time())
    # one synthetic candle
    ohlcv = [[ts, price, price, price, price, vol]]
    return {"data": {"attributes": {"ohlcv_list": ohlcv}}, "source": "dexscreener_synthetic"}

# ── Routes ──────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def spa():
    try:
        with open("static/index.html") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Cryptyos API</h1><p><a href='/docs'>Docs</a></p>"

@app.get("/chains")
def chains():
    return CHAINS

@app.get("/chart/{chain}/{pool_address}")
async def chart(
    chain: str, pool_address: str,
    timeframe: str = Query("hour", enum=["minute", "hour", "day"]),
    aggregate: int = Query(1),
    limit: int = Query(100, le=1000),
):
    """
    OHLCV chart data. Priority:
    1. Binance (free, no key, 6000 weight/min) — for CEX tickers like BTCUSDT
    2. GeckoTerminal (10 req/min, cached 5min) — for DEX pool addresses
    3. DexScreener synthetic fallback — single candle from current price
    """
    # 1. Try Binance for non-address tickers
    binance_sym = _binance_symbol(pool_address)
    if binance_sym:
        try:
            return await _binance_klines(binance_sym, timeframe, limit)
        except HTTPException as e:
            if e.status_code not in (400, 404):
                raise  # re-raise non-symbol errors

    # 2. GeckoTerminal for DEX pool addresses
    network = CHAINS.get(chain.lower())
    if not network:
        raise HTTPException(400, f"Unsupported chain. Use: {list(CHAINS)}")
    try:
        return await _gecko_klines(network, pool_address, timeframe, aggregate, limit)
    except HTTPException as e:
        if e.status_code != 429:
            raise

    # 3. DexScreener synthetic fallback (only reached on 429)
    return await _synthetic_klines(chain, pool_address, limit)

@app.get("/trades/{chain}/{pool_address}")
async def trades(chain: str, pool_address: str):
    """Recent trades — GeckoTerminal, cached 15s."""
    network = CHAINS.get(chain.lower())
    if not network:
        raise HTTPException(400, f"Unsupported chain. Use: {list(CHAINS)}")
    return await get_cached(f"{GECKO}/networks/{network}/pools/{pool_address}/trades", ttl=15)

@app.get("/token/{chain}/{address}")
async def token_info(chain: str, address: str):
    """Token info — DexScreener v1 (300 req/min), cached 20s."""
    return await get_cached(f"{DEXSCREENER_V1}/tokens/v1/{chain}/{address}", ttl=20)

@app.get("/pairs/{chain}/{address}")
async def pairs(chain: str, address: str):
    """All pairs — DexScreener v1, cached 20s."""
    return await get_cached(f"{DEXSCREENER_V1}/token-pairs/v1/{chain}/{address}", ttl=20)

@app.get("/search")
async def search(q: str = Query(..., min_length=2)):
    """Search — DexScreener (300 req/min), cached 30s."""
    return await get_cached(f"{DEXSCREENER}/search", {"q": q}, ttl=30)

@app.get("/trending")
async def trending():
    """Trending metas — DexScreener, cached 60s."""
    return await get_cached(f"{DEXSCREENER_V1}/metas/trending/v1", ttl=60)

@app.get("/portfolio/{chain}/{wallet}")
async def portfolio(chain: str, wallet: str):
    """Wallet token pairs — DexScreener v1, cached 30s."""
    if chain.lower() not in CHAINS:
        raise HTTPException(400, f"Unsupported chain. Use: {list(CHAINS)}")
    return await get_cached(f"{DEXSCREENER_V1}/tokens/v1/{chain}/{wallet}", ttl=30)

@app.get("/swap/chains")
async def swap_chains():
    return await get_cached(f"{LIFI}/chains", ttl=600)

@app.get("/swap/tokens")
async def swap_tokens(chain: int = Query(...)):
    return await get_cached(f"{LIFI}/tokens", {"chains": chain}, ttl=300)

@app.get("/swap/quote")
async def swap_quote(
    from_chain: int = Query(...), to_chain: int = Query(...),
    from_token: str = Query(...), to_token: str = Query(...),
    from_amount: str = Query(...), from_address: str = Query(...),
):
    return await get(f"{LIFI}/quote", {
        "fromChain": from_chain, "toChain": to_chain,
        "fromToken": from_token, "toToken": to_token,
        "fromAmount": from_amount, "fromAddress": from_address,
    })
