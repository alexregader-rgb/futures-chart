import os
import asyncio
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from tinkoff.invest import AsyncClient, CandleInterval

TOKEN = os.environ.get("TINKOFF_TOKEN")
app = FastAPI()

# Отдаём главную страницу
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

# Список всех фьючерсов
@app.get("/futures")
async def get_futures():
    async with AsyncClient(TOKEN) as client:
        resp = await client.instruments.futures()
        result = []
        for f in resp.instruments:
            if f.for_iis:
                continue
            result.append({
                "ticker": f.ticker,
                "figi": f.figi,
                "name": f.name,
            })
        # Сортируем по тикеру
        result.sort(key=lambda x: x["ticker"])
        return result

# История свечей
@app.get("/history/{figi}")
async def get_history(figi: str, interval: str = "5m"):
    interval_map = {
        "1m": CandleInterval.CANDLE_INTERVAL_1_MIN,
        "5m": CandleInterval.CANDLE_INTERVAL_5_MIN,
        "15m": CandleInterval.CANDLE_INTERVAL_15_MIN,
        "1h": CandleInterval.CANDLE_INTERVAL_HOUR,
        "1d": CandleInterval.CANDLE_INTERVAL_DAY,
    }
    async with AsyncClient(TOKEN) as client:
        now = datetime.now(timezone.utc)
        resp = await client.market_data.get_candles(
            figi=figi,
            from_=now - timedelta(days=7),
            to=now,
            interval=interval_map.get(interval, CandleInterval.CANDLE_INTERVAL_5_MIN),
        )
    candles = []
    for c in resp.candles:
        candles.append({
            "time": int(c.time.timestamp()),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": c.volume,
        })
    return {"candles": candles}

# WebSocket для стрима котировок
@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    current_figi = None

    async with AsyncClient(TOKEN) as client:
        async with client.create_market_data_stream() as stream:
            # Читаем сообщения от клиента и от API параллельно
            async def read_client():
                nonlocal current_figi
                while True:
                    data = await ws.receive_json()
                    if data.get("action") == "subscribe":
                        current_figi = data["figi"]
                        await stream.subscribe_candles(
                            current_figi,
                            CandleInterval.CANDLE_INTERVAL_1_MIN,
                        )

            client_task = asyncio.create_task(read_client())

            try:
                async for candle in stream.candles():
                    if candle.figi == current_figi:
                        await ws.send_json({
                            "time": int(candle.time.timestamp()),
                            "open": float(candle.open),
                            "high": float(candle.high),
                            "low": float(candle.low),
                            "close": float(candle.close),
                            "volume": candle.volume,
                            "is_complete": candle.is_complete,
                        })
            finally:
                client_task.cancel()
