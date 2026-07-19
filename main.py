"""DYGUESS 2.0 抖音云中转服务"""
import asyncio, json, logging, os
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logger = logging.getLogger("relay")

WS_TOKEN = os.environ.get("WS_TOKEN", "dyguess2024")
app = FastAPI(title="DYGUESS 2.0 Relay")
ws_clients: set[WebSocket] = set()


def detect_msg_type(data: dict) -> str:
    """根据字段特征判断消息类型"""
    if data.get("content"):
        return "chat"
    if data.get("gift_num") is not None:
        return "gift"
    if data.get("like_num") is not None:
        return "like"
    if data.get("enter_room_type") == 1:
        return "enter"
    if data.get("enter_room_type") == 2:
        return "leave"
    return "unknown"


async def broadcast(message):
    dead = set()
    payload = json.dumps(message, ensure_ascii=False)
    for ws in ws_clients:
        try: await ws.send_text(payload)
        except: dead.add(ws)
    if dead:
        ws_clients.difference_update(dead)
        logger.info(f"清理 {len(dead)} 个断开的 WS 连接，剩余 {len(ws_clients)}")


@app.get("/health")
async def health():
    return {"status": "ok", "clients": len(ws_clients)}


@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    try: raw = json.loads(body)
    except: raise HTTPException(400, detail="invalid json")
    items = raw if isinstance(raw, list) else [raw]
    forwarded = 0
    for item in items:
        msg_type = detect_msg_type(item)
        logger.info(f"回调 type={msg_type} user={item.get('nickname','?')}")
        await broadcast({"msg_type": msg_type, "data": item})
        forwarded += 1
    return {"ok": True, "forwarded": forwarded}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    logger.info(f"WS 客户端接入，当前 {len(ws_clients)}")
    try:
        while True:
            d = await ws.receive_text()
            if d.strip() == "ping": await ws.send_text("pong")
    except: pass
    finally:
        ws_clients.discard(ws)
        logger.info(f"WS 客户端断开，剩余 {len(ws_clients)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
