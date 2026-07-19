"""DYGUESS 2.0 抖音云中转服务"""
import asyncio, json, logging, os
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logger = logging.getLogger("relay")

WS_TOKEN = os.environ.get("WS_TOKEN", "dyguess2024")
app = FastAPI(title="DYGUESS 2.0 Relay")
ws_clients: set[WebSocket] = set()


def find_msg_type(obj, depth=0):
    """递归查找消息类型字段，兼容各种嵌套结构"""
    if depth > 5 or not isinstance(obj, dict):
        return None
    for key in ("msg_type", "event", "event_type", "type", "method", "Type", "Event", "MsgType"):
        if key in obj:
            return obj[key]
    for v in obj.values():
        if isinstance(v, dict):
            r = find_msg_type(v, depth + 1)
            if r:
                return r
    return None


def find_data(obj, depth=0):
    """递归查找真正的消息体（跳过外层包装）"""
    if depth > 5 or not isinstance(obj, dict):
        return obj
    for key in ("data", "payload", "message", "msg", "body", "Data", "Payload", "Message"):
        if key in obj and isinstance(obj[key], (dict, list)):
            return find_data(obj[key], depth + 1)
    return obj


async def broadcast(message):
    dead = set()
    payload = json.dumps(message, ensure_ascii=False)
    for ws in ws_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.add(ws)
    if dead:
        ws_clients.difference_update(dead)
        logger.info(f"清理 {len(dead)} 个断开的 WS 连接，剩余 {len(ws_clients)}")


@app.get("/health")
async def health():
    return {"status": "ok", "clients": len(ws_clients)}


@app.post("/callback")
async def callback(request: Request):
    """兼容单条对象和数组两种回调格式，自动提取嵌套消息"""
    body = await request.body()
    raw_body = body.decode("utf-8", errors="replace")
    logger.info(f"===== 收到回调 原始body: {raw_body[:500]} =====")

    try:
        raw = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"回调数据不是合法 JSON: {e}")
        raise HTTPException(400, detail="invalid json")

    items = raw if isinstance(raw, list) else [raw]
    forwarded = 0

    for item in items:
        logger.info(f"条目完整JSON: {json.dumps(item, ensure_ascii=False)[:500]}")
        msg_type = find_msg_type(item) or "unknown"
        data = find_data(item)
        if not isinstance(data, dict):
            data = item
        logger.info(f"识别: type={msg_type}, data_keys={list(data.keys())[:10]}")

        # 组装成标准化格式再转发
        normalized = {"msg_type": msg_type, "data": data}
        await broadcast(normalized)
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
            if d.strip() == "ping":
                await ws.send_text("pong")
    except Exception:
        pass
    finally:
        ws_clients.discard(ws)
        logger.info(f"WS 客户端断开，剩余 {len(ws_clients)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
