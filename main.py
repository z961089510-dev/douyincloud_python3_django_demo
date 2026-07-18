"""
DYGUESS 2.0 抖音云中转服务
接收抖音内网专线的互动数据回调，通过 WebSocket 转发给本地游戏 exe。

架构:
  抖音直播间 → 内网专线 → [抖音云本服务] → WebSocket → 本地exe(barrrage_client.py)
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("relay")

# ============================================================
# 配置（通过环境变量注入，更安全）
# ============================================================
CALLBACK_SECRET = os.environ.get("CALLBACK_SECRET", "change-me-in-production")
WS_TOKEN = os.environ.get("WS_TOKEN", "dyguess2024")

app = FastAPI(title="DYGUESS 2.0 Relay")

# WebSocket 客户端连接池
ws_clients: set[WebSocket] = set()


# ============================================================
# 回调数据校验
# ============================================================
def verify_callback_signature(body: bytes, signature: str) -> bool:
    """用 HMAC-SHA256 校验回调请求签名。
    抖音回调会带 X-Douyin-Signature 头，我们用 CALLBACK_SECRET 验证。
    """
    if not signature:
        return False
    expected = hmac.new(
        CALLBACK_SECRET.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ============================================================
# 广播给所有连接的 WS 客户端
# ============================================================
async def broadcast(message: dict):
    """向所有连接的本地 exe 发送消息。"""
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


# ============================================================
# HTTP 接口
# ============================================================

@app.get("/health")
async def health():
    """健康检查。抖音云会定期检查服务是否存活。"""
    return {"status": "ok", "clients": len(ws_clients)}


@app.post("/callback")
async def callback(request: Request):
    """
    接收抖音内网专线的互动数据回调。

    抖音会把弹幕/礼物/点赞/入场数据 POST 到这个地址。
    本服务收到后立即广播给所有连接的本地 exe。
    """
    # 获取签名（如果抖音回调带了的话）
    signature = request.headers.get("X-Douyin-Signature", "")
    body = await request.body()

    # 如果配置了 CALLBACK_SECRET 且回调带了签名，则校验
    if CALLBACK_SECRET != "change-me-in-production" and signature:
        if not verify_callback_signature(body, signature):
            logger.warning("回调签名校验失败")
            raise HTTPException(status_code=403, detail="invalid signature")

    # 解析数据
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"回调数据不是合法 JSON: {e}")
        raise HTTPException(status_code=400, detail="invalid json")

    msg_type = data.get("msg_type") or data.get("Type") or data.get("event")
    logger.info(
        f"收到回调: type={msg_type} "
        f"clients={len(ws_clients)} "
        f"body={body[:200]}"
    )

    # 广播给所有本地 exe
    await broadcast(data)

    return {"ok": True, "forwarded": len(ws_clients)}


@app.post("/api/push")
async def api_push(request: Request):
    """
    手动推送消息（调试用，带简单鉴权）。
    可以用来测试 WebSocket 通路是否正常。
    """
    auth = request.headers.get("Authorization", "")
    expected = f"Bearer {WS_TOKEN}"
    if auth != expected:
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await request.json()
    logger.info(f"手动推送: clients={len(ws_clients)}")
    await broadcast(body)
    return {"ok": True, "forwarded": len(ws_clients)}


# ============================================================
# WebSocket 接口（本地 exe 连这个）
# ============================================================

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    """本地 exe 通过这个 WebSocket 连接接收弹幕数据。"""
    # 可选：通过查询参数校验 token
    token = ws.query_params.get("token", "")
    if token and token != WS_TOKEN:
        await ws.close(code=4001, reason="unauthorized")
        return

    await ws.accept()
    ws_clients.add(ws)
    client_host = ws.client.host if ws.client else "unknown"
    logger.info(f"WS 客户端接入: {client_host}，当前共 {len(ws_clients)} 个连接")

    try:
        # 持续接收（保持连接，心跳由 uvicorn 自动处理）
        while True:
            # 客户端可能发来的心跳或指令
            data = await ws.receive_text()
            if data.strip() == "ping":
                await ws.send_text("pong")
            # 其他消息暂不处理
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"WS 连接异常: {e}")
    finally:
        ws_clients.discard(ws)
        logger.info(f"WS 客户端断开: {client_host}，剩余 {len(ws_clients)}")


# ============================================================
# 启动统计信息
# ============================================================

@app.on_event("startup")
async def startup():
    logger.info(
        "DYGUESS 2.0 Relay 启动成功\n"
        f"  WebSocket 端点: ws://<host>:8000/ws\n"
        f"  回调端点: POST /callback\n"
        f"  健康检查: GET /health"
    )


# ============================================================
# 直接运行（本地调试用）
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
