#!/bin/bash

# 抖音云上用 uvicorn 启动 FastAPI，支持 HTTP + WebSocket
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
