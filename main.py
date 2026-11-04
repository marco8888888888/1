from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── In-memory state ──────────────────────────────────────────────────────────

devices: dict[str, dict[str, Any]] = {}
dashboard_ws: set[WebSocket] = set()
device_ws: dict[str, WebSocket] = {}

default_rules: dict[str, Any] = {
    "daily_limit_minutes": 60,
    "quiet_start": "20:00",
    "quiet_end": "07:00",
    "bonus_minutes": 0,
    "paused_until": 0,
    "learn_mode": False,
    "night_mode": False,
    "app_rules": {},
}


def get_device(device_id: str) -> dict[str, Any]:
    if device_id not in devices:
        devices[device_id] = {
            "id": device_id,
            "name": device_id,
            "online": False,
            "last_seen": 0,
            "battery": -1,
            "charging": False,
            "wifi": "",
            "screen_on": False,
            "current_app": "",
            "location": None,
            "installed_apps": [],
            "usage_stats": [],
            "notifications": [],
            "screenshot": None,
            "photo": None,
            "rules": dict(default_rules),
        }
    return devices[device_id]


# ── Broadcast helpers ────────────────────────────────────────────────────────

async def broadcast_to_dashboards(msg: dict):
    payload = json.dumps(msg)
    dead: list[WebSocket] = []
    for ws in dashboard_ws:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        dashboard_ws.discard(ws)


async def send_to_device(device_id: str, msg: dict) -> bool:
    ws = device_ws.get(device_id)
    if ws is None:
        return False
    try:
        await ws.send_text(json.dumps(msg))
        return True
    except Exception:
        device_ws.pop(device_id, None)
        return False


# ── Heartbeat ────────────────────────────────────────────────────────────────

async def heartbeat_loop():
    while True:
        await asyncio.sleep(15)
        now = time.time()
        changed = False
        for d in devices.values():
            if d["online"] and now - d["last_seen"] > 30:
                d["online"] = False
                changed = True
        if changed:
            await broadcast_to_dashboards({"type": "devices", "devices": list(devices.values())})


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(heartbeat_loop())
    yield
    task.cancel()


# ── App ──────────────────────────────────────────────────────────────────────

STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


# ── Device WebSocket ─────────────────────────────────────────────────────────

@app.websocket("/ws/device/{device_id}")
async def ws_device(ws: WebSocket, device_id: str):
    await ws.accept()
    dev = get_device(device_id)
    dev["online"] = True
    dev["last_seen"] = time.time()
    device_ws[device_id] = ws

    await broadcast_to_dashboards({"type": "devices", "devices": list(devices.values())})
    await ws.send_text(json.dumps({"type": "rules", "rules": dev["rules"]}))

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            dev["last_seen"] = time.time()
            dev["online"] = True
            kind = msg.get("type")

            if kind == "status":
                for k in ("battery", "charging", "wifi", "screen_on", "current_app"):
                    if k in msg:
                        dev[k] = msg[k]
                await broadcast_to_dashboards({"type": "device_status", "device_id": device_id, **{k: dev[k] for k in ("battery", "charging", "wifi", "screen_on", "current_app", "online")}})

            elif kind == "location":
                dev["location"] = {"lat": msg["lat"], "lng": msg["lng"], "ts": time.time()}
                await broadcast_to_dashboards({"type": "location", "device_id": device_id, "location": dev["location"]})

            elif kind == "apps":
                dev["installed_apps"] = msg.get("apps", [])
                await broadcast_to_dashboards({"type": "apps", "device_id": device_id, "apps": dev["installed_apps"]})

            elif kind == "message_ack":
                await broadcast_to_dashboards({"type": "message_ack", "device_id": device_id, "text": msg.get("text", "")})

            elif kind == "usage_stats":
                dev["usage_stats"] = msg.get("stats", [])
                await broadcast_to_dashboards({"type": "usage_stats", "device_id": device_id, "stats": dev["usage_stats"]})

            elif kind == "notification":
                notif = {"app": msg.get("app", ""), "title": msg.get("title", ""), "text": msg.get("text", ""), "ts": time.time()}
                dev["notifications"] = (dev["notifications"] + [notif])[-50:]
                await broadcast_to_dashboards({"type": "notification", "device_id": device_id, "notification": notif})

            elif kind == "screenshot":
                dev["screenshot"] = msg.get("data", "")
                await broadcast_to_dashboards({"type": "screenshot", "device_id": device_id, "data": dev["screenshot"]})

            elif kind == "photo":
                dev["photo"] = msg.get("data", "")
                await broadcast_to_dashboards({"type": "photo", "device_id": device_id, "data": dev["photo"]})

            elif kind == "heartbeat":
                pass

    except WebSocketDisconnect:
        pass
    finally:
        dev["online"] = False
        device_ws.pop(device_id, None)
        await broadcast_to_dashboards({"type": "devices", "devices": list(devices.values())})


# ── Dashboard WebSocket ──────────────────────────────────────────────────────

@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    await ws.accept()
    dashboard_ws.add(ws)

    await ws.send_text(json.dumps({"type": "devices", "devices": list(devices.values())}))

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            kind = msg.get("type")
            device_id = msg.get("device_id", "")
            dev = get_device(device_id) if device_id else None

            if kind == "send_message" and dev:
                await send_to_device(device_id, {"type": "show_message", "text": msg.get("text", "")})

            elif kind == "lock" and dev:
                minutes = msg.get("minutes", 0)
                if minutes > 0:
                    dev["rules"]["paused_until"] = time.time() + minutes * 60
                else:
                    dev["rules"]["paused_until"] = time.time() + 999999
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "unlock" and dev:
                dev["rules"]["paused_until"] = 0
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "bonus_time" and dev:
                dev["rules"]["bonus_minutes"] = dev["rules"].get("bonus_minutes", 0) + msg.get("minutes", 15)
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "set_daily_limit" and dev:
                dev["rules"]["daily_limit_minutes"] = msg.get("minutes", 60)
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "set_quiet_hours" and dev:
                dev["rules"]["quiet_start"] = msg.get("start", "20:00")
                dev["rules"]["quiet_end"] = msg.get("end", "07:00")
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "set_app_rule" and dev:
                pkg = msg.get("package", "")
                rule = msg.get("rule", "limited")
                if pkg:
                    dev["rules"]["app_rules"][pkg] = rule
                    await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "set_learn_mode" and dev:
                dev["rules"]["learn_mode"] = msg.get("enabled", False)
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "set_night_mode" and dev:
                dev["rules"]["night_mode"] = msg.get("enabled", False)
                await send_to_device(device_id, {"type": "rules", "rules": dev["rules"]})

            elif kind == "request_location" and dev:
                await send_to_device(device_id, {"type": "get_location"})

            elif kind == "request_apps" and dev:
                await send_to_device(device_id, {"type": "get_apps"})

            elif kind == "ring_device" and dev:
                await send_to_device(device_id, {"type": "ring"})

            elif kind == "request_screenshot" and dev:
                await send_to_device(device_id, {"type": "take_screenshot"})

            elif kind == "request_photo" and dev:
                camera = msg.get("camera", "back")
                await send_to_device(device_id, {"type": "take_photo", "camera": camera})

            elif kind == "request_usage_stats" and dev:
                await send_to_device(device_id, {"type": "get_usage_stats"})

            if dev:
                await broadcast_to_dashboards({"type": "devices", "devices": list(devices.values())})

    except WebSocketDisconnect:
        pass
    finally:
        dashboard_ws.discard(ws)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
