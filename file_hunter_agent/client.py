"""WebSocket client — outbound connection to the central File Hunter server.

Handles registration, command reception, and result streaming with
automatic reconnection.
"""

import asyncio
import json
import logging
import os
import platform

import websockets

from file_hunter_agent import config
from file_hunter_agent.services import scanner
from file_hunter_agent.services import rawconvert, transcode

logger = logging.getLogger("file_hunter_agent")

# Send queue — scanner and other modules push messages here
_send_queue: asyncio.Queue = asyncio.Queue()

# Connection state
_ws = None
_connected = False
_shutting_down = False


async def send_message(msg: dict):
    """Queue a message for sending to the server."""
    await _send_queue.put(msg)


def _build_ws_url() -> str:
    """Build WebSocket URL from config."""
    server = config.get("server_url", "")
    token = config.get("token", "")
    # Convert http(s) to ws(s)
    ws_url = server.replace("https://", "wss://").replace("http://", "ws://")
    return f"{ws_url}/ws/agent?token={token}"


async def run_client():
    """Main client loop — connects, registers, handles messages. Reconnects on failure."""
    global _ws, _connected

    # Wire up send functions and detect encoder
    scanner.set_send_fn(send_message)
    transcode.set_send_fn(send_message)
    transcode.detect_encoder()
    rawconvert.set_send_fn(send_message)
    rawconvert.detect()

    while True:
        try:
            ws_url = _build_ws_url()
            logger.info("Connecting to %s", ws_url.split("?")[0])

            async with websockets.connect(
                ws_url,
                ping_interval=30,
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                _ws = ws
                _connected = True

                # Send registration message
                await ws.send(
                    json.dumps(
                        {
                            "type": "register",
                            "token": config.get("token", ""),
                            "hostname": platform.node(),
                            "os": f"{platform.system()} {platform.release()}",
                            "httpPort": config.get("http_port", 8001),
                            "httpHost": config.get("http_host", "0.0.0.0"),
                            "locations": config.get_locations_with_status(),
                            "scanning": scanner.is_scanning(),
                            "scanPath": scanner.get_current_path(),
                            "capabilities": [
                                "tsv_tree",
                                "hash_partial_batch",
                                "stream_first_scan",
                                "quick_scan",
                                "files_copy",
                            ] + (["ffmpeg"] if transcode.is_available() else [])
                              + (["dcraw"] if rawconvert.is_available() else []),
                            "ffmpegEncoder": transcode.get_encoder_info() if transcode.is_available() else None,
                        }
                    )
                )

                # Run receive, send, and path monitor loops concurrently
                receive_task = asyncio.create_task(_receive_loop(ws))
                send_task = asyncio.create_task(_send_loop(ws))
                monitor_task = asyncio.create_task(_path_monitor())

                done, pending = await asyncio.wait(
                    [receive_task, send_task, monitor_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            ConnectionRefusedError,
            OSError,
        ) as e:
            logger.warning("WebSocket disconnected: %s", e)

        except asyncio.CancelledError:
            logger.info("WebSocket client shutting down")
            return

        finally:
            _ws = None
            _connected = False

        if _shutting_down:
            logger.info("WebSocket client shutting down")
            return
        logger.info("Reconnecting in 5 seconds...")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("WebSocket client shutting down")
            return


async def _receive_loop(ws):
    """Handle incoming commands from the server."""
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type == "registered":
                agent_id = msg.get("agentId")
                logger.info("Registered as agent #%s", agent_id)

            elif msg_type == "scan":
                path = msg.get("path", "")
                root_path = msg.get("root_path", path)
                logger.info(
                    "Received scan command for path: '%s' (root: '%s')",
                    path,
                    root_path,
                )
                if path:
                    if not config.is_path_allowed(path):
                        logger.warning(
                            "Scan rejected: path '%s' not in allowed locations",
                            path,
                        )
                        await send_message(
                            {
                                "type": "scan_error",
                                "path": path,
                                "error": "Path is not within a configured location.",
                            }
                        )
                        continue
                    logger.info("Scan requested: %s", path)
                    started = await scanner.start_scan(path, root_path)
                    if not started:
                        logger.warning("Scan not started: already running")
                        await send_message(
                            {
                                "type": "scan_error",
                                "path": path,
                                "error": "A scan is already running.",
                            }
                        )
                    else:
                        logger.info("Scan started successfully for: %s", path)
                else:
                    logger.warning("Scan command received with empty path")

            elif msg_type == "scan_cancel":
                logger.info("Scan cancel requested")
                scanner.cancel_scan()

            elif msg_type == "error":
                logger.error("Server error: %s", msg.get("error", ""))

    except websockets.exceptions.ConnectionClosed as e:
        if e.rcvd:
            logger.info(
                "Server closed connection: %s %s", e.rcvd.code, e.rcvd.reason or ""
            )
        else:
            logger.info("Server closed connection: no close frame received")


async def _path_monitor():
    """Poll location paths and send updates when availability changes."""
    last_status: dict[str, bool] = {}
    for loc in config.get_locations():
        last_status[loc["path"]] = os.path.isdir(loc["path"])

    while True:
        await asyncio.sleep(10)
        changed = False
        for loc in config.get_locations():
            current = os.path.isdir(loc["path"])
            if last_status.get(loc["path"]) != current:
                changed = True
            last_status[loc["path"]] = current

        if changed:
            await send_message(
                {
                    "type": "locations_updated",
                    "locations": config.get_locations_with_status(),
                }
            )
            logger.info("Location path availability changed, sent update")


async def _send_loop(ws):
    """Drain the send queue and forward messages to the server."""
    while True:
        msg = await _send_queue.get()
        try:
            await ws.send(json.dumps(msg))
        except (websockets.exceptions.ConnectionClosed, OSError):
            # Put message back for retry after reconnect
            await _send_queue.put(msg)
            return
