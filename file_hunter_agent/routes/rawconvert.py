"""Raw convert route — start a camera raw to JPEG conversion via dcraw."""

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error
from file_hunter_agent.services.rawconvert import is_converting, start_convert


async def rawconvert_start(request: Request):
    """POST /rawconvert — start converting a camera raw file to JPEG."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error("Path is not within a configured location.", status=403)
    if is_converting():
        return json_error("A raw conversion is already running.")
    started = await start_convert(path)
    if not started:
        return json_error("Failed to start raw conversion.")
    return json_ok({"started": True})
