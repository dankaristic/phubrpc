"""Fetch one canonical video or its supported download tiers."""

import asyncio
import json
import math
import os
import re
import signal
import sys
from pathlib import Path

from . import worker_pb2 as pb

URL = re.compile(
    r"https://www\.pornhub\.com/view_video\.php\?viewkey=[A-Za-z0-9_-]{1,32}"
)
TIERS = {480: "480p", 720: "720p", 1080: "1080p", 2160: "4K"}
MAX_BYTES = 2 * 1024**3


class MediaError(Exception):
    pass


def workspace_size(folder):
    size = 0
    for path in folder.iterdir():
        try:
            size += path.stat().st_size
        except FileNotFoundError:
            continue  # yt-dlp renamed a fragment during the scan.
    return size


async def command(*args, workspace=None, limit=0):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        cwd=workspace,
        start_new_session=True,
    )
    output = asyncio.create_task(process.communicate())
    try:
        while not output.done():
            await asyncio.wait({output}, timeout=1)
            if workspace and workspace_size(workspace) > limit:
                raise MediaError("Temporary storage budget exceeded")
        stdout, _ = await output
        if process.returncode:
            raise MediaError(
                f"{Path(args[0]).name} exited with code {process.returncode}"
            )
        return stdout
    finally:
        if process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        await output


async def download_command(args, workspace, limit, progress):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        cwd=workspace, start_new_session=True,
    )
    try:
        while line := await process.stdout.readline():
            if workspace_size(workspace) > limit:
                raise MediaError("Temporary storage budget exceeded")
            if progress and line.startswith(b"BYOC:"):
                try:
                    _, current, total, estimated, speed, eta = line.decode().strip().split(":", 5)
                    downloaded = int(current)
                    size = next((int(value) for value in (total, estimated)
                                 if value.isdecimal() and int(value) >= downloaded), 0)
                    await progress(downloaded, size,
                                   int(float(speed)) if speed not in {"NA", "None"} else 0,
                                   int(float(eta)) if eta not in {"NA", "None"} else 0)
                except (ValueError, UnicodeDecodeError):
                    continue
        if await process.wait():
            raise MediaError(f"yt-dlp exited with code {process.returncode}")
    finally:
        if process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()


def ytdlp(*args):
    return (
        sys.executable,
        "-m",
        "yt_dlp",
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--no-progress",
        "--socket-timeout",
        "12",
        *args,
    )


async def metadata(url):
    info = json.loads(await command(*ytdlp("--dump-single-json", "--", url)))
    heights = set()
    for fmt in info.get("formats") or []:
        if fmt.get("vcodec") == "none" or fmt.get("ext") != "mp4":
            continue
        dimensions = [v for v in (fmt.get("height"), fmt.get("width")) if v]
        if dimensions:
            heights.add(min(dimensions))
    tiers = [tier for height, tier in TIERS.items() if height in heights]
    if not tiers:
        raise MediaError("No compatible MP4 quality available")
    return pb.Metadata(
        title=str(info.get("title") or "")[:256],
        uploader=str(info.get("uploader") or "")[:128],
        duration_seconds=int(info.get("duration") or 0),
        qualities=tiers,
    )


async def download(job, folder, progress=None):
    height = next(h for h, name in TIERS.items() if name == job.quality)
    limit = min(job.max_bytes, MAX_BYTES)
    if limit <= 0:
        raise MediaError("Invalid byte limit")
    sizes = [
        f"[height={height}][width>=?{height}]",
        f"[width={height}][height>=?{height}]",
    ]
    choices = [
        f"{kind}[ext=mp4]{size}{audio}"
        for kind, audio in (("b", ""), ("bv", "+ba[ext=m4a]"), ("bv", ""))
        for size in sizes
    ]
    await download_command(
        ytdlp(
            "--quiet",
            "--progress",
            "--newline",
            "--progress-delta", "2",
            "--progress-template",
            "download:BYOC:%(progress.downloaded_bytes)s:%(progress.total_bytes)s:%(progress.total_bytes_estimate)s:%(progress.speed)s:%(progress.eta)s",
            "--max-filesize",
            str(limit),
            "-f",
            "/".join(choices),
            "--merge-output-format",
            "mp4",
            "-o",
            "video.%(ext)s",
            "--",
            job.url,
        ),
        workspace=folder,
        limit=3 * limit + 16 * 1024**2,
        progress=progress,
    )
    path = folder / "video.mp4"
    if not path.is_file() or not 0 < path.stat().st_size <= limit:
        raise MediaError("Missing or oversized download")
    raw = await command(
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height:format=duration",
        "-of",
        "json",
        str(path),
    )
    probe = json.loads(raw)
    video = probe["streams"][0]
    w, h = video["width"], video["height"]
    duration = float(probe["format"]["duration"])
    if min(w, h) != height or not math.isfinite(duration) or not 0 < duration <= 86400:
        raise MediaError("Downloaded video does not match the assignment")
    return path, w, h, duration
