"""Pairing state, one-job execution and replay-safe reporting."""

import asyncio
import base64
import fcntl
import json
import logging
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

import grpc
from telethon.tl.types import Channel, DocumentAttributeVideo
from telethon.utils import get_peer_id

from . import media
from . import worker_pb2 as pb

LOG = logging.getLogger("phubrpc")
RETRYABLE = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.CANCELLED,
}


@contextmanager
def exclusive(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (directory / "worker.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another worker is using this state directory") from None
        yield


def save(path, value):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
        name = file.name
        try:
            file.write(json.dumps(value).encode())
            file.flush()
            os.fsync(file.fileno())
        except BaseException:
            os.unlink(name)
            raise
    os.replace(name, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


async def call(method, request, auth):
    delay = 1
    while True:
        try:
            return await method(request, metadata=auth, timeout=10)
        except grpc.aio.AioRpcError as exc:
            if exc.code() not in RETRYABLE:
                raise
            LOG.warning("Hub unavailable (%s), retrying in %ss", exc.code().name, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


async def heartbeat(stub, auth):
    while True:
        await asyncio.sleep(30)
        await call(stub.Heartbeat, pb.Empty(), auth)


async def check_channel(telegram, channel_id):
    channel = await telegram.get_entity(channel_id)
    if (
        not isinstance(channel, Channel)
        or get_peer_id(channel) != channel_id
        or not channel.broadcast
        or channel.megagroup
        or channel.username
        or channel.usernames
        or channel.noforwards
        or channel.signatures
    ):
        raise media.MediaError(
            "Ingest requires the paired private broadcast channel with saving enabled and signatures off"
        )
    if not channel.creator and not (
        channel.admin_rights and channel.admin_rights.post_messages
    ):
        raise media.MediaError("The worker bot needs permission to post in its channel")


async def execute(job, telegram, channel, workspace):
    if not media.URL.fullmatch(job.url):
        raise media.MediaError("Unsupported URL")
    if not job.quality:
        async with asyncio.timeout(25):
            return pb.Result(job_id=job.job_id, metadata=await media.metadata(job.url))
    if job.quality not in media.TIERS.values():
        raise media.MediaError("Unsupported quality")
    async with asyncio.timeout(3500):
        path, w, h, duration = await media.download(job, workspace)
        await check_channel(telegram, channel)
        message = await telegram.send_file(
            channel,
            path,
            caption=job.job_id,
            parse_mode=None,
            mime_type="video/mp4",
            attributes=[
                DocumentAttributeVideo(
                    duration=duration, w=w, h=h, supports_streaming=True
                )
            ],
            supports_streaming=True,
        )
        return pb.Result(job_id=job.job_id, message_id=message.id)


async def jobs(stub, auth, telegram, channel, directory):
    journal = directory / "pending.json"
    while True:
        if journal.exists():
            record = json.loads(journal.read_text())
            if not re.fullmatch(r"job-[a-zA-Z0-9_-]+", record["folder"]):
                raise ValueError("Invalid worker journal")
            workspace = directory / record["folder"]
            # An interruption during execution can leave an ambiguous Telegram upload.
            # Close that assignment instead of posting a second copy.
            result = (
                pb.Result.FromString(base64.b64decode(record["result"]))
                if "result" in record
                else pb.Result(job_id=record["job_id"], failed=True)
            )
        else:
            job = await call(stub.ClaimJob, pb.Empty(), auth)
            if not job.job_id:
                await asyncio.sleep(1.5)
                continue
            workspace = Path(tempfile.mkdtemp(dir=directory, prefix="job-"))
            record = {"job_id": job.job_id, "folder": workspace.name}
            save(journal, record)
            try:
                result = await execute(job, telegram, channel, workspace)
            except Exception as exc:  # noqa: BLE001 -- close this assignment before claiming another
                # Third-party errors can include signed media URLs and credentials.
                reason = (
                    str(exc)
                    if isinstance(exc, media.MediaError)
                    else type(exc).__name__
                )
                LOG.warning("Job %s failed (%s)", job.job_id, reason)
                result = pb.Result(job_id=job.job_id, failed=True)
            record["result"] = base64.b64encode(result.SerializeToString()).decode()
            save(journal, record)

        try:
            receipt = await call(stub.Report, result, auth)
            if result.WhichOneof("outcome") == "message_id" and receipt.state in {
                "reported",
                "archiving",
            }:
                await asyncio.sleep(2)
                continue
        except grpc.aio.AioRpcError as exc:
            if (
                exc.code()
                in {grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.ALREADY_EXISTS}
                and not result.failed
            ):
                failed = pb.Result(job_id=result.job_id, failed=True)
                record["result"] = base64.b64encode(failed.SerializeToString()).decode()
                save(journal, record)
                continue
            if exc.code() != grpc.StatusCode.NOT_FOUND:
                raise
        if workspace.exists():
            shutil.rmtree(workspace)
        journal.unlink()
