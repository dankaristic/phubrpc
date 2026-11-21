import argparse
import asyncio
import getpass
import json
import logging
import os
import shutil
from pathlib import Path

import grpc
from telethon import TelegramClient
from telethon.errors import RPCError

from . import worker_pb2 as pb
from . import worker_pb2_grpc as rpc
from .media import MediaError
from .runtime import call, check_channel, exclusive, heartbeat, jobs, save

LOG = logging.getLogger(__name__)
CONFIG_KEYS = {"TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_BOT_TOKEN", "WORKER_HTTP_PROXY"}


def load_env_file(path, required_file=False):
    """Read worker credentials from a private file without executing shell code."""
    if not path.exists():
        if required_file:
            raise ValueError(f"Credential file not found: {path}")
        return
    if os.name != "nt" and path.stat().st_mode & 0o077:
        raise ValueError(f"Credential file must be private (chmod 600 {path})")
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not separator or key not in CONFIG_KEYS:
            raise ValueError(f"Invalid worker setting on line {number} of {path}")
        if value.startswith(("'", '"')):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"Unclosed quote on line {number} of {path}")
            value = value[1:-1]
        os.environ.setdefault(key, value)


def required(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Set {name} before running the worker")
    return value


async def start(args, directory):
    state = directory / "registration.json"
    if args.action == "register":
        if state.exists():
            raise ValueError("This directory already has a registration")
        target = args.hub
        ca = str(args.ca.resolve()) if args.ca else None
    else:
        if not state.exists():
            raise ValueError("Run worker register first")
        credentials = json.loads(state.read_text())
        target = credentials["hub"]
        ca = credentials["ca"]
    tls = grpc.ssl_channel_credentials(Path(ca).read_bytes() if ca else None)
    async with grpc.aio.secure_channel(target, tls) as connection:
        stub = rpc.WorkerHubStub(connection)
        if args.action == "register":
            code = getpass.getpass("One-time pairing code: ").strip()
            if not code:
                raise ValueError("A pairing code is required")
            response = await stub.Register(pb.Pairing(code=code), timeout=15)
            if (
                not response.token
                or not response.worker_id
                or response.ingest_channel_id >= 0
            ):
                raise ValueError("Hub returned incomplete registration")
            save(
                state,
                {
                    "hub": target,
                    "ca": ca,
                    "worker_id": response.worker_id,
                    "channel": response.ingest_channel_id,
                    "token": response.token,
                },
            )
            print(
                f"Registered worker {response.worker_id} for channel {response.ingest_channel_id}."
            )
            return

        for tool in ("ffmpeg", "ffprobe"):
            if not shutil.which(tool):
                raise ValueError(f"Install {tool} before running the worker")
        token = required("TELEGRAM_BOT_TOKEN")
        telegram = TelegramClient(
            str(directory / "telegram"),
            int(required("TELEGRAM_API_ID")),
            required("TELEGRAM_API_HASH"),
        )
        auth = (("authorization", "Bearer " + credentials["token"]),)
        try:
            await telegram.start(bot_token=token)
            me = await telegram.get_me()
            if not me.bot or str(me.id) != token.partition(":")[0]:
                raise ValueError("Telegram session belongs to a different account")
            try:
                await check_channel(telegram, credentials["channel"])
            except ValueError:
                # A new MTProto session may need an update to learn the channel access hash.
                LOG.warning(
                    "Post a setup message in the ingest channel within 60 seconds."
                )
                for _ in range(12):
                    await asyncio.sleep(5)
                    try:
                        await check_channel(telegram, credentials["channel"])
                        break
                    except ValueError:
                        continue
                else:
                    raise MediaError(
                        "Could not resolve the ingest channel. Check the bot membership and channel ID."
                    )
            # Authenticate before claiming anything and keep heartbeats independent of uploads.
            await call(stub.Heartbeat, pb.Empty(), auth)
            LOG.info("Connected as worker bot %s", me.id)
            loops = [
                asyncio.create_task(heartbeat(stub, auth)),
                asyncio.create_task(
                    jobs(stub, auth, telegram, credentials["channel"], directory)
                ),
            ]
            try:
                await asyncio.gather(*loops)
            finally:
                for task in loops:
                    task.cancel()
                await asyncio.gather(*loops, return_exceptions=True)
        finally:
            await telegram.disconnect()


def main():
    parser = argparse.ArgumentParser(prog="worker")
    commands = parser.add_subparsers(dest="action", required=True)
    register = commands.add_parser(
        "register", help="Pair this machine with the main bot"
    )
    register.add_argument("--hub", required=True, metavar="HOST:PORT")
    register.add_argument(
        "--ca", type=Path, help="PEM CA certificate for a private hub"
    )
    run = commands.add_parser("run", help="Process assignments until interrupted")
    run.add_argument("--env-file", type=Path, help="Credential file (default: STATE_DIR/.env)")
    default = (
        Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "phubrpc"
    )
    for command in (register, run):
        command.add_argument("--state-dir", type=Path, default=default)
    args = parser.parse_args()
    os.umask(0o077)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        directory = args.state_dir.expanduser().resolve()
        with exclusive(directory):
            if args.action == "run":
                load_env_file(
                    args.env_file.expanduser().resolve() if args.env_file else directory / ".env",
                    required_file=bool(args.env_file),
                )
            asyncio.run(start(args, directory))
    except KeyboardInterrupt:
        pass
    except grpc.aio.AioRpcError as exc:
        parser.exit(
            1,
            f"Hub request failed ({exc.code().name}). Check the endpoint, TLS and pairing.\n",
        )
    except RPCError as exc:
        parser.exit(
            1,
            f"Telegram request failed ({type(exc).__name__}). Check the worker credentials and channel permissions.\n",
        )
    except (ValueError, OSError, MediaError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
