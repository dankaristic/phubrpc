import asyncio
import base64
import functools
import http.server
import json
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import grpc
from telethon.tl.types import Channel, ChatAdminRights

from phubrpc import cli, media, runtime
from phubrpc import worker_pb2 as pb
from phubrpc import worker_pb2_grpc as rpc

URL = "https://www.pornhub.com/view_video.php?viewkey=example"


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    async def test_replay_after_restart(self):
        directory = self.root / "job-restart"
        directory.mkdir()
        result = pb.Result(job_id="a1", message_id=12)
        runtime.save(
            self.root / "pending.json",
            {
                "folder": directory.name,
                "job_id": "a1",
                "result": base64.b64encode(result.SerializeToString()).decode(),
            },
        )
        stub = SimpleNamespace(
            Report=AsyncMock(return_value=pb.Receipt(state="copied")),
            ClaimJob=AsyncMock(side_effect=asyncio.CancelledError),
        )
        telegram = AsyncMock()
        with self.assertRaises(asyncio.CancelledError):
            await runtime.jobs(stub, (), telegram, -1000000000099, self.root)
        self.assertEqual(stub.Report.await_args.args[0], result)
        telegram.send_file.assert_not_awaited()
        self.assertFalse(directory.exists())

    async def test_group_or_public_channel_rejected(self):
        telegram = AsyncMock()
        for broadcast, megagroup, username in [
            (False, True, None),
            (True, False, "public"),
        ]:
            telegram.get_entity.return_value = Channel(
                id=99,
                title="Test",
                photo=None,
                date=None,
                broadcast=broadcast,
                megagroup=megagroup,
                username=username,
                admin_rights=ChatAdminRights(post_messages=True),
            )
            with self.assertRaises(media.MediaError):
                await runtime.check_channel(telegram, -1000000000099)

    async def test_interrupted_upload_reports_failure_without_reposting(self):
        temp = self.root / "job-unknown"
        temp.mkdir()
        runtime.save(self.root / "pending.json", {"job_id": "x", "folder": temp.name})
        stub = SimpleNamespace(
            Report=AsyncMock(return_value=pb.Receipt(state="failed")),
            ClaimJob=AsyncMock(side_effect=asyncio.CancelledError),
        )
        telegram = AsyncMock()
        with self.assertRaises(asyncio.CancelledError):
            await runtime.jobs(stub, (), telegram, -1000000000099, self.root)
        self.assertEqual(
            stub.Report.await_args.args[0], pb.Result(job_id="x", failed=True)
        )
        telegram.send_file.assert_not_awaited()

    async def test_url_and_size_checks(self):
        job = pb.Assignment(job_id="one", url="https://example.org", quality="720p")
        with self.assertRaises(media.MediaError):
            await runtime.execute(job, None, 0, self.root)
        job.url = URL
        with self.assertRaises(media.MediaError):
            await runtime.execute(job, None, 0, self.root)

    async def test_cli_pair_metadata_and_upload(self):
        cert, key = self.root / "cert.pem", self.root / "key.pem"
        await asyncio.to_thread(
            subprocess.run,
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            capture_output=True,
        )
        video = self.root / "sample.mp4"
        await asyncio.to_thread(
            subprocess.run,
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=size=1280x720:rate=4:duration=1",
                "-c:v",
                "mpeg4",
                "-movflags",
                "+faststart",
                str(video),
            ],
            check=True,
        )
        handler = functools.partial(
            http.server.SimpleHTTPRequestHandler, directory=str(self.root)
        )
        httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        info = self.root / "info.json"
        info.write_text(
            json.dumps(
                {
                    "id": "test",
                    "title": "Fixture",
                    "uploader": "Test",
                    "duration": 1,
                    "extractor": "generic",
                    "webpage_url": URL,
                    "formats": [
                        {
                            "format_id": "720",
                            "ext": "mp4",
                            "vcodec": "mpeg4",
                            "acodec": "none",
                            "width": 1280,
                            "height": 720,
                            "url": f"http://127.0.0.1:{httpd.server_port}/sample.mp4",
                        }
                    ],
                }
            )
        )
        completed = asyncio.Event()
        assigned = []
        reports = []
        heartbeats = []

        class Hub(rpc.WorkerHubServicer):
            async def Register(self, request, context):
                if request.code != "pair-once":
                    await context.abort(
                        grpc.StatusCode.PERMISSION_DENIED, "Invalid code"
                    )
                return pb.Credentials(
                    worker_id="test",
                    token="test-bearer",
                    ingest_channel_id=-1000000000099,
                )

            async def Heartbeat(self, request, context):
                heartbeats.append(dict(context.invocation_metadata())["authorization"])
                return pb.Empty()

            async def ClaimJob(self, request, context):
                if len(assigned) == 2:
                    completed.set()
                    return pb.Assignment()
                quality = "720p" if assigned else ""
                assigned.append(quality)
                return pb.Assignment(
                    job_id=str(len(assigned)),
                    quality=quality,
                    url=URL,
                    max_bytes=2000000,
                )

            async def Report(self, request, context):
                reports.append(request)
                if request.WhichOneof("outcome") == "progress":
                    return pb.Receipt(state="assigned")
                uploads = sum(r.WhichOneof("outcome") == "message_id" for r in reports)
                return pb.Receipt(state="reported" if uploads == 1 else "copied")

        server = grpc.aio.server()
        rpc.add_WorkerHubServicer_to_server(Hub(), server)
        port = server.add_secure_port(
            "127.0.0.1:0",
            grpc.ssl_server_credentials([(key.read_bytes(), cert.read_bytes())]),
        )
        await server.start()
        self.addAsyncCleanup(server.stop, 0)
        state = self.root / "state"
        state.mkdir()
        with patch("getpass.getpass", return_value="pair-once"):
            await cli.start(
                SimpleNamespace(action="register", hub=f"localhost:{port}", ca=cert),
                state,
            )
        self.assertEqual((state / "registration.json").stat().st_mode & 0o777, 0o600)
        telegram = AsyncMock()
        telegram.get_me.return_value = SimpleNamespace(id=10, bot=True)
        telegram.get_entity.return_value = Channel(
            id=99,
            title="Ingest",
            photo=None,
            date=None,
            broadcast=True,
            admin_rights=ChatAdminRights(post_messages=True),
        )
        observed = []

        async def send(channel, path, **kwargs):
            observed.append((channel, path.read_bytes(), kwargs))
            return SimpleNamespace(id=22)

        telegram.send_file.side_effect = send
        actual_ytdlp = media.ytdlp

        # Replace only extraction with local fixture metadata. The downloader, format
        # selector, file transfer, ffprobe and RPC client run normally.
        def fixture_ytdlp(*args):
            return actual_ytdlp("--load-info-json", str(info), *args[:-2])

        with (
            patch.dict(
                "os.environ",
                TELEGRAM_API_ID="100",
                TELEGRAM_API_HASH="hash",
                TELEGRAM_BOT_TOKEN="10:test",
            ),
            patch.object(cli, "TelegramClient", return_value=telegram),
            patch.object(media, "ytdlp", fixture_ytdlp),
        ):
            task = asyncio.create_task(cli.start(SimpleNamespace(action="run"), state))
            try:
                await asyncio.wait_for(completed.wait(), timeout=30)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(set(heartbeats), {"Bearer test-bearer"})
        self.assertEqual(reports[0].metadata.qualities, ["720p"])
        self.assertIn("downloading", [r.progress.stage for r in reports
                                      if r.WhichOneof("outcome") == "progress"])
        self.assertIn("uploading", [r.progress.stage for r in reports
                                    if r.WhichOneof("outcome") == "progress"])
        self.assertEqual([r.message_id for r in reports
                          if r.WhichOneof("outcome") == "message_id"], [22, 22])
        channel, data, kwargs = observed[0]
        self.assertEqual(len(observed), 1)
        self.assertEqual(
            (channel, data, kwargs["caption"]),
            (-1000000000099, video.read_bytes(), "2"),
        )
        (attr,) = kwargs["attributes"]
        self.assertEqual((attr.w, attr.h), (1280, 720))
        self.assertFalse((state / "pending.json").exists())
