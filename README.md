# phubrpc

The RPC implementation is currently under development and not available on the main bot. You can request to become a beta tester by messaging me on Telegram [@Dankaristic](https://t.me/Dankaristic)

Prime refers to the Running instance of the Pornhub Downloader Bot. It can be found here [@PornhubDownloaderobot](https://t.me/pornhubdownloaderobot?start=c_github). The Prime bot is closed source, meaning code access is restricted.

Run these commands on the **Linux machine that will download the videos**.  Prime can stay on its own VPS. With Coolify in front of Prime, the worker connects to your public hub hostname on **port 443**.

## 1. Install the worker on the download machine

Open Bash in the worker source folder (the one containing `pyproject.toml` and `phubrpc/`). You need Python 3.11 or newer and FFmpeg. On Debian or Ubuntu you can install FFmpeg and venv support with `sudo apt install ffmpeg python3-venv`.

```bash
python3 --version
python3 -m venv .venv
.venv/bin/python -m pip install .
.venv/bin/worker --help
```

The help screen should list `register` and `run`. After every `git pull`, rerun `.venv/bin/python -m pip install -U .` before starting the worker; `git pull` updates the source folder but not the copy installed in `.venv`.

## 2. Get a pairing code in Telegram

1. Create a worker bot with `@BotFather` and keep its token on the download machine. The digits before the colon in the token are the **worker bot ID**.
2. Create a private broadcast channel. Its admins should be you, your worker bot (permission to post), and Prime (permission to delete). Turn off signatures and **Restrict Saving Content**.
3. To find the **channel ID**, post a message and copy its link. For example, a link ending in `/c/1234567890/7` gives the marked channel ID `-1001234567890`.
4. From your BYOC-enabled Telegram account, DM the **Prime bot serving this hub**. For example, if your worker bot ID is `123456789` and your channel ID is `-1001234567890`, send:

   ```text
   /byoc pair 123456789 -1001234567890
   ```

Use your own two IDs. Prime replies with a code valid for ten minutes.

## 3. Register once on the download machine

Enter the pairing code when prompted. The terminal hides your input.

```bash
.venv/bin/worker register --hub rpc.dank.pw:443
```

Wait for `Registered worker ... for channel ...`. For Coolify's public certificate, the hostname and port are sufficient. 

## 4. Start downloading

Save your Telegram application's API ID and hash from `my.telegram.org` → **API development tools**, and the **worker bot's** BotFather token in the worker state directory. Registration has already created that directory. Create `~/.local/state/phubrpc/.env` with these lines (or use your chosen `--state-dir`):

```dotenv
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_BOT_TOKEN=your_worker_bot_token
# Optional if this machine needs an outbound proxy:
# WORKER_HTTP_PROXY=http://user:password@proxy-host:port
```

Keep the file private, then start the worker:

```bash
chmod 600 ~/.local/state/phubrpc/.env
.venv/bin/worker run
```

The worker reads this file on every start. It also checks `.env` in the folder where you run it, so `~/phubrpc/.env` works when you start it from `~/phubrpc`. The state directory file takes precedence, followed by the current folder; nonempty shell variables override both. Use `worker run --env-file PATH` for a specific file.

If the download machine's public IP gets a 403 from the video site, set `WORKER_HTTP_PROXY` in the file to a working outbound HTTP, HTTPS, or SOCKS proxy.

This setting applies to both metadata lookups and video downloads. The proxy must be able to reach the video page, not just the site's homepage; Coolify's inbound HTTPS proxy does not change the worker's outbound IP. To check the proxy before starting the worker, run `.venv/bin/python -m yt_dlp --proxy "$WORKER_HTTP_PROXY" --dump-single-json -- 'VIDEO_LINK' >/dev/null`. If the command still returns 403, run `.venv/bin/python -m yt_dlp --list-impersonate-targets` and verify that at least one browser target is available. Reinstall the worker with `.venv/bin/python -m pip install -U .` to install the browser impersonation dependency. A proxy whose IP is rejected on the video page can still return 403 after this; try another outbound IP or run the worker from a network where the page loads.

Leave it running. If the terminal asks for a setup message, post any text in the private channel within 60 seconds. Once you see `Connected as worker bot`, DM Prime `/byoc`. It should show **online**. Send Prime a video link and select an uncached quality priced at zero coins for a first test.

After a reboot, use the same Linux account and worker folder and run `.venv/bin/worker run`, or set up the background service below. The pairing and `.env` credentials are already saved. To revoke access, send `/byoc revoke` to Prime.

## Run in the background with systemd

On a Linux machine with systemd, a service keeps the worker running after you close the terminal and starts it again after a reboot. Stop any foreground worker with Ctrl+C first; only one worker can use a state directory at a time.

Find the absolute worker folder and state directory paths:

```bash
pwd -P
readlink -f ~/.local/state/phubrpc
```

Create `/etc/systemd/system/phubrpc.service` with `sudo nano /etc/systemd/system/phubrpc.service`. Replace the username and paths below with the values on your machine. Use the **same Linux account** that ran `worker register`, so the service can read its saved pairing, Telegram session, and `.env` file. For the root account, the home directory is `/root` rather than `/home/root`.

```ini
[Unit]
Description=phubrpc private worker
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=YOUR_LINUX_USERNAME
WorkingDirectory=/absolute/path/to/phubrpc
ExecStart=/absolute/path/to/phubrpc/.venv/bin/worker run --state-dir /absolute/path/to/phubrpc-state
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start the service, then check its status and logs:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now phubrpc
sudo systemctl status phubrpc
sudo journalctl -u phubrpc -f
```

Use `sudo systemctl stop phubrpc` to stop it. After pulling and reinstalling worker updates, run `sudo systemctl restart phubrpc`. The service reads the saved `.env` automatically; you do not need to put credentials in the service file.

## State and retries

Both commands accept `--state-dir PATH`. The default is `$XDG_STATE_HOME/phubrpc` or `~/.local/state/phubrpc`. Keep this directory private. It contains the hub credential, the worker bot’s Telegram session, and temporary media. Allow room for the selected formats and their merged MP4.

The worker handles one job at a time and sends a heartbeat every 30 seconds. It checks the downloaded file’s actual size, duration, and dimensions before uploading. If a report is interrupted, the worker retries with the saved message ID. If it stops during an upload before recording a message ID, it reports the job as failed on restart. An unconfirmed Telegram post may need manual removal. Prime cleans up confirmed ingest posts.

Restart with the same state directory to resume reporting. To replace a revoked registration, request a new pairing code and use a fresh state directory for both commands.

## Development

Run `python -m unittest discover -s tests -v` in the installed environment. Tests use a loopback TLS hub, a generated video, FFmpeg, yt-dlp, and a simulated Telegram upload. OpenSSL is needed for the temporary test certificate.

The wire contract is in [worker.proto](phubrpc/worker.proto). To regenerate the Python bindings after a contract change, install `grpcio-tools==1.85.3` and run

```sh
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. phubrpc/worker.proto
```

Licensed under [Apache-2.0](LICENSE).
