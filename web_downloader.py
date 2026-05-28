#!/usr/bin/env python3
"""
DriveSeed Web Downloader Backend Server.
Provides a premium, responsive local API server for the index.html frontend.
"""

import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from html import unescape
from pathlib import Path
from queue import Empty, PriorityQueue
from urllib.parse import parse_qs, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import webbrowser

from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
import tkinter as tk
from tkinter import filedialog

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from env_loader import load_env_file, session_file_exists

# Load environments
load_env_file()

# Core Scraper and Resolver imports
try:
    from batch_episodes import resolve_link, scrape_links
    HAS_BATCH = True
except ImportError:
    HAS_BATCH = False

from direct_downloader import (
    HEADERS,
    get_driveseed_download_url,
    get_driveseed_file_metadata,
)

try:
    from telegram_fallback import TelegramDownloadError
    HAS_TELEGRAM_FALLBACK = True
except ImportError:
    HAS_TELEGRAM_FALLBACK = False
    class TelegramDownloadError(Exception):
        pass

try:
    from movie_search import search_movies, extract_download_options, resolve_search_domains
except ImportError:
    pass

# Shared requests session
def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=Retry(total=2, backoff_factor=0.3),
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# DNS Pre-warming
DNS_DOMAINS = [
    "driveseed.org",
    "instant.video-leech.pro",
    "cdn.video-leech.pro",
    "instant.video-gen.xyz",
    "cdn.video-gen.xyz",
    "video-seed.dev",
    "video-seed.pro",
    "video-downloads.googleusercontent.com",
    "cloud.unblockedgames.world",
]

def prewarm_dns():
    def _resolve(host):
        try:
            socket.getaddrinfo(host, 443)
        except Exception:
            pass
    with ThreadPoolExecutor(max_workers=len(DNS_DOMAINS)) as pool:
        pool.map(_resolve, DNS_DOMAINS)

MAX_CONCURRENT = 2
CHUNK_SIZE = 2 * 1024 * 1024  # 2 MB

TELEGRAM_VERBOSE_DEBUG = (
    os.getenv("TELEGRAM_VERBOSE_DEBUG", "1").strip().lower() not in ("0", "false", "no", "off")
)

def tg_debug(msg: str) -> None:
    if TELEGRAM_VERBOSE_DEBUG:
        ts = time.strftime("%H:%M:%S")
        print(f"[TG-DEBUG {ts}] {msg}", flush=True)

def fmt_bytes(num_bytes: int | None) -> str:
    if not num_bytes or num_bytes <= 0:
        return "unknown"
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1000 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1000
    return f"{num_bytes} B"

def parse_size_hint_bytes(text: str | None) -> int | None:
    if not text:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*([kmgt]?i?b)", text, re.IGNORECASE)
    if not m:
        return None
    value = float(m.group(1))
    unit = m.group(2).lower()
    multipliers = {
        "b": 1,
        "kb": 1000,
        "mb": 1000 ** 2,
        "gb": 1000 ** 3,
        "tb": 1000 ** 4,
        "kib": 1024,
        "mib": 1024 ** 2,
        "gib": 1024 ** 3,
        "tib": 1024 ** 4,
    }
    factor = multipliers.get(unit)
    if not factor:
        return None
    return int(value * factor)


# ── Virtual State Card Manager ───────────────────────────────────────────

class VirtualDownloadCard:
    """A thread-safe model representation of a single download card state."""
    def __init__(self, index, filename, method=""):
        self.index = index
        self.filename = filename
        self.method = method
        self.state = 0  # 0=Pending, 1=Downloading, 2=Done, 3=Failed
        self.progress = 0.0
        self.detail = "Pending"
        self.status = "Pending"
        self.action_text = ""
        self.action_state = "normal"
        self.item_data = None
        self.url = ""

    def set_method(self, method):
        self.method = method

    def set_status(self, status):
        self.status = status

    def set_progress(self, val):
        self.progress = val

    def set_detail(self, detail):
        self.detail = detail

    def set_action(self, text, command=None, state="normal"):
        self.action_text = text
        self.action_state = state

    def hide_action(self):
        self.action_text = ""

    def mark_done(self):
        self.state = 2
        self.status = "✓ Done"
        self.progress = 1.0
        if not self.detail or self.detail == "Pending":
            self.detail = "Already downloaded"
        self.hide_action()

    def mark_failed(self, reason=""):
        self.state = 3
        self.status = "✗ Failed"
        self.progress = 0.0
        if reason:
            self.detail = reason
        self.set_action("Retry")

    def mark_downloading(self):
        self.state = 1
        self.status = "Downloading…"

    def mark_pending(self):
        self.state = 0
        self.status = "Pending"
        self.detail = "Retrying…"

    def to_json(self):
        return {
            "index": self.index,
            "filename": self.filename,
            "method": self.method,
            "state": self.state,
            "progress": self.progress,
            "detail": self.detail,
            "status": self.status,
            "action_text": self.action_text,
            "action_state": self.action_state,
            "resolved_url": self.url
        }


# ── Standalone Download Manager Core ─────────────────────────────────────

class DownloaderBackend:
    def __init__(self):
        self.output_dir = os.path.expanduser("/media/so9ic/HDD/Downloads/Movies")
        self.cloud_mode = os.getenv("CLOUD_MODE", "false").lower() == "true" or "DISPLAY" not in os.environ
        self.cards: list[VirtualDownloadCard] = []
        self.download_queue = PriorityQueue()
        self._active_threads = 0
        self._lock = threading.Lock()
        self._telegram_lock = threading.Lock()

        self._done_count = 0
        self._fail_count = 0
        self._total_count = 0

        # Start MODLIST polling loop
        self._launch_modlist_poller()

    def _launch_modlist_poller(self):
        def _poll():
            print("[*] Launching modlist background domain poller...", flush=True)
            while True:
                try:
                    resolve_search_domains(force_refresh=True)
                except Exception as e:
                    print(f"[-] Modlist poller error: {e}", flush=True)
                time.sleep(30)
        threading.Thread(target=_poll, daemon=True).start()

    def _get_telegram_ready_status(self):
        if not HAS_TELEGRAM_FALLBACK:
            return "Telegram: Not Ready", "red"

        api_id = os.getenv("TELEGRAM_API_ID", "").strip()
        api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        if not api_id or not api_hash:
            return "Telegram: Config Missing", "amber"

        session_name = os.getenv("TELEGRAM_SESSION", "tgseed_session").strip() or "tgseed_session"
        if not session_file_exists(session_name):
            return "Telegram: Login Needed", "amber"

        return "Telegram: Ready", "green"

    # ── Folder picker native dialog ──
    def ask_directory(self) -> str:
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            folder = filedialog.askdirectory(
                initialdir=self.output_dir,
                title="Select Save Directory"
            )
            root.destroy()
            return folder
        except Exception as e:
            print(f"[-] Native dialog failed: {e}", flush=True)
            return ""

    def start_pipeline(self, url, output_dir):
        self.output_dir = output_dir

        with self._lock:
            self._done_count = 0
            self._fail_count = 0
            self._total_count = 0
            self._active_threads = 0
            self.cards.clear()

        while not self.download_queue.empty():
            try:
                self.download_queue.get_nowait()
            except Empty:
                break

        threading.Thread(target=prewarm_dns, daemon=True).start()
        threading.Thread(target=self._resolve_pipeline, args=(url,), daemon=True).start()

    def _resolve_pipeline(self, url):
        try:
            if "tgseed.link" in url:
                self._resolve_single_telegram_url(url)
                return

            if ".r2.dev/" in url:
                self._resolve_single_cloud_url(url)
                return

            if "driveseed.org" in url:
                self._resolve_single_driveseed(url, 0)
                return

            if not HAS_BATCH:
                print("[-] Batch module missing")
                return

            links = scrape_links(url)
            if not links:
                print("[-] No download links found on page")
                return

            total = len(links)
            with self._lock:
                self._total_count = total

            # Create placeholder cards
            for i in range(total):
                card = VirtualDownloadCard(i + 1, "Resolving…", "…")
                card.set_status("Resolving…")
                self.cards.append(card)

            existing = set()
            if os.path.isdir(self.output_dir):
                existing = set(os.listdir(self.output_dir))

            workers_started = [0]
            workers_lock = threading.Lock()

            def _fused_resolve(i, link):
                _, name, ds_url = resolve_link(i, link, session=SESSION)
                source_link = dict(link) if isinstance(link, dict) else None
                size_hint = parse_size_hint_bytes(name)
                origin_url = link.get("url", "") if isinstance(link, dict) else ""
                origin_meta_size = None
                if "driveseed.org" in origin_url:
                    _, origin_meta_size = get_driveseed_file_metadata(origin_url)

                expected_size = origin_meta_size or size_hint

                if ds_url and "tgseed.link" in ds_url:
                    fname = name or f"Link {i + 1}"
                    fname = re.sub(r'[<>:"/\\|?*]', "_", fname)
                    return i, {
                        "filename": fname,
                        "download_url": ds_url,
                        "method": "TELEGRAM",
                        "target_dir": self.output_dir,
                        "expected_size_bytes": expected_size,
                        "source_link": source_link,
                        "source_index": i,
                        "source_name_hint": name,
                        "source_driveseed_url": None,
                    }

                if ds_url and ".r2.dev/" in ds_url:
                    fname = os.path.basename(urlparse(ds_url).path) or name or f"download_{i + 1}"
                    fname = re.sub(r'[<>:"/\\|?*]', "_", fname)
                    return i, {
                        "filename": fname,
                        "download_url": ds_url,
                        "method": "CLOUD",
                        "target_dir": self.output_dir,
                        "expected_size_bytes": expected_size,
                        "source_link": source_link,
                        "source_index": i,
                        "source_name_hint": name,
                        "source_driveseed_url": None,
                    }

                if not ds_url or "driveseed.org" not in ds_url:
                    return i, {
                        "filename": name or f"Link {i + 1}",
                        "download_url": None,
                        "method": "",
                        "error": "Not a driveseed link",
                    }

                meta_fname, raw_meta_size = get_driveseed_file_metadata(ds_url)
                meta_size = raw_meta_size or expected_size
                dl_url, fname, method = get_driveseed_download_url(ds_url)
                if not fname:
                    fname = meta_fname or os.path.basename(urlparse(dl_url).path) or name or f"download_{i + 1}"
                fname = re.sub(r'[<>:"/\\|?*]', "_", fname)
                
                return i, {
                    "filename": fname,
                    "download_url": dl_url,
                    "method": method,
                    "target_dir": self.output_dir,
                    "expected_size_bytes": meta_size,
                    "source_link": source_link,
                    "source_index": i,
                    "source_name_hint": name,
                    "source_driveseed_url": ds_url,
                }

            done_count = 0
            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = {pool.submit(_fused_resolve, i, lnk): i for i, lnk in enumerate(links)}
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        idx, item = future.result()
                    except Exception as e:
                        item = {
                            "filename": f"Failed link {idx + 1}",
                            "download_url": None,
                            "method": "",
                            "error": str(e),
                        }

                    done_count += 1
                    
                    if item.get("download_url"):
                        fname = item["filename"]
                        card = self.cards[idx]
                        card.filename = fname
                        card.set_method(item.get("method", ""))
                        card.item_data = item
                        card.url = item["download_url"]

                        if fname in existing:
                            card.set_detail("Already downloaded")
                            card.mark_done()
                            with self._lock:
                                self._done_count += 1
                        else:
                            if item.get("method") == "TELEGRAM":
                                card.set_status("Manual Telegram")
                                exp = item.get("expected_size_bytes")
                                card.set_detail(f"Click Download to open Telegram Desktop. Expected: {fmt_bytes(exp)}" if exp else "Click Download to open Telegram Desktop.")
                                card.set_action("Download", lambda index=idx: self.start_telegram_manual(index))
                            else:
                                if self.cloud_mode:
                                    card.status = "✓ Ready"
                                    card.state = 2
                                    card.progress = 1.0
                                    card.detail = "Direct link resolved! Click 'Download to Device' below."
                                    with self._lock:
                                        self._done_count += 1
                                else:
                                    card.set_status("Queued")
                                    self.download_queue.put((idx, item))
                                    with workers_lock:
                                        while workers_started[0] < MAX_CONCURRENT:
                                            workers_started[0] += 1
                                            threading.Thread(target=self._download_worker, daemon=True).start()
                    else:
                        card = self.cards[idx]
                        card.filename = item["filename"]
                        card.set_method(item.get("method", ""))
                        card.mark_failed(item.get("error", "Could not resolve"))
                        with self._lock:
                            self._fail_count += 1

        except Exception as e:
            print(f"[-] Resolve error: {e}", flush=True)

    def _resolve_single_driveseed(self, url, idx):
        with self._lock:
            self._total_count = 1
        card = VirtualDownloadCard(1, "Resolving…", "…")
        self.cards.append(card)

        try:
            meta_fname, meta_size = get_driveseed_file_metadata(url)
            dl_url, fname, method = get_driveseed_download_url(url)
            if not fname:
                fname = meta_fname or os.path.basename(urlparse(dl_url).path) or "download_1"
            fname = re.sub(r'[<>:"/\\|?*]', "_", fname)
            
            item = {
                "filename": fname,
                "download_url": dl_url,
                "method": method,
                "target_dir": self.output_dir,
                "expected_size_bytes": meta_size,
                "source_link": None,
                "source_index": 0,
                "source_name_hint": fname,
                "source_driveseed_url": url,
            }
            card.filename = fname
            card.set_method(method)
            card.item_data = item
            card.url = dl_url

            existing = set(os.listdir(self.output_dir)) if os.path.isdir(self.output_dir) else set()
            if fname in existing:
                card.mark_done()
                with self._lock:
                    self._done_count += 1
                return

            if self.cloud_mode:
                card.status = "✓ Ready"
                card.state = 2
                card.progress = 1.0
                card.detail = "Direct link resolved! Click 'Download to Device' below."
                with self._lock:
                    self._done_count += 1
                return

            self.download_queue.put((0, item))
            threading.Thread(target=self._download_worker, daemon=True).start()

        except Exception as e:
            card.mark_failed(str(e))
            with self._lock:
                self._fail_count += 1

    def _resolve_single_cloud_url(self, url):
        with self._lock:
            self._total_count = 1
        fname = os.path.basename(urlparse(url).path) or "download_1"
        fname = re.sub(r'[<>:"/\\|?*]', "_", fname)
        card = VirtualDownloadCard(1, fname, "CLOUD")
        self.cards.append(card)

        item = {
            "filename": fname,
            "download_url": url,
            "method": "CLOUD",
            "target_dir": self.output_dir,
            "expected_size_bytes": None,
            "source_link": None,
            "source_index": 0,
            "source_name_hint": fname,
            "source_driveseed_url": None,
        }
        card.item_data = item
        card.url = url

        existing = set(os.listdir(self.output_dir)) if os.path.isdir(self.output_dir) else set()
        if fname in existing:
            card.mark_done()
            with self._lock:
                self._done_count += 1
            return

        if self.cloud_mode:
            card.status = "✓ Ready"
            card.state = 2
            card.progress = 1.0
            card.detail = "Direct link resolved! Click 'Download to Device' below."
            with self._lock:
                self._done_count += 1
            return

        self.download_queue.put((0, item))
        threading.Thread(target=self._download_worker, daemon=True).start()

    def _resolve_single_telegram_url(self, url):
        with self._lock:
            self._total_count = 1
        card = VirtualDownloadCard(1, "Telegram file", "TELEGRAM")
        self.cards.append(card)

        item = {
            "filename": "Telegram file",
            "download_url": url,
            "method": "TELEGRAM",
            "target_dir": self.output_dir,
            "expected_size_bytes": None,
            "source_link": None,
            "source_index": 0,
            "source_name_hint": "Telegram file",
            "source_driveseed_url": None,
        }
        card.item_data = item
        card.url = url
        card.set_status("Manual Telegram")
        card.set_detail("Click Download to open Telegram Desktop.")
        card.set_action("Download", lambda: self.start_telegram_manual(0))

    def _download_worker(self):
        while True:
            try:
                idx, item = self.download_queue.get(timeout=5)
            except Empty:
                break

            with self._lock:
                self._active_threads += 1

            card = self.cards[idx]
            card.mark_downloading()

            try:
                url = item["download_url"]
                method = item.get("method", "")
                filename = item["filename"]
                target_dir = item.get("target_dir", self.output_dir)
                filepath = os.path.join(target_dir, filename)
                part_path = filepath + ".part"

                if method == "TELEGRAM" or "tgseed.link" in url:
                    self.start_telegram_manual(idx)
                    continue

                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    with self._lock:
                        self._done_count += 1
                        self._active_threads -= 1
                    card.set_detail(f"Already downloaded ({file_size / (1024 * 1024):.1f} MB)")
                    card.mark_done()
                    continue

                resume_from = 0
                if os.path.exists(part_path):
                    resume_from = os.path.getsize(part_path)

                headers = {}
                if resume_from > 0:
                    headers["Range"] = f"bytes={resume_from}-"
                    card.set_detail(f"Resuming from {resume_from / (1024 * 1024):.1f} MB…")

                resp = SESSION.get(url, stream=True, timeout=300, headers=headers)

                if resp.status_code == 200 and resume_from > 0:
                    resume_from = 0
                elif resp.status_code not in (200, 206):
                    resp.raise_for_status()

                total = int(resp.headers.get("content-length", 0)) + resume_from
                downloaded = resume_from
                start_time = time.time()
                last_update = 0

                mode = "ab" if resume_from > 0 and resp.status_code == 206 else "wb"

                with open(part_path, mode) as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            now = time.time()

                            if now - last_update < 0.25:
                                continue
                            last_update = now

                            elapsed = now - start_time
                            speed = (downloaded - resume_from) / elapsed if elapsed > 0 else 0
                            pct = downloaded / total if total else 0

                            mb_down = downloaded / (1024 * 1024)
                            speed_str = self._fmt_speed(speed)
                            if total:
                                mb_total = total / (1024 * 1024)
                                remaining = (total - downloaded) / speed if speed > 0 else 0
                                eta_str = self._fmt_time(remaining)
                                detail = f"{mb_down:.1f}/{mb_total:.1f} MB  •  {speed_str}  •  ETA {eta_str}"
                            else:
                                detail = f"{mb_down:.1f} MB  •  {speed_str}"

                            card.set_progress(pct)
                            card.set_detail(detail)

                os.rename(part_path, filepath)

                total_mb = (total or downloaded) / (1024 * 1024)
                elapsed = time.time() - start_time
                avg = self._fmt_speed((downloaded - resume_from) / elapsed) if elapsed > 0 else "–"

                with self._lock:
                    self._done_count += 1
                card.mark_done()
                card.set_detail(f"{total_mb:.1f} MB  •  avg {avg}")

            except Exception as e:
                with self._lock:
                    self._fail_count += 1
                card.mark_failed(str(e))

            finally:
                with self._lock:
                    self._active_threads -= 1

    def _fmt_speed(self, bytes_per_sec: float) -> str:
        if bytes_per_sec <= 0:
            return "0 B/s"
        for unit in ("B/s", "KB/s", "MB/s", "GB/s"):
            if bytes_per_sec < 1000 or unit == "GB/s":
                return f"{bytes_per_sec:.1f} {unit}"
            bytes_per_sec /= 1000
        return f"{bytes_per_sec:.1f} B/s"

    def _fmt_time(self, seconds: float) -> str:
        if seconds <= 0:
            return "0s"
        s = int(seconds)
        h = s // 3600
        s %= 3600
        m = s // 60
        s %= 60
        if h > 0:
            return f"{h}h {m}m"
        if m > 0:
            return f"{m}m {s}s"
        return f"{s}s"

    # ── Advanced Telegram Manual Download logic ──
    def start_telegram_manual(self, idx, is_retry=False):
        if idx >= len(self.cards):
            return
        card = self.cards[idx]
        item = card.item_data or {}
        
        if is_retry:
            removed = self._cleanup_telegram_retry_artifacts(item)
            card.set_detail(f"Retrying… cleaned {removed} partial file(s).")
            card.set_progress(0)
            card.set_action("Retrying…")
        else:
            card.set_action("Waiting…")

        item["attempted_manual"] = True
        threading.Thread(
            target=self._run_telegram_manual_download,
            args=(idx, item, is_retry),
            daemon=True,
        ).start()

    def _run_telegram_manual_download(self, idx, item, is_retry=False):
        card = self.cards[idx]
        target_dir = item.get("target_dir", self.output_dir)
        watch_timeout_raw = os.getenv("TELEGRAM_WATCH_TIMEOUT", "").strip()
        watch_timeout = max(0, int(watch_timeout_raw)) if watch_timeout_raw else 0

        with self._lock:
            self._active_threads += 1

        try:
            watch_dirs = self._telegram_watch_dirs()
            existing_watch_dirs = [p for p in watch_dirs if p.is_dir()]
            if not existing_watch_dirs:
                raise FileNotFoundError("No Telegram Desktop download folder found.")

            baseline = self._snapshot_files(watch_dirs)
            started_at = time.time()

            if self._telegram_lock.locked():
                card.set_detail("Waiting for previous Telegram request to finish…")

            with self._telegram_lock:
                card.set_detail("Resolving Telegram deep protocol link…")
                from telegram_fallback import get_telegram_client_and_bot
                
                # Fetch Telegram credentials safely
                client, bot_username = get_telegram_client_and_bot()
                
                # Fetch target bot and start token
                url = item["download_url"]
                parsed = urlparse(unescape(url).strip())
                qs = parse_qs(parsed.query)
                start_param = qs.get("start", [""])[0].strip()
                bot_name = qs.get("bot", [bot_username])[0].lstrip("@").strip()

                if not start_param:
                    raise TelegramDownloadError("Invalid fallback url start param")

                deep_link = f"tg://resolve?domain={bot_name}&start={start_param}"
                card.set_detail("Launching Telegram app…")
                self._open_telegram_with_link(deep_link)

                card.set_action("Watching…")
                card.set_detail("Telegram opened. Click Start in your Telegram client…")

                # Progress loop looking for new file
                new_file = None
                size_hint = item.get("expected_size_bytes")
                
                while True:
                    if watch_timeout > 0 and (time.time() - started_at > watch_timeout):
                        raise TimeoutError(f"Timeout: click Start in Telegram within {watch_timeout}s")

                    current = self._snapshot_files(watch_dirs)
                    candidates = []
                    
                    for fpath, (sz, mt) in current.items():
                        if fpath not in baseline:
                            candidates.append((fpath, sz, mt))
                        else:
                            old_sz, _ = baseline[fpath]
                            if sz > old_sz:
                                candidates.append((fpath, sz, mt))

                    # Filter partial files (.part)
                    active_parts = [c for c in candidates if c[0].endswith(".part")]
                    if active_parts:
                        new_file = Path(active_parts[0][0])
                        break

                    # Check for quick complete files
                    completed_files = [c for c in candidates if not c[0].endswith(".part") and not c[0].endswith(".download")]
                    if completed_files:
                        new_file = Path(completed_files[0][0])
                        break

                    time.sleep(0.5)

                # Monitor download progress
                tg_debug(f"[Telegram Monitor] Found active download: {new_file}")
                is_part = new_file.suffix == ".part"
                actual_file = new_file.with_suffix("") if is_part else new_file
                
                last_size = 0
                last_time = time.time()

                while True:
                    if not new_file.exists() and not actual_file.exists():
                        raise FileNotFoundError("Downloading file vanished")

                    cur_size = 0
                    if new_file.exists():
                        cur_size = new_file.stat().st_size
                    elif actual_file.exists():
                        cur_size = actual_file.stat().st_size
                        if not is_part or (is_part and cur_size >= (size_hint or 0)):
                            break

                    now = time.time()
                    elapsed = now - last_time
                    speed = (cur_size - last_size) / elapsed if elapsed > 0 else 0
                    last_size = cur_size
                    last_time = now

                    if size_hint:
                        pct = min(0.99, cur_size / size_hint)
                        card.set_progress(pct)
                        card.set_detail(f"Downloading: {cur_size/(1024*1024):.1f}/{size_hint/(1024*1024):.1f} MB  •  {self._fmt_speed(speed)}")
                    else:
                        card.set_detail(f"Downloading: {cur_size/(1024*1024):.1f} MB  •  {self._fmt_speed(speed)}")

                    time.sleep(0.5)

                card.set_progress(0.99)
                card.set_detail("Copying completed Telegram file to downloads directory…")

                # Move downloaded file to target
                dest_path = self._pick_dest_path(target_dir, actual_file.name, item.get("source_name_hint"))
                shutil.copy2(actual_file, dest_path)
                
                try:
                    actual_file.unlink()
                except Exception:
                    pass

                with self._lock:
                    self._done_count += 1
                card.mark_done()
                card.set_detail(f"{os.path.getsize(dest_path)/(1024*1024):.1f} MB  •  avg Telegram")

        except Exception as e:
            with self._lock:
                self._fail_count += 1
            card.mark_failed(str(e))
            card.set_action("Retry", lambda index=idx: self.start_telegram_manual(index, is_retry=True))

        finally:
            with self._lock:
                self._active_threads -= 1

    def _cleanup_telegram_retry_artifacts(self, item) -> int:
        watch_dirs = self._telegram_watch_dirs()
        name_hint = item.get("source_name_hint", "")
        if not name_hint:
            return 0
        
        base_name, _ = os.path.splitext(name_hint)
        removed = 0
        
        for w in watch_dirs:
            if not w.is_dir():
                continue
            for entry in w.iterdir():
                if entry.is_file() and base_name in entry.name:
                    if entry.suffix in (".part", ".download") or entry.name == name_hint:
                        try:
                            entry.unlink()
                            removed += 1
                        except Exception:
                            pass
        return removed

    @staticmethod
    def _telegram_watch_dirs() -> list[Path]:
        dirs = []
        raw = os.getenv("TELEGRAM_DESKTOP_DOWNLOAD_DIR", "").strip()
        if raw:
            for part in re.split(r"[:,]", raw):
                p = part.strip()
                if p:
                    dirs.append(Path(p).expanduser())
        dirs.append(Path("~/Downloads/Telegram Desktop").expanduser())
        dirs.append(Path("~/Downloads").expanduser())
        dirs.append(Path("~/.var/app/org.telegram.desktop/data/TelegramDesktop/tdata/temp_data").expanduser())
        dirs.append(Path("~/.var/app/org.telegram.desktop/data/TelegramDesktop").expanduser())

        unique = []
        seen = set()
        for p in dirs:
            key = str(p)
            if key not in seen:
                seen.add(key)
                unique.append(p)
        return unique

    @staticmethod
    def _snapshot_files(paths: list[Path]) -> dict[str, tuple[int, float]]:
        snap = {}
        seen = set()
        for base in paths:
            if not base.is_dir():
                continue
            try:
                for root, _, files in os.walk(base):
                    for name in files:
                        f = Path(root) / name
                        key = str(f.resolve())
                        if key in seen:
                            continue
                        seen.add(key)
                        try:
                            st = f.stat()
                        except Exception:
                            continue
                        snap[key] = (st.st_size, st.st_mtime)
            except Exception:
                continue
        return snap

    @staticmethod
    def _pick_dest_path(target_dir: str, src_name: str, preferred_name: str | None = None) -> str:
        os.makedirs(target_dir, exist_ok=True)
        final_name = (preferred_name or "").strip() or src_name

        src_ext = os.path.splitext(src_name)[1]
        pref_base, pref_ext = os.path.splitext(final_name)
        if src_ext and not pref_ext:
            final_name = f"{pref_base}{src_ext}"

        base, ext = os.path.splitext(final_name)
        candidate = os.path.join(target_dir, final_name)
        n = 1
        while os.path.exists(candidate):
            candidate = os.path.join(target_dir, f"{base} ({n}){ext}")
            n += 1
        return candidate

    @staticmethod
    def _is_telegram_running() -> bool:
        try:
            result = subprocess.run(["pgrep", "-f", "-i", "telegram"], capture_output=True, timeout=2)
            return result.returncode == 0
        except Exception:
            return False

    @classmethod
    def _open_telegram_with_link(cls, deep_link: str) -> None:
        if cls._is_telegram_running():
            subprocess.run(["xdg-open", deep_link], capture_output=True, timeout=5)
        else:
            try:
                subprocess.Popen(["telegram-desktop"])
            except Exception:
                subprocess.Popen(["telegram"])
            time.sleep(5)
            subprocess.run(["xdg-open", deep_link], capture_output=True, timeout=5)


# Global Download Manager reference
DOWNLOAD_MGR = DownloaderBackend()


# ── Threaded HTTP Request Handler & API Router ───────────────────────────

class APIRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Silence annoying standard console access logs
        pass

    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        
        # 1. Serve frontend index.html static file
        if parsed.path == '/' or parsed.path == '/index.html':
            try:
                with open('index.html', 'rb') as f:
                    content = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                self.wfile.write(content)
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error loading index.html: {e}".encode('utf-8'))
            return

        # 2. API Status endpoint
        if parsed.path == '/api/status':
            tg_text, tg_color = DOWNLOAD_MGR._get_telegram_ready_status()
            self.send_json({
                "done_count": DOWNLOAD_MGR._done_count,
                "fail_count": DOWNLOAD_MGR._fail_count,
                "total_count": DOWNLOAD_MGR._total_count,
                "active_threads": DOWNLOAD_MGR._active_threads,
                "output_dir": DOWNLOAD_MGR.output_dir,
                "cloud_mode": DOWNLOAD_MGR.cloud_mode,
                "telegram": {
                    "text": tg_text,
                    "color": tg_color
                }
            })
            return

        # 3. API Downloads list endpoint
        if parsed.path == '/api/downloads':
            self.send_json({
                "downloads": [card.to_json() for card in DOWNLOAD_MGR.cards]
            })
            return

        # 4. Same-page details movie qualities extractor
        if parsed.path == '/api/detail':
            qs = parse_qs(parsed.query)
            target_url = qs.get("url", [""])[0]
            if not target_url:
                self.send_json({"error": "Missing url query param"}, 400)
                return

            try:
                # Scrape download option buttons using movie_search library
                options = extract_download_options(target_url)
                self.send_json({"options": options})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # 5. Server-Sent Events (SSE) Search Card Streamer!
        if parsed.path == '/api/search/stream':
            qs = parse_qs(parsed.query)
            query = qs.get("q", [""])[0].strip()
            category = qs.get("cat", ["All"])[0]

            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            # Map category parameter to the expected format in movie_search.py
            cat_map = {
                "all": None,
                "hollywood": ["hollywood"],
                "bollywood": ["bollywood"],
                "anime": ["animeflix"]
            }
            cats = cat_map.get(category.lower(), None)

            # Execute streaming search and write SSE events chunk-by-chunk in real time!
            try:
                write_lock = threading.Lock()

                def _on_result(item):
                    event_data = {
                        "status": "item",
                        "item": {
                            "title": item.get("title", ""),
                            "url": item.get("url", ""),
                            "thumbnail": item.get("thumbnail", ""),
                            "category": item.get("category", "All")
                        }
                    }
                    with write_lock:
                        try:
                            self.wfile.write(f"data: {json.dumps(event_data)}\n\n".encode('utf-8'))
                            self.wfile.flush()
                        except Exception:
                            pass

                # Perform parallel category search using callback
                search_movies(query, cats, on_result_callback=_on_result)
                
                # Write search completion event
                with write_lock:
                    self.wfile.write(f"data: {json.dumps({'status': 'done'})}\n\n".encode('utf-8'))
                    self.wfile.flush()
            except Exception as e:
                err_data = {"status": "error", "message": str(e)}
                try:
                    self.wfile.write(f"data: {json.dumps(err_data)}\n\n".encode('utf-8'))
                    self.wfile.flush()
                except Exception:
                    pass
            return

        # Not Found
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ""

        # 1. Native folder browser folder picker trigger
        if parsed.path == '/api/choose-folder':
            folder = DOWNLOAD_MGR.ask_directory()
            if folder:
                self.send_json({"cancelled": False, "path": folder})
            else:
                self.send_json({"cancelled": True})
            return

        # 2. Queue Direct URL/Quality link
        if parsed.path == '/api/download':
            try:
                data = json.loads(body)
                url = data.get("url")
                output_dir = data.get("output_dir", DOWNLOAD_MGR.output_dir)
                
                if not url:
                    self.send_json({"error": "Missing url body param"}, 400)
                    return

                DOWNLOAD_MGR.start_pipeline(url, output_dir)
                self.send_json({"status": "success", "message": "Pipeline initiated"})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # 3. Retry individual failed card
        if parsed.path == '/api/retry':
            try:
                data = json.loads(body)
                idx = int(data.get("index", -1))
                if idx < 0 or idx >= len(DOWNLOAD_MGR.cards):
                    self.send_json({"error": "Invalid index"}, 400)
                    return

                card = DOWNLOAD_MGR.cards[idx]
                item = card.item_data
                
                if not item:
                    self.send_json({"error": "Card has no metadata item data"}, 400)
                    return

                card.mark_pending()
                if item.get("method") == "TELEGRAM":
                    DOWNLOAD_MGR.start_telegram_manual(idx, is_retry=True)
                else:
                    DOWNLOAD_MGR.download_queue.put((idx, item))
                    threading.Thread(target=DOWNLOAD_MGR._download_worker, daemon=True).start()

                self.send_json({"status": "success"})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # 4. Retry all failed cards concurrently
        if parsed.path == '/api/retry-all':
            retried = 0
            for idx, card in enumerate(DOWNLOAD_MGR.cards):
                if card.state == 3:  # Failed state
                    item = card.item_data
                    if not item:
                        continue
                    card.mark_pending()
                    retried += 1
                    if item.get("method") == "TELEGRAM":
                        DOWNLOAD_MGR.start_telegram_manual(idx, is_retry=True)
                    else:
                        DOWNLOAD_MGR.download_queue.put((idx, item))
            
            if retried > 0:
                threading.Thread(target=DOWNLOAD_MGR._download_worker, daemon=True).start()
            self.send_json({"status": "success", "retried": retried})
            return

        self.send_response(404)
        self.end_headers()


# Threaded TCPServer helper to support concurrent requests beautifully
class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    print(r"""
    ___       _            ____                _ 
   /   \ _ __(_)   _____  / ___|  ___  ___  __| |
  / /\ /| '__| \ \ / / _ \ \___ \ / _ \/ _ \/ _` |
 / /_// | |  | |\ V /  __/  ___) |  __/  __/ (_| |
/___,'  |_|  |_| \_/ \___| |____/ \___|\___|\__,_|
                                                 
    """, flush=True)
    print("=== DriveSeed Standalone Web Downloader Server ===", flush=True)

    port = int(os.environ.get("PORT", 5555))
    server_address = ('', port)
    
    try:
        httpd = ThreadedHTTPServer(server_address, APIRequestHandler)
    except OSError as e:
        print(f"[-] Error starting server on port {port}: {e}", flush=True)
        sys.exit(1)

    print(f"[+] Server started successfully on port {port}", flush=True)
    
    # Auto-open browser window in background only if not in cloud mode
    if not DOWNLOAD_MGR.cloud_mode:
        print("[*] Automatically launching your browser window...", flush=True)
        webbrowser.open(f"http://localhost:{port}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[-] Keyboard interrupt received. Shutting down server...", flush=True)
        httpd.server_close()
        sys.exit(0)

if __name__ == "__main__":
    main()
