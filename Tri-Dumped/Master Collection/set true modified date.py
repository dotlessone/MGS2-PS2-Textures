from __future__ import annotations

import csv
import hashlib
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict
import ctypes
from ctypes import wintypes
import time


# ==========================================================
# CONFIG
# ==========================================================
ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection")
CSV_PATH = ROOT / "Metadata" / "mgs2_ps2_sha1_version_dates.csv"

MAX_WORKERS = os.cpu_count() or 8


# ==========================================================
# WINDOWS CREATION TIME SETTER
# ==========================================================
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

CreateFileW = kernel32.CreateFileW
SetFileTime = kernel32.SetFileTime
CloseHandle = kernel32.CloseHandle

CreateFileW.argtypes = [
    wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
    wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE
]
CreateFileW.restype = wintypes.HANDLE

SetFileTime.argtypes = [
    wintypes.HANDLE,
    ctypes.POINTER(wintypes.FILETIME),
    ctypes.POINTER(wintypes.FILETIME),
    ctypes.POINTER(wintypes.FILETIME),
]
SetFileTime.restype = wintypes.BOOL


GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
FILE_SHARE_READ = 1
FILE_SHARE_WRITE = 2


def unix_to_filetime(unix_time: int) -> wintypes.FILETIME:
    # FILETIME = 100-ns intervals since Jan 1, 1601
    ft = int((unix_time + 11644473600) * 10000000)
    return wintypes.FILETIME(ft & 0xFFFFFFFF, ft >> 32)


def set_creation_time(path: Path, unix_time: int) -> None:
    handle = CreateFileW(
        str(path),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_EXISTING,
        0,
        None,
    )

    if handle == wintypes.HANDLE(-1).value:
        raise OSError(f"Failed to open file: {path}")

    try:
        ft = unix_to_filetime(unix_time)
        if not SetFileTime(handle, ctypes.byref(ft), None, None):
            raise OSError(f"SetFileTime failed for {path}")
    finally:
        CloseHandle(handle)


# ==========================================================
# SHA1
# ==========================================================
def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(8 * 1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# ==========================================================
# LOAD CSV
# ==========================================================
def load_sha1_map(csv_path: Path) -> Dict[str, int]:
    out: Dict[str, int] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        for row in reader:
            if not row or row[0].startswith("#"):
                continue

            sha1 = row[0].strip().lower()
            unix_time = int(row[3])

            # keep earliest if duplicates exist
            if sha1 not in out or unix_time < out[sha1]:
                out[sha1] = unix_time

    return out


# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    print("Loading SHA1 map...")
    sha1_map = load_sha1_map(CSV_PATH)
    print(f"Loaded {len(sha1_map)} SHA1 entries")

    print("Scanning PNG files...")
    png_files = list(ROOT.rglob("*.png"))
    total = len(png_files)
    print(f"Found {total} PNGs")

    lock = threading.Lock()
    processed = 0
    matched = 0
    skipped = 0
    start_time = time.time()

    def worker(path: Path):
        nonlocal processed, matched, skipped

        try:
            file_sha1 = sha1_file(path)

            if file_sha1 in sha1_map:
                ts = sha1_map[file_sha1]

                # set modified + access time
                os.utime(path, (ts, ts))

                # set creation time
                set_creation_time(path, ts)

                with lock:
                    matched += 1
            else:
                with lock:
                    skipped += 1

        except Exception as e:
            print(f"[ERROR] {path}: {e}")

        finally:
            with lock:
                processed += 1

    print("Processing...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, p) for p in png_files]

        last_update = 0

        for _ in as_completed(futures):
            now = time.time()

            if now - last_update >= 1:
                with lock:
                    elapsed = now - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = total - processed
                    eta = remaining / rate if rate > 0 else 0

                    print(
                        f"\rProcessed: {processed}/{total} | "
                        f"Matched: {matched} | Skipped: {skipped} | "
                        f"ETA: {int(eta)}s",
                        end="",
                        flush=True
                    )

                last_update = now

    print("\n\nDone.")
    print(f"Matched: {matched}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()