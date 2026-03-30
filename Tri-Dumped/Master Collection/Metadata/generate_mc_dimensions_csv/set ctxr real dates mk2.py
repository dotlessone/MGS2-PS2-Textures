from __future__ import annotations

import csv
import ctypes
import hashlib
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from ctypes import wintypes
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ==========================================================
# CONFIG
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent

METADATA_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_dimensions_including_override_folders.csv"
)

EXE_NAME = "metal gear solid2.exe"

MAX_WORKERS = max(4, os.cpu_count() or 1)
HASH_CHUNK_SIZE = 8 * 1024 * 1024
PROGRESS_INTERVAL_SECONDS = 1.0

LOG_MATCHED = SCRIPT_DIR / "set_dates_matched.txt"
LOG_FAILED = SCRIPT_DIR / "set_dates_failed.txt"


# ==========================================================
# WINDOWS FILE TIME
# ==========================================================
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

CreateFileW = kernel32.CreateFileW
CreateFileW.argtypes = [
    wintypes.LPCWSTR,
    wintypes.DWORD,
    wintypes.DWORD,
    wintypes.LPVOID,
    wintypes.DWORD,
    wintypes.DWORD,
    wintypes.HANDLE,
]
CreateFileW.restype = wintypes.HANDLE

SetFileTime = kernel32.SetFileTime
SetFileTime.argtypes = [
    wintypes.HANDLE,
    ctypes.POINTER(wintypes.FILETIME),
    ctypes.POINTER(wintypes.FILETIME),
    ctypes.POINTER(wintypes.FILETIME),
]
SetFileTime.restype = wintypes.BOOL

CloseHandle = kernel32.CloseHandle
CloseHandle.argtypes = [wintypes.HANDLE]
CloseHandle.restype = wintypes.BOOL

INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

GENERIC_WRITE = 0x40000000
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
OPEN_EXISTING = 3
FILE_ATTRIBUTE_NORMAL = 0x00000080
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

EPOCH_AS_FILETIME = 116444736000000000
HUNDREDS_OF_NANOSECONDS = 10000000


def unix_time_to_filetime(unix_time: int) -> wintypes.FILETIME:
    filetime_value = EPOCH_AS_FILETIME + (unix_time * HUNDREDS_OF_NANOSECONDS)
    return wintypes.FILETIME(
        filetime_value & 0xFFFFFFFF,
        (filetime_value >> 32) & 0xFFFFFFFF,
    )


def set_file_creation_and_modified_time(path: Path, unix_time: int) -> None:
    filetime = unix_time_to_filetime(unix_time)

    handle = CreateFileW(
        str(path),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        None,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        if not SetFileTime(handle, ctypes.byref(filetime), None, ctypes.byref(filetime)):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        CloseHandle(handle)

    os.utime(path, (unix_time, unix_time))


# ==========================================================
# HELPERS
# ==========================================================
def ensure_safe_location() -> None:
    candidates = [
        SCRIPT_DIR / EXE_NAME,
        SCRIPT_DIR.parent / EXE_NAME,
        SCRIPT_DIR.parent.parent / EXE_NAME,
    ]

    for candidate in candidates:
        if candidate.is_file():
            print(f"Safety check passed: found '{EXE_NAME}' at {candidate}")
            return

    raise RuntimeError(
        f"Safety check failed. '{EXE_NAME}' was not found alongside the script, "
        f"in the parent folder, or in the grandparent folder."
    )


def sha1_file(path: Path) -> str:
    digest = hashlib.sha1()

    with path.open("rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def load_metadata(csv_path: Path) -> Dict[str, int]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Metadata CSV not found: {csv_path}")

    out: Dict[str, int] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"mc_ctxr_sha1", "version_unix_time"}
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"Metadata CSV missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            sha1_value = (row.get("mc_ctxr_sha1") or "").strip().lower()
            version_unix_time = (row.get("version_unix_time") or "").strip()

            if not sha1_value or not version_unix_time:
                continue

            try:
                unix_time = int(version_unix_time)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid version_unix_time '{version_unix_time}' for sha1 '{sha1_value}'"
                ) from exc

            out[sha1_value] = unix_time

    return out


def find_files_to_scan() -> List[Path]:
    excluded_paths = {
        METADATA_CSV.resolve(),
        Path(__file__).resolve(),
        LOG_MATCHED.resolve(),
        LOG_FAILED.resolve(),
    }

    files: List[Path] = []

    for path in SCRIPT_DIR.rglob("*"):
        if not path.is_file():
            continue

        try:
            resolved = path.resolve()
        except OSError:
            resolved = path

        if resolved in excluded_paths:
            continue

        files.append(path)

    files.sort(key=lambda p: str(p).lower())
    return files


def relpath_str(path: Path) -> str:
    try:
        return path.relative_to(SCRIPT_DIR).as_posix()
    except ValueError:
        return str(path)


# ==========================================================
# PROGRESS
# ==========================================================
class ProgressState:
    def __init__(self, total: int) -> None:
        self.total = total
        self.processed = 0
        self.matched = 0
        self.updated = 0
        self.failed = 0
        self._lock = threading.Lock()
        self._start = time.time()

    def increment_processed(self) -> None:
        with self._lock:
            self.processed += 1

    def increment_matched(self) -> None:
        with self._lock:
            self.matched += 1

    def increment_updated(self) -> None:
        with self._lock:
            self.updated += 1

    def increment_failed(self) -> None:
        with self._lock:
            self.failed += 1

    def snapshot(self) -> Tuple[int, int, int, int, int, float]:
        with self._lock:
            elapsed = time.time() - self._start
            return (
                self.total,
                self.processed,
                self.matched,
                self.updated,
                self.failed,
                elapsed,
            )


def progress_worker(state: ProgressState, stop_event: threading.Event) -> None:
    while not stop_event.wait(PROGRESS_INTERVAL_SECONDS):
        total, processed, matched, updated, failed, elapsed = state.snapshot()

        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = total - processed
        eta_seconds = remaining / rate if rate > 0 else 0.0

        print(
            f"Processed {processed}/{total} | "
            f"Matched {matched} | "
            f"Updated {updated} | "
            f"Failed {failed} | "
            f"ETA {eta_seconds:.1f}s"
        )


# ==========================================================
# WORKER
# ==========================================================
def process_file(
    path: Path,
    sha1_to_unix: Dict[str, int],
) -> Dict[str, Optional[str]]:
    sha1_value = sha1_file(path).lower()
    unix_time = sha1_to_unix.get(sha1_value)

    if unix_time is None:
        return {
            "status": "no_match",
            "path": str(path),
            "sha1": sha1_value,
            "unix_time": None,
            "error": None,
        }

    set_file_creation_and_modified_time(path, unix_time)

    return {
        "status": "matched_and_updated",
        "path": str(path),
        "sha1": sha1_value,
        "unix_time": str(unix_time),
        "error": None,
    }


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    ensure_safe_location()

    sha1_to_unix = load_metadata(METADATA_CSV)
    print(f"Loaded {len(sha1_to_unix)} mc_ctxr_sha1 -> version_unix_time mappings.")

    files = find_files_to_scan()
    if not files:
        print("No files found under the script folder.")
        if LOG_MATCHED.exists():
            LOG_MATCHED.unlink()
        if LOG_FAILED.exists():
            LOG_FAILED.unlink()
        return 0

    print(f"Found {len(files)} files to scan.")
    print(f"Using {MAX_WORKERS} worker threads.")

    state = ProgressState(total=len(files))
    stop_event = threading.Event()
    progress_thread = threading.Thread(
        target=progress_worker,
        args=(state, stop_event),
        daemon=True,
    )
    progress_thread.start()

    matched_lines: List[str] = []
    failed_lines: List[str] = []

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_file, path, sha1_to_unix): path
                for path in files
            }

            for future in as_completed(futures):
                path = futures[future]

                try:
                    result = future.result()

                    if result["status"] == "matched_and_updated":
                        state.increment_matched()
                        state.increment_updated()
                        matched_lines.append(
                            f"{relpath_str(path)},{result['sha1']},{result['unix_time']}"
                        )

                except Exception as exc:
                    state.increment_failed()
                    failed_lines.append(f"{relpath_str(path)},{exc}")

                finally:
                    state.increment_processed()

    finally:
        stop_event.set()
        progress_thread.join()

    matched_lines.sort(key=str.lower)
    failed_lines.sort(key=str.lower)

    if matched_lines:
        with LOG_MATCHED.open("w", encoding="utf-8", newline="\n") as f:
            for line in matched_lines:
                f.write(line + "\n")
    else:
        if LOG_MATCHED.exists():
            LOG_MATCHED.unlink()

    if failed_lines:
        with LOG_FAILED.open("w", encoding="utf-8", newline="\n") as f:
            for line in failed_lines:
                f.write(line + "\n")
    else:
        if LOG_FAILED.exists():
            LOG_FAILED.unlink()

    total, processed, matched, updated, failed, elapsed = state.snapshot()

    print()
    print(f"Processed: {processed}/{total}")
    print(f"Matched:   {matched}")
    print(f"Updated:   {updated}")
    print(f"Failed:    {failed}")
    print(f"Elapsed:   {elapsed:.2f}s")

    if matched_lines:
        print(f"Wrote matched log: {LOG_MATCHED}")

    if failed_lines:
        print(f"Wrote failure log: {LOG_FAILED}")

    if failed:
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())