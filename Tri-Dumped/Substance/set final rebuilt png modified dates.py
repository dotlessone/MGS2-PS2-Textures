from __future__ import annotations

import csv
import ctypes
import os
import sys
from ctypes import wintypes
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List
from concurrent.futures import ThreadPoolExecutor, as_completed


# ==========================================================
# CONFIG
# ==========================================================
CSV_NAME = "Compilation Dates.csv"
REGION_COLUMN = "REGION"
DATE_COLUMN = "DATE"
TIME_COLUMN = "TIME"
GMT_OFFSET_COLUMN = "GMT_OFFSET"

RECURSIVE = True
MAX_WORKERS = max(4, os.cpu_count() or 4)


# ==========================================================
# WINDOWS FILETIME SETUP
# ==========================================================
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

CreateFileW = kernel32.CreateFileW
SetFileTime = kernel32.SetFileTime
CloseHandle = kernel32.CloseHandle

INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

GENERIC_WRITE = 0x40000000
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
OPEN_EXISTING = 3
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

EPOCH_AS_FILETIME = 116444736000000000
HUNDREDS_OF_NANOSECONDS = 10000000


# ==========================================================
# DATA TYPES
# ==========================================================
@dataclass(frozen=True)
class RegionTimestamp:
    region: str
    dt_utc: datetime


# ==========================================================
# HELPERS
# ==========================================================
def parse_offset(offset_text: str) -> timezone:
    sign = -1 if offset_text.startswith("-") else 1
    hours, minutes = map(int, offset_text[1:].split(":"))
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def parse_csv_timestamp(date_text: str, time_text: str, offset_text: str) -> datetime:
    offset = parse_offset(offset_text)

    if "." in time_text:
        base, frac = time_text.split(".", 1)
        frac = (frac + "000000")[:6]
        dt_local = datetime.strptime(f"{date_text} {base}", "%Y-%m-%d %H:%M:%S")
        dt_local = dt_local.replace(microsecond=int(frac), tzinfo=offset)
    else:
        dt_local = datetime.strptime(f"{date_text} {time_text}", "%Y-%m-%d %H:%M:%S")
        dt_local = dt_local.replace(tzinfo=offset)

    return dt_local.astimezone(timezone.utc)


def datetime_to_filetime(dt_utc: datetime) -> wintypes.FILETIME:
    filetime_int = int(dt_utc.timestamp() * HUNDREDS_OF_NANOSECONDS) + EPOCH_AS_FILETIME
    return wintypes.FILETIME(filetime_int & 0xFFFFFFFF, filetime_int >> 32)


def set_windows_creation_time(path: Path, dt_utc: datetime) -> None:
    creation_ft = datetime_to_filetime(dt_utc)

    handle = CreateFileW(
        str(path),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        None,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )
    if handle == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        if not SetFileTime(handle, ctypes.byref(creation_ft), None, None):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        CloseHandle(handle)


def set_file_times(path: Path, dt_utc: datetime) -> None:
    ts = dt_utc.timestamp()
    os.utime(path, (ts, ts))
    set_windows_creation_time(path, dt_utc)


def load_region_timestamps(csv_path: Path) -> Dict[str, RegionTimestamp]:
    result: Dict[str, RegionTimestamp] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            region = row[REGION_COLUMN].strip().upper()
            dt_utc = parse_csv_timestamp(
                row[DATE_COLUMN],
                row[TIME_COLUMN],
                row[GMT_OFFSET_COLUMN],
            )

            result[region] = RegionTimestamp(region, dt_utc)

    return result


def iter_region_files(region_dir: Path) -> List[Path]:
    if RECURSIVE:
        return [p for p in region_dir.rglob("*") if p.is_file()]
    return [p for p in region_dir.iterdir() if p.is_file()]


# ==========================================================
# MULTITHREADED WORK
# ==========================================================
def process_region(region_dir: Path, region_timestamp: RegionTimestamp) -> int:
    files = iter_region_files(region_dir)
    total = len(files)

    if total == 0:
        return 0

    completed = 0

    def worker(path: Path):
        set_file_times(path, region_timestamp.dt_utc)
        return path

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, f) for f in files]

        for future in as_completed(futures):
            future.result()
            completed += 1

            # lightweight progress
            if completed % 1000 == 0 or completed == total:
                print(f"{region_dir.name}: {completed}/{total}")

    return completed


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if os.name != "nt":
        print("Windows only")
        return 1

    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / CSV_NAME

    region_map = load_region_timestamps(csv_path)

    total = 0

    for region, ts in region_map.items():
        region_dir = script_dir / region

        if not region_dir.exists():
            print(f"Missing folder: {region}")
            continue

        count = process_region(region_dir, ts)
        total += count

        print(f"[DONE] {region}: {count} files")

    print(f"\nTotal: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())