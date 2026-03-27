from __future__ import annotations

import ctypes
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List


# ==========================================================
# CONFIG
# ==========================================================
#TARGET_TIMESTAMP_TEXT = "2003-02-28,10:54:40.00,GMT OFFSET -08:00" #substance us
#TARGET_TIMESTAMP_TEXT = "2002-07-17,12:16:21.00,GMT OFFSET +09:00" #document

MAX_WORKERS = max(4, os.cpu_count() or 4)


# ==========================================================
# WINDOWS API
# ==========================================================
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

GENERIC_WRITE = 0x40000000
FILE_WRITE_ATTRIBUTES = 0x0100
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
OPEN_EXISTING = 3
FILE_ATTRIBUTE_NORMAL = 0x00000080
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


class FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", ctypes.c_uint32),
        ("dwHighDateTime", ctypes.c_uint32),
    ]


kernel32.CreateFileW.argtypes = [
    ctypes.c_wchar_p,
    ctypes.c_uint32,
    ctypes.c_uint32,
    ctypes.c_void_p,
    ctypes.c_uint32,
    ctypes.c_uint32,
    ctypes.c_void_p,
]
kernel32.CreateFileW.restype = ctypes.c_void_p

kernel32.SetFileTime.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(FILETIME),
    ctypes.c_void_p,
    ctypes.POINTER(FILETIME),
]
kernel32.SetFileTime.restype = ctypes.c_int

kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
kernel32.CloseHandle.restype = ctypes.c_int


# ==========================================================
# HELPERS
# ==========================================================
def pause_and_exit(code: int = 1) -> None:
    try:
        input("\nPress ENTER to exit...")
    except EOFError:
        pass
    raise SystemExit(code)


def parse_target_datetime() -> datetime:
    parts = [part.strip() for part in TARGET_TIMESTAMP_TEXT.split(",")]
    if len(parts) != 3:
        raise ValueError(f"Invalid TARGET_TIMESTAMP_TEXT: {TARGET_TIMESTAMP_TEXT}")

    date_part = parts[0]
    time_part = parts[1]
    offset_part = parts[2]

    if not offset_part.upper().startswith("GMT OFFSET "):
        raise ValueError(f"Invalid GMT offset section: {offset_part}")

    offset_text = offset_part[len("GMT OFFSET "):].strip()

    local_dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S.%f")
    tz = timezone(parse_utc_offset(offset_text))
    return local_dt.replace(tzinfo=tz)


def parse_utc_offset(offset_text: str) -> timedelta:
    sign = 1
    text = offset_text.strip()

    if text.startswith("-"):
        sign = -1
        text = text[1:]
    elif text.startswith("+"):
        text = text[1:]

    hours_str, minutes_str = text.split(":")
    hours = int(hours_str)
    minutes = int(minutes_str)

    return sign * timedelta(hours=hours, minutes=minutes)


def datetime_to_filetime(dt: datetime) -> FILETIME:
    if dt.tzinfo is None:
        raise ValueError("datetime_to_filetime requires an aware datetime")

    utc_dt = dt.astimezone(timezone.utc)
    epoch = datetime(1601, 1, 1, tzinfo=timezone.utc)
    delta = utc_dt - epoch

    total_100ns = (
        (delta.days * 24 * 60 * 60 * 10_000_000)
        + (delta.seconds * 10_000_000)
        + (delta.microseconds * 10)
    )

    return FILETIME(
        dwLowDateTime=total_100ns & 0xFFFFFFFF,
        dwHighDateTime=(total_100ns >> 32) & 0xFFFFFFFF,
    )


def set_creation_and_modified_time(path: Path, target_dt: datetime) -> None:
    filetime = datetime_to_filetime(target_dt)

    handle = kernel32.CreateFileW(
        str(path),
        FILE_WRITE_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        None,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        result = kernel32.SetFileTime(
            handle,
            ctypes.byref(filetime),
            None,
            ctypes.byref(filetime),
        )
        if not result:
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel32.CloseHandle(handle)


def process_file(path: Path, target_dt: datetime) -> str:
    set_creation_and_modified_time(path, target_dt)
    return path.name


def get_png_files(script_dir: Path) -> List[Path]:
    return sorted(
        path
        for path in script_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".png"
    )


# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    if os.name != "nt":
        print("Error: Windows only.")
        pause_and_exit(1)

    script_dir = Path(__file__).resolve().parent

    try:
        target_dt = parse_target_datetime()
    except Exception as exc:
        print(f"Error parsing timestamp: {exc}")
        pause_and_exit(1)

    png_files = get_png_files(script_dir)

    if not png_files:
        print("No PNG files found.")
        pause_and_exit(0)

    print(f"Target timestamp: {TARGET_TIMESTAMP_TEXT}")
    print(f"Files: {len(png_files)}")
    print(f"Threads: {min(MAX_WORKERS, len(png_files))}\n")

    success = 0
    failures: List[str] = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(png_files))) as executor:
        futures = {executor.submit(process_file, p, target_dt): p for p in png_files}

        for future in as_completed(futures):
            path = futures[future]
            try:
                future.result()
                success += 1
                print(f"OK: {path.name}")
            except Exception as exc:
                failures.append(f"{path.name}: {exc}")
                print(f"ERROR: {path.name}: {exc}")

    print("\nDone.")
    print(f"Success: {success}")
    print(f"Failed: {len(failures)}")

    if failures:
        print("\nFailures:")
        for f in failures:
            print(f)
        pause_and_exit(1)

    pause_and_exit(0)


if __name__ == "__main__":
    main()