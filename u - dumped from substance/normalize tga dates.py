import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ==========================================================
# CONFIGURATION
# ==========================================================
ROOT_DIR = Path(__file__).parent  # or replace with Path("C:/path/to/start")
TARGET_EXTENSION = ".tga"

# Desired timestamp (GMT+09:00)
tz_japan = timezone(timedelta(hours=9))
desired_time = datetime(2002, 10, 23, 13, 38, 38, tzinfo=tz_japan)
# Convert to Unix timestamp (in local system time)
timestamp = desired_time.timestamp()

# ==========================================================
# MAIN
# ==========================================================
def set_file_times(path: Path, ts: float):
    try:
        os.utime(path, (ts, ts))  # modify access and modified times
        try:
            # On Windows, change creation time
            import ctypes
            from ctypes import wintypes, byref

            FILE_WRITE_ATTRIBUTES = 0x100
            handle = ctypes.windll.kernel32.CreateFileW(
                str(path),
                FILE_WRITE_ATTRIBUTES,
                0,
                None,
                3,
                0,
                None
            )
            if handle != -1:
                ctime = wintypes.FILETIME()
                # Convert seconds to 100-ns intervals since Jan 1, 1601 (UTC)
                intervals = int((ts + 11644473600) * 10000000)
                ctime.dwLowDateTime = intervals & 0xFFFFFFFF
                ctime.dwHighDateTime = intervals >> 32
                ctypes.windll.kernel32.SetFileTime(handle, byref(ctime), None, None)
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception as e:
            print(f"[WARN] Creation time update failed for {path}: {e}")
    except Exception as e:
        print(f"[ERROR] Could not set times for {path}: {e}")


def main():
    count = 0
    for root, _, files in os.walk(ROOT_DIR):
        for name in files:
            if name.lower().endswith(TARGET_EXTENSION):
                file_path = Path(root) / name
                set_file_times(file_path, timestamp)
                count += 1
    print(f"Updated timestamps on {count} .tga files.")


if __name__ == "__main__":
    main()
