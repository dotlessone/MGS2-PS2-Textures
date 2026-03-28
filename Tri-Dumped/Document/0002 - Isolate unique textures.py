from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ==========================================================
# CONFIG
# ==========================================================
WORK_DIR = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document")

SHA1_LIST_PATHS = [
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\USA\Sons of Liberty_USA_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Spain\Sons of Liberty_Spain_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Korea\Sons of Liberty_Korea_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Japan (Premium)\Sons of Liberty_Japan (Premium)_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Japan (Daini)\Sons of Liberty_Japan (Daini)_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Japan\Sons of Liberty_Japan_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Italy\Sons of Liberty_Italy_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Europe (Demo)\Sons of Liberty_Europe (Demo)_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Europe\Sons of Liberty_Europe_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\EU\Substance_EU_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\JP\Substance_JP_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\KR\Substance_KR_ALL_SHA1s.txt",
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\US\Substance_US_ALL_SHA1s.txt",
]

MAX_WORKERS = max(4, os.cpu_count() or 4)

LOCK = Lock()

# ==========================================================
# HELPERS
# ==========================================================
def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_all_sha1s() -> set[str]:
    sha1s = set()

    for p in SHA1_LIST_PATHS:
        path = Path(p)
        if not path.exists():
            raise RuntimeError(f"Missing SHA1 list: {path}")

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().lower()
                if line:
                    sha1s.add(line)

    return sha1s


def remove_empty_dirs(root: Path) -> None:
    # bottom-up cleanup
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        p = Path(dirpath)
        if not any(p.iterdir()):
            try:
                p.rmdir()
            except Exception:
                pass


# ==========================================================
# MAIN
# ==========================================================
def main():
    if not WORK_DIR.exists():
        raise RuntimeError(f"WORK_DIR does not exist: {WORK_DIR}")

    print("Loading SHA1 lists...")
    all_sha1s = load_all_sha1s()
    print(f"Loaded {len(all_sha1s)} SHA1s")

    print("Scanning PNG files...")
    png_files = [p for p in WORK_DIR.rglob("*.png")]

    total = len(png_files)
    if total == 0:
        print("No PNGs found.")
        return

    print(f"Found {total} PNGs")

    start_time = time.time()
    last_update = 0

    processed = 0
    deleted = 0

    def worker(path: Path):
        nonlocal deleted

        try:
            sha1 = sha1_of_file(path)

            if sha1 in all_sha1s:
                path.unlink()
                with LOCK:
                    deleted += 1

        except Exception as e:
            print(f"\nError processing {path}: {e}")

        return 1

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, p) for p in png_files]

        for f in as_completed(futures):
            processed += f.result()

            now = time.time()
            if now - last_update >= 1:
                elapsed = now - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = total - processed
                eta = remaining / rate if rate > 0 else 0

                print(
                    f"\rProcessed: {processed}/{total} | Deleted: {deleted} | ETA: {int(eta)}s",
                    end="",
                    flush=True,
                )

                last_update = now

    print()
    print(f"Done. Deleted {deleted} files.")

    print("Cleaning empty folders...")
    remove_empty_dirs(WORK_DIR)

    print("Finished.")


if __name__ == "__main__":
    main()