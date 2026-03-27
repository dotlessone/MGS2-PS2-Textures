from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Tuple


# =========================================================
# CONFIG
# =========================================================
CSV_PATH = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

SOURCE_ROOT = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Japan\PNG"
)

DEST_ROOT = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Sons of Liberty\Japan"
)

SHA1_COLUMN = "mc_tri_dumped_sha1"
TEXTURE_NAME_COLUMN = "texture_name"

MAX_WORKERS = max(4, os.cpu_count() or 4)


# =========================================================
# GLOBALS (for progress)
# =========================================================
LOCK = Lock()


# =========================================================
# HELPERS
# =========================================================
def now_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def is_comment_or_empty(line: str) -> bool:
    stripped = line.strip()
    return not stripped or stripped.startswith(";") or stripped.startswith("//")


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def format_eta(seconds: float) -> str:
    if seconds <= 0 or seconds == float("inf"):
        return "--:--"

    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)

    if h > 0:
        return f"{h:02}:{m:02}:{s:02}"
    return f"{m:02}:{s:02}"


LAST_PRINT_TIME = 0.0


def print_progress(prefix: str, done: int, total: int, start_time: float) -> None:
    global LAST_PRINT_TIME

    now = time.time()

    # Only update once per second unless we're done
    if (now - LAST_PRINT_TIME) < 1.0 and done != total:
        return

    LAST_PRINT_TIME = now

    elapsed = now - start_time
    rate = done / elapsed if elapsed > 0 else 0
    remaining = (total - done) / rate if rate > 0 else float("inf")

    percent = (done / total) * 100 if total else 100

    msg = (
        f"\r{prefix} "
        f"{done:,}/{total:,} "
        f"({percent:6.2f}%) | "
        f"{rate:,.1f}/s | "
        f"ETA {format_eta(remaining)}"
    )

    sys.stdout.write(msg)
    sys.stdout.flush()


def remove_empty_dirs(root: Path) -> None:
    for current_root, _, _ in os.walk(root, topdown=False):
        p = Path(current_root)
        if p == root:
            continue
        try:
            next(p.iterdir())
        except StopIteration:
            p.rmdir()
        except OSError:
            pass


def read_csv_mapping(csv_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        filtered = (line for line in f if not is_comment_or_empty(line))
        reader = csv.DictReader(filtered)

        for row in reader:
            sha1 = row[SHA1_COLUMN].strip().lower()
            texture_name = row[TEXTURE_NAME_COLUMN].strip()

            if not sha1:
                continue

            existing = mapping.get(sha1)
            if existing and existing.lower() != texture_name.lower():
                raise RuntimeError(f"SHA1 collision in CSV: {sha1}")

            mapping[sha1] = texture_name

    return mapping


def collect_pngs(root: Path) -> List[Path]:
    return [p for p in root.rglob("*.png") if p.is_file()]


@dataclass
class FileResult:
    path: Path
    rel_parts: Tuple[str, ...]
    sha1: str


def hash_worker(path: Path, root: Path) -> FileResult:
    return FileResult(
        path=path,
        rel_parts=path.relative_to(root).parts,
        sha1=sha1_of_file(path),
    )


def write_csv(path: Path, header: List[str], rows: List[List[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    timestamp = now_timestamp()

    matched_log = Path(__file__).parent / f"matched_{timestamp}.csv"
    unmatched_log = Path(__file__).parent / f"unmatched_{timestamp}.csv"

    print("Loading CSV...")
    sha_map = read_csv_mapping(CSV_PATH)

    print("Collecting PNGs...")
    files = collect_pngs(SOURCE_ROOT)
    total = len(files)

    print(f"{total:,} files found\n")

    # =============================
    # HASHING PHASE
    # =============================
    print("Hashing...")

    start = time.time()
    results: List[FileResult] = []

    done = 0

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futures = [ex.submit(hash_worker, f, SOURCE_ROOT) for f in files]

        for future in as_completed(futures):
            results.append(future.result())

            with LOCK:
                done += 1
                if done % 50 == 0 or done == total:
                    print_progress("Hashing:", done, total, start)

    print()  # newline

    # =============================
    # PROCESSING PHASE
    # =============================
    print("Processing...")

    start = time.time()
    done = 0

    matched_rows: List[List[str]] = []
    unmatched_rows: List[List[str]] = []

    for item in results:
        tex = sha_map.get(item.sha1)

        if tex:
            dest = DEST_ROOT / f"{tex}.png"

            if dest.exists():
                if sha1_of_file(dest) != item.sha1:
                    raise RuntimeError(f"Collision:\n{item.path}\n{dest}")
                item.path.unlink()
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(item.path, dest)

            matched_rows.append([tex, *item.rel_parts])
        else:
            unmatched_rows.append(list(item.rel_parts))

        done += 1
        if done % 50 == 0 or done == total:
            print_progress("Processing:", done, total, start)

    print()

    print("Cleaning empty folders...")
    remove_empty_dirs(SOURCE_ROOT)

    # =============================
    # WRITE LOGS
    # =============================
    print("Writing logs...")

    max_m = max((len(r) for r in matched_rows), default=1)
    max_u = max((len(r) for r in unmatched_rows), default=1)

    mh = ["texture_name"] + [f"path_{i}" for i in range(max_m)]
    uh = [f"path_{i}" for i in range(max_u)]

    matched_rows = [r + [""] * (len(mh) - len(r)) for r in matched_rows]
    unmatched_rows = [r + [""] * (len(uh) - len(r)) for r in unmatched_rows]

    write_csv(matched_log, mh, matched_rows)
    write_csv(unmatched_log, uh, unmatched_rows)

    print("\nDone.")
    print(f"Matched:   {len(matched_rows):,}")
    print(f"Unmatched: {len(unmatched_rows):,}")
    print(matched_log)
    print(unmatched_log)


if __name__ == "__main__":
    main()