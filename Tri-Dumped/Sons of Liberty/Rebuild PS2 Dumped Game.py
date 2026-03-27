from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set, Tuple


# =========================================================
# CONFIG
# =========================================================
CSV_PATH = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

SOURCE_ROOT = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\JP\MGS2"
)

DEST_ROOT = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\JP"
)

SHA1_COLUMN = "mc_tri_dumped_sha1"
TEXTURE_NAME_COLUMN = "texture_name"

MANUAL_MATCHES_CSV_NAME = "manually_matched_textures.csv"
MANUAL_TEXTURE_NAME_COLUMN = "texture_name"
MANUAL_SHA1_COLUMN = "manual_sha1"

MAX_WORKERS = max(4, os.cpu_count() or 4)


# =========================================================
# GLOBALS
# =========================================================
LOCK = Lock()
LAST_PRINT_TIME = 0.0


# =========================================================
# HELPERS
# =========================================================
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


def print_progress(prefix: str, done: int, total: int, start_time: float) -> None:
    global LAST_PRINT_TIME

    now = time.time()

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


def add_sha_mapping(sha_to_texture: Dict[str, str], sha1: str, texture_name: str, source_name: str) -> None:
    existing = sha_to_texture.get(sha1)
    if existing and existing.lower() != texture_name.lower():
        raise RuntimeError(
            f"SHA1 collision while loading {source_name}: {sha1}\n"
            f"Existing texture_name: {existing}\n"
            f"New texture_name:      {texture_name}"
        )
    sha_to_texture[sha1] = texture_name


def read_base_csv_mapping(csv_path: Path) -> Tuple[Dict[str, str], Set[str]]:
    sha_to_texture: Dict[str, str] = {}
    all_texture_names: Set[str] = set()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        filtered = (line for line in f if not is_comment_or_empty(line))
        reader = csv.DictReader(filtered)

        for row in reader:
            sha1 = row[SHA1_COLUMN].strip().lower()
            texture_name = row[TEXTURE_NAME_COLUMN].strip()

            if not texture_name:
                continue

            all_texture_names.add(texture_name)

            if not sha1:
                continue

            add_sha_mapping(sha_to_texture, sha1, texture_name, str(csv_path))

    return sha_to_texture, all_texture_names


def read_manual_matches(
    manual_csv_path: Path,
    sha_to_texture: Dict[str, str],
    all_texture_names: Set[str],
) -> int:
    if not manual_csv_path.is_file():
        return 0

    added_count = 0

    with manual_csv_path.open("r", encoding="utf-8", newline="") as f:
        filtered = (line for line in f if not is_comment_or_empty(line))
        reader = csv.DictReader(filtered)

        for row in reader:
            texture_name = row[MANUAL_TEXTURE_NAME_COLUMN].strip()
            sha1 = row[MANUAL_SHA1_COLUMN].strip().lower()

            if not texture_name or not sha1:
                continue

            all_texture_names.add(texture_name)
            add_sha_mapping(sha_to_texture, sha1, texture_name, str(manual_csv_path))
            added_count += 1

    return added_count


def build_mapping(script_dir: Path) -> Tuple[Dict[str, str], Set[str], Path, int]:
    sha_to_texture, all_texture_names = read_base_csv_mapping(CSV_PATH)

    manual_csv_path = script_dir / MANUAL_MATCHES_CSV_NAME
    manual_count = read_manual_matches(manual_csv_path, sha_to_texture, all_texture_names)

    return sha_to_texture, all_texture_names, manual_csv_path, manual_count


def collect_pngs(root: Path) -> List[Path]:
    return [p for p in root.rglob("*.png") if p.is_file()]


def collect_dest_texture_names(root: Path) -> Set[str]:
    return {p.stem for p in root.rglob("*.png") if p.is_file()}


def write_csv(path: Path, header: List[str], rows: List[List[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


@dataclass
class FileResult:
    path: Path
    rel_path: str
    sha1: str


def hash_worker(path: Path, root: Path) -> FileResult:
    return FileResult(
        path=path,
        rel_path=path.relative_to(root).as_posix(),
        sha1=sha1_of_file(path),
    )


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    script_dir = Path(__file__).resolve().parent

    matched_log = script_dir / "matched_moves.csv"
    unmatched_log = script_dir / "unmatched_paths.csv"
    missing_textures_log = script_dir / "missing_textures.csv"

    print("Loading CSVs...")
    sha_map, all_texture_names, manual_csv_path, manual_count = build_mapping(script_dir)

    if manual_csv_path.is_file():
        print(f"Loaded manual matches: {manual_count:,} from {manual_csv_path}")
    else:
        print(f"No manual matches CSV found at: {manual_csv_path}")

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

    print()

    # =============================
    # PROCESSING PHASE
    # =============================
    print("Processing...")

    start = time.time()
    done = 0

    matched_rows: List[List[str]] = []
    unmatched_by_sha1: Dict[str, str] = {}

    for item in results:
        tex = sha_map.get(item.sha1)

        if tex:
            dest = DEST_ROOT / f"{tex}.png"

            if dest.exists():
                existing_dest_sha1 = sha1_of_file(dest)
                if existing_dest_sha1 != item.sha1:
                    raise RuntimeError(
                        f"Collision:\n"
                        f"Source: {item.path}\n"
                        f"Dest:   {dest}\n"
                        f"Source SHA1: {item.sha1}\n"
                        f"Dest SHA1:   {existing_dest_sha1}"
                    )
                item.path.unlink()
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(item.path), str(dest))

            matched_rows.append([tex, item.rel_path])
        else:
            if item.sha1 not in unmatched_by_sha1:
                unmatched_by_sha1[item.sha1] = item.rel_path

        done += 1
        if done % 50 == 0 or done == total:
            print_progress("Processing:", done, total, start)

    print()

    print("Cleaning empty folders...")
    remove_empty_dirs(SOURCE_ROOT)

    # =============================
    # MISSING TEXTURES
    # =============================
    print("Scanning DEST_ROOT for missing textures...")
    existing_dest_textures = collect_dest_texture_names(DEST_ROOT)
    missing_texture_rows = [[name] for name in sorted(all_texture_names - existing_dest_textures)]

    # =============================
    # WRITE LOGS
    # =============================
    print("Writing logs...")

    matched_rows.sort(key=lambda row: (row[0].lower(), row[1].lower()))
    unmatched_rows = [[rel_path] for _, rel_path in sorted(unmatched_by_sha1.items(), key=lambda x: x[1].lower())]

    write_csv(matched_log, ["texture_name", "relative_path"], matched_rows)
    write_csv(unmatched_log, ["relative_path"], unmatched_rows)
    write_csv(missing_textures_log, ["texture_name"], missing_texture_rows)

    print("\nDone.")
    print(f"Matched:               {len(matched_rows):,}")
    print(f"Unmatched SHA1s:       {len(unmatched_rows):,}")
    print(f"Missing textures:      {len(missing_texture_rows):,}")
    print(f"Manual matches added:  {manual_count:,}")
    print(matched_log)
    print(unmatched_log)
    print(missing_textures_log)


if __name__ == "__main__":
    main()