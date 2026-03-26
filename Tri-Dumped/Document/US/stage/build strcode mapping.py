from __future__ import annotations

import csv
import hashlib
import os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


# ==========================================================
# CONFIG
# ==========================================================
STAGE_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\US\MGS2")
MC_METADATA_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

MC_DELETE_LOG = STAGE_ROOT / "deleted_matching_mc_sha1s.csv"
REMAINING_MAP_CSV = STAGE_ROOT / "remaining_files_by_sha1.csv"
DEDUPE_DELETE_LOG = STAGE_ROOT / "deleted_duplicate_sha1s.csv"

MAX_WORKERS = max(4, os.cpu_count() or 4)
SHA1_BUFFER_SIZE = 8 * 1024 * 1024


# ==========================================================
# DATA TYPES
# ==========================================================
@dataclass(frozen=True)
class FileHashResult:
    path: Path
    sha1: str


# ==========================================================
# HELPERS
# ==========================================================
def pause_and_exit(code: int = 1) -> None:
    try:
        input("\nPress ENTER to exit...")
    finally:
        raise SystemExit(code)


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        while True:
            chunk = f.read(SHA1_BUFFER_SIZE)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest().lower()


def load_mc_sha1s(csv_path: Path) -> set[str]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"MC metadata CSV not found: {csv_path}")

    sha1s: set[str] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = "mc_tri_dumped_sha1"
        if not reader.fieldnames or required not in reader.fieldnames:
            raise ValueError(
                f"CSV is missing required column '{required}': {csv_path}"
            )

        for row in reader:
            value = (row.get("mc_tri_dumped_sha1") or "").strip().lower()
            if value:
                sha1s.add(value)

    return sha1s


def iter_pngs(root: Path) -> List[Path]:
    return sorted(
        path
        for path in root.rglob("*.png")
        if path.is_file()
    )


def hash_one(file_path: Path) -> FileHashResult:
    return FileHashResult(file_path, sha1_of_file(file_path))


def hash_files(paths: Sequence[Path]) -> List[FileHashResult]:
    results: List[FileHashResult] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(hash_one, path) for path in paths]

        processed = 0
        total = len(paths)

        for future in as_completed(futures):
            processed += 1

            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error hashing file: {e}")

            if processed % 250 == 0 or processed == total:
                print(f"Processed {processed:,}/{total:,}")

    return results


def build_log_row(root: Path, file_path: Path, sha1: str) -> List[str]:
    rel_parts = file_path.relative_to(root).parts
    subfolders = list(rel_parts[:-1])
    filename = rel_parts[-1]
    return [file_path.stem.lower(), sha1, *subfolders, filename]


def write_rows_csv(rows: Sequence[Sequence[str]], output_path: Path) -> None:
    max_columns = max((len(row) for row in rows), default=0)

    header = ["texture_name", "sha1"]
    if max_columns > 3:
        header.extend(f"subfolder_{i}" for i in range(1, max_columns - 2))
    header.append("filename")

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)

        for row in rows:
            padded = list(row) + [""] * (max_columns - len(row))
            writer.writerow(padded)


def remove_empty_dirs(root: Path) -> int:
    removed = 0

    dirs = sorted(
        [p for p in root.rglob("*") if p.is_dir()],
        key=lambda p: len(p.parts),
        reverse=True,
    )

    for d in dirs:
        try:
            if not any(d.iterdir()):
                d.rmdir()
                removed += 1
        except Exception:
            pass

    return removed


def safe_unlink(path: Path) -> bool:
    try:
        path.unlink()
        return True
    except Exception as e:
        print(f"Failed to delete {path}: {e}")
        return False


def relative_folder_key(root: Path, path: Path) -> str:
    rel_parent = path.parent.relative_to(root)
    return str(rel_parent).lower().replace("\\", "/")


def relative_file_key(root: Path, path: Path) -> str:
    return str(path.relative_to(root)).lower().replace("\\", "/")


# ==========================================================
# DEDUPE ASSIGNMENT
# ==========================================================
def choose_duplicate_keeps(
    root: Path,
    results: Sequence[FileHashResult],
) -> Dict[str, Path]:
    by_sha1: Dict[str, List[FileHashResult]] = defaultdict(list)
    for item in results:
        by_sha1[item.sha1].append(item)

    # Base count = files whose sha1 is already unique
    final_folder_counts: Counter[str] = Counter()

    for sha1, items in by_sha1.items():
        if len(items) == 1:
            folder = relative_folder_key(root, items[0].path)
            final_folder_counts[folder] += 1

    duplicate_groups = [
        (sha1, sorted(items, key=lambda x: relative_file_key(root, x.path)))
        for sha1, items in by_sha1.items()
        if len(items) > 1
    ]

    # Stable ordering so assignment is deterministic
    duplicate_groups.sort(
        key=lambda pair: (
            -len(pair[1]),
            pair[0],
        )
    )

    keep_map: Dict[str, Path] = {}

    for sha1, items in duplicate_groups:
        best_item: FileHashResult | None = None
        best_score: tuple[int, str, str] | None = None

        for item in items:
            folder = relative_folder_key(root, item.path)
            score = (
                final_folder_counts[folder] + 1,
                folder,
                relative_file_key(root, item.path),
            )

            if best_score is None or score > best_score:
                best_score = score
                best_item = item

        assert best_item is not None

        chosen_folder = relative_folder_key(root, best_item.path)
        final_folder_counts[chosen_folder] += 1
        keep_map[sha1] = best_item.path

    return keep_map


# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    if not STAGE_ROOT.is_dir():
        print(f"Error: stage folder not found:\n{STAGE_ROOT}")
        pause_and_exit()

    try:
        mc_sha1s = load_mc_sha1s(MC_METADATA_CSV)
    except Exception as e:
        print(f"Error loading MC metadata CSV:\n{e}")
        pause_and_exit()

    print(f"Loaded {len(mc_sha1s):,} unique MC SHA1s.")

    # ------------------------------------------------------
    # PASS 1: DELETE FILES WHOSE SHA1 EXISTS IN MC CSV
    # ------------------------------------------------------
    all_pngs = iter_pngs(STAGE_ROOT)
    if not all_pngs:
        print(f"No PNG files found under:\n{STAGE_ROOT}")
        return

    print(f"Found {len(all_pngs):,} PNG files for MC-match pass.")
    hashed = hash_files(all_pngs)

    mc_delete_rows: List[List[str]] = []
    mc_delete_paths: List[Path] = []

    for item in hashed:
        if item.sha1 in mc_sha1s:
            mc_delete_rows.append(build_log_row(STAGE_ROOT, item.path, item.sha1))
            mc_delete_paths.append(item.path)

    mc_delete_rows.sort(key=lambda row: [part.lower() for part in row])
    write_rows_csv(mc_delete_rows, MC_DELETE_LOG)
    print(f"Wrote MC delete log: {MC_DELETE_LOG}")

    deleted_mc_count = 0
    for path in mc_delete_paths:
        if safe_unlink(path):
            deleted_mc_count += 1

    print(f"Deleted {deleted_mc_count:,} PNG(s) matching MC SHA1s.")

    removed_dirs = remove_empty_dirs(STAGE_ROOT)
    print(f"Removed {removed_dirs:,} empty folder(s) after MC-match cleanup.")

    # ------------------------------------------------------
    # PASS 2: MAP REMAINING FILES
    # ------------------------------------------------------
    remaining_pngs = iter_pngs(STAGE_ROOT)
    if not remaining_pngs:
        print("No PNG files remain after MC-match cleanup.")
        return

    print(f"Found {len(remaining_pngs):,} remaining PNG files for dedupe pass.")
    remaining_hashed = hash_files(remaining_pngs)

    remaining_map_rows = [
        build_log_row(STAGE_ROOT, item.path, item.sha1)
        for item in remaining_hashed
    ]
    remaining_map_rows.sort(key=lambda row: [part.lower() for part in row])
    write_rows_csv(remaining_map_rows, REMAINING_MAP_CSV)
    print(f"Wrote remaining mapping CSV: {REMAINING_MAP_CSV}")

    # ------------------------------------------------------
    # PASS 3: DEDUPE REMAINING SHA1S
    # ------------------------------------------------------
    by_sha1: Dict[str, List[FileHashResult]] = defaultdict(list)
    for item in remaining_hashed:
        by_sha1[item.sha1].append(item)

    duplicate_sha1_count = sum(1 for items in by_sha1.values() if len(items) > 1)
    duplicate_file_count = sum(len(items) for items in by_sha1.values() if len(items) > 1)

    if duplicate_sha1_count == 0:
        print("No duplicate SHA1s remain.")
        removed_dirs = remove_empty_dirs(STAGE_ROOT)
        print(f"Removed {removed_dirs:,} empty folder(s) after final cleanup.")
        print("Done.")
        return

    print(
        f"Found {duplicate_sha1_count:,} duplicate SHA1 group(s) "
        f"covering {duplicate_file_count:,} file(s)."
    )

    keep_map = choose_duplicate_keeps(STAGE_ROOT, remaining_hashed)

    dedupe_delete_rows: List[List[str]] = []
    dedupe_delete_paths: List[Path] = []

    for sha1, items in by_sha1.items():
        if len(items) <= 1:
            continue

        keep_path = keep_map[sha1]

        for item in items:
            if item.path == keep_path:
                continue

            dedupe_delete_rows.append(build_log_row(STAGE_ROOT, item.path, item.sha1))
            dedupe_delete_paths.append(item.path)

    dedupe_delete_rows.sort(key=lambda row: [part.lower() for part in row])
    write_rows_csv(dedupe_delete_rows, DEDUPE_DELETE_LOG)
    print(f"Wrote duplicate delete log: {DEDUPE_DELETE_LOG}")

    deleted_dedupe_count = 0
    for path in dedupe_delete_paths:
        if safe_unlink(path):
            deleted_dedupe_count += 1

    print(f"Deleted {deleted_dedupe_count:,} duplicate PNG(s).")

    removed_dirs = remove_empty_dirs(STAGE_ROOT)
    print(f"Removed {removed_dirs:,} empty folder(s) after dedupe cleanup.")

    print("Done.")


if __name__ == "__main__":
    main()