from __future__ import annotations

import hashlib
import os
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


# ==========================================================
# CONFIG
# ==========================================================
EU_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Working\JP")
BASE_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection")

DRY_RUN = False
MAX_WORKERS = max(4, os.cpu_count() or 4)
HASH_BUFFER_SIZE = 8 * 1024 * 1024


# ==========================================================
# DATA
# ==========================================================
@dataclass(frozen=True)
class FilePair:
    eu_path: Path
    base_path: Path
    relative_path: Path


# ==========================================================
# HELPERS
# ==========================================================
def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(HASH_BUFFER_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def build_pairs() -> tuple[list[FilePair], list[FilePair]]:
    pairs: list[FilePair] = []
    missing_in_base: list[FilePair] = []

    for eu_path in EU_ROOT.rglob("*"):
        if not eu_path.is_file():
            continue

        relative_path = eu_path.relative_to(EU_ROOT)
        base_path = BASE_ROOT / relative_path

        pair = FilePair(
            eu_path=eu_path,
            base_path=base_path,
            relative_path=relative_path,
        )

        if base_path.is_file():
            pairs.append(pair)
        else:
            missing_in_base.append(pair)

    return pairs, missing_in_base


def compare_pair(pair: FilePair) -> tuple[FilePair, str, str]:
    eu_sha1 = sha1_of_file(pair.eu_path)
    base_sha1 = sha1_of_file(pair.base_path)
    return pair, eu_sha1, base_sha1


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if not EU_ROOT.is_dir():
        print(f"Error: EU root does not exist: {EU_ROOT}")
        return 1

    if not BASE_ROOT.is_dir():
        print(f"Error: Base root does not exist: {BASE_ROOT}")
        return 1

    pairs, missing_in_base = build_pairs()

    print(f"EU root:   {EU_ROOT}")
    print(f"Base root: {BASE_ROOT}")
    print(f"Dry run:   {DRY_RUN}")
    print()

    print(f"Files with base match: {len(pairs)}")
    print(f"Files missing in base: {len(missing_in_base)}")
    print()

    deleted_count = 0
    moved_count = 0
    different_count = 0
    error_count = 0

    # ======================================================
    # HANDLE MATCHING FILES (SHA1 CHECK)
    # ======================================================
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(compare_pair, pair): pair
            for pair in pairs
        }

        for future in as_completed(futures):
            pair = futures[future]

            try:
                pair_result, eu_sha1, base_sha1 = future.result()
            except Exception as exc:
                error_count += 1
                print(f"ERROR   {pair.relative_path} :: {exc}")
                continue

            if eu_sha1 != base_sha1:
                different_count += 1
                print(f"DIFF    {pair_result.relative_path}")
                continue

            if DRY_RUN:
                deleted_count += 1
                print(f"DELETE  {pair_result.relative_path}")
                continue

            try:
                pair_result.eu_path.unlink()
                deleted_count += 1
                print(f"DELETED {pair_result.relative_path}")
            except Exception as exc:
                error_count += 1
                print(f"ERROR   {pair_result.relative_path} :: delete failed :: {exc}")

    # ======================================================
    # HANDLE MISSING FILES (MOVE TO BASE)
    # ======================================================
    for pair in missing_in_base:
        if DRY_RUN:
            moved_count += 1
            print(f"MOVE    {pair.relative_path}")
            continue

        try:
            pair.base_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(pair.eu_path), str(pair.base_path))
            moved_count += 1
            print(f"MOVED   {pair.relative_path}")
        except Exception as exc:
            error_count += 1
            print(f"ERROR   {pair.relative_path} :: move failed :: {exc}")

    print()
    print("Done.")
    print(f"Deleted / would delete: {deleted_count}")
    print(f"Moved / would move:     {moved_count}")
    print(f"Different:              {different_count}")
    print(f"Errors:                 {error_count}")

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())