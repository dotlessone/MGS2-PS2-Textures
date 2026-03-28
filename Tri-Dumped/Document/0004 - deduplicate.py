from __future__ import annotations

import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Set, Tuple


EU_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\EU")
JP_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\JP")
US_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\US")

MAX_WORKERS = max(4, os.cpu_count() or 4)
HASH_CHUNK_SIZE = 8 * 1024 * 1024


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def find_pngs(root: Path) -> list[Path]:
    if not root.is_dir():
        raise RuntimeError(f"Folder does not exist: {root}")

    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".png"
    )


def build_hash_set(paths: Iterable[Path], label: str) -> Set[str]:
    paths = list(paths)
    total = len(paths)

    if total == 0:
        print(f"[{label}] No PNG files found.")
        return set()

    hashes: Set[str] = set()

    print(f"[{label}] Hashing {total} PNG file(s)...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {executor.submit(sha1_of_file, path): path for path in paths}

        processed = 0

        for future in as_completed(future_to_path):
            path = future_to_path[future]

            try:
                file_hash = future.result()
            except Exception as exc:
                raise RuntimeError(f"Failed hashing: {path} ({exc})") from exc

            hashes.add(file_hash)
            processed += 1

            if processed % 250 == 0 or processed == total:
                print(f"[{label}] {processed}/{total}")

    print(f"[{label}] Unique hashes: {len(hashes)}")
    return hashes


def collect_matches(paths: Iterable[Path], eu_hashes: Set[str], label: str) -> list[Tuple[Path, str]]:
    paths = list(paths)
    total = len(paths)

    if total == 0:
        print(f"[{label}] No PNG files found.")
        return []

    matches: list[Tuple[Path, str]] = []

    print(f"[{label}] Checking {total} PNG file(s)...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {executor.submit(sha1_of_file, path): path for path in paths}

        processed = 0

        for future in as_completed(future_to_path):
            path = future_to_path[future]

            try:
                file_hash = future.result()
            except Exception as exc:
                raise RuntimeError(f"Failed hashing: {path} ({exc})") from exc

            if file_hash in eu_hashes:
                matches.append((path, file_hash))

            processed += 1

            if processed % 250 == 0 or processed == total:
                print(f"[{label}] {processed}/{total}")

    print(f"[{label}] Matched for deletion: {len(matches)}")
    return matches


def delete_files(matches: Iterable[Tuple[Path, str]], label: str) -> int:
    deleted = 0

    for path, _file_hash in sorted(matches, key=lambda item: str(item[0]).lower()):
        try:
            path.unlink()
            deleted += 1
            print(f"[{label}] Deleted: {path}")
        except Exception as exc:
            raise RuntimeError(f"Failed deleting: {path} ({exc})") from exc

    return deleted


def remove_empty_dirs(root: Path) -> int:
    removed = 0

    for directory in sorted(
        (p for p in root.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    ):
        try:
            next(directory.iterdir())
        except StopIteration:
            directory.rmdir()
            removed += 1
        except Exception as exc:
            raise RuntimeError(f"Failed cleaning directory: {directory} ({exc})") from exc

    return removed


def main() -> int:
    print("Scanning EU PNG files...")
    eu_pngs = find_pngs(EU_ROOT)
    eu_hashes = build_hash_set(eu_pngs, "EU")

    if not eu_hashes:
        print("No EU hashes found. Nothing to delete.")
        return 0

    print("Scanning JP PNG files...")
    jp_pngs = find_pngs(JP_ROOT)
    jp_matches = collect_matches(jp_pngs, eu_hashes, "JP")

    print("Scanning US PNG files...")
    us_pngs = find_pngs(US_ROOT)
    us_matches = collect_matches(us_pngs, eu_hashes, "US")

    jp_deleted = delete_files(jp_matches, "JP")
    us_deleted = delete_files(us_matches, "US")

    jp_dirs_removed = remove_empty_dirs(JP_ROOT)
    us_dirs_removed = remove_empty_dirs(US_ROOT)

    print()
    print("Done.")
    print(f"JP deleted: {jp_deleted}")
    print(f"US deleted: {us_deleted}")
    print(f"JP empty dirs removed: {jp_dirs_removed}")
    print(f"US empty dirs removed: {us_dirs_removed}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(1)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)