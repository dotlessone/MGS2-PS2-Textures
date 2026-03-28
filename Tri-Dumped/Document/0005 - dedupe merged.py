from __future__ import annotations

import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\Merged")

MAX_WORKERS = max(4, os.cpu_count() or 4)
HASH_CHUNK_SIZE = 8 * 1024 * 1024


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def find_pngs(root: Path) -> List[Path]:
    if not root.is_dir():
        raise RuntimeError(f"Folder does not exist: {root}")

    return sorted(
        p
        for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() == ".png"
    )


def hash_all(paths: List[Path]) -> Dict[str, List[Path]]:
    total = len(paths)

    if total == 0:
        print("No PNG files found.")
        return {}

    print(f"Hashing {total} PNG file(s)...")

    sha_map: Dict[str, List[Path]] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {executor.submit(sha1_of_file, p): p for p in paths}

        processed = 0

        for future in as_completed(future_to_path):
            path = future_to_path[future]

            try:
                h = future.result()
            except Exception as exc:
                raise RuntimeError(f"Failed hashing: {path} ({exc})") from exc

            sha_map.setdefault(h, []).append(path)

            processed += 1
            if processed % 250 == 0 or processed == total:
                print(f"{processed}/{total}")

    print(f"Unique hashes: {len(sha_map)}")
    return sha_map


def deduplicate(sha_map: Dict[str, List[Path]]) -> int:
    deleted = 0

    for h, paths in sha_map.items():
        if len(paths) <= 1:
            continue

        # Keep the lexicographically smallest path
        paths_sorted = sorted(paths, key=lambda p: str(p).lower())
        keep = paths_sorted[0]
        to_delete = paths_sorted[1:]

        for path in to_delete:
            try:
                path.unlink()
                deleted += 1
                print(f"Deleted duplicate: {path} (kept: {keep})")
            except Exception as exc:
                raise RuntimeError(f"Failed deleting: {path} ({exc})") from exc

    return deleted


def remove_empty_dirs(root: Path) -> int:
    removed = 0

    for d in sorted(
        (p for p in root.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    ):
        try:
            next(d.iterdir())
        except StopIteration:
            d.rmdir()
            removed += 1
        except Exception as exc:
            raise RuntimeError(f"Failed cleaning directory: {d} ({exc})") from exc

    return removed


def main() -> int:
    pngs = find_pngs(ROOT)
    sha_map = hash_all(pngs)

    deleted = deduplicate(sha_map)
    removed_dirs = remove_empty_dirs(ROOT)

    print()
    print("Done.")
    print(f"Deleted duplicates: {deleted}")
    print(f"Empty dirs removed: {removed_dirs}")

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