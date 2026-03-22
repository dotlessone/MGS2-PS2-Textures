from __future__ import annotations

import csv
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple


# ==========================================================
# CONFIG
# ==========================================================
DUMP_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\dump")
TRI_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\US\TGA")

OUTPUT_LOG = DUMP_ROOT / "tri_dump_sha1_matches.csv"

MAX_WORKERS = min(32, max(4, (os.cpu_count() or 4)))
SHA1_BUFFER_SIZE = 4 * 1024 * 1024


# ==========================================================
# HELPERS
# ==========================================================
def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        while True:
            chunk = f.read(SHA1_BUFFER_SIZE)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def collect_tgas(root: Path) -> List[Path]:
    return sorted(path for path in root.rglob("*.tga") if path.is_file())


def hash_paths(paths: List[Path]) -> Dict[Path, str]:
    results: Dict[Path, str] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {executor.submit(sha1_of_file, path): path for path in paths}

        completed = 0
        total = len(paths)

        for future in as_completed(future_to_path):
            path = future_to_path[future]
            results[path] = future.result()

            completed += 1
            if completed % 500 == 0 or completed == total:
                print(f"Hashed {completed}/{total}: {path}")

    return results


def build_tri_sha1_map(tri_hashes: Dict[Path, str]) -> Dict[str, List[str]]:
    sha1_to_texture_names: Dict[str, List[str]] = {}

    for path, sha1 in tri_hashes.items():
        texture_name = path.stem
        sha1_to_texture_names.setdefault(sha1, []).append(texture_name)

    for sha1 in sha1_to_texture_names:
        sha1_to_texture_names[sha1].sort()

    return sha1_to_texture_names


def get_dump_fields(dump_path: Path) -> Tuple[str, str, str]:
    rel_parts = dump_path.relative_to(DUMP_ROOT).parts

    dump_subfolder = rel_parts[0] if len(rel_parts) >= 2 else ""
    nested_subfolder = rel_parts[1] if len(rel_parts) >= 3 else ""
    tga_stem = dump_path.stem

    return dump_subfolder, nested_subfolder, tga_stem


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if not DUMP_ROOT.is_dir():
        print(f"Error: dump root does not exist: {DUMP_ROOT}")
        return 1

    if not TRI_ROOT.is_dir():
        print(f"Error: tri-dumped root does not exist: {TRI_ROOT}")
        return 1

    print("Collecting dump TGAs...")
    dump_tgas = collect_tgas(DUMP_ROOT)
    print(f"Found {len(dump_tgas)} dump TGAs")

    print("Collecting tri-dumped TGAs...")
    tri_tgas = collect_tgas(TRI_ROOT)
    print(f"Found {len(tri_tgas)} tri-dumped TGAs")

    print("Hashing dump TGAs...")
    dump_hashes = hash_paths(dump_tgas)

    print("Hashing tri-dumped TGAs...")
    tri_hashes = hash_paths(tri_tgas)

    print("Building tri-dumped SHA1 lookup...")
    tri_sha1_map = build_tri_sha1_map(tri_hashes)

    print("Finding matches...")
    rows: List[Tuple[str, str, str, str]] = []

    for dump_path in sorted(dump_hashes):
        dump_sha1 = dump_hashes[dump_path]
        texture_names = tri_sha1_map.get(dump_sha1)

        if not texture_names:
            continue

        dump_subfolder, nested_subfolder, tga_stem = get_dump_fields(dump_path)

        for texture_name in texture_names:
            rows.append((texture_name, dump_subfolder, nested_subfolder, tga_stem))

    print(f"Writing log: {OUTPUT_LOG}")
    with OUTPUT_LOG.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        for row in rows:
            writer.writerow(row)

    print(f"Done. Wrote {len(rows)} matching row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())