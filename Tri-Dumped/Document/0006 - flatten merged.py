from __future__ import annotations

import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ==========================================================
# CONFIG
# ==========================================================
ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\Merged")
MAX_WORKERS = max(4, os.cpu_count() or 4)

LOCK = Lock()

# ==========================================================
# HELPERS
# ==========================================================
def build_flat_name(root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(root)
    parts = list(rel.parts)

    # last part is filename
    filename = parts[-1]

    # everything before that becomes prefix
    prefix_parts = parts[:-1]

    if not prefix_parts:
        return filename  # already in root

    return "_".join(prefix_parts + [filename])


def process_file(file_path: Path):
    if file_path.parent == ROOT:
        return  # already flattened

    new_name = build_flat_name(ROOT, file_path)
    dest = ROOT / new_name

    if dest.exists():
        # do NOT silently overwrite different files
        if file_path.stat().st_size != dest.stat().st_size:
            raise RuntimeError(f"Conflict: {file_path} -> {dest} already exists with different size")
        # same size, assume duplicate, just delete source
        file_path.unlink()
        return

    # move file
    shutil.move(str(file_path), str(dest))


def remove_empty_dirs(root: Path):
    # bottom-up cleanup
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        p = Path(dirpath)
        if p == root:
            continue
        if not any(p.iterdir()):
            p.rmdir()


# ==========================================================
# MAIN
# ==========================================================
def main():
    files = [p for p in ROOT.rglob("*") if p.is_file()]

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, f) for f in files]

        for f in as_completed(futures):
            f.result()

    remove_empty_dirs(ROOT)

    print("Done.")


if __name__ == "__main__":
    main()