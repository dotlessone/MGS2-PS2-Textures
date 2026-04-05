from __future__ import annotations

import os
import struct
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================================
# CONFIG
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR

OUTPUT_CSV = SCRIPT_DIR / "ctxr_single_mipmaps.csv"

MAX_WORKERS = max(4, os.cpu_count() or 4)

# ==========================================================
# CORE
# ==========================================================
def process_ctxr(ctxr_path: Path) -> tuple[str, str] | None:
    try:
        with ctxr_path.open("rb") as f:
            header = f.read(132)
            if len(header) < 132:
                return None

            mip_count = struct.unpack_from(">B", header, 0x26)[0]

            if mip_count != 1:
                return None

        stem = ctxr_path.stem

        rel_path = ctxr_path.relative_to(ROOT_DIR).parent
        if str(rel_path) == ".":
            rel_path_str = "/"
        else:
            rel_path_str = rel_path.as_posix() + "/"

        return stem, rel_path_str

    except Exception:
        return None


def build_csv_content(rows: list[tuple[str, str]]) -> str:
    lines = ["file_stem,relative_path"]

    for stem, rel_path in rows:
        lines.append(f"{stem},{rel_path}")

    return "\n".join(lines) + "\n"


def main() -> None:
    ctxr_files = list(ROOT_DIR.rglob("*.ctxr"))
    total = len(ctxr_files)

    print(f"Scanning {total} ctxr files...")

    results: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ctxr, p): p for p in ctxr_files}

        completed = 0

        for future in as_completed(futures):
            completed += 1

            result = future.result()
            if result:
                results.append(result)

            if completed % 1000 == 0 or completed == total:
                print(f"{completed}/{total}")

    # sort by path first, then stem
    results.sort(key=lambda x: (x[1], x[0]))

    new_content = build_csv_content(results)

    if OUTPUT_CSV.exists():
        old_content = OUTPUT_CSV.read_text(encoding="utf-8")
        if old_content == new_content:
            print("\nNo changes. CSV already up to date.")
            return

    OUTPUT_CSV.write_text(new_content, encoding="utf-8")

    print(f"\nDone. Found {len(results)} single-mip textures.")
    print(f"CSV written to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()