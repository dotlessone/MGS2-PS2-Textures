from __future__ import annotations

import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image


# ==========================================================
# CONFIG
# ==========================================================
ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Document\Merged")
MAX_WORKERS = max(4, os.cpu_count() or 4)


# ==========================================================
# HELPERS
# ==========================================================
def is_png(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".png"


def double_alpha_in_place(path: Path) -> str | None:
    with Image.open(path) as im:
        rgba = im.convert("RGBA")
        r, g, b, a = rgba.split()

        alpha_extrema = a.getextrema()
        if alpha_extrema == (255, 255):
            return None

        doubled_alpha = a.point(lambda v: 255 if v >= 128 else v * 2)
        merged = Image.merge("RGBA", (r, g, b, doubled_alpha))

        merged.save(path, format="PNG", optimize=False)

    return path.name


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if not ROOT.exists():
        print(f"Error: Folder does not exist: {ROOT}")
        return 1

    files = [p for p in ROOT.iterdir() if is_png(p)]

    if not files:
        print("No PNG files found.")
        return 0

    changed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(double_alpha_in_place, path): path for path in files}

        for future in as_completed(futures):
            path = futures[future]
            try:
                result = future.result()
                if result is not None:
                    changed += 1
                    print(f"Updated: {result}")
            except Exception as exc:
                print(f"Failed: {path} -> {exc}")
                return 1

    print(f"Done. Updated {changed} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())