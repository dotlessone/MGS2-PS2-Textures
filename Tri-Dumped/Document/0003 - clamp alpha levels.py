from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from PIL import Image


MAX_WORKERS = max(4, os.cpu_count() or 4)
ROOT = Path(__file__).resolve().parent


def pause_and_exit(code: int = 0) -> None:
    try:
        input("Press ENTER to exit...")
    except EOFError:
        pass
    raise SystemExit(code)


def is_png(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".png"


def all_pixels_invisible_and_strip_alpha(path: Path) -> tuple[Path, bool, str | None]:
    try:
        with Image.open(path) as img:
            rgba = img.convert("RGBA")
            r, g, b, a = rgba.split()

            alpha_extrema = a.getextrema()
            if alpha_extrema != (0, 0):
                return path, False, None

            rgb = Image.merge("RGB", (r, g, b))
            rgb.save(path, format="PNG", optimize=False)

        return path, True, None
    except Exception as exc:
        return path, False, str(exc)


def main() -> None:
    png_files = [p for p in ROOT.rglob("*.png") if is_png(p)]

    if not png_files:
        print(f"No PNG files found under: {ROOT}")
        pause_and_exit(0)

    changed_count = 0
    error_count = 0

    print(f"Root: {ROOT}")
    print(f"Found {len(png_files)} PNG file(s).")
    print(f"Using {MAX_WORKERS} worker(s).")
    print()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(all_pixels_invisible_and_strip_alpha, path) for path in png_files]

        for future in as_completed(futures):
            path, changed, error = future.result()

            if error is not None:
                error_count += 1
                print(f"[ERROR] {path}: {error}")
                continue

            if changed:
                changed_count += 1
                print(f"[STRIPPED] {path}")

    print()
    print(f"Done. Stripped alpha from {changed_count} file(s).")
    print(f"Errors: {error_count}")

    pause_and_exit(0 if error_count == 0 else 1)


if __name__ == "__main__":
    main()