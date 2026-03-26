from __future__ import annotations

import csv
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

from PIL import Image


# ==========================================================
# CONFIG
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent

OUTPUT_CSV = SCRIPT_DIR / "mc_tri_dumped_metadata.csv"
ALPHA_ONLY_0_CSV = SCRIPT_DIR / "alpha_only_0.csv"
ALPHA_GT_128_CSV = SCRIPT_DIR / "alpha_gt_128.csv"

MAX_WORKERS = max(4, os.cpu_count() or 4)
SHA1_BUFFER_SIZE = 8 * 1024 * 1024


# ==========================================================
# HELPERS
# ==========================================================
def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(SHA1_BUFFER_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def ceil_power_of_two(value: int) -> int:
    if value <= 1:
        return 1
    return 1 << (value - 1).bit_length()


def get_unique_alpha_levels(img: Image.Image) -> list[int]:
    if "A" not in img.getbands():
        return [255]

    alpha = img.getchannel("A")
    return sorted(set(alpha.getdata()))


def analyze_png(path: Path) -> dict[str, object]:
    file_sha1 = sha1_of_file(path)

    with Image.open(path) as img:
        width, height = img.size
        alpha_levels = get_unique_alpha_levels(img)

    return {
        "texture_name": path.stem.lower(),
        "mc_tri_dumped_sha1": file_sha1,
        "mc_tri_dumped_alpha_levels": alpha_levels,
        "mc_tri_dumped_width": width,
        "mc_tri_height": height,
        "mc_tri_width_ciel2": ceil_power_of_two(width),
        "mc_tri_height_ciel2": ceil_power_of_two(height),
    }


def iter_pngs(folder: Path) -> Iterable[Path]:
    for path in sorted(folder.iterdir(), key=lambda p: p.name.lower()):
        if not path.is_file():
            continue

        if path.suffix.lower() != ".png":
            continue

        yield path


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "texture_name",
                "mc_tri_dumped_sha1",
                "mc_tri_dumped_alpha_levels",
                "mc_tri_dumped_width",
                "mc_tri_height",
                "mc_tri_width_ciel2",
                "mc_tri_height_ciel2",
            ],
            lineterminator="\n",
        )
        writer.writeheader()

        for row in rows:
            row_copy = dict(row)
            row_copy["mc_tri_dumped_alpha_levels"] = str(row_copy["mc_tri_dumped_alpha_levels"])
            writer.writerow(row_copy)


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    png_files = list(iter_pngs(SCRIPT_DIR))

    if not png_files:
        print("No PNG files found.")
        return 0

    all_rows: list[dict[str, object]] = []
    alpha_only_0_rows: list[dict[str, object]] = []
    alpha_gt_128_rows: list[dict[str, object]] = []

    errors: list[tuple[Path, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_png, p): p for p in png_files}

        for future in as_completed(futures):
            path = futures[future]

            try:
                row = future.result()
                all_rows.append(row)

                alpha_levels: list[int] = row["mc_tri_dumped_alpha_levels"]

                # --- alpha only 0 ---
                if alpha_levels == [0]:
                    alpha_only_0_rows.append(row)

                # --- alpha > 128 ---
                if any(a > 128 for a in alpha_levels):
                    alpha_gt_128_rows.append(row)

            except Exception as exc:
                errors.append((path, str(exc)))

    # Sort everything
    all_rows.sort(key=lambda r: r["texture_name"])
    alpha_only_0_rows.sort(key=lambda r: r["texture_name"])
    alpha_gt_128_rows.sort(key=lambda r: r["texture_name"])

    # Write CSVs
    write_csv(OUTPUT_CSV, all_rows)
    write_csv(ALPHA_ONLY_0_CSV, alpha_only_0_rows)
    write_csv(ALPHA_GT_128_CSV, alpha_gt_128_rows)

    print(f"Wrote: {OUTPUT_CSV}")
    print(f"Wrote: {ALPHA_ONLY_0_CSV} ({len(alpha_only_0_rows)})")
    print(f"Wrote: {ALPHA_GT_128_CSV} ({len(alpha_gt_128_rows)})")

    if errors:
        print(f"\nErrors: {len(errors)}")
        for path, msg in errors:
            print(f"{path.name}: {msg}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())