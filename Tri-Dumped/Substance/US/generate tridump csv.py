from __future__ import annotations

import csv
import hashlib
import os
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

from PIL import Image

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)
LOCK = Lock()

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> Path:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL,
        )
        return Path(out.decode().strip())
    except subprocess.CalledProcessError:
        raise RuntimeError("Error: Not inside a Git repository or git not available.")


def sha1_of_file(path: Path) -> str:
    """Compute SHA1 of a file."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_next_power_of_two(x: int) -> int:
    """Return the next power of two >= x."""
    if x <= 0:
        return 1
    return 1 << (x - 1).bit_length()


def load_texture_name_casing(csv_path: Path) -> dict[str, str]:
    """
    Load casing map from mgs2_ps2_dimensions.csv.
    Returns a dict mapping lowercase name -> original casing.
    """
    if not csv_path.exists():
        print(f"Warning: {csv_path} not found. Casing preservation skipped.")
        return {}

    casing_map: dict[str, str] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "texture_name" not in reader.fieldnames:
            print(f"Warning: texture_name column not found in {csv_path}.")
            return {}

        for row in reader:
            tex_name = row["texture_name"]
            if tex_name:
                casing_map[tex_name.lower()] = tex_name

    print(f"Loaded {len(casing_map)} texture names from casing CSV.")
    return casing_map


# ==========================================================
# PROCESSORS
# ==========================================================
def process_png(png_path: Path):
    """Process one PNG, collect metadata, and return row data."""
    try:
        with Image.open(png_path) as img:
            width, height = img.size
            width_pow2 = get_next_power_of_two(width)
            height_pow2 = get_next_power_of_two(height)
            sha1_converted = sha1_of_file(png_path)

            return (
                png_path.stem,
                width,
                height,
                width_pow2,
                height_pow2,
                sha1_converted,
            )
    except Exception as e:
        with LOCK:
            print(f"[FAIL] {png_path.name}: {e}")
        return None


def collect_tga_sha1s(root_dir: Path) -> dict[str, str]:
    """Recursively collect SHA1s of all .tga files under root."""
    tga_map: dict[str, str] = {}
    tga_files = list(root_dir.rglob("*.tga"))
    print(f"Scanning {len(tga_files)} .tga files for SHA1 hashes...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(sha1_of_file, tga): tga for tga in tga_files}
        for future in as_completed(future_map):
            tga_path = future_map[future]
            try:
                sha1_val = future.result()
                tga_map[tga_path.stem.lower()] = sha1_val
            except Exception as e:
                with LOCK:
                    print(f"[TGA FAIL] {tga_path}: {e}")

    print(f"Collected SHA1 hashes for {len(tga_map)} TGAs.")
    return tga_map


# ==========================================================
# MAIN
# ==========================================================
def main():
    git_root = get_git_root()
    script_dir = Path(__file__).resolve().parent
    repo_root = git_root
    png_files = list(script_dir.glob("*.png"))

    if not png_files:
        print("No PNG files found in this folder.")
        return

    # Load texture casing map from Git-root CSV
    casing_csv = git_root / r"u - dumped from substance" / "mgs2_ps2_dimensions.csv"
    casing_map = load_texture_name_casing(casing_csv)

    print(f"Processing {len(png_files)} PNGs with {MAX_WORKERS} threads...")

    # Collect TGA hashes
    tga_sha1_map = collect_tga_sha1s(repo_root)

    results: list[tuple[str, str, int, int, int, int, str]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_png, p): p for p in png_files}
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue

            name = result[0]
            name_lower = name.lower()
            name_cased = casing_map.get(name_lower, name)
            tga_sha1 = tga_sha1_map.get(
                name_lower,
                "0000000000000000000000000000000000000000",
            )

            with LOCK:
                results.append((name_cased, tga_sha1, *result[1:]))
                print(f"[OK] {name_cased}")

    results.sort(key=lambda x: x[0].lower())

    output_path = Path(
        r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_ps2_dimensions.csv"
    )
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "texture_name",
                "tri_dumped_tga_sha1",
                "tri_dumped_width",
                "tri_dumped_height",
                "tri_dumped_width_pow2",
                "tri_dumped_height_pow2",
                "tri_dumped_png_converted_sha1",
            ]
        )
        for row in results:
            writer.writerow(
                [
                    row[0],  # texture_name (preserved casing)
                    row[1],  # tri_dumped_tga_sha1
                    row[2],  # tri_dumped_width
                    row[3],  # tri_dumped_height
                    row[4],  # tri_dumped_width_pow2
                    row[5],  # tri_dumped_height_pow2
                    row[6],  # tri_dumped_png_converted_sha1
                ]
            )

    print(f"\nMetadata saved to: {output_path}")


# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()