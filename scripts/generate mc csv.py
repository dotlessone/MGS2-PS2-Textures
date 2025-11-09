import os
import hashlib
import csv
import shutil
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)
OUTPUT_CSV = "png_metadata.csv"
TMP_DIR_NAME = "_tmp_noalpha"
LOCK = Lock()

# ==========================================================
# HELPERS
# ==========================================================
def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def remove_alpha_preserve_rgb(img: Image.Image) -> Image.Image:
    """Remove alpha channel without touching RGB values."""
    mode = img.mode

    if mode == "RGBA":
        r, g, b, a = img.split()
        return Image.merge("RGB", (r, g, b))

    if mode == "LA":
        l, a = img.split()
        return Image.merge("RGB", (l, l, l))

    if mode == "P":
        if "transparency" in img.info:
            rgba = img.convert("RGBA")
            r, g, b, a = rgba.split()
            return Image.merge("RGB", (r, g, b))
        return img.convert("RGB")

    return img.convert("RGB")

def process_png(png_path: Path, tmp_dir: Path):
    try:
        with Image.open(png_path) as img:
            width, height = img.size

            # Extract unique alpha levels if present
            if img.mode in ("RGBA", "LA"):
                alpha_channel = img.getchannel("A")
                unique_alphas = sorted(set(alpha_channel.getdata()))
            else:
                unique_alphas = []

            # SHA1 of original
            mc_resaved_sha1 = sha1_of_file(png_path)

            # Save alpha-stripped version
            tmp_out = tmp_dir / png_path.name
            no_alpha_img = remove_alpha_preserve_rgb(img)
            no_alpha_img.save(tmp_out, format="PNG", optimize=False)

            # SHA1 of stripped version
            mc_alpha_stripped_sha1 = sha1_of_file(tmp_out)

            return (
                png_path.stem,          # texture_name
                width,                  # mc_width
                height,                 # mc_height
                unique_alphas,          # mc_alpha_levels
                mc_resaved_sha1,        # mc_resaved_sha1
                mc_alpha_stripped_sha1  # mc_alpha_stripped_sha1
            )
    except Exception as e:
        with LOCK:
            print(f"[FAIL] {png_path.name}: {e}")
        return None

# ==========================================================
# MAIN
# ==========================================================
def main():
    script_dir = Path(__file__).resolve().parent
    png_files = list(script_dir.glob("*.png"))

    if not png_files:
        print("No PNG files found in this folder.")
        return

    tmp_dir = script_dir / TMP_DIR_NAME
    tmp_dir.mkdir(exist_ok=True)

    print(f"Processing {len(png_files)} PNGs with {MAX_WORKERS} threads...")
    print(f"Temporary directory for alpha-removed copies: {tmp_dir}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_png, p, tmp_dir): p for p in png_files}
        for future in as_completed(futures):
            result = future.result()
            if result:
                with LOCK:
                    results.append(result)
                    print(f"[OK] {result[0]}")

    # Deduplicate mc_alpha_stripped_sha1 values
    hash_counts = defaultdict(int)
    for _, _, _, _, _, sha1_val in results:
        hash_counts[sha1_val] += 1

    for i, row in enumerate(results):
        if hash_counts[row[5]] > 1:
            results[i] = (*row[:5], "0000000000000000000000000000000000000000")

    results.sort(key=lambda x: x[0].lower())

    output_path = script_dir / OUTPUT_CSV
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "texture_name",
            "mc_width",
            "mc_height",
            "mc_alpha_levels",
            "mc_resaved_sha1",
            "mc_alpha_stripped_sha1"
        ])
        for row in results:
            writer.writerow([row[0], row[1], row[2], str(row[3]), row[4], row[5]])

    print(f"\nMetadata saved to: {output_path}")

    # Clean up temp folder
    try:
        shutil.rmtree(tmp_dir)
        print(f"Temporary folder removed: {tmp_dir}")
    except Exception as e:
        print(f"Warning: failed to remove temp folder: {e}")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
