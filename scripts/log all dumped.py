import os
import csv
import subprocess
import hashlib
import shutil
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from PIL import Image

# ==========================================================
# CONFIGURATION
# ==========================================================
EXCLUDED_DIRS = {
    "t - dumped from sons of liberty",
    "u - dumped from substance",
    "_temp"
}
OUTPUT_CSV = "pcsx2_dumped_sha1_log.csv"
TEMP_FOLDER = "_temp"
MAX_WORKERS = max(4, os.cpu_count() or 4)
ZERO_HASH = "0000000000000000000000000000000000000000"

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> str:
    """Return absolute path to the Git repository root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except subprocess.CalledProcessError:
        raise RuntimeError("Not inside a Git repository or Git not available.")

def sha1_file(path: str) -> str:
    """Compute SHA-1 hash of a file efficiently."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def resave_image(src: str, dest: str):
    """Resave PNG via PIL (no optimization)."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with Image.open(src) as img:
        img.save(dest, format="PNG", optimize=False)

def strip_alpha(src: str, dest: str):
    """Remove alpha channel and save RGB-only version."""
    with Image.open(src) as img:
        rgb = img.convert("RGB")
        rgb.save(dest, format="PNG", optimize=False)

def analyze_alpha_levels(src: str):
    """Return unique alpha levels and image dimensions."""
    with Image.open(src) as img:
        width, height = img.size
        if img.mode in ("RGBA", "LA"):
            alpha = img.getchannel("A")
            unique_values = sorted(set(alpha.getdata()))
        else:
            unique_values = [255]
        return unique_values, width, height

# ==========================================================
# MAIN
# ==========================================================
def main():
    repo_root = get_git_root()
    output_path = os.path.join(repo_root, OUTPUT_CSV)
    temp_root = os.path.join(repo_root, TEMP_FOLDER)

    print(f"Repository root: {repo_root}")
    print(f"Temp copy directory: {temp_root}")
    print(f"Writing SHA-1 log to: {output_path}")
    print(f"Using {MAX_WORKERS} threads...")

    # Clean/create temp folder
    if os.path.exists(temp_root):
        print("Cleaning existing temp folder...")
        shutil.rmtree(temp_root)
    os.makedirs(temp_root, exist_ok=True)

    # Gather PNGs
    png_files = []
    for root, dirs, files in os.walk(repo_root):
        rel = os.path.relpath(root, repo_root)
        if any(rel.lower() == ex.lower() or rel.lower().startswith(ex.lower() + os.sep) for ex in EXCLUDED_DIRS):
            dirs[:] = []
            continue

        for file in files:
            if file.lower().endswith(".png"):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, repo_root)
                temp_path = os.path.join(temp_root, rel_path)
                rgb_path = os.path.splitext(temp_path)[0] + "_rgb.png"
                png_files.append((full_path, temp_path, rgb_path))

    total = len(png_files)
    print(f"Found {total} PNG files to resave, strip alpha, and hash.")

    lock = Lock()
    results = []
    completed = 0

    def process(entry):
        src, temp_path, rgb_path = entry
        try:
            dumped_sha1 = sha1_file(src)
            alpha_levels, width, height = analyze_alpha_levels(src)
            alpha_json = json.dumps(alpha_levels)

            # Resave full image (RGBA)
            resave_image(src, temp_path)
            resaved_sha1 = sha1_file(temp_path)

            # Strip alpha channel (RGB only)
            strip_alpha(temp_path, rgb_path)
            rgb_sha1 = sha1_file(rgb_path)

            return dumped_sha1, resaved_sha1, rgb_sha1, alpha_json, width, height
        except Exception as e:
            return f"ERROR:{e}", "", "", "[]", "", ""

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process, e): e for e in png_files}
        for future in as_completed(futures):
            row = future.result()
            with lock:
                results.append(row)
                completed += 1
                if completed % 100 == 0 or completed == total:
                    print(f"[{completed}/{total}] processed")

    # ==========================================================
    # Deduplicate alpha-stripped hashes (RGB-only)
    # ==========================================================
    alpha_counts = Counter(r[2] for r in results if r[2])
    duplicates = {sha1 for sha1, count in alpha_counts.items() if count > 1}

    if duplicates:
        print(f"Found {len(duplicates)} duplicate RGB-only hashes; zeroing them out...")
        for i, row in enumerate(results):
            dumped_sha1, resaved_sha1, rgb_sha1, alpha_json, width, height = row
            if rgb_sha1 in duplicates:
                results[i] = (dumped_sha1, resaved_sha1, ZERO_HASH, alpha_json, width, height)

    results.sort(key=lambda x: x[0].lower())

    # ==========================================================
    # Write CSV
    # ==========================================================
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "pcsx2_dumped_sha1",
            "pcsx2_resaved_sha1",
            "pcsx2_alpha_stripped_sha1",
            "pcsx2_alpha_levels",
            "pcsx2_width",
            "pcsx2_height",
        ])
        writer.writerows(results)

    print(f"\n✅ Done. Logged {len(results)} PNG hashes to:")
    print(output_path)
    print(f"Temp files saved in: {temp_root}")
    print("\nInspect '_temp' before deletion.")

    print("Cleaning up temporary folder...")
    shutil.rmtree(temp_root, ignore_errors=True)
    print("Temporary folder deleted.")


if __name__ == "__main__":
    main()
