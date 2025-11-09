import os
import subprocess
import hashlib
import csv
import time
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tempfile import NamedTemporaryFile
from threading import Lock
from PIL import Image

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)
ZERO_SHA1 = "0000000000000000000000000000000000000000"

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> str:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except subprocess.CalledProcessError:
        raise RuntimeError("Not inside a Git repository.")

def sha1_of_file(path: str) -> str:
    """Compute SHA-1 of a file."""
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha1.update(chunk)
    return sha1.hexdigest()

def resave_png(src_path: str, tmp_dir: str) -> str:
    """Resave PNG to tmp folder and return the new file path."""
    os.makedirs(tmp_dir, exist_ok=True)
    base = os.path.basename(src_path)
    tmp_path = os.path.join(tmp_dir, base)
    try:
        with Image.open(src_path) as img:
            img.save(tmp_path, format="PNG", optimize=True)
    except Exception as e:
        raise RuntimeError(f"Failed to resave {src_path}: {e}")
    return tmp_path

def analyze_alpha(img: Image.Image):
    """Return (unique_alpha_levels, width, height)."""
    width, height = img.size
    if img.mode not in ("RGBA", "LA"):
        return [], width, height
    alpha = img.getchannel("A").getdata()
    unique = sorted(set(alpha))
    return unique, width, height

def format_eta(elapsed, done, total):
    if done == 0:
        return "estimating..."
    rate = elapsed / done
    remaining = rate * (total - done)
    mins, secs = divmod(int(remaining), 60)
    return f"{mins:02d}:{secs:02d}"

# ==========================================================
# MAIN
# ==========================================================
def main():
    repo_root = get_git_root()
    target_dir = os.path.join(repo_root, "d - substance")
    csv_path = os.path.join(repo_root, "pcsx2_dumped_sha1_log.csv")

    if not os.path.isdir(target_dir):
        print(f"ERROR: Target directory not found: {target_dir}")
        return

    # Load existing SHA1s
    existing_hashes = set()
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            for row in reader:
                if row and len(row) > 0:
                    existing_hashes.add(row[0].strip())

    # Collect all PNGs
    all_pngs = []
    for root, _, files in os.walk(target_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                all_pngs.append(os.path.join(root, fn))

    total = len(all_pngs)
    if total == 0:
        print(f"No PNGs found in {target_dir}")
        return

    print(f"Found {total} PNG files. Using {MAX_WORKERS} threads...")

    new_entries = []
    tmp_folders = set()
    lock = Lock()
    start_time = time.time()

    def process_file(path):
        dumped_sha1 = sha1_of_file(path)
        if dumped_sha1 in existing_hashes:
            return None, None

        tmp_dir = os.path.join(os.path.dirname(path), "_tmp_resave")
        resaved_path = resave_png(path, tmp_dir)
        resaved_sha1 = sha1_of_file(resaved_path)

        with Image.open(path) as img:
            alpha_levels, width, height = analyze_alpha(img)

        alpha_str = f"{alpha_levels}".replace(" ", "")
        return [
            dumped_sha1,
            resaved_sha1,
            ZERO_SHA1,
            alpha_str,
            width,
            height,
        ], tmp_dir

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, p): p for p in all_pngs}
        done_count = 0

        for future in as_completed(futures):
            try:
                result, tmp_dir = future.result()
                if result:
                    with lock:
                        new_entries.append(result)
                        existing_hashes.add(result[0])
                        tmp_folders.add(tmp_dir)
            except Exception as e:
                print(f"\nError processing {futures[future]}: {e}")

            done_count += 1
            elapsed = time.time() - start_time
            eta = format_eta(elapsed, done_count, total)
            pct = (done_count / total) * 100
            print(f"\rProcessed {done_count}/{total} ({pct:.1f}%) | ETA: {eta}", end="", flush=True)

    print("\nFinished hashing. Writing results...")

    header = [
        "pcsx2_dumped_sha1",
        "pcsx2_resaved_sha1",
        "pcsx2_alpha_stripped_sha1",
        "pcsx2_alpha_levels",
        "pcsx2_width",
        "pcsx2_height",
    ]

    if not os.path.isfile(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

    if new_entries:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for entry in new_entries:
                writer.writerow(entry)
        print(f"Added {len(new_entries)} new entries.")
    else:
        print("No new entries to add.")

    # Sort CSV alphabetically
    print("Sorting CSV alphabetically...")
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print("CSV is empty.")
        return

    header = rows[0]
    data = rows[1:]
    data.sort(key=lambda x: x[0].lower())

    with NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8") as tmpfile:
        writer = csv.writer(tmpfile)
        writer.writerow(header)
        writer.writerows(data)
        tmpname = tmpfile.name

    os.replace(tmpname, csv_path)
    print(f"Sorted and updated: {csv_path}")

    # Cleanup temporary folders
    print("Cleaning up temporary folders...")
    for folder in tmp_folders:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception as e:
            print(f"Warning: Failed to delete {folder}: {e}")

    print("All temporary folders removed. Done.")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
