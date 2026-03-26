import os
import csv
import hashlib
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
import threading

# ==========================================================
# CONFIGURATION
# ==========================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PNG_DIR = SCRIPT_DIR
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
TMP_DIR = os.path.join(SCRIPT_DIR, "_tmp")
DEST_SCRIPT = os.path.join(PARENT_DIR, "final_verification_nov22_2025", "0000 - get matching sha1 files.py")
CSV_PATH = os.path.join(PARENT_DIR, "pcsx2_manual_sha1_matches.csv")
DIMENSIONS_CSV = os.path.join(PARENT_DIR, "u - dumped from substance", "mgs2_ps2_dimensions.csv")
ERROR_LOG = os.path.join(SCRIPT_DIR, "move_error_log.txt")
MAX_WORKERS = max(4, os.cpu_count() or 4)
LOCK = threading.Lock()

# ==========================================================
# HELPERS
# ==========================================================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def calc_sha1(file_path):
    """Compute SHA1 hash of a file."""
    h = hashlib.sha1()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_alpha_levels(path):
    """Return a sorted list of unique alpha values for a PNG file."""
    try:
        with Image.open(path) as img:
            if img.mode not in ("RGBA", "LA"):
                return [255]  # Fully opaque if no alpha channel
            alpha = img.getchannel("A")
            values = sorted(set(alpha.getdata()))
            return values
    except Exception as e:
        return [f"ERROR: {e}"]

def resave_image(path):
    """Re-save a PNG file in place."""
    try:
        with Image.open(path) as img:
            img.save(path, format="PNG", optimize=False)
        return True, None
    except Exception as e:
        return False, str(e)

def read_existing_names():
    """Return set of existing filenames from CSV."""
    existing = set()
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing.add(row[0].strip())
    return existing

def load_dimensions_csv():
    """Load texture_name -> (width, height) mapping."""
    ref = {}
    if not os.path.exists(DIMENSIONS_CSV):
        print(f"[!] Dimensions CSV not found: {DIMENSIONS_CSV}")
        return ref

    with open(DIMENSIONS_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("texture_name", "").strip()
            try:
                w = int(row.get("tri_dumped_width", 0))
                h = int(row.get("tri_dumped_height", 0))
                ref[name] = (w, h)
            except ValueError:
                continue
    print(f"[+] Loaded {len(ref)} reference dimension entries.")
    return ref

def append_to_csv(rows):
    """Append list of (filename, sha1_original, sha1_resaved, pcsx2_alpha_levels, pcsx2_width, pcsx2_height) to CSV."""
    file_exists = os.path.exists(CSV_PATH)
    with LOCK, open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "filename",
                "sha1_original",
                "sha1_resaved",
                "pcsx2_alpha_levels",
                "pcsx2_width",
                "pcsx2_height"
            ])
        writer.writerows(rows)

def sort_csv_alphabetically():
    """Sort CSV alphabetically by filename (case-insensitive)."""
    if not os.path.exists(CSV_PATH):
        return
    with LOCK, open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = list(csv.reader(f))
        if len(reader) <= 1:
            return
    header = reader[0]
    data = reader[1:]
    data.sort(key=lambda r: r[0].lower())
    with LOCK, open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

def log_error(msg):
    """Append error message to log."""
    with LOCK, open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def cleanup_folder(path):
    """Remove folder if empty."""
    try:
        if os.path.isdir(path):
            for f in os.listdir(path):
                try:
                    os.remove(os.path.join(path, f))
                except Exception:
                    pass
            if not os.listdir(path):
                os.rmdir(path)
    except Exception:
        pass

# ==========================================================
# MAIN LOGIC
# ==========================================================
def process_png(path, existing_names, ref_dimensions):
    """Process a single PNG file (verify dimensions + hash + resave + alpha levels)."""
    name = os.path.splitext(os.path.basename(path))[0]

    if name in existing_names:
        return ("duplicate", name, None, None, None, None, None)

    try:
        # --- Dimension check ---
        if name not in ref_dimensions:
            msg = f"[DIMENSION ERROR] {path}: no reference found"
            print(f"[ERROR] {name}: no reference dimensions")
            log_error(msg)
            return ("error", name, None, None, None, None, None)

        expected_w, expected_h = ref_dimensions[name]

        with Image.open(path) as img:
            actual_w, actual_h = img.size

        if (actual_w != expected_w) or (actual_h != expected_h):
            msg = (f"[DIMENSION ERROR] {path}: expected {expected_w}x{expected_h}, "
                   f"got {actual_w}x{actual_h}")
            print(f"[ERROR] {name}: dimension mismatch ({actual_w}x{actual_h} vs {expected_w}x{expected_h})")
            log_error(msg)
            return ("error", name, None, None, None, None, None)

        # --- Compute alpha levels ---
        alpha_levels = get_alpha_levels(path)

        # --- Hash/resave workflow ---
        sha1_original = calc_sha1(path)

        ensure_dir(TMP_DIR)
        tmp_path = os.path.join(TMP_DIR, os.path.basename(path))
        shutil.copy2(path, tmp_path)

        ok, err = resave_image(tmp_path)
        if not ok:
            log_error(f"[RESAVE ERROR] {path}: {err}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return ("error", name, None, None, None, None, None)

        sha1_resaved = calc_sha1(tmp_path)
        try:
            os.remove(tmp_path)
        except Exception as e:
            log_error(f"[TMP DELETE ERROR] {tmp_path}: {e}")

        if sha1_original == sha1_resaved:
            msg = f"[HASH ERROR] {path}: identical hashes ({sha1_original})"
            print(f"[ERROR] {name}: identical hashes")
            log_error(msg)
            return ("error", name, sha1_original, sha1_resaved, str(alpha_levels), actual_w, actual_h)

        return ("success", name, sha1_original, sha1_resaved, str(alpha_levels), actual_w, actual_h)

    except Exception as e:
        log_error(f"[PROCESS ERROR] {path}: {e}")
        return ("error", name, None, None, None, None, None)

def main():
    if os.path.exists(ERROR_LOG):
        os.remove(ERROR_LOG)

    png_files = [os.path.join(PNG_DIR, f) for f in os.listdir(PNG_DIR) if f.lower().endswith(".png")]
    if not png_files:
        print("[!] No PNG files found in this folder.")
        cleanup_folder(TMP_DIR)
        input("\nPress Enter to continue...")
        return

    ref_dimensions = load_dimensions_csv()
    if not ref_dimensions:
        print("[!] No dimension reference data loaded. Aborting.")
        cleanup_folder(TMP_DIR)
        input("\nPress Enter to continue...")
        return

    print(f"[+] Found {len(png_files)} PNG files.")
    existing_names = read_existing_names()

    results = []
    duplicates = []
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_png, path, existing_names, ref_dimensions): path for path in png_files
        }
        for future in as_completed(future_to_file):
            path = future_to_file[future]
            try:
                status, name, sha1_orig, sha1_resaved, alpha, w, h = future.result()
                if status == "duplicate":
                    duplicates.append(name)
                elif status == "error":
                    errors.append(name)
                elif status == "success":
                    results.append((name, sha1_orig, sha1_resaved, alpha, w, h))
                    print(f"[OK] {name}: {sha1_orig} -> {sha1_resaved}")
            except Exception as e:
                errors.append(os.path.basename(path))
                log_error(f"[FUTURE ERROR] {path}: {e}")

    if results:
        append_to_csv(results)
        sort_csv_alphabetically()
        print(f"[+] Appended {len(results)} new entries to {CSV_PATH} (sorted alphabetically)")

        if os.path.exists(DEST_SCRIPT):
            print(f"[+] Calling external move script: {DEST_SCRIPT}")
            try:
                subprocess.run(["py", DEST_SCRIPT], check=True)
            except Exception as e:
                log_error(f"[RUN ERROR] {DEST_SCRIPT}: {e}")
                print(f"[!] Failed to call external mover script: {e}")
        else:
            print(f"[!] Move script not found: {DEST_SCRIPT}")

    cleanup_folder(TMP_DIR)

    if duplicates or errors:
        if duplicates:
            print(f"\n[!] {len(duplicates)} duplicate filenames skipped:")
            for d in duplicates:
                print(f"    - {d}")
        if errors:
            print(f"\n[!] {len(errors)} processing, dimension, or hash errors encountered:")
            for e in errors:
                print(f"    - {e}")
        if os.path.exists(ERROR_LOG) and os.path.getsize(ERROR_LOG) > 0:
            print(f"\n[!] Detailed errors logged to: {ERROR_LOG}")
        input("\nPress Enter to continue...")

    elif not results:
        print("[!] No new entries added.")
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()
