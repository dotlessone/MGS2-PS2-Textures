import os
import hashlib
import threading
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image

# ==========================================================
# SHA-1 RENAMING
# ==========================================================
def calc_sha1(path):
    """Compute SHA-1 hash of a file (hex, lowercase)."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def rename_file(path):
    """Rename file to its SHA-1 (skip if already correct)."""
    try:
        hval = calc_sha1(path)
        folder = os.path.dirname(path)
        new_name = hval + ".png"
        new_path = os.path.join(folder, new_name)

        # Skip if already correct
        if os.path.basename(path).lower() == new_name.lower():
            return (os.path.basename(path), "Already correct", True, None)

        # Avoid overwriting unrelated file
        if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(path):
            os.remove(new_path)

        os.rename(path, new_path)
        return (os.path.basename(path), new_name, True, None)
    except Exception as e:
        return (os.path.basename(path), None, False, str(e))

# ==========================================================
# ALPHA VALIDATION
# ==========================================================
def has_invalid_alpha(path):
    """Return True if image has no alpha channel or any alpha > 128."""
    try:
        with Image.open(path) as img:
            if img.mode not in ("RGBA", "LA"):
                # No alpha channel at all
                return True
            alpha = img.getchannel("A")
            extrema = alpha.getextrema()
            if extrema and extrema[1] > 128:
                return True
    except Exception:
        return True  # treat unreadable as invalid
    return False

def move_invalid_alpha(path, invalid_dir):
    """Move file if invalid alpha detected."""
    try:
        if has_invalid_alpha(path):
            dest = os.path.join(invalid_dir, os.path.basename(path))
            if not os.path.exists(dest):
                shutil.move(path, dest)
            return path
    except Exception:
        pass
    return None

# ==========================================================
# MAIN
# ==========================================================
def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    png_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]
    total = len(png_files)
    if total == 0:
        print("No .png files found in this directory.")
        return

    print(f"[+] Found {total} PNG files. Starting SHA-1 renaming...")

    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(rename_file, f): f for f in png_files}

        for future in as_completed(futures):
            old_name, new_name, success, err = future.result()
            with lock:
                completed += 1
                if success:
                    print(f"[OK] {old_name} -> {new_name}")
                else:
                    print(f"[!] Failed {old_name}: {err}")

    print(f"\n[+] SHA-1 rename complete. Starting alpha validation scan...")

    invalid_dir = os.path.join(folder, "INVALID ALPHA LEVELS")
    os.makedirs(invalid_dir, exist_ok=True)

    renamed_pngs = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]

    moved = []
    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(move_invalid_alpha, p, invalid_dir): p for p in renamed_pngs}
        for future in as_completed(futures):
            result = future.result()
            if result:
                moved.append(result)

    print(f"[+] Moved {len(moved)} PNGs to '{invalid_dir}' (no alpha or alpha > 128).")
    print("[+] Done.")

if __name__ == "__main__":
    main()
