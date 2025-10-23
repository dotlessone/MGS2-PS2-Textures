import os
import csv
import hashlib
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def calc_sha1(path):
    """Compute SHA-1 hash of the file (hex, lowercase)."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def extract_alpha_levels(path):
    """Return sorted list of unique alpha values (0–255)."""
    try:
        with Image.open(path) as img:
            if img.mode not in ("RGBA", "LA"):
                return [255]  # Fully opaque if no alpha channel
            alpha = img.getchannel("A")
            unique_values = sorted(set(alpha.getdata()))
            return unique_values
    except Exception:
        return ["ERROR"]

def process_file(path):
    """Process one PNG: width, height, alpha levels, and SHA-1."""
    try:
        with Image.open(path) as img:
            w, h = img.size
        alphas = extract_alpha_levels(path)
        sha1_hash = calc_sha1(path)
        base = os.path.splitext(os.path.basename(path))[0]  # remove .png
        return (base, w, h, str(alphas), sha1_hash)
    except Exception as e:
        base = os.path.splitext(os.path.basename(path))[0]
        return (base, "ERROR", "ERROR", f"[ERROR: {e}]", "ERROR")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_csv = os.path.join(script_dir, "mc_dimension_and_sha1.csv")

    png_files = [f for f in os.listdir(script_dir) if f.lower().endswith(".png")]
    if not png_files:
        print("No .png files found in this directory.")
        return

    total = len(png_files)
    print(f"[+] Found {total} PNG files. Starting multi-threaded processing...")

    rows = []
    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(process_file, os.path.join(script_dir, f)): f for f in png_files}

        for future in as_completed(futures):
            rows.append(future.result())
            with lock:
                completed += 1
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed / total) * 100:.1f}%)")

    # Write results
    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["texture_name", "mc_width", "mc_height", "mc_alpha_levels", "mc_resaved_sha1"])
        writer.writerows(sorted(rows, key=lambda x: x[0].lower()))

    print(f"\n[+] Done. Processed {total} PNG files.")
    print(f"[+] Results written to: {output_csv}")

if __name__ == "__main__":
    main()
