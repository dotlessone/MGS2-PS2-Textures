import os
import csv
import zlib
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def calc_crc32(path):
    """Compute CRC32 hash of the file (hex, lowercase)."""
    prev = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            prev = zlib.crc32(chunk, prev)
    return format(prev & 0xFFFFFFFF, "08x")

def extract_alpha_levels(path):
    """Return sorted list of unique alpha values (0–255)."""
    try:
        with Image.open(path) as img:
            if img.mode not in ("RGBA", "LA"):
                return [255]
            alpha = img.getchannel("A")
            unique_values = sorted(set(alpha.getdata()))
            return unique_values
    except Exception:
        return ["ERROR"]

def process_file(path):
    """Process one PNG: width, height, alpha levels, and CRC32."""
    try:
        with Image.open(path) as img:
            w, h = img.size
        alphas = extract_alpha_levels(path)
        crc = calc_crc32(path)
        base = os.path.splitext(os.path.basename(path))[0]  # remove .png
        return (base, w, h, str(alphas), crc)
    except Exception as e:
        base = os.path.splitext(os.path.basename(path))[0]
        return (base, "ERROR", "ERROR", f"[ERROR: {e}]", "ERROR")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_csv = os.path.join(script_dir, "mgs2_mc_dimensions.csv")

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
        writer.writerow(["filename", "mc_width", "mc_height", "alpha_levels", "ctxr3_extracted_crc32"])
        writer.writerows(sorted(rows, key=lambda x: x[0].lower()))

    print(f"\n[+] Done. Processed {total} PNG files.")
    print(f"[+] Results written to: {output_csv}")

if __name__ == "__main__":
    main()
