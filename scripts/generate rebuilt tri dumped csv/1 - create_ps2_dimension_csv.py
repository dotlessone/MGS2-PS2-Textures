import os
import csv
import math
import hashlib
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def next_pow2(n: int) -> int:
    """Return the next power of two >= n."""
    if n < 1:
        return 1
    return 1 << (n - 1).bit_length()

def calc_sha1(path: str) -> str:
    """Compute SHA-1 hash of the file (hex, lowercase)."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def process_file(path: str):
    """Process a single TGA file: get SHA-1, dimensions, and next power-of-two sizes."""
    try:
        with Image.open(path) as img:
            w, h = img.size
        sha1_hash = calc_sha1(path)
        w2 = next_pow2(w)
        h2 = next_pow2(h)
        base_name = os.path.splitext(os.path.basename(path))[0]  # remove .tga
        return base_name, sha1_hash, w, h, w2, h2
    except Exception as e:
        base_name = os.path.splitext(os.path.basename(path))[0]
        return base_name, f"ERROR: {e}", 0, 0, 0, 0

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    tga_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".tga")]

    total = len(tga_files)
    if total == 0:
        print("[!] No TGA files found.")
        return

    print(f"[+] Found {total} TGA files. Starting multi-threaded processing...")

    out_csv = os.path.join(folder, "tri_dumped_dimensions_and_sha1.csv")
    lock = threading.Lock()
    completed = 0

    with open(out_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "texture_name",
            "tri_dumped_sha1",
            "tri_dumped_width",
            "tri_dumped_height",
            "tri_dumped_width_pow2",
            "tri_dumped_height_pow2"
        ])

        max_workers = max(4, os.cpu_count() or 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_file, f): f for f in tga_files}

            for future in as_completed(futures):
                result = future.result()
                with lock:
                    writer.writerow(result)
                    completed += 1
                    if completed % 250 == 0 or completed == total:
                        print(f"[Progress] {completed}/{total} processed ({(completed / total) * 100:.1f}%)")

    print(f"\n[+] Done. All results logged to {out_csv}")

if __name__ == "__main__":
    main()
