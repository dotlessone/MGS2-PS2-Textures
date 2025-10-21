import os
import zlib
import csv
import math
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def next_pow2(n: int) -> int:
    """Return the next power of two >= n."""
    if n < 1:
        return 1
    return 1 << (n - 1).bit_length()

def calc_crc32(path: str) -> str:
    """Compute CRC32 hash of the file."""
    prev = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            prev = zlib.crc32(chunk, prev)
    return format(prev & 0xFFFFFFFF, "08x")

def process_file(path: str):
    """Process a single TGA file: get CRC32, dimensions, and ceil^2 dimensions."""
    try:
        with Image.open(path) as img:
            w, h = img.size
        crc = calc_crc32(path)
        w2 = next_pow2(w)
        h2 = next_pow2(h)
        return os.path.basename(path), crc, w, h, w2, h2
    except Exception as e:
        return os.path.basename(path), f"ERROR: {e}", 0, 0, 0, 0

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    tga_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".tga")]

    total = len(tga_files)
    if total == 0:
        print("[!] No TGA files found.")
        return

    print(f"[+] Found {total} TGA files. Starting multi-threaded processing...")

    out_csv = os.path.join(folder, "tga_dimensions_log.csv")
    lock = threading.Lock()
    completed = 0

    with open(out_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["filename", "crc32", "width", "height", "width_pow2", "height_pow2"])

        max_workers = max(4, os.cpu_count() or 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_file, f): f for f in tga_files}

            for future in as_completed(futures):
                result = future.result()
                with lock:
                    writer.writerow(result)
                    completed += 1
                    if completed % 250 == 0 or completed == total:
                        print(f"[Progress] {completed}/{total} processed ({(completed/total)*100:.1f}%)")

    print(f"\n[+] Done. All results logged to {out_csv}")

if __name__ == "__main__":
    main()
