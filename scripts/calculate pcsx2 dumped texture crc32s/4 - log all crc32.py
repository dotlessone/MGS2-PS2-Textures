import os
import csv
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def calc_crc32(path):
    """Compute CRC32 hash of the file (hex, lowercase)."""
    prev = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            prev = zlib.crc32(chunk, prev)
    return format(prev & 0xFFFFFFFF, "08x")

def process_file(path):
    """Return (filename_without_extension, crc32)."""
    try:
        crc = calc_crc32(path)
        base = os.path.splitext(os.path.basename(path))[0]
        return (base, crc)
    except Exception as e:
        return (os.path.splitext(os.path.basename(path))[0], f"ERROR: {e}")

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    output_csv = os.path.join(folder, "png_crc32_log.csv")

    png_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]
    total = len(png_files)

    if total == 0:
        print("No .png files found in this directory.")
        return

    print(f"[+] Found {total} PNG files. Starting CRC32 scan...")

    rows = []
    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(process_file, f): f for f in png_files}

        for future in as_completed(futures):
            rows.append(future.result())
            with lock:
                completed += 1
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed / total) * 100:.1f}%)")

    # Write the CSV log
    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["filename", "crc32"])
        writer.writerows(sorted(rows, key=lambda x: x[0].lower()))

    print(f"\n[+] Done. Logged {len(rows)} CRC32 entries to {output_csv}")

if __name__ == "__main__":
    main()
