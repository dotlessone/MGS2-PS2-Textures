import os
import csv
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import Counter

def calc_sha1(path):
    """Compute SHA-1 hash of the file (hex, lowercase)."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def process_file(path):
    """Return (filename_without_extension, sha1)."""
    try:
        sha1_val = calc_sha1(path)
        base = os.path.splitext(os.path.basename(path))[0]
        return (base, sha1_val)
    except Exception as e:
        return (os.path.splitext(os.path.basename(path))[0], f"ERROR: {e}")

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    output_csv = os.path.join(folder, "tri_dumped_alpha_stripped_sha1.csv")

    png_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]
    total = len(png_files)

    if total == 0:
        print("No .png files found in this directory.")
        return

    print(f"[+] Found {total} PNG files. Starting SHA-1 generation...")

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

    # --- Detect duplicate SHA-1s ---
    sha_counts = Counter(sha for _, sha in rows if not sha.startswith("ERROR"))
    duplicates = {sha for sha, count in sha_counts.items() if count > 1}

    if duplicates:
        print(f"[!] Found {len(duplicates)} duplicate SHA-1 values. Marking all as zeroes...")
        rows = [(name, "0000000000000000000000000000000000000000") if sha in duplicates else (name, sha) for name, sha in rows]
    else:
        print("[+] No duplicate SHA-1s found.")

    # --- Write CSV ---
    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["texture_name", "tri_dumped_alpha_stripped_sha1"])
        writer.writerows(sorted(rows, key=lambda x: x[0].lower()))

    print(f"\n[+] Done. Wrote {len(rows)} entries to {output_csv}")

if __name__ == "__main__":
    main()
