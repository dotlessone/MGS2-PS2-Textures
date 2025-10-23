import os
import csv
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

def process_file(path):
    """Process a single PNG file and return (filename, alpha_levels)."""
    base = os.path.splitext(os.path.basename(path))[0]
    levels = get_alpha_levels(path)
    return (base, str(levels))

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    output_csv = os.path.join(folder, "3 - pcsx2_dumped_alpha_levels.csv")

    png_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]
    total = len(png_files)

    if total == 0:
        print("No .png files found in this directory.")
        return

    print(f"[+] Found {total} PNG files. Starting multithreaded alpha scan...")

    rows = []
    completed = 0
    lock = threading.Lock()

    # Thread pool for concurrency
    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(process_file, f): f for f in png_files}

        for future in as_completed(futures):
            rows.append(future.result())
            with lock:
                completed += 1
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed / total) * 100:.1f}%)")

    # Write output CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["pcsx2_dumped_sha1", "pcsx2_alpha_levels"])
        writer.writerows(sorted(rows, key=lambda x: x[0].lower()))

    print(f"\n[+] Done. Logged alpha levels for {total} PNGs.")
    print(f"[+] Output written to: {output_csv}")

if __name__ == "__main__":
    main()
