import os
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def calc_crc32(path):
    """Compute CRC32 hash of a file (hex, lowercase)."""
    prev = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            prev = zlib.crc32(chunk, prev)
    return format(prev & 0xFFFFFFFF, "08x")

def rename_file(path):
    """Rename file to its CRC32 (skip if already correct)."""
    try:
        crc = calc_crc32(path)
        folder = os.path.dirname(path)
        new_name = crc + ".png"
        new_path = os.path.join(folder, new_name)

        # Skip if already correctly named
        if os.path.basename(path).lower() == new_name.lower():
            return (os.path.basename(path), "Already correct", True, None)

        # Don't delete if src == dst (same path)
        if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(path):
            os.remove(new_path)

        os.rename(path, new_path)
        return (os.path.basename(path), new_name, True, None)
    except Exception as e:
        return (os.path.basename(path), None, False, str(e))

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    png_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]

    total = len(png_files)
    if total == 0:
        print("No .png files found in this directory.")
        return

    print(f"[+] Found {total} PNG files. Starting CRC32 renaming...")

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
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed / total) * 100:.1f}%)")

    print(f"\n[+] Done. Renamed {total} PNGs to their CRC32 hashes (safe to re-run).")

if __name__ == "__main__":
    main()
