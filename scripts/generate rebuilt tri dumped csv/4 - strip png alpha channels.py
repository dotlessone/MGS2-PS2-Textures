import os
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def resave_noalpha(path):
    """Re-save PNG without alpha channel (convert to RGB)."""
    try:
        with Image.open(path) as img:
            if img.mode in ("RGBA", "LA"):
                img = img.convert("RGB")  # drop alpha
            elif img.mode == "P":
                img = img.convert("RGB")  # convert palette-based images too
            img.save(path, format="PNG", optimize=False)
        return (os.path.basename(path), True, None)
    except Exception as e:
        return (os.path.basename(path), False, str(e))

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    png_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".png")]

    total = len(png_files)
    if total == 0:
        print("No .png files found in this directory.")
        return

    print(f"[+] Found {total} PNG files. Starting multi-threaded re-save (no alpha)...")

    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(resave_noalpha, f): f for f in png_files}

        for future in as_completed(futures):
            name, success, err = future.result()
            with lock:
                completed += 1
                if not success:
                    print(f"[!] Failed {name}: {err}")
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed / total) * 100:.1f}%)")

    print(f"\n[+] Done. Re-saved {total} PNGs with alpha removed.")

if __name__ == "__main__":
    main()
