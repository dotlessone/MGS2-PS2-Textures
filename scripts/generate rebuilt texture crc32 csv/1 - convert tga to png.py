import os
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def convert_tga_to_png(path):
    """Convert a single .tga to .png (overwrite if exists)."""
    try:
        base = os.path.splitext(os.path.basename(path))[0]
        out_path = os.path.join(os.path.dirname(path), base + ".png")

        with Image.open(path) as img:
            img.save(out_path, format="PNG", optimize=False)

        return (base, True, None)
    except Exception as e:
        return (os.path.basename(path), False, str(e))

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    tga_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".tga")]
    total = len(tga_files)

    if total == 0:
        print("No .tga files found in this directory.")
        return

    print(f"[+] Found {total} TGA files. Starting conversion (overwrite enabled)...")

    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(convert_tga_to_png, f): f for f in tga_files}

        for future in as_completed(futures):
            name, success, err = future.result()
            with lock:
                completed += 1
                if not success:
                    print(f"[!] Failed {name}: {err}")
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed / total) * 100:.1f}%)")

    print(f"\n[+] Done. Converted {total} TGAs to PNG (overwritten where applicable).")

if __name__ == "__main__":
    main()
