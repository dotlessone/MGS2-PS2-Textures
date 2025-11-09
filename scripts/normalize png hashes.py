import os
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)
LOCK = threading.Lock()

# ==========================================================
# FUNCTIONS
# ==========================================================
def resave_png(png_path: Path):
    try:
        with Image.open(png_path) as img:
            img.save(png_path, format="PNG", optimize=False)
        with LOCK:
            print(f"[OK] {png_path.name}")
    except Exception as e:
        with LOCK:
            print(f"[FAIL] {png_path.name} - {e}")

def resave_all_pngs():
    script_dir = Path(__file__).resolve().parent
    png_files = list(script_dir.glob("*.png"))

    if not png_files:
        print("No PNG files found in this folder.")
        return

    print(f"Resaving {len(png_files)} PNGs using {MAX_WORKERS} threads...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(resave_png, p) for p in png_files]
        for _ in as_completed(futures):
            pass

    print("All PNGs processed.")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    resave_all_pngs()
