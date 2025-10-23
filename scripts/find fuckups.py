import os
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================================
# CONFIGURATION
# ==========================================================
ROOT_DIR = r"C:\Development\Git\MGS2-PS2-Textures"  # change this
OUTPUT_FILE = os.path.join(ROOT_DIR, "sha1_found_files.txt")
MAX_WORKERS = os.cpu_count() or 8

TARGET_HASHES = {
    "2cf48d0c3364be2104f3421e84f656731ea58848",
    "811ec08c834f659460a71955da7e634b7c2cfbf8",
    "987904eb304acfb4a918ea78eedbfa41202560f8",
    "f82a744da2ab4f3d6be49fcfca87a52fe2d4e816",
}

# ==========================================================
# HELPERS
# ==========================================================
def sha1_file(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def worker(path: str):
    try:
        if sha1_file(path) in TARGET_HASHES:
            return path
    except Exception:
        pass
    return None

# ==========================================================
# MAIN
# ==========================================================
def main():
    pngs = []
    for root, _, files in os.walk(ROOT_DIR):
        for name in files:
            if name.lower().endswith(".png"):
                pngs.append(os.path.join(root, name))

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for fut in as_completed([pool.submit(worker, p) for p in pngs]):
            result = fut.result()
            if result:
                results.append(result)

    results.sort()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(results))

if __name__ == "__main__":
    main()
