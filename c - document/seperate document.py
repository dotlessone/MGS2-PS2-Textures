import os
import subprocess
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
from pathlib import Path

# ==========================================================
# CONFIGURATION
# ==========================================================
def get_git_root() -> Path:
    """Return absolute path to the root of the current git repo."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return Path(out.decode().strip())
    except Exception:
        raise RuntimeError("Failed to determine Git repo root. Run this script inside a Git repository.")

GIT_ROOT = get_git_root()
TARGET_DIR = GIT_ROOT / "c - document"
MAX_WORKERS = max(4, os.cpu_count() or 4)

# ==========================================================
# HELPERS
# ==========================================================
def has_only_alpha_128(path: Path) -> bool:
    """Return True if the image has an alpha channel and all alpha values are 128."""
    try:
        with Image.open(path) as img:
            if "A" not in img.getbands():
                return False
            alpha = img.getchannel("A")
            unique = set(alpha.getdata())
            return unique == {128}
    except Exception:
        return False

def move_to_opaque(path: Path):
    """Move file into an 'opaque' subfolder if not already there."""
    if "opaque" in [p.lower() for p in path.parts]:
        return False
    dest_dir = path.parent / "opaque"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / path.name
    shutil.move(str(path), str(dest_path))
    return True

# ==========================================================
# MAIN LOGIC
# ==========================================================
def main():
    print(f"[+] Git root: {GIT_ROOT}")
    print(f"[+] Scanning PNGs in: {TARGET_DIR}")

    png_files = [p for p in TARGET_DIR.rglob("*.png")]
    print(f"[+] Found {len(png_files)} PNG files to check.")

    moved_count = 0
    checked = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(has_only_alpha_128, p): p for p in png_files}
        for fut in as_completed(futures):
            path = futures[fut]
            checked += 1
            try:
                if fut.result():
                    if move_to_opaque(path):
                        moved_count += 1
                        print(f"[MOVE] {path} -> {path.parent / 'opaque' / path.name}")
            except Exception as e:
                print(f"[ERROR] {path}: {e}")

            if checked % 250 == 0 or checked == len(png_files):
                print(f"[Progress] {checked}/{len(png_files)} checked, {moved_count} moved.")

    print(f"\n[Summary] Checked: {checked}, Moved: {moved_count}")
    print("[+] Done.")

# ==========================================================
if __name__ == "__main__":
    main()
