import os
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
import shutil

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> Path:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL
        )
        return Path(out.decode().strip())
    except Exception as e:
        raise RuntimeError(f"Not inside a Git repository: {e}")


# ==========================================================
# CONFIG
# ==========================================================
GIT_ROOT = get_git_root()
ROOT = GIT_ROOT / "final_verification_nov22_2025"
ALPHA_DIR = ROOT / "needs redump"
MAX_WORKERS = max(4, os.cpu_count() or 4)

ALPHA_DIR.mkdir(exist_ok=True)


# ==========================================================
# ALPHA CHECK
# ==========================================================
def file_has_alpha(path: Path) -> bool:
    """Return True if ANY pixel has alpha > 128."""
    try:
        with Image.open(path) as img:
            if img.mode not in ("RGBA", "LA"):
                return False

            alpha = img.getchannel("A")
            for pixel in alpha.getdata():
                if pixel > 128:
                    return True

    except Exception as e:
        print(f"Error processing {path}: {e}")

    return False


def process_file(path: Path):
    if file_has_alpha(path):
        dest = ALPHA_DIR / path.name
        shutil.move(str(path), str(dest))
        return f"MOVED: {path}"
    return None


# ==========================================================
# MAIN
# ==========================================================
def main():
    png_files = list(ROOT.rglob("*.png"))

    print(f"Scanning directory: {ROOT}")
    print(f"Found {len(png_files)} PNG files. Scanning...")

    moved = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(process_file, p): p for p in png_files}

        for f in as_completed(futures):
            result = f.result()
            if result:
                print(result)
                moved += 1

    print(f"Done. Moved {moved} files to {ALPHA_DIR}")


if __name__ == "__main__":
    main()
    import subprocess
    try:
        generate_script = os.path.join(os.path.dirname(__file__), "0009 - remove empty folders.py")
        if os.path.exists(generate_script):
            print(f"\n[Final Step] Launching: {os.path.basename(generate_script)}...")
            result = subprocess.run(
                ["python", generate_script],
                check=False,
                capture_output=True,
                text=True
            )
        else:
            print(f"[Final Step] Skipped — {generate_script} not found.")
    except Exception as e:
        print(f"[!] Failed to run 0001 - generate list of remaining stages.py: {e}")
