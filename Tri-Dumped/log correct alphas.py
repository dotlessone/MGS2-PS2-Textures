import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from PIL import Image

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)
ALPHA_THRESHOLD = 128


# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> str:
    """Return absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except subprocess.CalledProcessError:
        raise RuntimeError("Run this inside a Git repository.")


def find_low_alpha_tgas(directories):
    """
    Recursively find .tga images where all alpha <= ALPHA_THRESHOLD.
    Returns list of (relative_path, max_alpha).
    """
    results = []
    lock = Lock()

    def process_image(path):
        try:
            with Image.open(path) as img:
                if img.mode not in ("RGBA", "LA"):
                    return None
                alpha = img.getchannel("A")
                extrema = alpha.getextrema()
                if extrema and extrema[1] <= ALPHA_THRESHOLD:
                    return (path, extrema[1])
        except Exception:
            return None
        return None

    def process_folder(folder):
        local = []
        for entry in os.scandir(folder):
            if entry.is_dir(follow_symlinks=False):
                futures.append(executor.submit(process_folder, entry.path))
            elif entry.is_file() and entry.name.lower().endswith(".tga"):
                result = process_image(entry.path)
                if result:
                    local.append(result)
        with lock:
            results.extend(local)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for d in directories:
            if os.path.isdir(d):
                futures.append(executor.submit(process_folder, d))
        for _ in as_completed(futures):
            pass

    return results


# ==========================================================
# MAIN
# ==========================================================
def main():
    repo_root = get_git_root()
    final_rebuilt = os.path.join(repo_root, "u - dumped from substance", "dump", "Final Rebuilt")
    mixed_alpha = os.path.join(final_rebuilt, "mixed_alpha")
    good_alpha = os.path.join(final_rebuilt, "good_alpha")

    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "valid alpha levels.log")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== LOW-ALPHA TGA FILES (alpha <= 128) ===\n")

    print("Scanning for low-alpha TGAs...")
    results = find_low_alpha_tgas([mixed_alpha, good_alpha])
    results_sorted = sorted(results, key=lambda x: x[0].lower())

    with open(log_path, "a", encoding="utf-8") as f:
        for path, max_alpha in results_sorted:
            rel = os.path.relpath(path, repo_root)
            f.write(f"{rel}\tmax_alpha={max_alpha}\n")

        f.write(f"\nTOTAL: {len(results_sorted)} low-alpha TGA files found.\n")

    print(f"Found {len(results_sorted)} low-alpha TGA files.")
    print(f"Log written to: {log_path}")


if __name__ == "__main__":
    main()
