import os
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
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
# EMPTY DIR CHECK
# ==========================================================
def try_remove_dir(path: Path):
    """Try to remove the directory if it's empty. Return True if removed."""
    try:
        # Only remove real directories, not symlinks
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
            return str(path)
    except Exception:
        pass
    return None


# ==========================================================
# MAIN
# ==========================================================
def main():
    git_root = get_git_root()
    print(f"Git root detected: {git_root}")

    # Gather all directories (deepest first)
    dirs = sorted(
        [p for p in git_root.rglob("*") if p.is_dir()],
        key=lambda x: len(x.parts),
        reverse=True
    )

    print(f"Scanning {len(dirs)} directories for emptiness...")

    removed_total = 0
    lock = Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as exe:
        futures = {exe.submit(try_remove_dir, d): d for d in dirs}

        for f in as_completed(futures):
            result = f.result()
            if result:
                with lock:
                    removed_total += 1
                print(f"REMOVED: {result}")

    print(f"Done. Removed {removed_total} empty directories.")


if __name__ == "__main__":
    main()
