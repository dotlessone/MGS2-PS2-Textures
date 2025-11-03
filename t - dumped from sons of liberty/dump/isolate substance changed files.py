import os
import subprocess
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from collections import defaultdict

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> Path:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return Path(out.decode().strip())
    except Exception:
        raise RuntimeError("Failed to determine git repo root. Run this script inside a Git repository.")


def calc_sha1(path: Path) -> str:
    """Compute SHA1 hash for a file."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def index_files(root_dir: Path, ext: str) -> dict[str, list[Path]]:
    """Build a dictionary mapping SHA1 -> list of paths."""
    index = defaultdict(list)
    lock = Lock()

    def worker(p: Path):
        try:
            sha1 = calc_sha1(p)
            with lock:
                index[sha1].append(p)
        except Exception as e:
            with lock:
                print(f"[ERROR] {p}: {e}")

    files = list(root_dir.rglob(f"*.{ext.lower()}"))
    print(f"Indexing {len(files)} files...")

    with ThreadPoolExecutor() as executor:
        for _ in as_completed([executor.submit(worker, f) for f in files]):
            pass

    return index


def deduplicate(index: dict[str, list[Path]], final_rebuilt_dir: Path):
    """Delete duplicates, keeping 'Final Rebuilt' versions when possible."""
    print_lock = Lock()
    deleted = 0
    duplicate_groups = sum(1 for v in index.values() if len(v) > 1)

    print(f"Found {duplicate_groups} duplicate hash groups.\n")

    for sha1, paths in index.items():
        if len(paths) < 2:
            continue

        keep = None

        # Prefer the version inside Final Rebuilt
        for p in paths:
            if final_rebuilt_dir in p.parents:
                keep = p
                break

        if not keep:
            # Default: keep first one arbitrarily
            keep = paths[0]

        for p in paths:
            if p == keep:
                continue
            try:
                os.remove(p)
                with print_lock:
                    print(f"[DELETED] {p} (duplicate of {keep.name})")
                deleted += 1
            except Exception as e:
                with print_lock:
                    print(f"[ERROR] Failed to delete {p}: {e}")

    print(f"\nDeduplication complete. Deleted {deleted} duplicates total.")


def remove_empty_dirs(root_dir: Path):
    """Recursively remove all empty directories under root_dir."""
    removed = 0
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        if not dirnames and not filenames:
            try:
                Path(dirpath).rmdir()
                print(f"[REMOVED EMPTY] {dirpath}")
                removed += 1
            except Exception:
                pass
    print(f"\nRemoved {removed} empty directories.")


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    REPO_ROOT = get_git_root()
    T_DIR = REPO_ROOT / "t - dumped from sons of liberty" / "dump"
    FINAL_REBUILT = T_DIR / "Final Rebuilt"

    if not T_DIR.exists():
        raise RuntimeError(f"Missing directory: {T_DIR}")
    if not FINAL_REBUILT.exists():
        raise RuntimeError(f"Missing directory: {FINAL_REBUILT}")

    print("Building hash index...")
    index = index_files(T_DIR, "tga")

    print("\nDeduplicating files...")
    deduplicate(index, FINAL_REBUILT)

    print("\nCleaning up empty folders...")
    remove_empty_dirs(T_DIR)

    print("\nAll tasks complete.")
