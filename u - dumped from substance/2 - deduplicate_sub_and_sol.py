import os
import zlib
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import time

# === CONFIG ===
SUBSTANCE_DIR = Path(r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance")
SOL_DIR = Path(r"C:\Development\Git\MGS2-PS2-Textures\t - dumped from sons of liberty")
LOG_FILE = Path("crc32_dedupe_log.txt")
MAX_WORKERS = (os.cpu_count() or 8) * 2
LOCK = threading.Lock()

# === HELPERS ===
def compute_crc32(path: Path) -> int:
    crc = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            crc = zlib.crc32(chunk, crc)
    return crc & 0xFFFFFFFF


def print_progress(current, total, start_time, prefix="Progress"):
    elapsed = time.time() - start_time
    rate = current / elapsed if elapsed > 0 else 0
    remaining = (total - current) / rate if rate > 0 else 0
    eta = str(timedelta(seconds=int(remaining)))
    percent = (current / total) * 100
    bar_len = 40
    filled = int(bar_len * percent / 100)
    bar = "#" * filled + "-" * (bar_len - filled)
    sys.stdout.write(f"\r{prefix}: [{bar}] {percent:5.1f}% ({current}/{total}) ETA {eta}")
    sys.stdout.flush()


def gather_tgas(base: Path):
    return {f.relative_to(base): f for f in base.rglob("*.tga")}


def safe_delete(path: Path) -> bool:
    try:
        os.remove(path)
        return True
    except Exception:
        return False


def try_remove_empty_dir(path: Path) -> bool:
    try:
        if not any(path.iterdir()):
            path.rmdir()
            return True
    except Exception:
        pass
    return False


def remove_empty_dirs(base: Path) -> int:
    removed = 0
    dirs = []
    for root, subdirs, _ in os.walk(base, topdown=False):
        for d in subdirs:
            dirs.append(Path(root) / d)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(try_remove_empty_dir, d): d for d in dirs}
        for future in as_completed(futures):
            if future.result():
                removed += 1
    return removed


# === MAIN ===
def main():
    print(f"Using up to {MAX_WORKERS} threads\n")

    print("Scanning folders...")
    substance_files = gather_tgas(SUBSTANCE_DIR)
    sol_files = gather_tgas(SOL_DIR)
    print(f"  Substance: {len(substance_files)} files")
    print(f"  SoL:       {len(sol_files)} files")

    rel_paths = sorted(set(sol_files.keys()) | set(substance_files.keys()), key=lambda p: str(p).lower())

    matched = []
    mismatched = []
    missing_in_sol = []
    missing_in_substance = []
    deleted = 0
    total = len(rel_paths)
    progress = 0
    start_time = time.time()

    def process_path(rel_path):
        sub_path = substance_files.get(rel_path)
        sol_path = sol_files.get(rel_path)

        if not sub_path and not sol_path:
            return None
        if sub_path and not sol_path:
            return ("missing_sol", rel_path)
        if sol_path and not sub_path:
            return ("missing_substance", rel_path)

        try:
            sub_crc = compute_crc32(sub_path)
            sol_crc = compute_crc32(sol_path)
        except Exception as e:
            return ("error", f"{rel_path}: {e}")

        if sub_crc == sol_crc:
            if safe_delete(sol_path):
                return ("deleted", rel_path)
            else:
                return ("failed_delete", rel_path)
        else:
            return ("mismatch", f"{rel_path} | Substance: {sub_crc:08X} | SoL: {sol_crc:08X}")

    print("\nComparing files (multithreaded)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_path, rel): rel for rel in rel_paths}
        for future in as_completed(futures):
            result = future.result()
            with LOCK:
                progress += 1
                print_progress(progress, total, start_time, prefix="Comparisons")
            if not result:
                continue
            kind, data = result
            if kind == "deleted":
                deleted += 1
                matched.append(f"{data} | deleted")
            elif kind == "failed_delete":
                matched.append(f"{data} | FAILED DELETE")
            elif kind == "mismatch":
                mismatched.append(data)
            elif kind == "missing_sol":
                missing_in_sol.append(str(data))
            elif kind == "missing_substance":
                missing_in_substance.append(str(data))
    sys.stdout.write("\n")

    print("Cleaning up empty SoL folders...")
    removed_dirs = remove_empty_dirs(SOL_DIR)

    print("\nWriting log...")
    with open(LOG_FILE, "w", encoding="utf-8") as log:
        log.write(f"CRC32 Deduplication Log - {datetime.now()}\n")
        log.write(f"Threads used: {MAX_WORKERS}\n\n")
        log.write(f"Substance: {SUBSTANCE_DIR}\n")
        log.write(f"SoL: {SOL_DIR}\n\n")
        log.write(f"Deleted SoL files: {deleted}\n")
        log.write(f"Empty SoL folders removed: {removed_dirs}\n")
        log.write(f"Missing in SoL: {len(missing_in_sol)}\n")
        log.write(f"Missing in Substance: {len(missing_in_substance)}\n")
        log.write(f"Mismatched CRC32s: {len(mismatched)}\n\n")

        log.write("\n--- Deleted Files ---\n\n")
        for m in matched:
            log.write(m + "\n")

        log.write("\n--- Mismatched CRC32s ---\n\n")
        for m in mismatched:
            log.write(m + "\n")

        log.write("\n--- Missing in SoL ---\n\n")
        for m in missing_in_sol:
            log.write(m + "\n")

        log.write("\n--- Missing in Substance ---\n\n")
        for m in missing_in_substance:
            log.write(m + "\n")

    print(f"\nDone.")
    print(f"  {deleted} SoL duplicates deleted.")
    print(f"  {removed_dirs} empty folders removed.")
    print(f"  Log written to {LOG_FILE}")


if __name__ == "__main__":
    main()
