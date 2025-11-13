import os
import subprocess
import hashlib
import shutil
import csv
import difflib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from PIL import Image

# ==========================================================
# CONFIGURATION
# ==========================================================
FORCE_DELETE_PATTERNS = [
    r"-00002640\.png$",
    r"-80402642\.png$",
    r"-80c02202\.png$",
    r"e610aa167a687a0f-00001a2c\.png$",

]
FORCE_DELETE_REGEXES = [re.compile(p, re.IGNORECASE) for p in FORCE_DELETE_PATTERNS]
EXCLUDE_REGEXES = []

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> Path:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return Path(out.decode().strip())
    except Exception as e:
        raise RuntimeError(f"Failed to determine git repo root: {e}")

def calc_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def collect_hashes(root: Path) -> set[str]:
    sha1s = set()
    paths = [p for p in root.rglob("*") if p.is_file()]
    total = len(paths)
    lock = Lock()
    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as ex:
        futures = {ex.submit(calc_sha1, p): p for p in paths}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                sha = fut.result()
                with lock:
                    sha1s.add(sha)
            except Exception as e:
                print(f"[ERROR] Failed to hash {futures[fut]}: {e}")
            if i % 250 == 0:
                print(f"[Progress] {i}/{total} hashed from {root.name}")
    return sha1s

def load_valid_stages(csv_path: Path) -> set[str]:
    stages = set()
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith(";"):
                continue
            if len(row) > 1:
                stages.add(row[1].strip().lower())
    return stages

def load_valid_dimensions(dim_csv: Path) -> set[tuple[int, int]]:
    dims = set()
    with open(dim_csv, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                w = int(row["tri_dumped_width"])
                h = int(row["tri_dumped_height"])
                dims.add((w, h))
            except Exception:
                continue
    return dims

def get_image_size(path: Path) -> tuple[int, int] | None:
    try:
        with Image.open(path) as img:
            return img.width, img.height
    except Exception:
        return None

def matches_force_delete(filename: str) -> bool:
    return any(rx.search(filename) for rx in FORCE_DELETE_REGEXES)

def matches_exclude(filename: str) -> bool:
    return any(rx.match(filename) for rx in EXCLUDE_REGEXES)

def prompt_stage_name(valid_stages: set[str]) -> str:
    while True:
        stage_name = input("\nEnter stage name (new subfolder name): ").strip()
        if not stage_name:
            print("[ABORTED] Empty input. Please enter a valid stage name.")
            continue
        lower = stage_name.lower()
        if lower in valid_stages:
            print(f"[VALID] Stage name '{stage_name}' confirmed in CSV.")
            return stage_name
        suggestion = difflib.get_close_matches(lower, valid_stages, n=1, cutoff=0.7)
        if suggestion:
            print(f"[INVALID] '{stage_name}' not found. Did you mean '{suggestion[0]}'?")
        else:
            print(f"[INVALID] '{stage_name}' not found in mgs2_texture_map.csv. Try again.")

def ensure_explorer_open(folder: Path):
    ps_script = r"""
    $target = (Resolve-Path '%s').Path.ToLower()
    $open = (New-Object -ComObject Shell.Application).Windows() | ForEach-Object {
        try { $_.Document.Folder.Self.Path.ToLower() } catch {}
    } | Where-Object { $_ -eq $target }
    if (-not $open) { Start-Process explorer.exe $target }
    """ % folder
    try:
        subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"[WARN] Could not check/open Explorer window: {e}")


import ctypes
import psutil
import time

def focus_pcsx2():
    """Bring pcsx2-qt.exe to the foreground if the process is running."""
    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32

    hwnd = None

    # Try to find a window owned by pcsx2-qt.exe
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] and proc.info['name'].lower() == "pcsx2-qt.exe":
            hwnd = user32.GetTopWindow(0)
            while hwnd:
                pid = ctypes.c_ulong()
                user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
                if pid.value == proc.info['pid']:
                    break
                hwnd = user32.GetWindow(hwnd, 2)  # GW_HWNDNEXT
            break

    if hwnd:
        user32.ShowWindow(hwnd, 9)  # SW_RESTORE
        time.sleep(0.1)
        user32.SetForegroundWindow(hwnd)
        print("PCSX2 window focused.")
    else:
        print("PCSX2 window not found.")


# ==========================================================
# MAIN
# ==========================================================
def main():
    repo_root = get_git_root()
    verif_root = repo_root / "final_verification_nov22_2025"
    substance_root = repo_root / "d - substance"
    csv_path = repo_root / "u - dumped from substance" / "mgs2_texture_map.csv"
    dim_csv = repo_root / "u - dumped from substance" / "mgs2_ps2_dimensions.csv"
    process_script = substance_root / "process dumped.py"

    if not verif_root.exists():
        raise FileNotFoundError(f"Missing directory: {verif_root}")
    if not substance_root.exists():
        raise FileNotFoundError(f"Missing directory: {substance_root}")
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV: {csv_path}")
    if not dim_csv.exists():
        raise FileNotFoundError(f"Missing dimensions CSV: {dim_csv}")

    # ======================================================
    # SNAPSHOT OF ALL FILES AT SCRIPT START
    # ======================================================
    initial_pngs = [p for p in substance_root.rglob("*.png")]
    print(f"[INFO] Snapshot taken: {len(initial_pngs)} PNG(s) existed when script started.")

    top_pngs = [p for p in initial_pngs if p.parent == substance_root]
    subfolder_pngs = [p for p in initial_pngs if p.parent != substance_root]

    if not top_pngs:
        print(f"[ABORTED] No top-level PNG files found in {substance_root}.")
        return

    # ------------------------------------------------------
    # STEP -1: DEDUPLICATE BY SHA1 IN TOP FOLDER
    # ------------------------------------------------------
    print("[*] Deduplicating top-level PNGs by SHA1...")
    seen_sha1 = {}
    duplicates_removed = 0
    deduped_pngs = []

    for p in top_pngs:
        try:
            sha = calc_sha1(p)
        except Exception as e:
            print(f"[ERROR] Could not hash {p}: {e}")
            continue

        if sha in seen_sha1:
            print(f"[DELETE] Duplicate SHA1 {sha[:8]}... -> {p.name} (keeping {seen_sha1[sha].name})")
            try:
                p.unlink()
                duplicates_removed += 1
            except Exception as e:
                print(f"[ERROR] Failed to delete duplicate {p}: {e}")
        else:
            seen_sha1[sha] = p
            deduped_pngs.append(p)

    top_pngs = deduped_pngs
    print(f"[INFO] Removed {duplicates_removed} duplicate PNG(s). Remaining: {len(top_pngs)}")

    if not top_pngs:
        print("[ABORTED] No PNGs remain after deduplication.")
        return

    # ------------------------------------------------------
    # STEP -0: REMOVE TOP-LEVEL PNGs THAT EXIST IN SUBFOLDERS
    # ------------------------------------------------------
    print("[*] Removing top-level PNGs that duplicate files in subfolders...")
    if not subfolder_pngs:
        print("[INFO] No subfolder PNGs found. Skipping SHA1 cross-check.")
    else:
        sub_sha1s = set()
        lock = Lock()
        with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as ex:
            futures = {ex.submit(calc_sha1, p): p for p in subfolder_pngs}
            total = len(futures)
            for i, fut in enumerate(as_completed(futures), 1):
                try:
                    sha = fut.result()
                    with lock:
                        sub_sha1s.add(sha)
                except Exception as e:
                    print(f"[ERROR] Failed to hash subfolder file {futures[fut]}: {e}")
                if i % 250 == 0:
                    print(f"[Progress] {i}/{total} subfolder hashes collected")

        removed_subdup = 0
        kept_pngs = []
        for p in top_pngs:
            try:
                sha = calc_sha1(p)
                if sha in sub_sha1s:
                    print(f"[DELETE] {p.name} (duplicate SHA1 found in subfolder)")
                    p.unlink()
                    removed_subdup += 1
                else:
                    kept_pngs.append(p)
            except Exception as e:
                print(f"[ERROR] Could not check {p}: {e}")

        top_pngs = kept_pngs
        print(f"[INFO] Removed {removed_subdup} top-level duplicate(s). Remaining: {len(top_pngs)}")

    if not top_pngs:
        print("[ABORTED] No PNGs remain after subfolder duplicate filtering.")
        return

    # ------------------------------------------------------
    # STEP 0: FORCE DELETE REGEX MATCHES
    # ------------------------------------------------------
    print("[*] Removing force-delete regex matches...")
    removed_forced = 0
    remaining_pngs = []
    for p in top_pngs:
        if matches_force_delete(p.name):
            print(f"[DELETE] {p.name} (matched force-delete regex)")
            try:
                p.unlink()
                removed_forced += 1
            except Exception as e:
                print(f"[ERROR] Failed to delete {p}: {e}")
        else:
            remaining_pngs.append(p)
    print(f"[INFO] Removed {removed_forced} force-deleted PNG(s). Remaining: {len(remaining_pngs)}")

    if not remaining_pngs:
        print("[ABORTED] No PNGs remain after force-delete filtering.")
        return

    # ------------------------------------------------------
    # STEP 1: DELETE EXCLUDED FILENAMES (GENERAL)
    # ------------------------------------------------------
    print("[*] Removing excluded filenames...")
    removed_excluded = 0
    filtered_pngs = []
    for p in remaining_pngs:
        if matches_exclude(p.name):
            print(f"[DELETE] {p.name} (matched exclude pattern)")
            try:
                p.unlink()
                removed_excluded += 1
            except Exception as e:
                print(f"[ERROR] Failed to delete {p}: {e}")
        else:
            filtered_pngs.append(p)
    print(f"[INFO] Removed {removed_excluded} excluded PNG(s). Remaining: {len(filtered_pngs)}")

    if not filtered_pngs:
        print("[ABORTED] No PNGs remain after exclude filtering.")
        return

    # ------------------------------------------------------
    # STEP 2: LOAD STAGE/DIMENSION DATA
    # ------------------------------------------------------
    print(f"[*] Loading valid stage names from {csv_path.name}...")
    valid_stages = load_valid_stages(csv_path)
    print(f"[+] Loaded {len(valid_stages)} unique stage names from CSV.")

    print(f"[*] Loading valid (width,height) pairs from {dim_csv.name}...")
    valid_dims = load_valid_dimensions(dim_csv)
    print(f"[+] Loaded {len(valid_dims)} unique dimension pairs from CSV.")

    # ------------------------------------------------------
    # STEP 3: FILTER BY DIMENSIONS
    # ------------------------------------------------------
    print("[*] Checking PNG dimensions...")
    removed_dim = 0
    kept_pngs = []
    for p in filtered_pngs:
        size = get_image_size(p)
        if size is None:
            print(f"[ERROR] Could not read image size for {p.name}")
            continue
        if size not in valid_dims:
            print(f"[DELETE] {p.name} (invalid dimensions {size[0]}x{size[1]})")
            try:
                p.unlink()
                removed_dim += 1
            except Exception as e:
                print(f"[ERROR] Failed to delete {p}: {e}")
        else:
            kept_pngs.append(p)
    print(f"[INFO] Removed {removed_dim} invalid-dimension PNG(s). Remaining: {len(kept_pngs)}")

    if not kept_pngs:
        print("[ABORTED] No PNGs remain after dimension filtering.")
        return

    # ------------------------------------------------------
    # STEP 3.5: FILTER BY ALPHA (reject if any pixel alpha > 128)
    # ------------------------------------------------------
    print("[*] Checking alpha channels (must be <= 128)...")
    removed_alpha = 0
    alpha_kept = []

    for p in kept_pngs:
        try:
            with Image.open(p) as img:
                if img.mode not in ("RGBA", "LA"):
                    alpha_kept.append(p)
                    continue

                alpha = img.getchannel("A")
                extrema = alpha.getextrema()
                max_alpha = extrema[1]

                if max_alpha > 128:
                    print(f"[DELETE] {p.name} (alpha {max_alpha} > 128)")
                    p.unlink()
                    removed_alpha += 1
                else:
                    alpha_kept.append(p)

        except Exception as e:
            print(f"[ERROR] Failed alpha check for {p}: {e}")

    print(f"[INFO] Removed {removed_alpha} PNG(s) for alpha > 128. Remaining: {len(alpha_kept)}")

    if not alpha_kept:
        print("[ABORTED] No PNGs remain after alpha filtering.")
        return

    kept_pngs = alpha_kept

    # ------------------------------------------------------
    # STEP 4: SHA1 CHECKS
    # ------------------------------------------------------
    print(f"[*] Collecting SHA1s from {verif_root}...")
    verif_hashes = collect_hashes(verif_root)
    print(f"[+] Collected {len(verif_hashes)} unique hashes.")

    deleted_count = 0
    for entry in kept_pngs:
        try:
            sha = calc_sha1(entry)
            if sha in verif_hashes:
                print(f"[DELETE] {entry.name} (SHA1 matched existing verification file)")
                entry.unlink()
                deleted_count += 1
        except Exception as e:
            print(f"[ERROR] Failed to process {entry}: {e}")
    print(f"[DONE] Removed {deleted_count} duplicate PNG file(s).")

    # ------------------------------------------------------
    # STEP 5: MOVE REMAINING FILES
    # ------------------------------------------------------
    remaining_pngs = [p for p in initial_pngs if p.exists() and p.parent == substance_root]
    if not remaining_pngs:
        print("[INFO] No remaining PNGs to move after cleanup.")
        return

    stage_name = prompt_stage_name(valid_stages)
    stage_dir = substance_root / stage_name
    stage_dir.mkdir(parents=True, exist_ok=True)

    moved_count = 0
    for entry in remaining_pngs:
        dest = stage_dir / entry.name
        try:
            shutil.move(str(entry), str(dest))
            moved_count += 1
            print(f"[MOVED] {entry.name} -> {stage_dir.name}/")
        except Exception as e:
            print(f"[ERROR] Failed to move {entry}: {e}")

    print(f"[DONE] Moved {moved_count} PNG file(s) into '{stage_dir.name}'.")
    ensure_explorer_open(stage_dir)

    # ------------------------------------------------------
    # STEP 6: RUN "process dumped.py"
    # ------------------------------------------------------
    focus_pcsx2();
    
    if process_script.exists():
        print(f"[*] Running follow-up script: {process_script.name}")
        try:
            subprocess.run(["python", str(process_script)], check=True)
            print("[+] process dumped.py completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] process dumped.py failed: {e}")
    else:
        print(f"[WARN] Missing script: {process_script}")

# ==========================================================
if __name__ == "__main__":
    main()
