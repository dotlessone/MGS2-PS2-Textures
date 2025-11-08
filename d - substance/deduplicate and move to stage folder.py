import os
import subprocess
import hashlib
import shutil
import csv
import difflib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

# ==========================================================
# HELPERS
# ==========================================================

def get_git_root() -> Path:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return Path(out.decode().strip())
    except Exception as e:
        raise RuntimeError(f"Failed to determine git repo root: {e}")

def calc_sha1(path: Path) -> str:
    """Compute SHA1 hash for a file."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def collect_hashes(root: Path) -> set[str]:
    """Recursively collect SHA1 hashes for all files under root."""
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
    """Load valid stage names from mgs2_texture_map.csv."""
    stages = set()
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith(";"):
                continue
            if len(row) > 1:
                stages.add(row[1].strip().lower())
    return stages

def prompt_stage_name(valid_stages: set[str]) -> str:
    """Prompt user for a stage name, re-requesting until valid."""
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
    """Check if folder is already open in Explorer, and open it if not."""
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

# ==========================================================
# MAIN
# ==========================================================

def main():
    repo_root = get_git_root()
    verif_root = repo_root / "final_verification_nov22_2025"
    substance_root = repo_root / "d - substance"
    csv_path = repo_root / "u - dumped from substance" / "mgs2_texture_map.csv"

    # Sanity checks
    if not verif_root.exists():
        raise FileNotFoundError(f"Missing directory: {verif_root}")
    if not substance_root.exists():
        raise FileNotFoundError(f"Missing directory: {substance_root}")
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV: {csv_path}")

    # ------------------------------------------------------
    # PRECHECK: Make sure PNGs exist in top-level folder
    # ------------------------------------------------------
    top_pngs = [p for p in substance_root.iterdir() if p.is_file() and p.suffix.lower() == ".png"]
    if not top_pngs:
        print(f"[ABORTED] No PNG files found in the top-level of {substance_root}. Nothing to do.")
        return
    print(f"[INFO] Found {len(top_pngs)} PNG file(s) in {substance_root}.")

    # ------------------------------------------------------
    # LOAD VALID STAGES
    # ------------------------------------------------------
    print(f"[*] Loading valid stage names from {csv_path.name}...")
    valid_stages = load_valid_stages(csv_path)
    print(f"[+] Loaded {len(valid_stages)} unique stage names from CSV.")

    # ------------------------------------------------------
    # COLLECT VERIFICATION HASHES
    # ------------------------------------------------------
    print(f"[*] Collecting SHA1s from {verif_root}...")
    verif_hashes = collect_hashes(verif_root)
    print(f"[+] Collected {len(verif_hashes)} unique hashes.")

    # ------------------------------------------------------
    # DELETE DUPLICATE PNGS
    # ------------------------------------------------------
    print(f"[*] Scanning top-level PNG files in {substance_root}...")
    deleted_count = 0
    for entry in top_pngs:
        try:
            sha = calc_sha1(entry)
            if sha in verif_hashes:
                print(f"[DELETE] {entry.name} (SHA1 matched)")
                entry.unlink()
                deleted_count += 1
        except Exception as e:
            print(f"[ERROR] Failed to process {entry}: {e}")

    print(f"[DONE] Removed {deleted_count} duplicate PNG file(s).")

    # ------------------------------------------------------
    # MOVE REMAINING PNG FILES INTO STAGE SUBFOLDER
    # ------------------------------------------------------
    remaining_pngs = [p for p in substance_root.iterdir() if p.is_file() and p.suffix.lower() == ".png"]
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

    # ------------------------------------------------------
    # OPEN STAGE FOLDER IN EXPLORER IF NOT ALREADY OPEN
    # ------------------------------------------------------
    ensure_explorer_open(stage_dir)

# ==========================================================
if __name__ == "__main__":
    main()
