import os
import csv
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)

EXCLUDE_PATTERNS = [
    r"^sr.{3}_alp_ovl.*",
    r"^dammydoll_alp.bmp",
    r"^medicine_rabel_alp_ovl.bmp",
    r"^gasmask_alp.bmp",
]

EXCLUDE_REGEXES = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]

# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> str:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except subprocess.CalledProcessError:
        raise RuntimeError("Failed to determine Git repo root. Run this inside a Git repository.")


def scan_files(root_dirs, exts):
    """Recursively collect all lowercase base filenames matching given extensions from multiple roots."""
    results = set()
    lock = Lock()

    def process_folder(folder):
        local = []
        for entry in os.scandir(folder):
            if entry.is_dir(follow_symlinks=False):
                futures.append(executor.submit(process_folder, entry.path))
            elif entry.is_file():
                name_lower = entry.name.lower()
                for ext in exts:
                    if name_lower.endswith(ext):
                        local.append(os.path.splitext(entry.name.lower())[0])
                        break
        with lock:
            results.update(local)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for root in root_dirs:
            if os.path.isdir(root):
                futures.append(executor.submit(process_folder, root))
        for _ in as_completed(futures):
            pass

    return results


def is_excluded(name: str) -> bool:
    """Return True if the lowercase filename matches any exclude regex."""
    for regex in EXCLUDE_REGEXES:
        if regex.match(name):
            return True
    return False


def load_ps2_texture_names(dim_csv_path):
    """Load all valid PS2 texture names from the dimensions CSV."""
    valid_names = set()
    with open(dim_csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tex = (row.get("texture_name") or "").strip().lower()
            if tex:
                valid_names.add(tex)
    return valid_names


# ==========================================================
# MAIN
# ==========================================================
def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = get_git_root()

    csv_dir = os.path.join(repo_root, "u - dumped from substance")
    texture_map_csv = os.path.join(csv_dir, "mgs2_texture_map.csv")
    ps2_dim_csv = os.path.join(csv_dir, "mgs2_ps2_dimensions.csv")

    log_path = os.path.join(script_dir, "missing_stage_counts.log")
    per_stage_dir = os.path.join(script_dir, "missing textures per stage")

    os.makedirs(per_stage_dir, exist_ok=True)

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("")

    def log(msg):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    if not os.path.isfile(texture_map_csv):
        raise FileNotFoundError(f"CSV not found at: {texture_map_csv}")
    if not os.path.isfile(ps2_dim_csv):
        raise FileNotFoundError(f"PS2 dimensions CSV not found at: {ps2_dim_csv}")

    print(f"Detected repo root: {repo_root}")
    print(f"Loading texture map from: {texture_map_csv}")
    print(f"Loading PS2 dimensions from: {ps2_dim_csv}")
    print("Scanning PNGs and TGAs...")

    log(f"Detected repo root: {repo_root}")
    log(f"Texture map CSV: {texture_map_csv}")
    log(f"PS2 dimensions CSV: {ps2_dim_csv}")
    log(f"Scanning directories:")
    log(f"  - {script_dir}")
    log(f"  - {os.path.join(repo_root, 'u - dumped from substance', 'dump', 'Final Rebuilt', 'half_alpha')}")
    log(f"  - {os.path.join(repo_root, 'u - dumped from substance', 'dump', 'Final Rebuilt', 'opaque')}")
    log("")

    # ==========================================================
    # LOAD PS2 VALID NAMES
    # ==========================================================
    valid_ps2_names = load_ps2_texture_names(ps2_dim_csv)
    print(f"Loaded {len(valid_ps2_names)} valid PS2 texture names.")
    log(f"Loaded {len(valid_ps2_names)} valid PS2 texture names.\n")

    # ==========================================================
    # LOAD TEXTURE MAP CSV
    # ==========================================================
    csv_entries = []
    with open(texture_map_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith(";"):
                continue
            texture_filename, stage, tri_strcode, texture_strcode = row
            name = texture_filename.strip().lower()
            if name in valid_ps2_names:
                csv_entries.append((name, stage.strip()))

    stage_to_textures = defaultdict(set)
    for tex, stage in csv_entries:
        stage_to_textures[stage].add(tex)

    # ==========================================================
    # SCAN DIRECTORIES
    # ==========================================================
    half_alpha_dir = os.path.join(repo_root, "u - dumped from substance", "dump", "Final Rebuilt", "half_alpha")
    opaque_dir = os.path.join(repo_root, "u - dumped from substance", "dump", "Final Rebuilt", "opaque")
    root_dirs = [script_dir, half_alpha_dir, opaque_dir]

    found_names = scan_files(root_dirs, [".png", ".tga"])
    print(f"Found {len(found_names)} PNG/TGA files total.")
    log(f"Found {len(found_names)} PNG/TGA files total.\n")

    # ==========================================================
    # DETECT MISSING
    # ==========================================================
    missing_per_stage = defaultdict(list)
    all_missing_unique = set()

    for stage, texnames in stage_to_textures.items():
        texnames_lower = [t.lower() for t in texnames if t.lower() in valid_ps2_names]
        filtered_texnames = [t for t in texnames_lower if not is_excluded(t)]
        missing = [t for t in filtered_texnames if t not in found_names]

        if missing:
            missing_per_stage[stage].extend(missing)
            all_missing_unique.update(missing)

    # ==========================================================
    # LOG: SORTED BY MISSING COUNT
    # ==========================================================
    sorted_desc = sorted(((k, len(v)) for k, v in missing_per_stage.items()), key=lambda x: x[1], reverse=True)
    header_desc = "=== MISSING TEXTURE COUNTS BY STAGE (DESCENDING) ==="
    print("\n" + header_desc)
    log(header_desc)
    if not sorted_desc:
        msg = "All textures accounted for."
        print(msg)
        log(msg)
    else:
        for stage, count in sorted_desc:
            line = f"{stage}\t{count}"
            print(line)
            log(line)
    log("")
    total_line = f"TOTAL UNIQUE MISSING: {len(all_missing_unique)}"
    print("\n" + total_line)
    log(total_line)

    # ==========================================================
    # LOG: SORTED ALPHABETICALLY
    # ==========================================================
    sorted_alpha = sorted(((k, len(v)) for k, v in missing_per_stage.items()), key=lambda x: x[0].lower())
    header_alpha = "\n=== MISSING TEXTURE COUNTS BY STAGE (ALPHABETICAL) ==="
    print(header_alpha)
    log(header_alpha)
    if not sorted_alpha:
        msg = "All textures accounted for."
        print(msg)
        log(msg)
    else:
        for stage, count in sorted_alpha:
            line = f"{stage}\t{count}"
            print(line)
            log(line)

    # ==========================================================
    # PER-STAGE LOGS (ONLY REWRITE IF CHANGED)
    # ==========================================================
    existing_logs = {f for f in os.listdir(per_stage_dir) if f.lower().endswith(".txt")}
    written_logs = set()

    for stage, missing in missing_per_stage.items():
        if not missing:
            continue

        stage_filename = f"{stage}.txt"
        written_logs.add(stage_filename)
        stage_log_path = os.path.join(per_stage_dir, stage_filename)

        new_contents = "\n".join(sorted(set(missing), key=str.lower)) + "\n"

        old_contents = None
        if os.path.exists(stage_log_path):
            try:
                with open(stage_log_path, "r", encoding="utf-8") as f:
                    old_contents = f.read()
            except Exception:
                old_contents = None

        if old_contents != new_contents:
            with open(stage_log_path, "w", encoding="utf-8") as f:
                f.write(new_contents)

    # Remove stale logs
    for old_log in existing_logs - written_logs:
        try:
            os.remove(os.path.join(per_stage_dir, old_log))
        except Exception as e:
            print(f"Failed to remove stale log {old_log}: {e}")

    print(f"\nPer-stage logs written to: {per_stage_dir}")
    print(f"Log written to: {log_path}")


if __name__ == "__main__":
    main()
