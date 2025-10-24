import os
import csv
import hashlib
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from PIL import Image

# ==========================================================
# CONFIGURATION
# ==========================================================
ROOT_DIR = r"C:\Development\Git\MGS2-PS2-Textures"
DEST_DIR = r"C:\Development\Git\MGS2-PS2-Textures\final_verification_nov22_2025"
CONFLICT_DIR = os.path.join(DEST_DIR, "conflicted")
DIMENSIONS_CSV = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_mc_dimensions.csv"
PCSX2_SHA1_LOG = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_dumped_sha1_log.csv"

PASS1_CSV = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_mc_sha1_matches.csv"
PASS2_CSV = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_tri_sha1_matches.csv"

EXCLUDE_DIRS = ["Self Remade", "Renamed Copies - Better LODs"]

# ==========================================================
# UTILITIES
# ==========================================================
def calc_sha1(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def log_line(line, lock, log_path):
    with lock:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")

def get_unique_alpha_values(path):
    try:
        with Image.open(path) as img:
            if "A" not in img.getbands():
                return [255]
            alpha = img.getchannel("A")
            return sorted(set(alpha.getdata()))
    except Exception:
        return ["error"]

# ==========================================================
# CSV LOADERS
# ==========================================================
def load_sha1_mapping(csv_path, log_path):
    ALLOWED_ALPHA_ZERO_TEXTURES = {
        "0076cd21","00c1181b","00f7f08f","00f8f08f",
        "blk_msk_alp.bmp","d_sky_hasira_alp.bmp","fat_shues_ovl_alp_emap_mod1120.bmp",
        "magazine_tx_alp.bmp","null_msk.bmp","sky_n3_alp.bmp",
    }

    mapping = {}
    skipped = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "pcsx2_dumped_sha1" not in reader.fieldnames or "texture_name" not in reader.fieldnames or "pcsx2_alpha_levels" not in reader.fieldnames:
            raise ValueError(f"{csv_path} missing required columns")

        for row in reader:
            sha1_val = (row["pcsx2_dumped_sha1"] or "").strip().lower()
            tex_name = (row["texture_name"] or "").strip()
            alpha_levels = (row["pcsx2_alpha_levels"] or "").strip()
            if not sha1_val or not tex_name:
                continue
            if alpha_levels == "[0]" and tex_name.lower() not in ALLOWED_ALPHA_ZERO_TEXTURES:
                skipped.append((sha1_val, tex_name))
                continue
            mapping.setdefault(sha1_val, set()).add(tex_name)

    total_pairs = sum(len(v) for v in mapping.values())
    print(f"[+] Loaded {len(mapping)} SHA-1 entries ({total_pairs} mappings) from {os.path.basename(csv_path)}")

    if skipped:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n[Skipped Alpha=0 Entries]\n")
            for sha1_val, tex_name in skipped:
                f.write(f"sha1={sha1_val}, texture={tex_name}\n")

    return mapping

def load_pcsx2_sha1_list(csv_path):
    sha1s = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sha1 = (row.get("pcsx2_dumped_sha1") or "").strip().lower()
            if sha1 and sha1 != "0000000000000000000000000000000000000000":
                sha1s.add(sha1)
    return sha1s

# ==========================================================
# FILE DISCOVERY
# ==========================================================
def find_pngs(root_dir, exclude_dir, extra_excludes):
    result = []
    for dirpath, _, filenames in os.walk(root_dir):
        abs_path = os.path.abspath(dirpath).lower()
        if os.path.abspath(exclude_dir).lower() in abs_path:
            continue
        if any(excl.lower() in abs_path for excl in extra_excludes):
            continue
        for fn in filenames:
            if fn.lower().endswith(".png"):
                result.append(os.path.join(dirpath, fn))
    return result

def collect_existing_sha1s(dest_dir):
    existing = {}
    for root, _, files in os.walk(dest_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                path = os.path.join(root, fn)
                sha1 = calc_sha1(path)
                existing.setdefault(sha1, []).append(path)
    return existing

# ==========================================================
# PROCESSING
# ==========================================================
def all_mapped_files_present(sha1, mapping, existing_sha1s):
    if sha1 not in mapping:
        return False
    expected = {f"{name}.png".lower() for name in mapping[sha1]}
    actual = {os.path.basename(p).lower() for p in existing_sha1s.get(sha1, [])}
    return expected.issubset(actual)

def get_conflict_path(base_name):
    os.makedirs(CONFLICT_DIR, exist_ok=True)
    counter = 1
    while True:
        candidate = os.path.join(CONFLICT_DIR, f"{base_name}-[{counter}].png")
        if not os.path.exists(candidate):
            return candidate
        counter += 1

def process_file(path, mapping, log_lock, existing_sha1s, log_path, conflict_check=False):
    try:
        sha1 = calc_sha1(path)
        rel_path = os.path.relpath(path, ROOT_DIR).replace("\\", "/")
        original_name = os.path.splitext(os.path.basename(path))[0].lower()

        if sha1 in existing_sha1s and all_mapped_files_present(sha1, mapping, existing_sha1s):
            os.remove(path)
            log_line(f"[DELETE] {rel_path} (sha1: {sha1})", log_lock, log_path)
            return (path, sha1, None, False)

        if sha1 in mapping:
            tex_names = mapping[sha1]
            first_tex = next(iter(tex_names))
            dest_first = os.path.join(DEST_DIR, f"{first_tex}.png")
            os.makedirs(os.path.dirname(dest_first), exist_ok=True)

            # Conflict handling (only for 2nd pass)
            if conflict_check and os.path.exists(dest_first):
                existing_sha1 = calc_sha1(dest_first)
                if existing_sha1 != sha1:
                    conflict_path = get_conflict_path(first_tex)
                    shutil.move(path, conflict_path)
                    log_line(f"[CONFLICT] {rel_path} -> {os.path.relpath(conflict_path, ROOT_DIR)} (sha1: {sha1}, existing_sha1: {existing_sha1})", log_lock, log_path)
                    return (path, sha1, None, False)

            shutil.move(path, dest_first)
            existing_sha1s.setdefault(sha1, []).append(dest_first)
            if "-" not in original_name and original_name != first_tex.lower():
                log_line(f"{rel_path} -> {first_tex}.png (sha1: {sha1})", log_lock, log_path)

            for tex_name in list(tex_names)[1:]:
                dest_copy = os.path.join(DEST_DIR, f"{tex_name}.png")
                if not os.path.exists(dest_copy):
                    shutil.copy2(dest_first, dest_copy)
                    existing_sha1s.setdefault(sha1, []).append(dest_copy)
                    if "-" not in original_name and original_name != tex_name.lower():
                        log_line(f"[COPY] {first_tex}.png -> {tex_name}.png (sha1: {sha1})", log_lock, log_path)
            return (path, sha1, tex_names, True)

        return (path, sha1, None, False)
    except Exception as e:
        log_line(f"[ERROR] {path} -> {e}", log_lock, log_path)
        return (path, "ERROR", str(e), False)

# ==========================================================
# VERIFICATION STEPS
# ==========================================================
def check_duplicate_names(root_dir, verify_dir, exclude_dirs, log_lock, log_path):
    log_line("\n[Duplicate Check] Scanning for duplicate filenames...", log_lock, log_path)
    verified_map = {}
    for root, _, files in os.walk(verify_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                verified_map[fn.lower()] = os.path.join(root, fn)

    conflicts = []
    for dirpath, _, filenames in os.walk(root_dir):
        abs_path = os.path.abspath(dirpath).lower()
        if os.path.abspath(verify_dir).lower() in abs_path:
            continue
        if any(excl.lower() in abs_path for excl in exclude_dirs):
            continue
        for fn in filenames:
            lower_name = fn.lower()
            if lower_name.endswith(".png") and lower_name in verified_map:
                outside_path = os.path.join(dirpath, fn)
                inside_path = verified_map[lower_name]
                outside_sha1 = calc_sha1(outside_path)
                inside_sha1 = calc_sha1(inside_path)
                if outside_sha1 != inside_sha1:
                    conflicts.append((outside_path, inside_path))
    if conflicts:
        log_line(f"[Duplicate Check] {len(conflicts)} conflicts found.", log_lock, log_path)
        for o, i in conflicts:
            log_line(f"  {o} <-> {i}", log_lock, log_path)
    else:
        log_line("[Duplicate Check] No duplicate conflicts found.", log_lock, log_path)

def verify_hash_presence(verify_dir, pcsx2_sha1s, log_lock, log_path):
    log_line("\n[Hash Verification] Checking coverage...", log_lock, log_path)
    missing = []
    for root, _, files in os.walk(verify_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                path = os.path.join(root, fn)
                sha1 = calc_sha1(path)
                if sha1 not in pcsx2_sha1s:
                    missing.append((fn, sha1))
    if missing:
        log_line(f"[Hash Verification] {len(missing)} unknown SHA-1s:", log_lock, log_path)
        for fn, sha1 in missing:
            log_line(f"  - {fn} ({sha1})", log_lock, log_path)
    else:
        log_line("[Hash Verification] All verified hashes known.", log_lock, log_path)

def verify_missing_textures(dim_csv, verify_dir, log_lock, log_path):
    expected = set()
    with open(dim_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            n = (row.get("texture_name") or "").strip().lower()
            if n:
                expected.add(n)
    actual = {os.path.splitext(f)[0].lower() for _, _, files in os.walk(verify_dir) for f in files if f.lower().endswith(".png")}
    missing = sorted(expected - actual)
    if missing:
        log_line(f"[Verification] Missing {len(missing)} textures:", log_lock, log_path)
        for m in missing:
            log_line(f"  - {m}", log_lock, log_path)
    else:
        log_line("[Verification] No missing textures.", log_lock, log_path)

def reverification_pass(mapping, dest_dir, log_lock, log_path):
    log_line("\n[Reverification] Ensuring all mapped outputs exist...", log_lock, log_path)
    restored = []
    for root, _, files in os.walk(dest_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                path = os.path.join(root, fn)
                sha1 = calc_sha1(path)
                if sha1 not in mapping:
                    continue
                expected = {f"{t}.png".lower() for t in mapping[sha1]}
                for e in expected:
                    exp_path = os.path.join(dest_dir, e)
                    if not os.path.exists(exp_path):
                        shutil.copy2(path, exp_path)
                        restored.append((path, exp_path))
    if restored:
        log_line(f"[Reverification] Restored {len(restored)} missing textures.", log_lock, log_path)
    else:
        log_line("[Reverification] All verified OK.", log_lock, log_path)

# ==========================================================
# PASS RUNNER
# ==========================================================
def run_pass(csv_path, pass_name, conflict_check=False):
    log_path = os.path.join(os.path.dirname(__file__), f"{pass_name}_verification_log.txt")
    open(log_path, "w", encoding="utf-8").close()
    log_lock = threading.Lock()

    mapping = load_sha1_mapping(csv_path, log_path)
    pcsx2_sha1s = load_pcsx2_sha1_list(PCSX2_SHA1_LOG)
    existing_sha1s = collect_existing_sha1s(DEST_DIR)
    png_files = find_pngs(ROOT_DIR, DEST_DIR, EXCLUDE_DIRS)

    total = len(png_files)
    if total == 0:
        log_line("[!] No PNG files found to process.", log_lock, log_path)
        return

    print(f"\n===== Running {pass_name} ({total} PNGs) =====")
    completed = 0
    moved = 0
    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(process_file, f, mapping, log_lock, existing_sha1s, log_path, conflict_check): f for f in png_files}
        for future in as_completed(futures):
            _, _, _, success = future.result()
            completed += 1
            if success:
                moved += 1
            if completed % 250 == 0 or completed == total:
                print(f"[{pass_name}] {completed}/{total} ({(completed/total)*100:.1f}%) - {moved} moved")

    # Post-run verifications
    check_duplicate_names(ROOT_DIR, DEST_DIR, EXCLUDE_DIRS, log_lock, log_path)
    verify_hash_presence(DEST_DIR, pcsx2_sha1s, log_lock, log_path)
    verify_missing_textures(DIMENSIONS_CSV, DEST_DIR, log_lock, log_path)
    reverification_pass(mapping, DEST_DIR, log_lock, log_path)
    log_line(f"\n[Summary] {pass_name}: Moved {moved}/{total} textures.", log_lock, log_path)
    print(f"[+] {pass_name} complete. Log: {log_path}")

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    run_pass(PASS1_CSV, "MC_Pass", conflict_check=False)
    run_pass(PASS2_CSV, "TRI_Pass", conflict_check=True)
    print("\n[+] Both passes completed.")
