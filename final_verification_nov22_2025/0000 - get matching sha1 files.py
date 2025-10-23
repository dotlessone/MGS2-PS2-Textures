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
CSV_PATH = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_tri_sha1_matches.csv"
ROOT_DIR = r"C:\Development\Git\MGS2-PS2-Textures"
DEST_DIR = r"C:\Development\Git\MGS2-PS2-Textures\final_verification_nov22_2025"
DIMENSIONS_CSV = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_mc_dimensions.csv"
PCSX2_SHA1_LOG = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_dumped_sha1_log.csv"

EXCLUDE_DIRS = ["Self Remade", "Renamed Copies - Better LODs"]
LOG_PATH = os.path.join(os.path.dirname(__file__), "final_verification_log.txt")

# ==========================================================
# UTILITIES
# ==========================================================
def calc_sha1(path):
    """Compute SHA-1 hash of a file."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def log_line(line, lock):
    """Thread-safe log writer."""
    with lock:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")


def get_unique_alpha_values(path):
    """Return sorted unique alpha levels in the image."""
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
def load_sha1_mapping(csv_path):
    """Load pcsx2_dumped_sha1 → [texture_name1, texture_name2, ...] mapping from CSV."""
    mapping = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "pcsx2_dumped_sha1" not in reader.fieldnames or "texture_name" not in reader.fieldnames:
            raise ValueError("CSV must contain 'pcsx2_dumped_sha1' and 'texture_name' columns.")
        for row in reader:
            sha1_val = row["pcsx2_dumped_sha1"].strip().lower()
            tex_name = row["texture_name"].strip()
            if sha1_val:
                mapping.setdefault(sha1_val, set()).add(tex_name)
    total_pairs = sum(len(v) for v in mapping.values())
    print(f"[+] Loaded {len(mapping)} unique SHA-1 entries ({total_pairs} total texture mappings).")
    return mapping


def load_pcsx2_sha1_list(csv_path):
    """Load all pcsx2_dumped_sha1 values from pcsx2_dumped_sha1_log.csv."""
    sha1s = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "pcsx2_dumped_sha1" not in reader.fieldnames:
            raise ValueError("pcsx2_dumped_sha1_log.csv must contain 'pcsx2_dumped_sha1' column.")
        for row in reader:
            sha1 = row["pcsx2_dumped_sha1"].strip().lower()
            if sha1 and sha1 != "0000000000000000000000000000000000000000":
                sha1s.add(sha1)
    print(f"[+] Loaded {len(sha1s)} SHA-1 entries from pcsx2_dumped_sha1_log.csv")
    return sha1s


# ==========================================================
# FILE DISCOVERY
# ==========================================================
def find_pngs(root_dir, exclude_dir, extra_excludes):
    """Recursively locate all PNG files, skipping excluded directories."""
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
    """Recursively compute SHA-1 hashes for all PNGs in the verification folder."""
    existing = {}
    for root, _, files in os.walk(dest_dir):
        for fn in files:
            if not fn.lower().endswith(".png"):
                continue
            path = os.path.join(root, fn)
            sha1 = calc_sha1(path)
            existing.setdefault(sha1, []).append(path)
    print(f"[+] Found {len(existing)} unique SHA-1s in verification folder.")
    return existing


# ==========================================================
# PROCESSING
# ==========================================================
def all_mapped_files_present(sha1, mapping, existing_sha1s):
    """Return True if all mapped textures for a SHA-1 already exist."""
    if sha1 not in mapping:
        return False
    expected = {f"{name}.png".lower() for name in mapping[sha1]}
    actual = {os.path.basename(p).lower() for p in existing_sha1s.get(sha1, [])}
    return expected.issubset(actual)


def process_file(path, mapping, log_lock, existing_sha1s):
    """Deploy PNGs to all mapped destinations; only delete when all mapped outputs already exist."""
    try:
        sha1 = calc_sha1(path)
        rel_path = os.path.relpath(path, ROOT_DIR).replace("\\", "/")

        # Delete only if all mapped files already exist
        if sha1 in existing_sha1s and all_mapped_files_present(sha1, mapping, existing_sha1s):
            os.remove(path)
            log_line(f"[DELETE] {rel_path} (sha1: {sha1}) - all mapped textures already exist", log_lock)
            return (path, sha1, None, False)

        if sha1 in mapping:
            tex_names = mapping[sha1]
            first_tex = next(iter(tex_names))
            dest_first = os.path.join(DEST_DIR, f"{first_tex}.png")
            os.makedirs(os.path.dirname(dest_first), exist_ok=True)
            shutil.move(path, dest_first)
            existing_sha1s.setdefault(sha1, []).append(dest_first)
            log_line(f"{rel_path} -> renamed to: {first_tex}.png (sha1: {sha1})", log_lock)

            for tex_name in list(tex_names)[1:]:
                dest_copy = os.path.join(DEST_DIR, f"{tex_name}.png")
                os.makedirs(os.path.dirname(dest_copy), exist_ok=True)
                if not os.path.exists(dest_copy):
                    shutil.copy2(dest_first, dest_copy)
                    existing_sha1s.setdefault(sha1, []).append(dest_copy)
                    log_line(f"[COPY] {first_tex}.png -> {tex_name}.png (sha1: {sha1})", log_lock)
            return (path, sha1, list(tex_names), True)

        return (path, sha1, None, False)

    except Exception as e:
        log_line(f"[ERROR] {path} -> {e}", log_lock)
        return (path, "ERROR", str(e), False)


# ==========================================================
# ANALYSIS PASSES
# ==========================================================
def check_duplicate_names(root_dir, verify_dir, exclude_dirs, log_lock):
    """Check duplicate filenames outside verification folder."""
    log_line("\n[Duplicate Check] Scanning for duplicate filenames outside verification folder...", log_lock)
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
                outside_alphas = get_unique_alpha_values(outside_path)
                inside_alphas = get_unique_alpha_values(inside_path)
                rel_outside = os.path.relpath(outside_path, root_dir).replace("\\", "/")
                conflicts.append((rel_outside, outside_sha1, outside_alphas, inside_sha1, inside_alphas))

    if conflicts:
        log_line(f"[Duplicate Check] Found {len(conflicts)} duplicate filename conflicts:", log_lock)
        for rel_outside, sha1_out, alphas_out, sha1_in, alphas_in in sorted(conflicts, key=lambda x: x[0].lower()):
            log_line(
                f"{rel_outside} ([external: sha1={sha1_out}, alpha={alphas_out}], [verified: sha1={sha1_in}, alpha={alphas_in}])",
                log_lock,
            )
    else:
        log_line("[Duplicate Check] No duplicate filenames found outside verification folder.", log_lock)


def verify_hash_presence(verify_dir, pcsx2_sha1s, log_lock):
    """Ensure all SHA-1s in verification folder exist in pcsx2_dumped_sha1_log.csv."""
    log_line("\n[Hash Verification] Checking SHA-1 coverage...", log_lock)
    missing = []

    for root, _, files in os.walk(verify_dir):
        for fn in files:
            if not fn.lower().endswith(".png"):
                continue
            path = os.path.join(root, fn)
            sha1 = calc_sha1(path)
            if sha1 not in pcsx2_sha1s:
                rel = os.path.relpath(path, ROOT_DIR).replace("\\", "/")
                missing.append((rel, sha1))

    if missing:
        log_line(f"[Hash Verification] Found {len(missing)} unknown SHA-1s:", log_lock)
        for rel, sha1 in missing:
            log_line(f"  - {rel} (sha1: {sha1})", log_lock)
    else:
        log_line("[Hash Verification] All verified textures are present in pcsx2_dumped_sha1_log.csv.", log_lock)


def verify_missing_textures(dim_csv, verify_dir, log_lock):
    """Ensure all texture_name entries from mgs2_mc_dimensions.csv exist."""
    if not os.path.exists(dim_csv):
        log_line(f"[!] Dimensions CSV not found: {dim_csv}", log_lock)
        return

    log_line("\n[Verification] Checking for missing CSV entries...", log_lock)
    expected = set()
    with open(dim_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "texture_name" not in reader.fieldnames:
            raise ValueError("mgs2_mc_dimensions.csv must contain 'texture_name'.")
        for row in reader:
            name = (row["texture_name"] or "").strip().lower()
            if name:
                expected.add(name)

    actual = set()
    for root, _, files in os.walk(verify_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                actual.add(os.path.splitext(fn)[0].lower())

    missing = sorted(expected - actual)
    if missing:
        log_line(f"[Verification] Missing {len(missing)} textures:", log_lock)
        for name in missing:
            log_line(f"  - {name}", log_lock)
    else:
        log_line("[Verification] No missing textures found.", log_lock)


def reverification_pass(mapping, dest_dir, log_lock):
    """Recheck all verified files and ensure all mapped outputs exist."""
    log_line("\n[Reverification] Scanning for missing mapped textures...", log_lock)
    restored = []
    missing_links = []

    for root, _, files in os.walk(dest_dir):
        for fn in files:
            if not fn.lower().endswith(".png"):
                continue
            path = os.path.join(root, fn)
            sha1 = calc_sha1(path)
            if sha1 not in mapping:
                missing_links.append((path, sha1))
                continue

            expected = {f"{t}.png".lower() for t in mapping[sha1]}
            for exp in expected:
                exp_path = os.path.join(dest_dir, exp)
                if not os.path.exists(exp_path):
                    shutil.copy2(path, exp_path)
                    restored.append((path, exp_path, sha1))

    if missing_links:
        log_line(f"[Reverification] {len(missing_links)} verified files are unmapped:", log_lock)
        for path, sha1 in missing_links:
            log_line(f"  - {os.path.relpath(path, ROOT_DIR)} (sha1: {sha1})", log_lock)
    else:
        log_line("[Reverification] All verified files are properly mapped.", log_lock)

    if restored:
        log_line(f"[Reverification] Restored {len(restored)} missing mapped textures:", log_lock)
        for src, dst, sha1 in restored:
            log_line(f"  - {os.path.relpath(src, ROOT_DIR)} -> {os.path.relpath(dst, ROOT_DIR)} (sha1: {sha1})", log_lock)


# ==========================================================
# MAIN
# ==========================================================
def main():
    open(LOG_PATH, "w", encoding="utf-8").close()
    log_lock = threading.Lock()

    if not os.path.exists(CSV_PATH) or not os.path.exists(ROOT_DIR):
        log_line("[!] Missing required CSV or root path.", log_lock)
        return

    mapping = load_sha1_mapping(CSV_PATH)
    pcsx2_sha1s = load_pcsx2_sha1_list(PCSX2_SHA1_LOG)
    existing_sha1s = collect_existing_sha1s(DEST_DIR)

    png_files = find_pngs(ROOT_DIR, DEST_DIR, EXCLUDE_DIRS)
    total = len(png_files)

    if total == 0:
        log_line("[!] No PNG files found to process.", log_lock)
        return

    print(f"[+] Found {total} PNG files to check.")
    log_line(f"[+] Found {total} PNG files to check.", log_lock)

    completed = 0
    moved = 0
    progress_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
        futures = {executor.submit(process_file, f, mapping, log_lock, existing_sha1s): f for f in png_files}
        for future in as_completed(futures):
            _, _, _, success = future.result()
            with progress_lock:
                completed += 1
                if success:
                    moved += 1
                if completed % 250 == 0 or completed == total:
                    print(f"[Progress] {completed}/{total} ({(completed/total)*100:.1f}%) - {moved} moved")

    log_line(f"\n[Summary] Moved {moved}/{total} textures to verification folder.", log_lock)

    check_duplicate_names(ROOT_DIR, DEST_DIR, EXCLUDE_DIRS, log_lock)
    verify_hash_presence(DEST_DIR, pcsx2_sha1s, log_lock)
    verify_missing_textures(DIMENSIONS_CSV, DEST_DIR, log_lock)
    reverification_pass(mapping, DEST_DIR, log_lock)

    print(f"\n[+] All logs written to: {LOG_PATH}")


if __name__ == "__main__":
    main()
