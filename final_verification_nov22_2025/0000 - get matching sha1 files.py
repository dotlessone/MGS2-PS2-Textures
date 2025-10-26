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
PS2_DIMENSIONS_CSV = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_ps2_dimensions.csv"
PCSX2_SHA1_LOG = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_dumped_sha1_log.csv"

PASS1_CSV = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_mc_sha1_matches.csv"
PASS2_CSV = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_tri_sha1_matches.csv"
MANUAL_CSV = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_manual_sha1_matches.csv"

EXCLUDE_DIRS = ["Self Remade", "Renamed Copies - Better LODs"]

# ==========================================================
# COMBINED LOG CONFIGURATION
# ==========================================================
COMBINED_LOG_PATH = os.path.join(os.path.dirname(__file__), "Get Matching SHA1 Files.log")
LOG_LOCK = threading.Lock()
LOG_MEMORY = {}
open(COMBINED_LOG_PATH, "w", encoding="utf-8").close()

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

def get_image_dimensions(path):
    try:
        with Image.open(path) as img:
            return img.width, img.height
    except Exception:
        return None, None

def get_unique_alpha_values(path):
    """Return sorted unique alpha values in an image (or [255] if opaque)."""
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

def load_manual_mapping(csv_path):
    mapping = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "pcsx2_dumped_sha1" not in reader.fieldnames or "texture_name" not in reader.fieldnames:
            raise ValueError(f"{csv_path} missing required columns: needs texture_name and pcsx2_dumped_sha1")
        for row in reader:
            sha1_val = (row["pcsx2_dumped_sha1"] or "").strip().lower()
            tex_name = (row["texture_name"] or "").strip()
            if sha1_val and tex_name:
                mapping.setdefault(sha1_val, set()).add(tex_name)
    print(f"[+] Loaded {len(mapping)} SHA-1 manual entries from {os.path.basename(csv_path)}")
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

def load_ps2_dimensions(csv_path):
    dims = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tex = (row.get("texture_name") or "").strip().lower()
            w = row.get("tri_dumped_width", "").strip()
            h = row.get("tri_dumped_height", "").strip()
            if tex and w.isdigit() and h.isdigit():
                dims[tex] = (int(w), int(h))
    return dims

# ==========================================================
# MANUAL VERIFICATION
# ==========================================================
def verify_manual_names_exist(manual_csv, mc_csv):
    with open(mc_csv, "r", encoding="utf-8", newline="") as f:
        mc_reader = csv.DictReader(f)
        mc_names = {row["texture_name"].strip().lower() for row in mc_reader if row.get("texture_name")}

    with open(manual_csv, "r", encoding="utf-8", newline="") as f:
        manual_reader = csv.DictReader(f)
        missing = []
        for row in manual_reader:
            tex = (row.get("texture_name") or "").strip().lower()
            if tex and tex not in mc_names:
                missing.append(tex)

    if missing:
        print(f"\n[!] {len(missing)} textures in {os.path.basename(manual_csv)} not found in MC list:")
        for m in missing:
            print(f"    - {m}")
        print("\n[!] Fix or remove these before continuing.")
        input("\nPress Enter to exit...")
        raise SystemExit(1)
    else:
        print(f"[+] All manual textures verified against MC list ({len(mc_names)} known).")

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
# VERIFICATION HELPERS
# ==========================================================
def check_duplicate_names(root_dir, verify_dir, exclude_dirs, log_lock, log_path):
    content_lines = []
    content_lines.append("\n[Duplicate Check] Scanning for duplicate filenames...")
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
        content_lines.append(f"[Duplicate Check] {len(conflicts)} conflicts found.")
        for o, i in conflicts:
            content_lines.append(f"  {o} <-> {i}")
    else:
        content_lines.append("[Duplicate Check] No duplicate conflicts found.")

    content = "\n".join(content_lines)
    if LOG_MEMORY.get("Duplicate Check") != content:
        LOG_MEMORY["Duplicate Check"] = content
        log_line(content, log_lock, log_path)

def verify_hash_presence(verify_dir, pcsx2_sha1s, log_lock, log_path):
    content_lines = []
    content_lines.append("\n[Hash Verification] Checking coverage...")
    missing = []
    for root, _, files in os.walk(verify_dir):
        for fn in files:
            if fn.lower().endswith(".png"):
                path = os.path.join(root, fn)
                sha1 = calc_sha1(path)
                if sha1 not in pcsx2_sha1s:
                    missing.append((fn, sha1))
    if missing:
        content_lines.append(f"[Hash Verification] {len(missing)} unknown SHA-1s:")
        for fn, sha1 in missing:
            content_lines.append(f"  - {fn} ({sha1})")
    else:
        content_lines.append("[Hash Verification] All verified hashes known.")

    content = "\n".join(content_lines)
    if LOG_MEMORY.get("Hash Verification") != content:
        LOG_MEMORY["Hash Verification"] = content
        log_line(content, log_lock, log_path)

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
    content_lines = []
    if missing:
        content_lines.append(f"[Verification] Missing {len(missing)} textures:")
        for m in missing:
            content_lines.append(f"  - {m}")
    else:
        content_lines.append("[Verification] No missing textures.")
    content = "\n".join(content_lines)
    if LOG_MEMORY.get("Verification") != content:
        LOG_MEMORY["Verification"] = content
        log_line(content, log_lock, log_path)

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
# FINAL DIMENSION VALIDATION (EXTERNAL FILES)
# ==========================================================
from concurrent.futures import ThreadPoolExecutor, as_completed

def check_external_wrong_dimensions(ps2_csv, root_dir, dest_dir, exclude_dirs):
    print("\n[Final Check] Verifying external PNG dimensions...")
    dims = load_ps2_dimensions(ps2_csv)
    checked = 0
    renamed = 0
    alpha_flagged = 0
    suggestions_logged = 0
    no_match_entries = []

    # =====================================================
    # Full-path blacklist (applies to all passes)
    # =====================================================
    FOLDER_BLACKLIST = [
        r"C:\Development\Git\MGS2-PS2-Textures\x - document specific",
    ]
    FOLDER_BLACKLIST = [os.path.abspath(p).lower() for p in FOLDER_BLACKLIST]

    # =====================================================
    # Gather all known verified texture names
    # =====================================================
    VERIFIED_DIR = r"C:\Development\Git\MGS2-PS2-Textures\final_verification_nov22_2025"
    VERIFIED_DIR = os.path.abspath(VERIFIED_DIR).lower()
    verified_names = set()
    for root, _, files in os.walk(VERIFIED_DIR):
        for fn in files:
            if fn.lower().endswith(".png"):
                verified_names.add(os.path.splitext(fn)[0].lower())

    log_path = os.path.join(os.path.dirname(__file__), "External PNG Status Checks.log")
    log_lock = threading.Lock()

    def is_blacklisted_dir(abs_path):
        abs_path = os.path.abspath(abs_path).lower()
        for bl in FOLDER_BLACKLIST:
            if abs_path == bl or abs_path.startswith(bl + os.sep):
                return True
        return False

    def ensure_tagged_filename(base_name_no_ext, want_wrong_name, want_bad_alpha):
        out = base_name_no_ext
        if want_wrong_name and " - wrong name" not in out.lower():
            out += " - WRONG NAME"
        if want_bad_alpha and " - bad alpha" not in out.lower():
            out += " - BAD ALPHA"
        return out

    with open(log_path, "w", encoding="utf-8") as log:
        log.write("============================================\n")
        log.write(" MGS2 PS2 Texture Dimension Mismatch Report\n")
        log.write("============================================\n\n")

        resolution_map = {}
        for tex, (w, h) in dims.items():
            resolution_map.setdefault((w, h), []).append(tex)

        logged_paths = set()
        mismatch_results = []

        # ------------------------------------------------
        # PASS 1: Dimension mismatch (+ BAD ALPHA tagging)
        # ------------------------------------------------
        for dirpath, _, files in os.walk(root_dir):
            abs_path_dir = os.path.abspath(dirpath)
            if (
                os.path.abspath(dest_dir).lower() in abs_path_dir.lower()
                or any(excl.lower() in abs_path_dir.lower() for excl in exclude_dirs)
                or is_blacklisted_dir(abs_path_dir)
            ):
                continue
            for fn in files:
                if not fn.lower().endswith(".png"):
                    continue

                path = os.path.join(dirpath, fn)
                tex = os.path.splitext(fn)[0].lower()
                real_w, real_h = get_image_dimensions(path)
                if not real_w or not real_h:
                    continue

                checked += 1
                if tex in dims:
                    expected_w, expected_h = dims[tex]
                    if real_w != expected_w or real_h != expected_h:
                        alpha_values = get_unique_alpha_values(path)
                        has_alpha_over_128 = any(isinstance(a, int) and a > 128 for a in alpha_values)
                        sha1_val = calc_sha1(path)
                        matches = resolution_map.get((real_w, real_h), [])

                        base_no_ext = os.path.splitext(fn)[0]
                        new_base = ensure_tagged_filename(base_no_ext, True, has_alpha_over_128)
                        new_path = os.path.join(dirpath, f"{new_base}.png")
                        if os.path.abspath(new_path) != os.path.abspath(path):
                            os.rename(path, new_path)
                            path = new_path

                        renamed += 1
                        if has_alpha_over_128:
                            alpha_flagged += 1
                        logged_paths.add(os.path.abspath(new_path))

                        mismatch_results.append({
                            "path": path,
                            "filename": os.path.basename(path),
                            "sha1": sha1_val,
                            "expected": f"{expected_w}x{expected_h}",
                            "actual": f"{real_w}x{real_h}",
                            "alpha": has_alpha_over_128,
                            "matches": matches,
                            "real_w": real_w,
                            "real_h": real_h,
                            "tex_name": tex
                        })

        # Sort mismatches by number of matches (ascending)
        mismatch_results.sort(key=lambda r: len(r["matches"]))

        # Write sorted mismatches to log with separated match sections
        for r in mismatch_results:
            already_identified = [m for m in r["matches"] if m.lower() in verified_names]
            not_identified = [m for m in r["matches"] if m.lower() not in verified_names]

            log.write(f"Filename: {r['filename']}\n")
            log.write(f"Path: {r['path']}\n")
            log.write(f"SHA1: {r['sha1']}\n")
            log.write(f"Expected: {r['expected']}\n")
            log.write(f"Actual:   {r['actual']}\n")
            log.write(f"Alpha >128: {'Yes' if r['alpha'] else 'No'}\n")

            if not_identified:
                full_path = os.path.abspath(r["path"])
                everything_line = f"\"{full_path}\"|<\"mgs2/base textures/\" /<{'|'.join(not_identified)}>.png>\n"
                log.write(everything_line)
                log.write("Possible matching textures (not already found):\n")
                for m in not_identified:
                    suffix = '\t!"/ALREADY FILE NAME/"' if m.lower() == r["tex_name"] else ""
                    log.write(f"  - {m}{suffix}\n")

            if already_identified:
                inline = "|".join(already_identified)
                log.write(f"Possible matching textures (already identified): {inline}\n")

            log.write(f"Total Possible Matches: {len(r['matches'])}\n")
            log.write("--------------------------------------------\n")

        print(f"[PASS1] Logged {len(mismatch_results)} dimension mismatches (sorted by possible match count).")

        # ------------------------------------------------
        # PASS 2: Alpha >128 (remaining)
        # ------------------------------------------------
        log.write("\n\n============================================\n")
        log.write(" Alpha >128 Check (All Non-Renamed PNGs)\n")
        log.write("============================================\n\n")

        for dirpath, _, files in os.walk(root_dir):
            if (
                is_blacklisted_dir(dirpath)
                or os.path.abspath(dest_dir).lower() in dirpath.lower()
                or any(excl.lower() in dirpath.lower() for excl in exclude_dirs)
            ):
                continue

            for fn in files:
                if not fn.lower().endswith(".png"):
                    continue

                path = os.path.join(dirpath, fn)
                abs_path_norm = os.path.abspath(path)
                if abs_path_norm in logged_paths:
                    continue

                alpha_values = get_unique_alpha_values(path)
                if alpha_values == [255]:
                    continue

                has_alpha_over_128 = any(a > 128 for a in alpha_values if isinstance(a, int))
                if not has_alpha_over_128:
                    continue

                alpha_flagged += 1
                base_no_ext = os.path.splitext(fn)[0]
                if " - bad alpha" not in base_no_ext.lower():
                    new_base = ensure_tagged_filename(base_no_ext, False, True)
                    new_path = os.path.join(dirpath, f"{new_base}.png")
                    os.rename(path, new_path)
                    path = new_path

                logged_paths.add(os.path.abspath(path))
                sha1_val = calc_sha1(path)
                real_w, real_h = get_image_dimensions(path)
                log.write(f"Filename: {os.path.basename(path)}\n")
                log.write(f"Path: {path}\n")
                log.write(f"SHA1: {sha1_val}\n")
                log.write(f"Resolution: {real_w}x{real_h}\n")
                log.write(f"Alpha >128: Yes\n")
                log.write("--------------------------------------------\n")
                print(f"[BAD ALPHA] {fn} -> {os.path.basename(path)}")

        # ------------------------------------------------
        # PASS 3: Suggestions for remaining unlogged PNGs (sorted by suggestion count)
        # ------------------------------------------------
        log.write("\n\n============================================\n")
        log.write(" Possible Texture Suggestions (Unlogged PNGs)\n")
        log.write("============================================\n\n")

        remaining_pngs = []
        for dirpath, _, files in os.walk(root_dir):
            if (
                is_blacklisted_dir(dirpath)
                or os.path.abspath(dest_dir).lower() in dirpath.lower()
                or any(excl.lower() in dirpath.lower() for excl in exclude_dirs)
            ):
                continue

            for fn in files:
                if fn.lower().endswith(".png"):
                    path = os.path.join(dirpath, fn)
                    if os.path.abspath(path) not in logged_paths:
                        remaining_pngs.append(path)

        print(f"[Pass3] Scanning {len(remaining_pngs)} remaining PNGs for dimension-based matches...")

        results = []

        def process_remaining_png(path):
            real_w, real_h = get_image_dimensions(path)
            if not real_w or not real_h:
                return None
            matches = resolution_map.get((real_w, real_h), [])
            tex_name = os.path.splitext(os.path.basename(path))[0].lower()
            name_in_matches = tex_name in matches
            sha1_val = calc_sha1(path)
            return {
                "path": path,
                "rel_path": os.path.relpath(path, root_dir),
                "sha1": sha1_val,
                "matches": matches,
                "not_identified": [m for m in matches if m.lower() not in verified_names],
                "already_identified": [m for m in matches if m.lower() in verified_names],
                "tex_name": tex_name,
                "name_in_matches": name_in_matches,
                "real_w": real_w,
                "real_h": real_h,
            }

        with ThreadPoolExecutor(max_workers=max(4, os.cpu_count() or 4)) as executor:
            futures = {executor.submit(process_remaining_png, p): p for p in remaining_pngs}
            for i, fut in enumerate(as_completed(futures), 1):
                res = fut.result()
                if res:
                    results.append(res)
                if i % 250 == 0 or i == len(futures):
                    print(f"[Pass3] Processed {i}/{len(futures)}")

        results.sort(key=lambda r: len(r["matches"]))

        for r in results:
            matches = r["matches"]
            if not matches:
                no_match_entries.append(f"  - {r['path']} ({r['real_w']}x{r['real_h']}, sha1: {r['sha1'][:8]}...)")
                continue

            log.write(f"Filename: {os.path.basename(r['path'])}\n")
            log.write(f"Path: {r['rel_path']}\n")

            if r["name_in_matches"]:
                log.write(f"SHA1: {os.path.basename(r['path'])},{r['sha1']}\n")
            else:
                log.write(f"SHA1: {r['sha1']}\n")

            log.write(f"Name Already In List of Possible Matches: {'Yes' if r['name_in_matches'] else 'No'}\n")
            log.write(f"Resolution: {r['real_w']}x{r['real_h']}\n")

            not_identified = r["not_identified"]
            already_identified = r["already_identified"]

            if not_identified:
                full_path = os.path.abspath(r["path"])
                everything_line = f"\"{full_path}\"|<\"mgs2/base textures/\" /<{'|'.join(not_identified)}>.png>\n"
                log.write(everything_line)
                log.write("Possible matching textures (not already found):\n")
                for m in not_identified:
                    suffix = '\t!"/ALREADY FILE NAME/"' if m.lower() == r["tex_name"] else ""
                    log.write(f"  - {m}{suffix}\n")
            if already_identified:
                inline = "|".join(already_identified)
                log.write(f"Possible matching textures (already identified): {inline}\n")

            log.write(f"Total Possible Matches: {len(matches)}\n")
            log.write("--------------------------------------------\n")

        suggestions_logged = len(results)

        # ------------------------------------------------
        # PASS 3B: Compact summary for all no-match files
        # ------------------------------------------------
        if no_match_entries:
            log.write("\n\n============================================\n")
            log.write(" No-Match Files (0 Possible Matches)\n")
            log.write("============================================\n")
            for entry in sorted(no_match_entries):
                log.write(f"{entry}\n")
            log.write("--------------------------------------------\n")

        # ------------------------------------------------
        # Summary
        # ------------------------------------------------
        log.write(f"\n[Summary]\nChecked: {checked}\nRenamed (wrong dims): {renamed}\nAlpha >128: {alpha_flagged}\nSuggestions logged: {suggestions_logged}\nNo-match files: {len(no_match_entries)}\n")
        log.write(f"Log file saved at: {log_path}\n")

    print(f"[Final Check] Checked {checked} PNGs, renamed {renamed} (wrong dims).")
    print(f"[Final Check] Alpha >128: {alpha_flagged}")
    print(f"[Final Check] Suggestions logged: {suggestions_logged}")
    print(f"[Final Check] No-match files: {len(no_match_entries)}")
    print(f"[Final Check] Log written to: {log_path}")

# ==========================================================
# PASS RUNNERS

# ==========================================================
# PASS RUNNERS (MERGED LOG)
# ==========================================================
def run_pass(csv_path, pass_name, conflict_check=False):
    log_path = COMBINED_LOG_PATH
    log_lock = LOG_LOCK

    # Header section
    with log_lock:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n\n============================================\n")
            f.write(f"[{pass_name}] Verification Log\n")
            f.write("============================================\n")

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

    check_duplicate_names(ROOT_DIR, DEST_DIR, EXCLUDE_DIRS, log_lock, log_path)
    verify_hash_presence(DEST_DIR, pcsx2_sha1s, log_lock, log_path)
    verify_missing_textures(DIMENSIONS_CSV, DEST_DIR, log_lock, log_path)
    reverification_pass(mapping, DEST_DIR, log_lock, log_path)
    log_line(f"\n[Summary] {pass_name}: Moved {moved}/{total} textures.", log_lock, log_path)
    print(f"[+] {pass_name} complete. Logged into combined log.")

def run_manual_pass(csv_path):
    pass_name = "Manual_Pass"
    log_path = COMBINED_LOG_PATH
    log_lock = LOG_LOCK

    # Header section
    with log_lock:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n\n============================================\n")
            f.write(f"[{pass_name}] Verification Log\n")
            f.write("============================================\n")

    mapping = load_manual_mapping(csv_path)
    if not mapping:
        print(f"[!] {os.path.basename(csv_path)} has no entries — skipping {pass_name}.")
        log_line(f"[!] No entries found in {os.path.basename(csv_path)}; skipping {pass_name}.", log_lock, log_path)
        return

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
        futures = {executor.submit(process_file, f, mapping, log_lock, existing_sha1s, log_path, False): f for f in png_files}
        for future in as_completed(futures):
            _, _, _, success = future.result()
            completed += 1
            if success:
                moved += 1
            if completed % 250 == 0 or completed == total:
                print(f"[{pass_name}] {completed}/{total} ({(completed/total)*100:.1f}%) - {moved} moved")

    check_duplicate_names(ROOT_DIR, DEST_DIR, EXCLUDE_DIRS, log_lock, log_path)
    verify_hash_presence(DEST_DIR, pcsx2_sha1s, log_lock, log_path)
    verify_missing_textures(DIMENSIONS_CSV, DEST_DIR, log_lock, log_path)
    reverification_pass(mapping, DEST_DIR, log_lock, log_path)
    log_line(f"\n[Summary] {pass_name}: Moved {moved}/{total} textures.", log_lock, log_path)
    print(f"[+] {pass_name} complete. Logged into combined log.")

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    verify_manual_names_exist(MANUAL_CSV, DIMENSIONS_CSV)
    run_pass(PASS1_CSV, "MC_Pass", conflict_check=False)
    run_pass(PASS2_CSV, "TRI_Pass", conflict_check=True)
    run_manual_pass(MANUAL_CSV)
    check_external_wrong_dimensions(PS2_DIMENSIONS_CSV, ROOT_DIR, DEST_DIR, EXCLUDE_DIRS)
    print(f"\n[+] Combined verification log saved at: {COMBINED_LOG_PATH}")
