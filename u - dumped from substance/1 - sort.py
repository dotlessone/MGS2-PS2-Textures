import os
import shutil
import csv
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Checks if a number is a power of two.
def is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0

# Rounds a number up to the next power of two.
def next_power_of_two(n: int) -> int:
    if n < 1:
        return 1
    return 1 << (n - 1).bit_length()

# Ensures a directory exists before use.
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# Analyzes a TGA file to determine its alpha type and whether dimensions are power-of-two.
def get_alpha_type_and_pow2(path: str):
    try:
        img = Image.open(path).convert("RGBA")
        alpha = img.getchannel("A")
        pixels = alpha.getdata()

        unique = set(pixels)
        if len(unique) == 1:
            val = next(iter(unique))
            if val == 0:
                alpha_type = "invisible"
            elif val == 128:
                alpha_type = "half"
            elif val == 255:
                alpha_type = "opaque"
            else:
                alpha_type = "bad"
        else:
            if all(129 <= a <= 254 for a in pixels):
                alpha_type = "bad"
            else:
                alpha_type = "mixed"

        w, h = img.size
        pow2 = is_power_of_two(w) and is_power_of_two(h)
        return (path, alpha_type, pow2, w, h)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return (path, "error", False, 0, 0)

# Loads expected dimensions from the CSV file.
def read_dimensions_csv(csv_path: str) -> dict:
    entries = {}
    if not os.path.isfile(csv_path):
        print("WARNING: mgs2_mc_dimensions.csv not found. Skipping verification.\n")
        return entries

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 3:
                name = row[0].lower()
                try:
                    w = int(row[1])
                    h = int(row[2])
                    entries[name] = (w, h)
                except ValueError:
                    continue
    print(f"Loaded {len(entries)} entries from mgs2_mc_dimensions.csv\n")
    return entries

def main():
    # --- Fixed working directory ---
    base_dir = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\dump\Final Rebuilt"

    # CSV is located in the same folder as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dimensions_csv = os.path.join(script_dir, "mgs2_mc_dimensions.csv")

    conflicted_dir = os.path.join(base_dir, "conflicted")
    log_path = os.path.join(base_dir, "bp_comparison_log.txt")

    subdirs = {
        "half": "half_alpha",
        "opaque": "opaque",
        "invisible": "invisible",
        "bad": "alpha_above_correct_levels",
        "mixed": "mixed_alpha"
    }

    # Collect conflicted filenames
    conflicted_names = set()
    if os.path.isdir(conflicted_dir):
        for f in os.listdir(conflicted_dir):
            if f.endswith(".tga"):
                conflicted_names.add(f.lower())
        print(f"Loaded {len(conflicted_names)} conflicted filenames for manual verification.\n")

    csv_data = read_dimensions_csv(dimensions_csv)

    for sub in subdirs.values():
        ensure_dir(os.path.join(base_dir, sub))

    files = [
        os.path.join(base_dir, f)
        for f in os.listdir(base_dir)
        if f.lower().endswith(".tga") and os.path.isfile(os.path.join(base_dir, f))
    ]

    results = []
    print(f"Scanning {len(files)} .tga files...")

    with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
        futures = {executor.submit(get_alpha_type_and_pow2, f): f for f in files}
        for future in as_completed(futures):
            results.append(future.result())

    counts = {k: 0 for k in subdirs}

    # Step 1: Classification (lowercase filenames during move)
    for path, alpha_type, pow2, w, h in results:
        if alpha_type not in subdirs:
            continue

        filename = os.path.basename(path).lower()
        alpha_folder = os.path.join(base_dir, subdirs[alpha_type])

        if filename in conflicted_names:
            dest_dir = os.path.join(alpha_folder, "manual_verification")
            ensure_dir(dest_dir)
            print(f"Conflict match: {filename} -> {subdirs[alpha_type]}/manual_verification/")
        else:
            dest_dir = os.path.join(alpha_folder, "power_of_2") if pow2 else alpha_folder
            ensure_dir(dest_dir)
            print(f"Moving {filename} -> {os.path.relpath(dest_dir, base_dir)}/")

        dest_path = os.path.join(dest_dir, filename)
        try:
            shutil.move(path, dest_path)
        except shutil.Error:
            os.replace(path, dest_path)

        counts[alpha_type] += 1

    # Step 2: CSV comparison
    print("\nVerifying dimensions against CSV...\n")
    skip_dirs = {"manual_verification", "match_found", "unmatch", "conflicted"}
    matched_count = 0
    unmatched_count = 0
    bp_not_remade = 0
    bp_remade = 0
    bp_mismatch = 0
    log_lines = []

    for root, dirs, files in os.walk(base_dir):
        parts = set(os.path.normpath(root).split(os.sep))
        if parts & skip_dirs:
            continue

        for f in files:
            if not f.lower().endswith(".tga"):
                continue

            full_path = os.path.join(root, f)
            name_no_tga = f[:-4].lower()

            if name_no_tga not in csv_data:
                unmatched_count += 1
                log_lines.append(f"[UNMATCHED] {f} | not found in mgs2_mc_dimensions.csv")
                continue

            matched_count += 1
            csv_w, csv_h = csv_data[name_no_tga]

            try:
                with Image.open(full_path) as img:
                    w, h = img.size
            except Exception:
                unmatched_count += 1
                log_lines.append(f"[UNMATCHED] {f} | could not read image dimensions")
                continue

            rw = next_power_of_two(w)
            rh = next_power_of_two(h)

            rel_subfolder = ""
            if csv_w == rw and csv_h == rh:
                bp_not_remade += 1
            elif csv_w > rw or csv_h > rh:
                bp_remade += 1
                rel_subfolder = "bp_remade"
                log_lines.append(f"[BP REMADE] {f} | original: {w}x{h} | ceil_p^2: {rw}x{rh} | hdc/mc: {csv_w}x{csv_h}")
            else:
                bp_mismatch += 1
                rel_subfolder = "bp_mismatch"
                log_lines.append(f"[MISMATCH] {f} | original: {w}x{h} | ceil_p^2: {rw}x{rh} | hdc/mc: {csv_w}x{csv_h}")

            if rel_subfolder:
                # Avoid creating nested bp_remade/bp_remade or bp_mismatch/bp_mismatch
                if os.path.basename(root).lower() != rel_subfolder:
                    dest_dir = os.path.join(root, rel_subfolder)
                    ensure_dir(dest_dir)
                    dest_path = os.path.join(dest_dir, f.lower())
                    try:
                        shutil.move(full_path, dest_path)
                    except shutil.Error:
                        os.replace(full_path, dest_path)


    # Step 3: Log output
    all_tga_names = set()
    for root, dirs, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith(".tga"):
                all_tga_names.add(f[:-4].lower())

    missing_tgas = [name for name in csv_data.keys() if name not in all_tga_names]

    grouped = {"REMADE": [], "MISMATCH": [], "UNMATCHED": [], "MISSING_TGA": []}

    for line in log_lines:
        if line.startswith("[BP REMADE]"):
            grouped["REMADE"].append(line)
        elif line.startswith("[MISMATCH]"):
            grouped["MISMATCH"].append(line)
        elif line.startswith("[UNMATCHED]"):
            grouped["UNMATCHED"].append(line)

    for name in missing_tgas:
        grouped["MISSING_TGA"].append(f"[MISSING_TGA] {name}.tga | present in CSV but no matching file found")

    for key in grouped:
        grouped[key].sort(key=lambda s: s.lower())

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"BP Comparison Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write("=" * 80 + "\n\n")

        for header, lines in grouped.items():
            if not lines:
                continue
            log.write(f"{header} FILES\n")
            log.write("-" * 80 + "\n")
            for l in lines:
                log.write(l + "\n")
            log.write("\n")

    print(f"Detailed results written to: {log_path}")

    # Step 4: Cleanup empty dirs
    for sub in subdirs.values():
        main_path = os.path.join(base_dir, sub)
        for nested in ["power_of_2", "manual_verification"]:
            nested_path = os.path.join(main_path, nested)
            if os.path.exists(nested_path) and not os.listdir(nested_path):
                os.rmdir(nested_path)
        if os.path.exists(main_path) and not os.listdir(main_path):
            os.rmdir(main_path)

    # Step 5: Summary
    print("\nSummary:")
    for k, v in counts.items():
        print(f"{subdirs[k]:<25} : {v} files")
    print(f"\nMatched in mgs2_mc_dimensions.csv : {matched_count}")
    print(f"Unmatched                         : {unmatched_count}")
    print(f"\nBP - Not Remade                   : {bp_not_remade}")
    print(f"BP - Remade                       : {bp_remade}")
    print(f"BP - Mismatch                     : {bp_mismatch}")

if __name__ == "__main__":
    main()
