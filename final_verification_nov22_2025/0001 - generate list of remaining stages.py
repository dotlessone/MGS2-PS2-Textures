import os
import csv
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock
from PIL import Image

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
# STAGE ALIASES
# ==========================================================
STAGE_ALIASES = {
    # =========================
    # Menu
    # =========================
    "n_title": ("Menu", "Title Screen"),
    "select": ("Menu", "Main Menu"),
    "mselect": ("Menu", "VR Missions Menu"),
    "tales": ("Menu", "Snake Tales Menu"),

    # =========================
    # Tanker
    # =========================
    "w00a": ("Tanker", "Deck A - Port"),
    "w00b": ("Tanker", "Deck A - Starboard (Olga)"),
    "w00c": ("Tanker", "Navigational Deck"),
    "w01a": ("Tanker", "Deck A Crew Quarters"),
    "w01b": ("Tanker", "Deck A Crew Quarters Starboard"),
    "w01c": ("Tanker", "Deck C Crew Quarters"),
    "w01d": ("Tanker", "Deck D Crew Quarters"),
    "w01e": ("Tanker", "Deck E Bridge"),
    "w01f": ("Tanker", "Deck A Crew Lounge"),
    "w02a": ("Tanker", "Engine Room"),
    "w03a": ("Tanker", "Deck 2 Port"),
    "w03b": ("Tanker", "Deck 2 Starboard"),
    "w04a": ("Tanker", "Hold No.1"),
    "w04b": ("Tanker", "Hold No.2"),
    "w04c": ("Tanker", "Hold No.3"),
    "d00t": ("Tanker", "George Washington Bridge"),
    "d01t": ("Tanker", "Navigational Deck (Russian Invasion)"),
    "d04t": ("Tanker", "Identifying Choppers"),
    "d05t": ("Tanker", "Olga Cutscene"),
    "d10t": ("Tanker", "Cutscene d10t"),
    "d11t": ("Tanker", "Cutscene d11t"),
    "d12t": ("Tanker", "Cutscene d12t"),
    "d12t3": ("Tanker", "Cutscene d12t3"),
    "d12t4": ("Tanker", "Cutscene d12t4"),
    "d13t": ("Tanker", "Cutscene d13t (NG+ Rewards)"),
    "d14t": ("Tanker", "Cutscene d14t"),

    # =========================
    # Plant
    # =========================
    "w11a": ("Plant", "Strut A - Dock"),
    "w11b": ("Plant", "Strut A - Dock (Bomb Disposal)"),
    "w11c": ("Plant", "Strut A - Dock (Fortune)"),
    "w12a": ("Plant", "Strut A Roof"),
    "w12b": ("Plant", "Strut A Pump Room"),
    "w12c": ("Plant", "Strut A Roof (Bomb)"),
    "w13a": ("Plant", "AB Connecting Bridge"),
    "w13b": ("Plant", "AB Connecting Bridge (Sensor B)"),
    "w14a": ("Plant", "Transformer Room"),
    "w15a": ("Plant", "BC Connecting Bridge"),
    "w15b": ("Plant", "BC Connecting Bridge (After Stillman)"),
    "w16a": ("Plant", "Dining Hall"),
    "w16b": ("Plant", "Dining Hall (Post Stillman)"),
    "w17a": ("Plant", "CD Connecting Bridge"),
    "w18a": ("Plant", "Sediment Pool"),
    "w19a": ("Plant", "DE Connecting Bridge"),
    "w20a": ("Plant", "Parcel Room"),
    "w20b": ("Plant", "Heliport"),
    "w20c": ("Plant", "Heliport (Bomb)"),
    "w20d": ("Plant", "Heliport (Post Ninja)"),
    "w21a": ("Plant", "EF Connecting Bridge"),
    "w21b": ("Plant", "EF Connecting Bridge 2"),
    "w22a": ("Plant", "Warehouse"),
    "w23a": ("Plant", "FA Connecting Bridge"),
    "w23b": ("Plant", "FA Connecting Bridge (Post Shell 2)"),
    "w24a": ("Plant", "Shell 1 Core - 1F"),
    "w24b": ("Plant", "Shell 1 Core - B1"),
    "w24c": ("Plant", "Shell 1 Core - Hostage Room"),
    "w24d": ("Plant", "Shell 1 Core - B2"),
    "w25a": ("Plant", "Shell Connecting Bridge"),
    "w25b": ("Plant", "Shell Connecting Bridge (Destroyed)"),
    "w25c": ("Plant", "Strut L Perimeter"),
    "w25d": ("Plant", "KL Connecting Bridge"),
    "w28a": ("Plant", "Sewage Treatment"),
    "w31a": ("Plant", "Shell 2 Core"),
    "w31b": ("Plant", "Shell 2 Filtration Chamber No.1"),
    "w31c": ("Plant", "Shell 2 Filtration Chamber No.2"),
    "w31d": ("Plant", "Shell 2 Core (With Emma)"),
    "w32a": ("Plant", "Oil Fence"),
    "w32b": ("Plant", "Oil Fence (Vamp Fight)"),
    "w41a": ("Plant", "GW - Stomach"),
    "w42a": ("Plant", "GW - Jejunum"),
    "w43a": ("Plant", "GW - Ascending Colon"),
    "w44a": ("Plant", "GW - Ileum"),
    "w45a": ("Plant", "GW - Sigmoid Colon"),
    "w46a": ("Plant", "GW - Rectum"),
    "w51a": ("Plant", "Arsenal Gear"),
    "w61a": ("Plant", "Federal Hall"),
    "museum": ("Plant", "Briefing"),
    "webdemo": ("Plant", "Web Demo"),
    "ending": ("Plant", "Results Screen"),
    "d001p01": ("Plant", "Plant Opening"),
    "d001p02": ("Plant", "Sea Dock Cutscene"),
    "d005p01": ("Plant", "Raiden On Elevator"),
    "d005p03": ("Plant", "Strut A Roof Cutscene"),
    "d010p01": ("Plant", "Meeting Vamp"),
    "d012p01": ("Plant", "ADUD"),
    "d014p01": ("Plant", "Stillman Cutscene"),
    "d021p01": ("Plant", "Fatman and Ninja"),
    "d036p03": ("Plant", "Hostage Cutscene"),
    "d036p05": ("Plant", "Shell 1 Cutscene"),
    "d045p01": ("Plant", "Cutscene d045p01"),
    "d046p01": ("Plant", "Cutscene d046p01"),
    "d053p01": ("Plant", "Cutscene d053p01"),
    "d055p01": ("Plant", "Cutscene d055p01"),
    "d063p01": ("Plant", "Cutscene d063p01"),
    "d065p02": ("Plant", "Cutscene d065p02"),
    "d070p01": ("Plant", "Cutscene d070p01"),
    "d070p09": ("Plant", "Cutscene d070p09"),
    "d070px9": ("Plant", "Cutscene d070px9"),
    "d078p01": ("Plant", "Cutscene d078p01"),
    "d080p01": ("Plant", "Cutscene d080p01"),
    "d080p06": ("Plant", "Cutscene d080p06"),
    "d080p07": ("Plant", "Cutscene d080p07"),
    "d080p08": ("Plant", "Cutscene d080p08"),
    "d082p01": ("Plant", "Cutscene d082p01"),

    # =========================
    # Alternate (Snake Tales)
    # =========================
    "a00a": ("Alternate", "Deck A - Port (Alternate)"),
    "a00b": ("Alternate", "Navigational Deck (Alternate)"),
    "a00c": ("Alternate", "Navigational Deck (Unused Alt)"),
    "a01a": ("Alternate", "Deck A Crew Quarters (Alternate)"),
    "a01b": ("Alternate", "Deck A Crew Quarters Starboard (Alternate)"),
    "a01c": ("Alternate", "Deck C Crew Quarters (Alternate)"),
    "a01d": ("Alternate", "Deck D Crew Quarters (Alternate)"),
    "a01e": ("Alternate", "Deck E Bridge (Alternate)"),
    "a01f": ("Alternate", "Deck A Crew Lounge (Alternate)"),
    "a02a": ("Alternate", "Engine Room (Alternate)"),
    "a03a": ("Alternate", "Deck 2 Port (Alternate)"),
    "a03b": ("Alternate", "Deck 2 Starboard (Alternate)"),
    "a04a": ("Alternate", "Hold No.1 (Alternate)"),
    "a04b": ("Alternate", "Hold No.2 (Alternate)"),
    "a04c": ("Alternate", "Hold No.3 (Alternate)"),
    "a11a": ("Alternate", "Strut A - Dock (Snake Tales)"),
    "a11b": ("Alternate", "Strut A - Dock (Bomb) (Snake Tales)"),
    "a11c": ("Alternate", "Strut A - Dock (Fortune) (Snake Tales)"),
    "a12a": ("Alternate", "Strut A Roof (Snake Tales)"),
    "a12b": ("Alternate", "Strut A Pump Room (Snake Tales)"),
    "a12c": ("Alternate", "Strut A Roof (Bomb) (Snake Tales)"),
    "a13a": ("Alternate", "AB Connecting Bridge (Snake Tales)"),
    "a13b": ("Alternate", "AB Connecting Bridge (Sensor B) (Snake Tales)"),
    "a14a": ("Alternate", "Transformer Room (Snake Tales)"),
    "a15a": ("Alternate", "BC Connecting Bridge (Snake Tales)"),
    "a15b": ("Alternate", "BC Connecting Bridge (After Stillman) (Snake Tales)"),
    "a16a": ("Alternate", "Dining Hall (Snake Tales)"),
    "a16b": ("Alternate", "Dining Hall (Post Stillman) (Snake Tales)"),
    "a17a": ("Alternate", "CD Connecting Bridge (Snake Tales)"),
    "a18a": ("Alternate", "Sediment Pool (Snake Tales)"),
    "a19a": ("Alternate", "DE Connecting Bridge (Snake Tales)"),
    "a20a": ("Alternate", "Parcel Room (Snake Tales)"),
    "a20b": ("Alternate", "Heliport (Snake Tales)"),
    "a20c": ("Alternate", "Heliport (Bomb) (Snake Tales)"),
    "a20d": ("Alternate", "Heliport (Post Ninja) (Snake Tales)"),
    "a21a": ("Alternate", "EF Connecting Bridge (Snake Tales)"),
    "a22a": ("Alternate", "Warehouse (Snake Tales)"),
    "a23b": ("Alternate", "FA Connecting Bridge (Snake Tales)"),
    "a24a": ("Alternate", "Shell 1 Core - 1F (Snake Tales)"),
    "a24b": ("Alternate", "Shell 1 Core - B1 (Snake Tales)"),
    "a24c": ("Alternate", "Shell 1 Core - Hostage Room (Snake Tales)"),
    "a24d": ("Alternate", "Shell 1 Core - B2 (Snake Tales)"),
    "a25a": ("Alternate", "Shell Connecting Bridge (Snake Tales)"),
    "a25b": ("Alternate", "Shell Connecting Bridge (Destroyed) (Snake Tales)"),
    "a25c": ("Alternate", "Strut L Perimeter (Snake Tales)"),
    "a25d": ("Alternate", "KL Connecting Bridge (Snake Tales)"),
    "a28a": ("Alternate", "Sewage Treatment (Snake Tales)"),
    "a31a": ("Alternate", "Shell 2 Core (Snake Tales)"),
    "a31b": ("Alternate", "Shell 2 Filtration Chamber No.1 (Snake Tales)"),
    "a31c": ("Alternate", "Shell 2 Filtration Chamber No.2 (Snake Tales)"),
    "a31d": ("Alternate", "Shell 2 Core (With Emma) (Snake Tales)"),
    "a32a": ("Alternate", "Oil Fence (Snake Tales)"),
    "a32b": ("Alternate", "Oil Fence (Vamp Fight) (Snake Tales)"),
    "a41a": ("Alternate", "GW - Stomach (Snake Tales)"),
    "a42a": ("Alternate", "GW - Jejunum (Snake Tales)"),
    "a43a": ("Alternate", "GW - Ascending Colon (Snake Tales)"),
    "a44a": ("Alternate", "GW - Ileum (Snake Tales)"),
    "a45a": ("Alternate", "GW - Sigmoid Colon (Snake Tales)"),
    "a46a": ("Alternate", "GW - Rectum (Snake Tales)"),
    "a51a": ("Alternate", "Arsenal Gear (Snake Tales)"),
    "a61a": ("Alternate", "Federal Hall (Snake Tales)"),

    # =========================
    # VR: Sneaking
    # =========================
    "vs01a": ("VR: Sneaking", "Level 1"),
    "vs02a": ("VR: Sneaking", "Level 2"),
    "vs03a": ("VR: Sneaking", "Level 3"),
    "vs04a": ("VR: Sneaking", "Level 4"),
    "vs05a": ("VR: Sneaking", "Level 5"),
    "vs06a": ("VR: Sneaking", "Level 6"),
    "vs07a": ("VR: Sneaking", "Level 7"),
    "vs08a": ("VR: Sneaking", "Level 8"),
    "vs09a": ("VR: Sneaking", "Level 9"),
    "vs10a": ("VR: Sneaking", "Level 10"),

    # =========================
    # VR: Variety
    # =========================
    "sp01a": ("VR: Variety", "Level 1"),
    "sp02a": ("VR: Variety", "Level 2"),
    "sp03a": ("VR: Variety", "Level 3"),
    "sp04a": ("VR: Variety", "Level 4"),
    "sp05a": ("VR: Variety", "Level 5"),
    "sp06a": ("VR: Variety", "Level 6"),
    "sp07a": ("VR: Variety", "Level 7"),
    "sp08a": ("VR: Variety", "Level 8"),

    # =========================
    # VR: First-Person
    # =========================
    "sp21a": ("VR: First-Person", "Level 1"),
    "sp22a": ("VR: First-Person", "Level 2"),
    "sp23a": ("VR: First-Person", "Level 3"),
    "sp24a": ("VR: First-Person", "Level 4"),
    "sp25a": ("VR: First-Person", "Level 5"),

    # =========================
    # VR: Streaking
    # =========================
    "st01a": ("VR: Streaking", "Level 1"),
    "st02a": ("VR: Streaking", "Level 2"),
    "st03a": ("VR: Streaking", "Level 3"),
    "st04a": ("VR: Streaking", "Level 4"),

    # =========================
    # VR: Weapons - SOCOM
    # =========================
    "wp01a": ("VR: Weapons - SOCOM", "Level 1"),
    "wp02a": ("VR: Weapons - SOCOM", "Level 2"),
    "wp03a": ("VR: Weapons - SOCOM", "Level 3"),
    "wp04a": ("VR: Weapons - SOCOM", "Level 4"),
    "wp05a": ("VR: Weapons - SOCOM", "Level 5"),

    # VR: Weapons - M4
    "wp11a": ("VR: Weapons - M4", "Level 1"),
    "wp12a": ("VR: Weapons - M4", "Level 2"),
    "wp13a": ("VR: Weapons - M4", "Level 3"),
    "wp14a": ("VR: Weapons - M4", "Level 4"),
    "wp15a": ("VR: Weapons - M4", "Level 5"),

    # VR: Weapons - Claymore
    "wp21a": ("VR: Weapons - Claymore", "Level 1"),
    "wp22a": ("VR: Weapons - Claymore", "Level 2"),
    "wp23a": ("VR: Weapons - Claymore", "Level 3"),
    "wp24a": ("VR: Weapons - Claymore", "Level 4"),
    "wp25a": ("VR: Weapons - Claymore", "Level 5"),

    # VR: Weapons - Grenade
    "wp31a": ("VR: Weapons - Grenade", "Level 1"),
    "wp32a": ("VR: Weapons - Grenade", "Level 2"),
    "wp33a": ("VR: Weapons - Grenade", "Level 3"),
    "wp34a": ("VR: Weapons - Grenade", "Level 4"),
    "wp35a": ("VR: Weapons - Grenade", "Level 5"),

    # VR: Weapons - PSG1
    "wp41a": ("VR: Weapons - PSG1", "Level 1"),
    "wp42a": ("VR: Weapons - PSG1", "Level 2"),
    "wp43a": ("VR: Weapons - PSG1", "Level 3"),
    "wp44a": ("VR: Weapons - PSG1", "Level 4"),
    "wp45a": ("VR: Weapons - PSG1", "Level 5"),

    # VR: Weapons - Stinger
    "wp51a": ("VR: Weapons - Stinger", "Level 1"),
    "wp52a": ("VR: Weapons - Stinger", "Level 2"),
    "wp53a": ("VR: Weapons - Stinger", "Level 3"),
    "wp54a": ("VR: Weapons - Stinger", "Level 4"),
    "wp55a": ("VR: Weapons - Stinger", "Level 5"),

    # VR: Weapons - Nikita
    "wp61a": ("VR: Weapons - Nikita", "Level 1"),
    "wp62a": ("VR: Weapons - Nikita", "Level 2"),
    "wp63a": ("VR: Weapons - Nikita", "Level 3"),
    "wp64a": ("VR: Weapons - Nikita", "Level 4"),
    "wp65a": ("VR: Weapons - Nikita", "Level 5"),

    # VR: Weapons - No Weapon
    "wp71a": ("VR: Weapons - No Weapon", "Level 1"),
    "wp72a": ("VR: Weapons - No Weapon", "Level 2"),
    "wp73a": ("VR: Weapons - No Weapon", "Level 3"),
    "wp74a": ("VR: Weapons - No Weapon", "Level 4"),
    "wp75a": ("VR: Weapons - No Weapon", "Level 5"),
}
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


def get_low_alpha_textures(directories):
    """
    Recursively find all .tga files in the given directories where all alpha <= 128.
    Returns a set of lowercase basenames without extension.
    """
    low_alpha = set()
    lock = Lock()

    def process_image(path):
        try:
            with Image.open(path) as img:
                if img.mode not in ("RGBA", "LA"):
                    return None
                alpha = img.getchannel("A")
                extrema = alpha.getextrema()
                if extrema and extrema[1] <= 128:
                    return os.path.splitext(os.path.basename(path).lower())[0]
        except Exception:
            return None
        return None

    def process_folder(folder):
        local = []
        for entry in os.scandir(folder):
            if entry.is_dir(follow_symlinks=False):
                futures.append(executor.submit(process_folder, entry.path))
            elif entry.is_file() and entry.name.lower().endswith(".tga"):
                result = process_image(entry.path)
                if result:
                    local.append(result)
        with lock:
            low_alpha.update(local)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for d in directories:
            if os.path.isdir(d):
                futures.append(executor.submit(process_folder, d))
        for _ in as_completed(futures):
            pass

    return low_alpha


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

    # wipe log
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
    final_rebuilt_dir = os.path.join(repo_root, "u - dumped from substance", "dump", "Final Rebuilt")
    half_alpha_dir = os.path.join(final_rebuilt_dir, "half_alpha")
    opaque_dir = os.path.join(final_rebuilt_dir, "opaque")
    mixed_alpha_dir = os.path.join(final_rebuilt_dir, "mixed_alpha")
    good_alpha_dir = os.path.join(final_rebuilt_dir, "good_alpha")

    root_dirs = [script_dir, half_alpha_dir, opaque_dir]
    found_names = scan_files(root_dirs, [".png", ".tga"])
    print(f"Found {len(found_names)} PNG/TGA files total.")
    log(f"Found {len(found_names)} PNG/TGA files total.\n")

    # ==========================================================
    # DETECT LOW ALPHA TGAs TO SKIP
    # ==========================================================
    print("Scanning mixed_alpha and good_alpha for low-alpha TGAs (<=128)...")
    low_alpha_textures = get_low_alpha_textures([mixed_alpha_dir, good_alpha_dir])
    print(f"Detected {len(low_alpha_textures)} correct mixed-alpha TGAs (will be skipped).")
    log(f"Detected {len(low_alpha_textures)} correct mixed-alpha TGAs (will be skipped).\n")

    # ==========================================================
    # DETECT MISSING
    # ==========================================================
    missing_per_stage = defaultdict(list)
    all_missing_unique = set()

    for stage, texnames in stage_to_textures.items():
        texnames_lower = [t.lower() for t in texnames if t.lower() in valid_ps2_names]
        filtered_texnames = [t for t in texnames_lower if not is_excluded(t)]
        missing = [
            t for t in filtered_texnames
            if t not in found_names and t not in low_alpha_textures
        ]

        if missing:
            missing_per_stage[stage].extend(missing)
            all_missing_unique.update(missing)

    # ==========================================================
    # LOGS
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
            alias = STAGE_ALIASES.get(stage.lower())
            if alias:
                category, desc = alias
                alias_str = f"[{category} | {desc}]"
            else:
                alias_str = ""
            line = f"{stage}\t{count}\t{alias_str}"
            print(line)
            log(line)

    log("")
    total_line = f"TOTAL UNIQUE MISSING: {len(all_missing_unique)}"
    print("\n" + total_line)
    log(total_line)

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
            alias = STAGE_ALIASES.get(stage.lower())
            if alias:
                category, desc = alias
                alias_str = f"[{category} | {desc}]"
            else:
                alias_str = ""
            line = f"{stage}\t{count}\t{alias_str}"
            print(line)
            log(line)

    # ==========================================================
    # PER-STAGE LOGS
    # ==========================================================
    existing_logs = {f for f in os.listdir(per_stage_dir) if f.lower().endswith(".txt")}
    written_logs = set()

    for stage, missing in missing_per_stage.items():
        if not missing:
            continue
        stage_filename = f"{stage}.txt"
        written_logs.add(stage_filename)
        stage_log_path = os.path.join(per_stage_dir, stage_filename)
        sorted_missing = sorted(set(missing), key=str.lower)
        joined_names = "|".join(sorted_missing)
        header_line = f"mgs2/base|mgs2-ps2-text /<{joined_names}>.png\n"
        new_contents = header_line + "\n".join(sorted_missing) + "\n"

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

    for old_log in existing_logs - written_logs:
        try:
            os.remove(os.path.join(per_stage_dir, old_log))
        except Exception as e:
            print(f"Failed to remove stale log {old_log}: {e}")

    print(f"\nPer-stage logs written to: {per_stage_dir}")
    print(f"Log written to: {log_path}")


if __name__ == "__main__":
    main()
