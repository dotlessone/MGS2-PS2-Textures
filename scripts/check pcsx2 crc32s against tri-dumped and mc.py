import csv
import os
import subprocess
from hashlib import md5

# ==========================================================
# CONFIGURATION
# ==========================================================
def get_git_root() -> str:
    """Return the absolute path to the Git repo root."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except subprocess.CalledProcessError:
        raise RuntimeError("Not inside a Git repository.")

repo_root = get_git_root()

pcsx2_csv = os.path.join(repo_root, "pcsx2_dumped_sha1_log.csv")
texture_csv = os.path.join(repo_root, "u - dumped from substance", "mgs2_ps2_dimensions.csv")
mc_csv = os.path.join(repo_root, "u - dumped from substance", "mgs2_mc_dimensions.csv")

# Output directly in repo root
output_csv = os.path.join(repo_root, "pcsx2_tri_sha1_matches.csv")
mc_output_csv = os.path.join(repo_root, "pcsx2_mc_sha1_matches.csv")

pcsx2_crc_fields = [
    "pcsx2_dumped_sha1",
    "pcsx2_resaved_sha1",
    "pcsx2_alpha_stripped_sha1",
]
texture_crc_fields = [
    "tri_dumped_tga_sha1",
    "tri_dumped_png_converted_sha1",
    "tri_dumped_alpha_stripped_sha1",
]
mc_crc_fields = ["mc_resaved_sha1"]

# ==========================================================
# LOAD CSV HELPERS
# ==========================================================
def load_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return reader.fieldnames, rows

# ==========================================================
# MERGE FUNCTION
# ==========================================================
def merge_crc(pcsx2_rows, pcsx2_crc_fields, target_rows, target_crc_fields, target_fieldnames, out_path, label, exclude_fields=None, ps2_ref_rows=None):
    ps2_ref_map = {}
    if ps2_ref_rows:
        for r in ps2_ref_rows:
            name = r.get("texture_name", "").strip().lower()
            w = r.get("tri_dumped_width", "").strip()
            h = r.get("tri_dumped_height", "").strip()
            if name and w and h:
                ps2_ref_map[name] = (w, h)

    crc_lookup = {}
    for row in target_rows:
        texname = row.get("texture_name", "").strip().lower()

        # MC dimension blacklist
        if label == "MC" and texname in ps2_ref_map:
            mc_w = row.get("mc_width", "").strip()
            mc_h = row.get("mc_height", "").strip()
            ps2_w, ps2_h = ps2_ref_map[texname]
            if mc_w != ps2_w or mc_h != ps2_h:
                continue

        # PS2 dimension blacklist
        if label == "PS2" and texname in ps2_ref_map:
            ps2_w, ps2_h = ps2_ref_map[texname]
            pcsx2_entry = next((r for r in pcsx2_rows if r.get("texture_name", "").strip().lower() == texname), None)
            if pcsx2_entry:
                pcsx2_w = pcsx2_entry.get("pcsx2_width", "").strip()
                pcsx2_h = pcsx2_entry.get("pcsx2_height", "").strip()
                if pcsx2_w != ps2_w or pcsx2_h != ps2_h:
                    continue

        for field in target_crc_fields:
            crc = row.get(field, "").strip().lower()
            if crc and crc != "0000000000000000000000000000000000000000":
                crc_lookup.setdefault(crc, []).append(row)

    merged_rows = []
    seen_hashes = set()

    for pcsx2_row in pcsx2_rows:
        for field in pcsx2_crc_fields:
            crc = pcsx2_row.get(field, "").strip().lower()
            if not crc or crc == "0000000000000000000000000000000000000000":
                continue
            if crc in crc_lookup:
                for match_row in crc_lookup[crc]:
                    merged = {**match_row, **pcsx2_row}
                    row_hash = md5("".join(merged.values()).encode("utf-8")).hexdigest()
                    if row_hash not in seen_hashes:
                        seen_hashes.add(row_hash)
                        merged_rows.append(merged)

    if not merged_rows:
        print(f"[-] No matches found for {label}.")
        return

    sort_key = "texture_name" if "texture_name" in merged_rows[0] else list(merged_rows[0].keys())[0]
    merged_rows.sort(key=lambda x: x.get(sort_key, "").lower())

    fieldnames = [f for f in target_fieldnames if not exclude_fields or f not in exclude_fields]
    fieldnames += [f for f in pcsx2_fieldnames if f not in fieldnames]
    fieldnames = [f for f in fieldnames if f not in ("pcsx2_width", "pcsx2_height")]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in merged_rows:
            for drop_field in ["pcsx2_width", "pcsx2_height"]:
                if drop_field in row:
                    del row[drop_field]

            if exclude_fields:
                for ef in exclude_fields:
                    row.pop(ef, None)

            writer.writerow(row)

    print(f"[+] {len(merged_rows)} unique {label} matches written to: {out_path}")

# ==========================================================
# MAIN EXECUTION
# ==========================================================
if __name__ == "__main__":
    tex_fieldnames, tex_rows = load_csv(texture_csv)
    pcsx2_fieldnames, pcsx2_rows = load_csv(pcsx2_csv)
    mc_fieldnames, mc_rows = load_csv(mc_csv)

    merge_crc(
        pcsx2_rows=pcsx2_rows,
        pcsx2_crc_fields=pcsx2_crc_fields,
        target_rows=tex_rows,
        target_crc_fields=texture_crc_fields,
        target_fieldnames=tex_fieldnames,
        out_path=output_csv,
        label="PS2",
        ps2_ref_rows=tex_rows,
    )

    merge_crc(
        pcsx2_rows=pcsx2_rows,
        pcsx2_crc_fields=pcsx2_crc_fields,
        target_rows=mc_rows,
        target_crc_fields=["mc_resaved_sha1"],
        target_fieldnames=mc_fieldnames,
        out_path=mc_output_csv,
        label="MC",
        exclude_fields=["mc_alpha_stripped_sha1"],
        ps2_ref_rows=tex_rows,
    )
    
    next_script = os.path.join(repo_root, "final_verification_nov22_2025", "0000 - get matching sha1 files.py")
    if os.path.isfile(next_script):
        print(f"[>] Running post-process script: {next_script}")
        result = subprocess.run(["python", next_script], capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
    else:
        print(f"[!] Post-process script not found at: {next_script}")
