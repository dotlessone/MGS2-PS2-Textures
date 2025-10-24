import csv
import os
from hashlib import md5

# ==========================================================
# CONFIGURATION
# ==========================================================
pcsx2_csv = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_dumped_sha1_log.csv"
texture_csv = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_ps2_dimensions.csv"
output_csv = r"C:\Development\Git\MGS2-PS2-Textures\scripts\pcsx2_tri_sha1_matches.csv"

mc_csv = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_mc_dimensions.csv"
mc_output_csv = r"C:\Development\Git\MGS2-PS2-Textures\scripts\pcsx2_mc_sha1_matches.csv"

pcsx2_crc_fields = [
    "pcsx2_dumped_sha1",
    "pcsx2_resaved_sha1",
    "pcsx2_alpha_stripped_sha1"
]
texture_crc_fields = [
    "tri_dumped_tga_sha1",
    "tri_dumped_png_converted_sha1",
    "tri_dumped_alpha_stripped_sha1"
]
mc_crc_fields = [
    "mc_resaved_sha1",
    "mc_alpha_stripped_sha1"
]

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
def merge_crc(pcsx2_rows, pcsx2_crc_fields, target_rows, target_crc_fields, target_fieldnames, out_path, label):
    crc_lookup = {}
    for row in target_rows:
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

    # --- Sort and write output ---
    if merged_rows:
        sort_key = "texture_name" if "texture_name" in merged_rows[0] else list(merged_rows[0].keys())[0]
        merged_rows.sort(key=lambda x: x.get(sort_key, "").lower())

        fieldnames = target_fieldnames + [f for f in pcsx2_fieldnames if f not in target_fieldnames]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged_rows)

        print(f"[+] {len(merged_rows)} unique {label} matches written to: {out_path}")
    else:
        print(f"[-] No matches found for {label}.")


# ==========================================================
# MAIN EXECUTION
# ==========================================================
if __name__ == "__main__":
    # --- Load CSVs ---
    tex_fieldnames, tex_rows = load_csv(texture_csv)
    pcsx2_fieldnames, pcsx2_rows = load_csv(pcsx2_csv)
    mc_fieldnames, mc_rows = load_csv(mc_csv)

    # --- Merge PS2 Dimensions ---
    merge_crc(
        pcsx2_rows=pcsx2_rows,
        pcsx2_crc_fields=pcsx2_crc_fields,
        target_rows=tex_rows,
        target_crc_fields=texture_crc_fields,
        target_fieldnames=tex_fieldnames,
        out_path=output_csv,
        label="PS2"
    )

    # --- Merge MC Dimensions ---
    merge_crc(
        pcsx2_rows=pcsx2_rows,
        pcsx2_crc_fields=pcsx2_crc_fields,
        target_rows=mc_rows,
        target_crc_fields=mc_crc_fields,
        target_fieldnames=mc_fieldnames,
        out_path=mc_output_csv,
        label="MC"
    )
