import csv
import os
from hashlib import md5

pcsx2_csv = r"C:\Development\Git\MGS2-PS2-Textures\pcsx2_dumped_png_crc32_log.csv"
texture_csv = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_ps2_dimensions.csv"
output_csv = os.path.join(os.path.dirname(pcsx2_csv), "crc32_matches_deduped.csv")

pcsx2_crc_fields = [
    "pcsx2_dumped_crc32",
    "pcxs2_normalized_png_crc32",
    "pcsx2_alpha_stripped_crc32"
]
texture_crc_fields = [
    "tri_dumped_tga_crc32",
    "tri_png_converted_crc32",
    "tri_alpha_stripped_crc32"
]

# --- Load texture CSV ---
with open(texture_csv, newline="", encoding="utf-8") as f:
    tex_reader = csv.DictReader(f)
    tex_fieldnames = tex_reader.fieldnames
    tex_rows = list(tex_reader)

# --- Load PCSX2 CSV ---
with open(pcsx2_csv, newline="", encoding="utf-8") as f:
    pcsx2_reader = csv.DictReader(f)
    pcsx2_fieldnames = pcsx2_reader.fieldnames
    pcsx2_rows = list(pcsx2_reader)

# --- Build CRC lookup ---
crc_lookup = {}
for row in tex_rows:
    for field in texture_crc_fields:
        crc = row.get(field, "").strip().lower()
        if crc and crc != "00000000":
            crc_lookup.setdefault(crc, []).append(row)

# --- Merge ---
merged_rows = []
seen_hashes = set()

for pcsx2_row in pcsx2_rows:
    for field in pcsx2_crc_fields:
        crc = pcsx2_row.get(field, "").strip().lower()
        if not crc or crc == "00000000":
            continue
        if crc in crc_lookup:
            for tex_row in crc_lookup[crc]:
                merged = {**tex_row, **pcsx2_row}
                # compute hash of row for deduplication
                row_hash = md5("".join(merged.values()).encode("utf-8")).hexdigest()
                if row_hash not in seen_hashes:
                    seen_hashes.add(row_hash)
                    merged_rows.append(merged)

# --- Write output ---
if merged_rows:
    fieldnames = tex_fieldnames + [f for f in pcsx2_fieldnames if f not in tex_fieldnames]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_rows)
    print(f"[+] {len(merged_rows)} unique matches written to: {output_csv}")
else:
    print("[-] No matches found.")
