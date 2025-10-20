import os
import zlib

# --- Target scan root (Sons of Liberty) ---
ROOT_DIR = r"C:\Development\Git\MGS2-PS2-Textures\t - dumped from sons of liberty\dump"

# --- Source of truth (Substance Final Rebuilt) ---
SOURCE_DIR = r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\dump\Final Rebuilt"

def compute_crc32(filepath):
    """Compute CRC32 for a file efficiently in chunks."""
    crc = 0
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            crc = zlib.crc32(chunk, crc)
    return format(crc & 0xFFFFFFFF, "08x")

def collect_source_crcs(source_dir):
    """Map CRC32 -> texture name (basename without extension) from SOURCE_DIR."""
    crc_map = {}
    for root, _, files in os.walk(source_dir):
        for name in files:
            if not name.lower().endswith(".tga"):
                continue
            path = os.path.join(root, name)
            try:
                crc = compute_crc32(path)
                texture_name = os.path.splitext(name)[0]
                crc_map[crc] = texture_name
            except Exception as e:
                print(f"[WARN] CRC32 failed for: {path} ({e})")
    print(f"[INFO] Collected {len(crc_map)} CRC32 entries from Substance Final Rebuilt.")
    return crc_map

def log_matching_files(root_dir, crc_map):
    """
    Scan ROOT_DIR for .tga files (skipping any dir named 'Final Rebuilt'),
    and log matches as: texture_name,stage,folder,filename_without_ext
    """
    matches = []
    for current_root, dirs, files in os.walk(root_dir, topdown=True):
        # Prune any directory named exactly 'Final Rebuilt' (case-insensitive)
        dirs[:] = [d for d in dirs if d.lower() != "final rebuilt"]

        for name in files:
            if not name.lower().endswith(".tga"):
                continue

            path = os.path.join(current_root, name)
            rel_path = os.path.relpath(path, root_dir)

            try:
                crc = compute_crc32(path)
                if crc not in crc_map:
                    continue

                texture_name = crc_map[crc]

                parts = rel_path.split(os.sep)
                # Expect something like: stage\folder\file.tga
                stage = parts[0] if len(parts) >= 1 else ""
                folder = parts[1] if len(parts) >= 2 else ""
                filename_wo_ext = os.path.splitext(parts[-1])[0] if parts else ""

                line = f"{texture_name},{stage},{folder},{filename_wo_ext}"
                matches.append(line)
                print(f"[MATCH] {line}")

            except Exception as e:
                print(f"[WARN] Failed to process: {path} ({e})")

    log_path = os.path.join(root_dir, "crc32_matches_from_substance.csv")
    with open(log_path, "w", encoding="utf-8") as f:
        for line in matches:
            f.write(line + "\n")

    print(f"[INFO] Logged {len(matches)} matches to: {log_path}")

if __name__ == "__main__":
    crc_map = collect_source_crcs(SOURCE_DIR)
    log_matching_files(ROOT_DIR, crc_map)
