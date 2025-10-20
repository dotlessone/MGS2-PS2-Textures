import os
import zlib

ROOT_DIR = r"C:\Development\Git\MGS2-PS2-Textures\t - dumped from sons of liberty\dump"
SOURCE_DIR = os.path.join(ROOT_DIR, "Final Rebuilt")

def compute_crc32(filepath):
    """Compute CRC32 for a file efficiently."""
    crc = 0
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            crc = zlib.crc32(chunk, crc)
    return format(crc & 0xFFFFFFFF, "08x")

def collect_source_crcs(source_dir):
    """Map CRC32 -> texture name (filename without path or extension)."""
    crc_map = {}
    for root, _, files in os.walk(source_dir):
        for name in files:
            if name.lower().endswith(".tga"):
                path = os.path.join(root, name)
                try:
                    crc = compute_crc32(path)
                    # strip .tga from texture name
                    texture_name = os.path.splitext(name)[0]
                    crc_map[crc] = texture_name
                except Exception as e:
                    print(f"[WARN] CRC32 failed for: {path} ({e})")
    print(f"[INFO] Collected {len(crc_map)} CRC32 entries from Final Rebuilt.")
    return crc_map

def log_matching_files(root_dir, source_dir, crc_map):
    """Find matching TGAs and log them; do NOT delete."""
    matches = []
    for subdir in os.listdir(root_dir):
        full_subdir = os.path.join(root_dir, subdir)
        if not os.path.isdir(full_subdir):
            continue
        if os.path.samefile(full_subdir, source_dir):
            continue  # skip Final Rebuilt

        for root, _, files in os.walk(full_subdir):
            for name in files:
                if not name.lower().endswith(".tga"):
                    continue
                path = os.path.join(root, name)
                rel_path = os.path.relpath(path, root_dir)
                try:
                    crc = compute_crc32(path)
                    if crc in crc_map:
                        texture_name = crc_map[crc]
                        parts = rel_path.split(os.sep)
                        if len(parts) >= 3:
                            stage = parts[0]
                            folder = parts[1]
                            filename = os.path.splitext(parts[-1])[0]
                            line = f"{texture_name},{stage},{folder},{filename}"
                            matches.append(line)
                            print(f"[MATCH] {line}")
                        else:
                            # handle unexpected path depth safely
                            line = f"{texture_name},,,{os.path.splitext(parts[-1])[0]}"
                            matches.append(line)
                            print(f"[MATCH] {line}")
                except Exception as e:
                    print(f"[WARN] Failed to process: {path} ({e})")

    log_path = os.path.join(root_dir, "crc32_matches.csv")
    with open(log_path, "w", encoding="utf-8") as f:
        for line in matches:
            f.write(line + "\n")

    print(f"[INFO] Logged {len(matches)} matches to: {log_path}")

if __name__ == "__main__":
    crc_map = collect_source_crcs(SOURCE_DIR)
    log_matching_files(ROOT_DIR, SOURCE_DIR, crc_map)
