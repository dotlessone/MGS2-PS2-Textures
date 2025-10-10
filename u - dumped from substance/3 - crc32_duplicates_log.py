import os
import zlib
from collections import defaultdict

# Directories to scan
FOLDERS = ["half_alpha", "mixed_alpha", "opaque", "bad_alpha"]
LOG_FILE = "crc32_duplicates_log.txt"

def compute_crc32(file_path):
    """Compute CRC32 checksum for a file."""
    buf_size = 65536  # 64 KB
    crc = 0
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(buf_size):
                crc = zlib.crc32(chunk, crc)
        return crc & 0xFFFFFFFF
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return None

def main():
    crc_map = defaultdict(list)

    # Recursively walk through all target folders
    for folder in FOLDERS:
        if not os.path.exists(folder):
            continue
        for root, _, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                crc = compute_crc32(file_path)
                if crc is not None:
                    crc_map[crc].append(file_path)

    # Find duplicates
    duplicates = {crc: paths for crc, paths in crc_map.items() if len(paths) > 1}

    # Write to log file
    with open(LOG_FILE, "w", encoding="utf-8") as log:
        if not duplicates:
            log.write("No duplicates found.\n")
        else:
            for crc, paths in sorted(duplicates.items(), key=lambda x: x[0]):
                log.write(f"CRC32: {crc:08X}\n")
                for p in sorted(paths):
                    log.write(f"  {p}\n")
                log.write("\n")

    print(f"Done. Logged {len(duplicates)} CRC32 duplicate groups to {LOG_FILE}")

if __name__ == "__main__":
    main()
