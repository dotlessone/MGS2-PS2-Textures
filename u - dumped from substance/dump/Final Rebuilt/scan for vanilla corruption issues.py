import os
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# ==========================================================
# CONFIGURATION
# ==========================================================
TARGET_DIR = Path(r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\dump\Final Rebuilt")
OUTPUT_LOG = Path("possible vanilla corruption issues.txt")

SUSPICIOUS_RGB = {
    (0, 240, 247),
    (0, 249, 3),
    (0, 241, 31),
    (36, 251, 41),
    (36, 251, 21),
    (0, 248, 43),
    (76, 251, 21),
    (221,245,137),
    (205,245,75),
    (177,182,172),
    (156,245,75),
    (48,245,33),
    (248,62,8),
    (8,136,137),
    (1,230,200),
    (1,230,184),
    (0,7,243),
    (185,230,197),
}

# ==========================================================
# HELPERS
# ==========================================================
def has_suspicious_color(path: Path) -> str | None:
    """Check if the TGA file contains any suspicious RGB pixel (ignoring alpha)."""
    try:
        with Image.open(path) as img:
            img = img.convert("RGBA")
            for r, g, b, _ in img.getdata():
                if (r, g, b) in SUSPICIOUS_RGB:
                    # Return filename only, without extension
                    return path.stem
    except Exception as e:
        print(f"[ERROR] {path}: {e}")
    return None


# ==========================================================
# MAIN
# ==========================================================
def main():
    tga_files = list(TARGET_DIR.rglob("*.tga"))
    if not tga_files:
        print("No .tga files found under 'mixed_alpha'.")
        return

    print(f"Scanning {len(tga_files)} .tga files using {multiprocessing.cpu_count()} threads...")

    matches = []
    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        future_to_path = {executor.submit(has_suspicious_color, p): p for p in tga_files}
        for i, fut in enumerate(as_completed(future_to_path), 1):
            result = fut.result()
            if result:
                matches.append(result)
                print(f"[MATCH] {result}")
            if i % 50 == 0 or i == len(tga_files):
                print(f"Progress: {i}/{len(tga_files)}")

    if matches:
        # Deduplicate and sort for readability
        matches = sorted(set(matches))
        OUTPUT_LOG.write_text("\n".join(matches), encoding="utf-8")
        print(f"\nLogged {len(matches)} possible corruption issues to {OUTPUT_LOG}")
    else:
        print("No suspicious RGB values found.")


if __name__ == "__main__":
    main()