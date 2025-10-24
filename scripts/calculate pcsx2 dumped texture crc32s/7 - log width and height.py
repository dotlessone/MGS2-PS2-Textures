import os
import csv
from PIL import Image

# ==========================================================
# MAIN
# ==========================================================
def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(folder, "4 - pcsx2_dumped_dimensions.csv")

    png_files = [f for f in os.listdir(folder) if f.lower().endswith(".png")]
    if not png_files:
        print("[!] No PNG files found in this folder.")
        return

    with open(log_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["filename", "width", "height"])

        for file in png_files:
            full_path = os.path.join(folder, file)
            try:
                with Image.open(full_path) as img:
                    width, height = img.size
                    name_without_ext = os.path.splitext(file)[0]
                    writer.writerow([name_without_ext, width, height])
            except Exception as e:
                print(f"[!] Failed to read {file}: {e}")

    print(f"[+] Logged {len(png_files)} PNGs to: {log_path}")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
