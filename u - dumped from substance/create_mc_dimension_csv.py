import os
import csv
from PIL import Image

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_csv = os.path.join(script_dir, "png_dimensions.csv")

    png_files = [f for f in os.listdir(script_dir) if f.lower().endswith(".png")]
    if not png_files:
        print("No .png files found in this directory.")
        return

    rows = []
    for f in png_files:
        path = os.path.join(script_dir, f)
        try:
            with Image.open(path) as img:
                w, h = img.size
                rows.append((f, w, h))
        except Exception as e:
            print(f"Error reading {f}: {e}")

    # Write results
    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["filename", "width", "height"])
        writer.writerows(rows)

    print(f"Processed {len(rows)} PNG files.")
    print(f"Results written to: {output_csv}")

if __name__ == "__main__":
    main()
