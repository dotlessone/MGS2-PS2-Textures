import os
import shutil
from PIL import Image

def is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0

def main():
    # Current working directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    pot_dir = os.path.join(current_dir, "power of 2")

    # Make the "power of 2" folder if it doesn't exist
    os.makedirs(pot_dir, exist_ok=True)

    # Iterate over all TGA files in this directory (no subfolders)
    for filename in os.listdir(current_dir):
        if filename.lower().endswith(".tga"):
            filepath = os.path.join(current_dir, filename)

            try:
                with Image.open(filepath) as img:
                    width, height = img.size

                if is_power_of_two(width) and is_power_of_two(height):
                    dest_path = os.path.join(pot_dir, filename)
                    print(f"Moving {filename} ({width}x{height}) → {dest_path}")
                    shutil.move(filepath, dest_path)
                else:
                    print(f"Skipping {filename} ({width}x{height}) – not power of 2")

            except Exception as e:
                print(f"Error reading {filename}: {e}")

if __name__ == "__main__":
    main()
