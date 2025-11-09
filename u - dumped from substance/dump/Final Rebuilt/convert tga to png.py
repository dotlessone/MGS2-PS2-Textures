import os
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_WORKERS = max(4, os.cpu_count() or 4)
OUTPUT_DIR_NAME = "converted"
ERROR_LOG = "conversion_errors.txt"
LOCK = Lock()

# ==========================================================
# HELPERS
# ==========================================================
def clamp_alpha_opaque(img: Image.Image) -> Image.Image:
    """Clamp all alpha values to a max of 128."""
    alpha = img.getchannel("A")
    alpha = alpha.point(lambda a: min(a, 128))
    if img.mode == "RGBA":
        r, g, b, _ = img.split()
        return Image.merge("RGBA", (r, g, b, alpha))
    elif img.mode == "LA":
        l, _ = img.split()
        return Image.merge("LA", (l, alpha))
    return img

def halve_alpha_with_check(img: Image.Image, tga_path: Path) -> Image.Image:
    """
    Adjust alpha levels for non-opaque textures:
      - If max alpha == 255: treat as fully opaque -> clamp all to 128.
      - If any alpha >128 (but not full 255): verify all nonzero >128 values are even.
        - If valid, halve all those values.
        - If invalid, raise ValueError listing bad levels.
      - Otherwise, no change.
    """
    alpha = img.getchannel("A")
    alpha_data = list(alpha.getdata())
    max_alpha = max(alpha_data)

    # Case 1: fully opaque (max alpha == 255)
    if max_alpha == 255:
        alpha = alpha.point(lambda a: min(a, 128))
        if img.mode == "RGBA":
            r, g, b, _ = img.split()
            return Image.merge("RGBA", (r, g, b, alpha))
        elif img.mode == "LA":
            l, _ = img.split()
            return Image.merge("LA", (l, alpha))
        return img

    # Case 2: partial >128 alphas
    if max_alpha > 128:
        invalid_values = sorted({a for a in alpha_data if a > 128 and a % 2 != 0 and a != 0})
        if invalid_values:
            counts = {}
            for a in alpha_data:
                if a in invalid_values:
                    counts[a] = counts.get(a, 0) + 1
            invalid_detail = ", ".join(f"{k}: {v}px" for k, v in sorted(counts.items()))
            raise ValueError(
                f"Alpha channel in '{tga_path}' has invalid non-even values >128 "
                f"({len(invalid_values)} unique): {invalid_detail}"
            )

        # Halve all >128 alphas
        alpha = alpha.point(lambda a: a // 2 if a > 128 else a)
        if img.mode == "RGBA":
            r, g, b, _ = img.split()
            return Image.merge("RGBA", (r, g, b, alpha))
        elif img.mode == "LA":
            l, _ = img.split()
            return Image.merge("LA", (l, alpha))
        return img

    # Case 3: nothing to do
    return img

def log_error(message: str, error_log_path: Path):
    with LOCK:
        with open(error_log_path, "a", encoding="utf-8") as logf:
            logf.write(message + "\n")

def convert_tga_to_png(tga_path: Path, output_dir: Path, error_log_path: Path):
    try:
        rel_name = tga_path.stem
        output_path = output_dir / f"{rel_name}.png"

        # Skip if already exists
        if output_path.exists():
            with LOCK:
                print(f"[SKIP] {rel_name}.png already exists")
            return

        # Special exceptions: copy directly without modification
        special_exceptions = {
            "crd_pp05.bmp_276e6284cf2e0055cf730766f998bbac.tga",
            "w04_demo_scr_haen_right_alp.bmp.tga",
        }

        if tga_path.name in special_exceptions:
            with Image.open(tga_path) as img:
                img.save(output_path, format="PNG", optimize=False)
            with LOCK:
                print(f"[SPECIAL] Copied {tga_path.name} directly to {output_path.name}")
            return

        # ----------------------------------------------------------
        # Normal logic begins here
        # ----------------------------------------------------------
        with Image.open(tga_path) as img:
            if img.mode not in ("RGBA", "RGB", "LA", "L"):
                img = img.convert("RGBA")

            has_alpha = img.mode in ("RGBA", "LA")

            if has_alpha:
                path_parts = [p.lower() for p in tga_path.parts]
                if "opaque" in path_parts:
                    img = clamp_alpha_opaque(img)
                    action = "clamped to 128"
                else:
                    alpha_channel = img.getchannel("A")
                    max_a = max(alpha_channel.getdata())
                    if max_a == 255:
                        # Fully opaque → clamp to 128
                        img = clamp_alpha_opaque(img)
                        action = "clamped to 128 (max alpha 255)"
                    elif max_a > 128:
                        img = halve_alpha_with_check(img, tga_path)
                        action = "halved alpha"
                    else:
                        action = "no change"
            else:
                action = "no alpha"

            img.save(output_path, format="PNG", optimize=False)

        with LOCK:
            print(f"[OK] {rel_name}.tga -> {output_path.name} ({action})")

    except Exception as e:
        msg = f"[FAIL] {tga_path}: {e}"
        log_error(msg, error_log_path)
        with LOCK:
            print(msg)


# ==========================================================
# MAIN
# ==========================================================
def main():
    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir / OUTPUT_DIR_NAME
    output_dir.mkdir(exist_ok=True)

    error_log_path = script_dir / ERROR_LOG
    if error_log_path.exists():
        error_log_path.unlink()  # clear old log

    tga_files = list(script_dir.rglob("*.tga"))
    if not tga_files:
        print("No .tga files found.")
        return

    print(f"Found {len(tga_files)} .tga files. Converting with {MAX_WORKERS} threads...")
    print(f"Output folder: {output_dir}")
    print(f"Error log: {error_log_path}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(convert_tga_to_png, path, output_dir, error_log_path) for path in tga_files]
        for _ in as_completed(futures):
            pass

    print("\nAll conversions complete.")
    print(f"Any errors have been logged to: {error_log_path}")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
