from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Set, Tuple

from PIL import Image


MAX_WORKERS = max(4, os.cpu_count() or 4)
CHUNK_SIZE = 8 * 1024 * 1024

MC_METADATA_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def is_png(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".png"


def gather_recursive_pngs(root: Path) -> List[Path]:
    return [p for p in root.rglob("*") if is_png(p)]


def gather_root_pngs(root: Path) -> List[Path]:
    return [
        p for p in root.iterdir()
        if p.is_file() and p.suffix.lower() == ".png"
    ]


def hash_worker(path: Path) -> Tuple[Path, str]:
    return path, sha1_of_file(path)


def choose_root_keeper(paths: List[Path], root: Path) -> Path:
    root_paths = [p for p in paths if p.parent.resolve() == root.resolve()]
    if root_paths:
        return sorted(root_paths, key=lambda p: p.name.lower())[0]

    return sorted(paths, key=lambda p: (len(p.parts), str(p).lower()))[0]


def remove_empty_dirs(root: Path) -> None:
    dirs = sorted(
        [p for p in root.rglob("*") if p.is_dir()],
        key=lambda p: len(p.parts),
        reverse=True,
    )

    for directory in dirs:
        if directory.resolve() == root.resolve():
            continue

        try:
            next(directory.iterdir())
        except StopIteration:
            try:
                directory.rmdir()
            except OSError:
                pass


def make_unique_temp_path(root: Path, sha1: str) -> Path:
    return root / f"__tmp_{sha1}_{uuid.uuid4().hex}.png"


def load_mc_sha1s(csv_path: Path) -> Set[str]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Master Collection metadata CSV not found: {csv_path}")

    out: Set[str] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_column = "mc_tri_dumped_sha1"
        if not reader.fieldnames or required_column not in reader.fieldnames:
            raise ValueError(
                f"CSV missing required column '{required_column}': {csv_path}"
            )

        for row in reader:
            value = (row.get(required_column) or "").strip().lower()
            if value:
                out.add(value)

    return out


def all_alpha_zero_make_rgb(path: Path) -> bool:
    with Image.open(path) as img:
        img.load()

        if "A" not in img.getbands():
            return False

        rgba = img.convert("RGBA")
        alpha = rgba.getchannel("A")

        if alpha.getbbox() is not None:
            return False

        rgb = rgba.convert("RGB")
        rgb.save(
            path,
            format="PNG",
            optimize=False,
        )
        return True


def alpha_fix_worker(path: Path) -> Tuple[Path, bool]:
    changed = all_alpha_zero_make_rgb(path)
    return path, changed


def main() -> int:
    script_dir = Path(__file__).resolve().parent

    print(f"Loading Master Collection SHA1 list from: {MC_METADATA_CSV}")
    mc_sha1s = load_mc_sha1s(MC_METADATA_CSV)
    print(f"Loaded {len(mc_sha1s)} unique Master Collection SHA1 values.")

    print(f"Scanning for PNG files under: {script_dir}")

    root_pngs = gather_root_pngs(script_dir)
    recursive_pngs = gather_recursive_pngs(script_dir)
    subfolder_pngs = [p for p in recursive_pngs if p.parent.resolve() != script_dir.resolve()]

    all_pngs = root_pngs + subfolder_pngs

    if not all_pngs:
        print("No PNG files found.")
        return 0

    print(f"Found {len(all_pngs)} PNG files total.")
    print(f"  Root PNGs: {len(root_pngs)}")
    print(f"  Subfolder PNGs: {len(subfolder_pngs)}")
    print(f"Hashing with {MAX_WORKERS} workers...")

    sha1_to_paths: Dict[str, List[Path]] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(hash_worker, path) for path in all_pngs]

        completed = 0
        total = len(futures)

        for future in as_completed(futures):
            path, sha1 = future.result()
            sha1_to_paths.setdefault(sha1.lower(), []).append(path)

            completed += 1
            if completed % 250 == 0 or completed == total:
                print(f"Hashed {completed}/{total}")

    print(f"Resolved to {len(sha1_to_paths)} unique SHA1 values.")

    temp_copies: List[Tuple[Path, Path]] = []
    paths_to_delete: List[Path] = []
    deleted_as_mc_matches = 0
    skipped_unique_sha1s = 0

    for sha1, paths in sorted(sha1_to_paths.items()):
        if sha1 in mc_sha1s:
            skipped_unique_sha1s += 1
            deleted_as_mc_matches += len(paths)
            paths_to_delete.extend(paths)
            continue

        keeper = choose_root_keeper(paths, script_dir)

        if keeper.parent.resolve() != script_dir.resolve():
            target_path = script_dir / f"{sha1}.png"
            temp_path = make_unique_temp_path(script_dir, sha1)
            shutil.copy2(keeper, temp_path)
            temp_copies.append((temp_path, target_path))

        for path in paths:
            if path.resolve() == keeper.resolve():
                continue

            paths_to_delete.append(path)

        if keeper.parent.resolve() != script_dir.resolve():
            paths_to_delete.append(keeper)

    print("Deleting Master Collection matches, duplicates, and superseded source files...")

    deleted_count = 0
    for path in paths_to_delete:
        try:
            if path.exists():
                path.unlink()
                deleted_count += 1
        except OSError as e:
            print(f"Failed to delete: {path}")
            print(f"  {e}")

    print("Finalizing pulled-in SHA1-named PNG files...")

    finalized_count = 0
    for temp_path, target_path in temp_copies:
        try:
            temp_path.replace(target_path)
            finalized_count += 1
        except OSError as e:
            print(f"Failed to finalize: {target_path}")
            print(f"  {e}")

    remove_empty_dirs(script_dir)

    remaining_root_pngs = sorted(
        gather_root_pngs(script_dir),
        key=lambda p: p.name.lower(),
    )

    print()
    print("Root file naming note:")
    print("  Existing root-level PNGs were not renamed.")
    print("  Only pulled-in subfolder PNGs were renamed to their SHA1.")
    print("  Any PNG whose SHA1 matched Master Collection metadata was deleted first.")

    mismatched_root_names = []
    for path in remaining_root_pngs:
        sha1 = sha1_of_file(path)
        expected_name = f"{sha1}.png"
        if path.name.lower() != expected_name.lower():
            mismatched_root_names.append((path.name, expected_name))

    if mismatched_root_names:
        print()
        print("Existing root PNGs whose names are not their SHA1:")
        for current_name, expected_name in mismatched_root_names:
            print(f"  {current_name} -> {expected_name}")

    print()
    print(f"Starting transparent-alpha RGB conversion pass on {len(remaining_root_pngs)} root PNG files...")

    changed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(alpha_fix_worker, path) for path in remaining_root_pngs]

        completed = 0
        total = len(futures)

        for future in as_completed(futures):
            path, changed = future.result()
            completed += 1

            if changed:
                changed_count += 1
                print(f"Converted all-transparent alpha PNG to RGB: {path.name}")

            if completed % 250 == 0 or completed == total:
                print(f"Alpha pass {completed}/{total}")

    print()
    print("Done.")
    print(f"Unique SHA1 groups found: {len(sha1_to_paths)}")
    print(f"Unique SHA1 groups deleted due to Master Collection match: {skipped_unique_sha1s}")
    print(f"Files deleted due to Master Collection match: {deleted_as_mc_matches}")
    print(f"Total files deleted: {deleted_count}")
    print(f"Files pulled into root as SHA1 names: {finalized_count}")
    print(f"RGB conversions performed: {changed_count}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(1)