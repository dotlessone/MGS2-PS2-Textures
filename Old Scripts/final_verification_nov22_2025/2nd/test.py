from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import DefaultDict, Dict, List, Tuple

from PIL import Image


# ==========================================================
# CONFIG
# ==========================================================
CSV_PATH = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

MAX_WORKERS = max(4, os.cpu_count() or 4)
TEMP_DIR_NAME = "_tmp_pil_sha1"
PNG_SUFFIX = ".png"

MATCHED_LOG_NAME = "renamed_and_moved.csv"
UNMATCHED_LOG_NAME = "unmatched_pngs.csv"


# ==========================================================
# HELPERS
# ==========================================================
def pause_and_exit(code: int = 1) -> None:
    try:
        input("\nPress ENTER to exit...")
    finally:
        raise SystemExit(code)


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_png_filename(texture_name: str) -> str:
    name = texture_name.strip()
    if not name:
        raise ValueError("Encountered empty texture_name in CSV.")
    if name.lower().endswith(PNG_SUFFIX):
        return name
    return f"{name}{PNG_SUFFIX}"


def load_sha1_to_texture_names(csv_path: Path) -> Dict[str, List[str]]:
    sha1_to_names: DefaultDict[str, List[str]] = defaultdict(list)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {"texture_name", "mc_tri_dumped_sha1"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(
                f"CSV is missing required columns: {', '.join(sorted(missing))}"
            )

        for row in reader:
            texture_name = (row.get("texture_name") or "").strip()
            sha1_value = (row.get("mc_tri_dumped_sha1") or "").strip().lower()

            if not texture_name or not sha1_value:
                continue

            sha1_to_names[sha1_value].append(texture_name)

    return dict(sha1_to_names)


def build_temp_normalized_copy(source_path: Path, temp_dir: Path, index: int) -> Path:
    temp_path = temp_dir / f"{source_path.stem}__{index:08d}.png"

    with Image.open(source_path) as img:
        img.load()

        if img.mode not in ("RGB", "RGBA", "L", "LA", "P"):
            img = img.convert("RGBA")

        img.save(
            temp_path,
            format="PNG",
            optimize=False,
        )

    return temp_path


def normalized_sha1_worker(
    source_path: Path,
    temp_dir: Path,
    index: int,
    print_lock: Lock,
) -> Tuple[Path, str]:
    temp_path = build_temp_normalized_copy(source_path, temp_dir, index)

    try:
        normalized_sha1 = sha1_of_file(temp_path)
        return source_path, normalized_sha1
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        except Exception as exc:
            with print_lock:
                print(f"Warning: Failed to delete temp file '{temp_path}': {exc}")


def verify_existing_destination(dest_path: Path, source_path: Path) -> None:
    if not dest_path.exists():
        return

    src_hash = sha1_of_file(source_path)
    dst_hash = sha1_of_file(dest_path)

    if src_hash == dst_hash:
        return

    raise RuntimeError(
        f"Destination already exists with different content:\n"
        f"  Source: {source_path}\n"
        f"  Dest:   {dest_path}"
    )


def allocate_targets(
    source_hashes: Dict[Path, str],
    sha1_to_texture_names: Dict[str, List[str]],
) -> Tuple[Dict[Path, List[str]], List[str], List[Tuple[Path, str]]]:
    matched_sources_by_sha1: DefaultDict[str, List[Path]] = defaultdict(list)

    for source_path, sha1_value in source_hashes.items():
        if sha1_value in sha1_to_texture_names:
            matched_sources_by_sha1[sha1_value].append(source_path)

    assignments: Dict[Path, List[str]] = {}
    warnings: List[str] = []
    unmatched_sources: List[Tuple[Path, str]] = []

    for sha1_value, source_paths in sorted(matched_sources_by_sha1.items()):
        texture_names = list(sha1_to_texture_names.get(sha1_value, []))

        if not texture_names:
            continue

        source_paths.sort(key=lambda p: p.name.lower())
        texture_names.sort(key=str.lower)

        source_count = len(source_paths)
        target_count = len(texture_names)

        if source_count >= target_count:
            for i, texture_name in enumerate(texture_names):
                assignments.setdefault(source_paths[i], []).append(texture_name)

            extra_sources = source_paths[target_count:]
            if extra_sources:
                warnings.append(
                    f"SHA1 {sha1_value} matched {source_count} source file(s) but only "
                    f"{target_count} CSV target(s). Extra source file(s) will be left untouched: "
                    f"{', '.join(p.name for p in extra_sources)}"
                )
            continue

        for i, texture_name in enumerate(texture_names):
            source_path = source_paths[i % source_count]
            assignments.setdefault(source_path, []).append(texture_name)

        warnings.append(
            f"SHA1 {sha1_value} matched {source_count} source file(s) but needs "
            f"{target_count} output file(s). Copies will be created as needed."
        )

    for source_path, sha1_value in sorted(source_hashes.items(), key=lambda item: item[0].name.lower()):
        if sha1_value not in sha1_to_texture_names:
            unmatched_sources.append((source_path, sha1_value))

    return assignments, warnings, unmatched_sources


def write_matched_log(
    log_path: Path,
    matched_rows: List[Tuple[str, str, str, str]],
) -> None:
    with log_path.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "original_filename",
                "normalized_sha1",
                "new_filename",
                "destination_path",
            ]
        )
        writer.writerows(matched_rows)


def write_unmatched_log(
    log_path: Path,
    unmatched_rows: List[Tuple[str, str]],
) -> None:
    with log_path.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["filename", "normalized_sha1"])
        writer.writerows(unmatched_rows)


# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    script_dir = Path(__file__).resolve().parent
    destination_dir = script_dir.parent
    temp_dir = script_dir / TEMP_DIR_NAME
    matched_log_path = script_dir / MATCHED_LOG_NAME
    unmatched_log_path = script_dir / UNMATCHED_LOG_NAME
    print_lock = Lock()

    if not CSV_PATH.is_file():
        print(f"Error: CSV not found:\n{CSV_PATH}")
        pause_and_exit()

    png_files = sorted(
        [
            path
            for path in script_dir.iterdir()
            if path.is_file() and path.suffix.lower() == PNG_SUFFIX
        ],
        key=lambda p: p.name.lower(),
    )

    if not png_files:
        print("No PNG files found in the same folder as the script.")
        pause_and_exit(0)

    try:
        sha1_to_texture_names = load_sha1_to_texture_names(CSV_PATH)
    except Exception as exc:
        print(f"Error loading CSV: {exc}")
        pause_and_exit()

    if not sha1_to_texture_names:
        print("Error: No usable SHA1 mappings were loaded from the CSV.")
        pause_and_exit()

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    source_hashes: Dict[Path, str] = {}

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(
                    normalized_sha1_worker,
                    source_path,
                    temp_dir,
                    index,
                    print_lock,
                ): source_path
                for index, source_path in enumerate(png_files)
            }

            for future in as_completed(future_map):
                source_path = future_map[future]
                try:
                    returned_path, normalized_sha1 = future.result()
                    source_hashes[returned_path] = normalized_sha1
                except Exception as exc:
                    print(f"Error processing '{source_path}': {exc}")
                    pause_and_exit()

        assignments, warnings, unmatched_sources = allocate_targets(
            source_hashes,
            sha1_to_texture_names,
        )

        if not assignments and not unmatched_sources:
            print("No usable PNGs were processed.")
            pause_and_exit()

        planned_destinations: DefaultDict[Path, List[Tuple[Path, str]]] = defaultdict(list)

        for source_path, texture_names in assignments.items():
            for texture_name in texture_names:
                dest_filename = ensure_png_filename(texture_name)
                dest_path = destination_dir / dest_filename
                planned_destinations[dest_path].append((source_path, texture_name))

        collision_errors: List[str] = []

        for dest_path, items in sorted(planned_destinations.items(), key=lambda item: item[0].name.lower()):
            unique_sources = {source_path.resolve() for source_path, _ in items}
            if len(unique_sources) > 1:
                collision_errors.append(
                    f"Multiple different source files would write to the same destination: "
                    f"{dest_path.name}"
                )

        if collision_errors:
            print("Destination collision errors detected:")
            for error in collision_errors:
                print(f"  {error}")
            pause_and_exit()

        matched_rows: List[Tuple[str, str, str, str]] = []
        unmatched_rows: List[Tuple[str, str]] = []

        moved_count = 0
        copied_count = 0

        for source_path in sorted(assignments.keys(), key=lambda p: p.name.lower()):
            texture_names = assignments[source_path]
            if not texture_names:
                continue

            normalized_sha1 = source_hashes[source_path]
            first_dest_path: Path | None = None

            for index, texture_name in enumerate(texture_names):
                dest_filename = ensure_png_filename(texture_name)
                dest_path = destination_dir / dest_filename

                verify_existing_destination(dest_path, source_path)

                if index == 0:
                    shutil.move(str(source_path), str(dest_path))
                    first_dest_path = dest_path
                    moved_count += 1
                else:
                    if first_dest_path is None:
                        raise RuntimeError(
                            f"Internal error: first destination missing for {source_path}"
                        )
                    shutil.copy2(str(first_dest_path), str(dest_path))
                    copied_count += 1

                matched_rows.append(
                    (
                        source_path.name,
                        normalized_sha1,
                        dest_filename,
                        str(dest_path),
                    )
                )

        for source_path, normalized_sha1 in unmatched_sources:
            unmatched_rows.append((source_path.name, normalized_sha1))

        write_matched_log(matched_log_path, matched_rows)
        write_unmatched_log(unmatched_log_path, unmatched_rows)

        if warnings:
            print("Warnings:")
            for warning in warnings:
                print(f"  {warning}")
            print()

        print("Done.")
        print(f"Moved originals: {moved_count}")
        print(f"Created copies:  {copied_count}")
        print(f"Matched log:     {matched_log_path}")
        print(f"Unmatched log:   {unmatched_log_path}")
        print(f"Destination:     {destination_dir}")

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()