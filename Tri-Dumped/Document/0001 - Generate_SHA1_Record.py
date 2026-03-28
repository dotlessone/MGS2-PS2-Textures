from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


MAX_WORKERS = max(4, os.cpu_count() or 4)
CHUNK_SIZE = 8 * 1024 * 1024

# ==========================================================
# CONFIG
# ==========================================================
VALID_PARENTS = {
    "Substance",
    "Document",
    "Trial",
	"Sons of Liberty",
}


def pause_and_exit(code: int = 0) -> None:
    try:
        input("Press ENTER to exit...")
    except EOFError:
        pass
    raise SystemExit(code)


def sha1_of_file(path: Path) -> str:
    sha1 = hashlib.sha1()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            sha1.update(chunk)

    return sha1.hexdigest()


def validate_location(script_dir: Path) -> str:
    parent = script_dir.name

    if parent not in VALID_PARENTS:
        print("ERROR: Script is in an unexpected location.")
        print(f"  Detected folder: '{parent}'")
        print(f"  Expected one of: {sorted(VALID_PARENTS)}")
        pause_and_exit(1)

    return parent


def build_output_name(parent: str, subfolder: str) -> str:
    return f"{parent}_{subfolder}_ALL_SHA1s.txt"


def find_png_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".png"
    )


def hash_pngs(png_files: list[Path]) -> tuple[set[str], list[tuple[Path, str]]]:
    unique_sha1s: set[str] = set()
    failures: list[tuple[Path, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {
            executor.submit(sha1_of_file, path): path
            for path in png_files
        }

        completed = 0
        total = len(future_to_path)

        for future in as_completed(future_to_path):
            path = future_to_path[future]
            completed += 1

            try:
                unique_sha1s.add(future.result())
            except Exception as exc:
                failures.append((path, str(exc)))

            if completed % 250 == 0 or completed == total:
                print(f"    Processed {completed}/{total}")

    return unique_sha1s, failures


def write_sha1_file(output_path: Path, sha1s: set[str]) -> None:
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for sha1 in sorted(sha1s):
            handle.write(f"{sha1}\n")


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    parent = validate_location(script_dir)

    subfolders = sorted(path for path in script_dir.iterdir() if path.is_dir())

    if not subfolders:
        print("No subfolders found.")
        pause_and_exit(0)

    overall_failures: list[tuple[Path, str]] = []
    created_count = 0
    skipped_count = 0

    print(f"Found {len(subfolders)} subfolder(s) under '{parent}'.")
    print(f"Hashing with {MAX_WORKERS} workers...")
    print()

    for subfolder in subfolders:
        output_path = subfolder / build_output_name(parent, subfolder.name)

        if output_path.exists():
            print(f"Skipping '{subfolder.name}' - output already exists: {output_path.name}")
            skipped_count += 1
            continue

        png_files = find_png_files(subfolder)

        if not png_files:
            print(f"Skipping '{subfolder.name}' - no PNG files found.")
            skipped_count += 1
            continue

        print(f"Processing '{subfolder.name}'")
        print(f"  Found {len(png_files)} PNG file(s).")

        unique_sha1s, failures = hash_pngs(png_files)
        write_sha1_file(output_path, unique_sha1s)

        print(f"  Unique SHA1 count: {len(unique_sha1s)}")
        print(f"  Output written to: {output_path}")
        print()

        overall_failures.extend(failures)
        created_count += 1

    print("Done.")
    print(f"Created: {created_count}")
    print(f"Skipped: {skipped_count}")

    if overall_failures:
        print()
        print(f"Failed to hash {len(overall_failures)} file(s):")
        for path, error in overall_failures:
            print(f"  {path} -> {error}")
        pause_and_exit(1)

    pause_and_exit(0)


if __name__ == "__main__":
    main()