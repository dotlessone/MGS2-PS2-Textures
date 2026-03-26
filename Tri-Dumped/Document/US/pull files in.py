from __future__ import annotations

import csv
import hashlib
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple


MAX_WORKERS = max(4, os.cpu_count() or 4)
HASH_BUFFER_SIZE = 8 * 1024 * 1024

LOG_NAME = "png_sha1_move_log.csv"
PNG_EXTENSION = ".png"


def pause_and_exit(code: int = 0) -> None:
    try:
        input("\nPress ENTER to exit...")
    except EOFError:
        pass
    raise SystemExit(code)


def get_script_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    return Path(__file__).resolve().parent


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(HASH_BUFFER_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def find_pngs(root: Path) -> List[Path]:
    out: List[Path] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        if path.suffix.lower() != PNG_EXTENSION:
            continue

        out.append(path)

    return out


def hash_worker(path: Path) -> Tuple[Path, str]:
    return (path, sha1_of_file(path))


def same_file(a: Path, b: Path) -> bool:
    try:
        return a.resolve().samefile(b.resolve())
    except FileNotFoundError:
        return False
    except OSError:
        return a.resolve() == b.resolve()


def main() -> None:
    root = get_script_dir()
    log_path = root / LOG_NAME

    png_files = find_pngs(root)

    if not png_files:
        print(f"No PNG files found under: {root}")
        pause_and_exit(0)

    print(f"Root: {root}")
    print(f"Found {len(png_files)} PNG files.")
    print(f"Using {MAX_WORKERS} worker threads for hashing.\n")

    hashed_results: List[Tuple[Path, str]] = []
    hash_errors: List[Tuple[Path, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(hash_worker, path): path for path in png_files}

        for future in as_completed(futures):
            src_path = futures[future]

            try:
                path, sha1 = future.result()
                hashed_results.append((path, sha1))
                print(f"HASHED    {path}")
            except Exception as exc:
                hash_errors.append((src_path, str(exc)))
                print(f"ERROR     {src_path}")
                print(f"          {exc}")

    if hash_errors:
        print("\nHashing failed for one or more files. Nothing was moved.")
        print(f"Hash errors: {len(hash_errors)}")
        pause_and_exit(1)

    hashed_results.sort(key=lambda item: (item[1], str(item[0]).lower()))

    moved = 0
    deleted_duplicates = 0
    unchanged = 0
    move_errors = 0

    log_rows: List[Tuple[str, str, str, str]] = []

    for src_path, sha1 in hashed_results:
        original_path_str = str(src_path.resolve())
        original_name = src_path.name
        dest_path = root / f"{sha1}{PNG_EXTENSION}"

        log_rows.append((original_path_str, sha1, original_name, str(dest_path.resolve())))

        try:
            if same_file(src_path, dest_path):
                if src_path.name.lower() == f"{sha1}{PNG_EXTENSION}":
                    unchanged += 1
                    print(f"UNCHANGED {src_path}")
                    continue

                if dest_path.exists():
                    raise RuntimeError(
                        f"Destination already exists unexpectedly: {dest_path}"
                    )

                shutil.move(str(src_path), str(dest_path))
                moved += 1
                print(f"RENAMED   {src_path} -> {dest_path}")
                continue

            if dest_path.exists():
                dest_sha1 = sha1_of_file(dest_path)

                if dest_sha1 != sha1:
                    raise RuntimeError(
                        f"Destination exists with different content: {dest_path}"
                    )

                src_path.unlink()
                deleted_duplicates += 1
                print(f"DELETED   {src_path} (duplicate of {dest_path})")
                continue

            shutil.move(str(src_path), str(dest_path))
            moved += 1
            print(f"MOVED     {src_path} -> {dest_path}")
        except Exception as exc:
            move_errors += 1
            print(f"ERROR     {src_path}")
            print(f"          {exc}")

    with log_path.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["original_path", "sha1", "original_filename", "destination_path"])
        writer.writerows(log_rows)

    print("\nDone.")
    print(f"Moved/Renamed:      {moved}")
    print(f"Deleted duplicates: {deleted_duplicates}")
    print(f"Already correct:    {unchanged}")
    print(f"Move errors:        {move_errors}")
    print(f"Log written to:     {log_path}")

    pause_and_exit(0 if move_errors == 0 else 1)


if __name__ == "__main__":
    main()