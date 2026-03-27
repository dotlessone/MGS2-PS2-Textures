from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

CSV_PATH = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

TARGET_COLUMN = "mc_tri_dumped_sha1"
TEXTURE_NAME_COLUMN = "texture_name"


def is_comment_or_empty(line: str) -> bool:
    stripped = line.strip()
    return not stripped or stripped.startswith(";") or stripped.startswith("//")


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        # Filter comments before feeding into DictReader
        filtered_lines = (line for line in f if not is_comment_or_empty(line))
        reader = csv.DictReader(filtered_lines)

        if TARGET_COLUMN not in reader.fieldnames:
            raise RuntimeError(f"Missing required column: {TARGET_COLUMN}")

        if TEXTURE_NAME_COLUMN not in reader.fieldnames:
            raise RuntimeError(f"Missing required column: {TEXTURE_NAME_COLUMN}")

        for row in reader:
            rows.append(row)

    return rows


def check_uniqueness(rows: list[dict[str, str]]) -> None:
    sha1_map: dict[str, list[str]] = defaultdict(list)

    for row in rows:
        sha1 = row[TARGET_COLUMN].strip().lower()
        texture_name = row[TEXTURE_NAME_COLUMN].strip().lower()

        if not sha1:
            continue

        sha1_map[sha1].append(texture_name)

    duplicates = {k: v for k, v in sha1_map.items() if len(v) > 1}

    if not duplicates:
        print("All mc_tri_dumped_sha1 values are unique.")
        return

    print(f"Found {len(duplicates)} duplicate SHA1 values:\n")

    for sha1, names in sorted(duplicates.items()):
        print(f"{sha1} ({len(names)} entries)")
        for name in sorted(names):
            print(f"  {name}")
        print()

    sys.exit(1)


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    rows = load_rows(CSV_PATH)
    check_uniqueness(rows)


if __name__ == "__main__":
    main()