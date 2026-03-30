from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Dict


SCRIPT_DIR = Path(__file__).resolve().parent

CSV_FILES = [
    SCRIPT_DIR / "mgs2_mc_dimensions.csv",
    SCRIPT_DIR / "mgs2_mc_dimensions_including_override_folders.csv",
]

LOG_FILE = SCRIPT_DIR / "region_specific_japan_only_updates.txt"


def normalize_relative_path(value: str) -> str:
    normalized = (value or "").replace("\\", "/").strip()

    while "//" in normalized:
        normalized = normalized.replace("//", "/")

    normalized = normalized.strip("/")

    return normalized


def resolve_ctxr_path(texture_name: str, relative_path: str) -> Path:
    rel_dir = normalize_relative_path(relative_path)

    if rel_dir:
        return SCRIPT_DIR / rel_dir / f"{texture_name}.ctxr"

    return SCRIPT_DIR / f"{texture_name}.ctxr"


def load_csv_rows(csv_path: Path) -> tuple[List[str], List[Dict[str, str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]

    if not fieldnames:
        raise ValueError(f"CSV has no header: {csv_path}")

    required = {"texture_name", "relative_path", "region_specific"}
    missing = required.difference(fieldnames)
    if missing:
        raise ValueError(
            f"CSV '{csv_path}' is missing required columns: {sorted(missing)}"
        )

    return fieldnames, rows


def write_csv_rows(csv_path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def process_csv(csv_path: Path) -> List[str]:
    if not csv_path.is_file():
        print(f"Skipping missing CSV: {csv_path}")
        return []

    fieldnames, rows = load_csv_rows(csv_path)
    updated_entries: List[str] = []

    for row in rows:
        texture_name = (row.get("texture_name") or "").strip()
        relative_path = row.get("relative_path") or ""

        if not texture_name:
            continue

        ctxr_path = resolve_ctxr_path(texture_name, relative_path)

        if not ctxr_path.is_file():
            row["region_specific"] = "japan_only"
            updated_entries.append(
                f"{csv_path.name},{texture_name},{normalize_relative_path(relative_path)},{ctxr_path}"
            )

    write_csv_rows(csv_path, fieldnames, rows)
    return updated_entries


def main() -> int:
    all_updates: List[str] = []

    for csv_path in CSV_FILES:
        updates = process_csv(csv_path)
        all_updates.extend(updates)
        print(f"{csv_path.name}: set japan_only on {len(updates)} row(s)")

    if all_updates:
        with LOG_FILE.open("w", encoding="utf-8", newline="\n") as f:
            for line in all_updates:
                f.write(line + "\n")
        print(f"Wrote log: {LOG_FILE}")
    else:
        if LOG_FILE.exists():
            LOG_FILE.unlink()
        print("No updates needed.")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())