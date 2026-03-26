from __future__ import annotations

import csv
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Iterable


# ==========================================================
# CONFIG
# ==========================================================
CSV_NAME = "mgs2_tri_mappings.csv"  # put your csv name here

# Columns that form the "combination"
KEY_COLUMNS = [
    "region_folder",
    "stage",
    "tri_strcode",
]

# Column that must resolve to ONE unique value
TARGET_COLUMN = "tri_name"

# Output logs
CONFLICT_LOG = "mgs2_tri_mappings_conflicts.log"
UNIQUE_LOG = "mgs2_tri_mappings_unique.log"

# If you don't care about unique mappings, set this to False
WRITE_UNIQUE = True


# ==========================================================
# HELPERS
# ==========================================================
def filtered_lines(path: Path) -> Iterable[str]:
    """
    Yield only valid CSV lines:
    - skip empty lines
    - skip lines starting with ';' or '//'
    """
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()

            if not stripped:
                continue

            if stripped.startswith(";") or stripped.startswith("//"):
                continue

            yield line


# ==========================================================
# CORE
# ==========================================================
def main() -> None:
    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / CSV_NAME

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    mapping: Dict[Tuple[str, ...], Set[str]] = defaultdict(set)

    reader = csv.DictReader(filtered_lines(csv_path))

    # Validate columns exist
    missing = [c for c in KEY_COLUMNS + [TARGET_COLUMN] if c not in reader.fieldnames]
    if missing:
        raise RuntimeError(f"Missing columns in CSV: {missing}")

    for row in reader:
        key = tuple((row[c] or "").strip() for c in KEY_COLUMNS)
        value = (row[TARGET_COLUMN] or "").strip()

        mapping[key].add(value)

    # ======================================================
    # ANALYZE
    # ======================================================
    conflicts: List[Tuple[Tuple[str, ...], Set[str]]] = []
    uniques: List[Tuple[Tuple[str, ...], str]] = []

    for key, values in mapping.items():
        if len(values) > 1:
            conflicts.append((key, values))
        else:
            uniques.append((key, next(iter(values))))

    conflicts.sort(key=lambda x: x[0])
    uniques.sort(key=lambda x: x[0])

    # ======================================================
    # WRITE OUTPUT
    # ======================================================
    conflict_path = script_dir / CONFLICT_LOG
    with open(conflict_path, "w", encoding="utf-8", newline="\n") as f:
        for key, values in conflicts:
            key_str = ",".join(key)
            values_str = "|".join(sorted(values))
            f.write(f"{key_str} -> {values_str}\n")

    print(f"Conflicts written: {len(conflicts)} -> {conflict_path}")

    if WRITE_UNIQUE:
        unique_path = script_dir / UNIQUE_LOG
        with open(unique_path, "w", encoding="utf-8", newline="\n") as f:
            for key, value in uniques:
                key_str = ",".join(key)
                f.write(f"{key_str} -> {value}\n")

        print(f"Unique mappings written: {len(uniques)} -> {unique_path}")


# ==========================================================
# ENTRY
# ==========================================================
if __name__ == "__main__":
    main()