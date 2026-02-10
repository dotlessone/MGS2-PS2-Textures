from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple, Set


MC_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_mc_dimensions.csv"
)
PS2_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_ps2_dimensions.csv"
)

OUTPUT_TXT = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mc_vs_ps2_dimension_mismatch.txt"
)


def ceil_pow2(value: int) -> int:
    if value <= 0:
        raise ValueError(f"Invalid dimension: {value}")
    return 1 << (value - 1).bit_length()


def load_mc(csv_path: Path) -> Dict[str, Tuple[int, int]]:
    out: Dict[str, Tuple[int, int]] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["texture_name"].strip()
            out[name] = (
                int(row["mc_width"]),
                int(row["mc_height"]),
            )

    return out


def load_ps2(csv_path: Path) -> Dict[str, Tuple[int, int]]:
    out: Dict[str, Tuple[int, int]] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["texture_name"].strip()
            out[name] = (
                int(row["tri_dumped_width"]),
                int(row["tri_dumped_height"]),
            )

    return out


def main() -> None:
    mc_data = load_mc(MC_CSV)
    ps2_data = load_ps2(PS2_CSV)

    mismatched_names: Set[str] = set()

    for name in mc_data.keys() & ps2_data.keys():
        mc_w, mc_h = mc_data[name]
        ps2_w, ps2_h = ps2_data[name]

        if (
            mc_w != ceil_pow2(ps2_w)
            or mc_h != ceil_pow2(ps2_h)
        ):
            mismatched_names.add(name)

    with OUTPUT_TXT.open("w", encoding="utf-8", newline="\n") as f:
        for name in sorted(mismatched_names):
            f.write(name + "\n")

    print(f"Wrote {len(mismatched_names)} texture names to:")
    print(OUTPUT_TXT)


if __name__ == "__main__":
    main()
