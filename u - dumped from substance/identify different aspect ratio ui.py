from __future__ import annotations

import csv
from pathlib import Path


MANUAL_UI_TEXTURES_PATH = Path(
    r"C:\Development\Git\Afevis-MGS2-Bugfix-Compilation\Texture Fixes\ps2 textures\manual_ui_textures.txt"
)

PS2_CSV_PATH = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_ps2_dimensions.csv"
)

MC_CSV_PATH = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\mgs2_mc_dimensions.csv"
)

OUTPUT_REPORT_PATH = Path(__file__).resolve().with_name("ui_aspect_ratio_report.txt")
MISSING_LOG_PATH = Path(__file__).resolve().with_name("ui_aspect_ratio_missing_entries.txt")


def normalize_key(value: str) -> str:
    return value.strip().lower()


def load_manual_ui_texture_names(path: Path) -> list[str]:
    names: set[str] = set()

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue

            key = normalize_key(line)
            if key:
                names.add(key)

    return sorted(names)


def load_csv_by_texture_name(path: Path) -> dict[str, dict[str, str]]:
    rows_by_name: dict[str, dict[str, str]] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            texture_name = (row.get("texture_name") or "").strip()
            if not texture_name:
                continue

            key = normalize_key(texture_name)
            rows_by_name[key] = row

    return rows_by_name


def parse_positive_int(row: dict[str, str], field_name: str, texture_name: str, source_name: str) -> int:
    raw_value = (row.get(field_name) or "").strip()

    if not raw_value:
        raise ValueError(f"{source_name}: missing {field_name} for {texture_name}")

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{source_name}: invalid {field_name} '{raw_value}' for {texture_name}") from exc

    if value <= 0:
        raise ValueError(f"{source_name}: non-positive {field_name} '{raw_value}' for {texture_name}")

    return value


def ceil_pow2(value: int) -> int:
    if value <= 0:
        raise ValueError(f"ceil_pow2 requires a positive integer, got {value}")

    return 1 << (value - 1).bit_length()


def mc_matches_exact_pow2_padded_ps2(
    mc_width: int,
    mc_height: int,
    ps2_width: int,
    ps2_height: int,
) -> bool:
    return (
        mc_width == ceil_pow2(ps2_width)
        and mc_height == ceil_pow2(ps2_height)
    )


def mc_is_scaled_pow2_padded_ps2(
    mc_width: int,
    mc_height: int,
    ps2_width: int,
    ps2_height: int,
) -> bool:
    base_w = ceil_pow2(ps2_width)
    base_h = ceil_pow2(ps2_height)

    if mc_width % base_w != 0 or mc_height % base_h != 0:
        return False

    scale_w = mc_width // base_w
    scale_h = mc_height // base_h

    return scale_w == scale_h


def aspect_ratio_differs(
    width_a: int,
    height_a: int,
    width_b: int,
    height_b: int,
) -> bool:
    return width_a * height_b != width_b * height_a


def compute_corrected_ps2_size(
    ps2_width: int,
    ps2_height: int,
    mc_width: int,
    mc_height: int,
) -> dict[str, object]:
    width_based_height_num = ps2_width * mc_height
    width_based_height_floor = max(1, width_based_height_num // mc_width)
    width_based_height_rounded = (width_based_height_num % mc_width) != 0

    height_based_width_num = ps2_height * mc_width
    height_based_width_floor = max(1, height_based_width_num // mc_height)
    height_based_width_rounded = (height_based_width_num % mc_height) != 0

    reduction_note: dict[str, int] | None = None

    if width_based_height_floor >= ps2_height:
        corrected_w = ps2_width
        corrected_h = width_based_height_floor
        rounded = width_based_height_rounded

        if (
            corrected_h == ps2_height
            and rounded
            and height_based_width_floor < ps2_width
        ):
            reduction_note = {
                "width": height_based_width_floor,
                "height": ps2_height,
            }
    else:
        corrected_w = max(ps2_width, height_based_width_floor)
        corrected_h = ps2_height
        rounded = height_based_width_rounded

        if (
            corrected_w == ps2_width
            and rounded
            and width_based_height_floor < ps2_height
        ):
            reduction_note = {
                "width": ps2_width,
                "height": width_based_height_floor,
            }

    return {
        "corrected_w": corrected_w,
        "corrected_h": corrected_h,
        "rounded": rounded,
        "reduction_note": reduction_note,
    }


def compute_aspect_delta_scale(
    ps2_width: int,
    ps2_height: int,
    mc_width: int,
    mc_height: int,
) -> tuple[float, float]:
    ps2_aspect = ps2_width / ps2_height
    mc_aspect = mc_width / mc_height

    ratio = mc_aspect / ps2_aspect

    if ratio >= 1.0:
        scale_x = ratio
        scale_y = 1.0
    else:
        scale_x = 1.0
        scale_y = 1.0 / ratio

    return scale_x, scale_y


def make_report_entry(
    texture_name: str,
    ps2_width: int,
    ps2_height: int,
    mc_width: int,
    mc_height: int,
) -> dict[str, object]:
    corrected_info = compute_corrected_ps2_size(
        ps2_width,
        ps2_height,
        mc_width,
        mc_height,
    )

    scale_x, scale_y = compute_aspect_delta_scale(
        ps2_width,
        ps2_height,
        mc_width,
        mc_height,
    )

    return {
        "texture_name": texture_name,
        "ps2_width": ps2_width,
        "ps2_height": ps2_height,
        "mc_width": mc_width,
        "mc_height": mc_height,
        "scale_x": scale_x,
        "scale_y": scale_y,
        "corrected_w": corrected_info["corrected_w"],
        "corrected_h": corrected_info["corrected_h"],
        "rounded": corrected_info["rounded"],
        "reduction_note": corrected_info["reduction_note"],
    }


def append_section(lines: list[str], title: str, entries: list[dict[str, object]]) -> None:
    lines.append(title)

    if not entries:
        lines.append("(none)")
        lines.append("")
        return

    for entry in entries:
        lines.append(str(entry["texture_name"]))

    lines.append("")
    lines.append("=" * 100)
    lines.append("")

    for entry in entries:
        suffix_parts: list[str] = []

        if bool(entry["rounded"]):
            suffix_parts.append("rounded")

        reduction_note = entry.get("reduction_note")
        if isinstance(reduction_note, dict):
            suffix_parts.append(
                f"raw reduction to {reduction_note['width']} x {reduction_note['height']}"
            )

        rounded_suffix = ""
        if suffix_parts:
            rounded_suffix = f" ({', '.join(suffix_parts)})"

        lines.append(str(entry["texture_name"]))
        lines.append(
            f"PS2: {entry['ps2_width']} x {entry['ps2_height']} -> "
            f"MC: {entry['mc_width']} x {entry['mc_height']}. "
            f"(Scaling factor: X={entry['scale_x']:.2f}, Y={entry['scale_y']:.2f})"
        )
        lines.append(
            f"Corrected PS2: {entry['corrected_w']} x {entry['corrected_h']}{rounded_suffix}"
        )
        lines.append("")


def build_report(
    actual_hits: list[dict[str, object]],
    filtered_by_multiple_rule: list[dict[str, object]],
) -> str:
    lines: list[str] = []

    append_section(lines, "Hit stems:", actual_hits)

    lines.append("")
    lines.append("#" * 100)
    lines.append("")

    append_section(
        lines,
        "Previously-hit stems excluded by the multiple-of-ceil-pow2 rule:",
        filtered_by_multiple_rule,
    )

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    manual_ui_names = load_manual_ui_texture_names(MANUAL_UI_TEXTURES_PATH)
    ps2_rows = load_csv_by_texture_name(PS2_CSV_PATH)
    mc_rows = load_csv_by_texture_name(MC_CSV_PATH)

    actual_hits: list[dict[str, object]] = []
    filtered_hits: list[dict[str, object]] = []
    missing_entries: list[str] = []

    for stem in manual_ui_names:
        ps2_row = ps2_rows.get(stem)
        mc_row = mc_rows.get(stem)

        if ps2_row is None and mc_row is None:
            missing_entries.append(f"{stem} | missing from both CSVs")
            continue

        if ps2_row is None:
            missing_entries.append(f"{stem} | missing from PS2 CSV")
            continue

        if mc_row is None:
            missing_entries.append(f"{stem} | missing from MC CSV")
            continue

        texture_name = (mc_row.get("texture_name") or ps2_row.get("texture_name") or stem).strip()

        try:
            ps2_w = parse_positive_int(ps2_row, "tri_dumped_width", texture_name, "PS2 CSV")
            ps2_h = parse_positive_int(ps2_row, "tri_dumped_height", texture_name, "PS2 CSV")
            mc_w = parse_positive_int(mc_row, "mc_width", texture_name, "MC CSV")
            mc_h = parse_positive_int(mc_row, "mc_height", texture_name, "MC CSV")
        except ValueError as exc:
            missing_entries.append(str(exc))
            continue

        if not aspect_ratio_differs(mc_w, mc_h, ps2_w, ps2_h):
            continue

        if mc_matches_exact_pow2_padded_ps2(mc_w, mc_h, ps2_w, ps2_h):
            continue

        entry = make_report_entry(texture_name, ps2_w, ps2_h, mc_w, mc_h)

        if mc_is_scaled_pow2_padded_ps2(mc_w, mc_h, ps2_w, ps2_h):
            filtered_hits.append(entry)
        else:
            actual_hits.append(entry)

    report_text = build_report(actual_hits, filtered_hits)

    OUTPUT_REPORT_PATH.write_text(report_text, encoding="utf-8")

    with MISSING_LOG_PATH.open("w", encoding="utf-8", newline="") as handle:
        for line in missing_entries:
            handle.write(line + "\n")

    print(f"Manual UI textures loaded: {len(manual_ui_names)}")
    print(f"Actual hits: {len(actual_hits)}")
    print(f"Filtered by multiple-of-ceil-pow2 rule: {len(filtered_hits)}")
    print(f"Output report: {OUTPUT_REPORT_PATH}")
    print(f"Missing/error log: {MISSING_LOG_PATH}")


if __name__ == "__main__":
    main()