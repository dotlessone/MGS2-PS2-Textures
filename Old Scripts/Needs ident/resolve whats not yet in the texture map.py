from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set


# ==========================================================
# CONFIG
# ==========================================================
DUMP_ROOT = Path(r"C:\Development\Git\MGS2-PS2-Textures\u - dumped from substance\dump")
FINAL_REBUILT_DIR = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\US\TGA")

TEXTURE_MAP_CSV = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs2_texture_map.csv")
SLOT_NUMBER_TO_NAME_CSV = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs3\extracted\SLOT\slot_number_to_name.csv")

ALL_MISSING_LOG = DUMP_ROOT / "_final_rebuilt_missing_all.txt"
LEFTOVER_STAGE_MISSING_LOG = DUMP_ROOT / "_final_rebuilt_missing_leftover_stages_only.txt"
PADDING_FIX_LOG = DUMP_ROOT / "_final_rebuilt_padding_needed.csv"
SUMMARY_LOG = DUMP_ROOT / "_final_rebuilt_missing_summary.txt"


# ==========================================================
# HELPERS
# ==========================================================
def normalize_token(value: str) -> str:
    return value.strip().lower()


def is_probably_slot_stage(stage_folder_name: str) -> bool:
    stripped = stage_folder_name.strip()
    return stripped.isdigit() and len(stripped) <= 3


def pad_slot_stage(stage_folder_name: str) -> str:
    return stage_folder_name.strip().zfill(3)


def parse_slot_stage_name(raw_value: str) -> Optional[str]:
    line = raw_value.strip()
    if not line:
        return None

    comment_pos = line.find("//")
    if comment_pos != -1:
        line = line[:comment_pos].rstrip()

    if ":" not in line:
        return None

    number_part, name_part = line.split(":", 1)
    number_part = number_part.strip()
    name_part = name_part.strip()

    if not number_part or not name_part:
        return None

    return f"{number_part}:{name_part}"


def load_slot_number_to_name(csv_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        for raw_line in f:
            parsed = parse_slot_stage_name(raw_line)
            if not parsed:
                continue

            number_part, stage_name = parsed.split(":", 1)
            mapping[number_part] = stage_name

    return mapping


def resolve_stage_name(
    raw_stage_folder: str,
    slot_number_to_name: Dict[str, str],
) -> Optional[str]:
    if is_probably_slot_stage(raw_stage_folder):
        padded = pad_slot_stage(raw_stage_folder)
        mapped = slot_number_to_name.get(padded)
        if mapped:
            return normalize_token(mapped)
        return None

    return normalize_token(raw_stage_folder)


def load_expected_texture_map(
    csv_path: Path,
) -> tuple[Dict[str, Set[str]], Dict[str, Set[str]], List[str]]:
    texture_to_stages: Dict[str, Set[str]] = {}
    stage_to_textures: Dict[str, Set[str]] = {}
    issues: List[str] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row_index, row in enumerate(reader, start=1):
            if not row:
                continue

            first = row[0].strip()
            if not first or first.startswith(";"):
                continue

            if len(row) < 4:
                if len(row) == 3:
                    continue

                issues.append(f"Line {row_index}: expected 4 columns, got {len(row)}")
                continue

            texture_filename = row[0].strip().lower()
            stage = normalize_token(row[1])

            if not texture_filename or not stage:
                issues.append(f"Line {row_index}: missing texture_filename or stage")
                continue

            texture_to_stages.setdefault(texture_filename, set()).add(stage)
            stage_to_textures.setdefault(stage, set()).add(texture_filename)

    return texture_to_stages, stage_to_textures, issues


def iter_dump_tga_files_excluding_final_rebuilt(root: Path) -> List[Path]:
    files: List[Path] = []

    for current_root, dirnames, filenames in os.walk(root):
        current_path = Path(current_root)

        dirnames[:] = [d for d in dirnames if d.lower() != "final rebuilt"]

        for filename in filenames:
            if filename.lower().endswith(".tga"):
                files.append(current_path / filename)

    return files


def collect_final_rebuilt_filenames(final_rebuilt_dir: Path) -> Set[str]:
    result: Set[str] = set()

    if not final_rebuilt_dir.is_dir():
        return result

    for entry in final_rebuilt_dir.iterdir():
        if not entry.is_file():
            continue

        if entry.suffix.lower() != ".tga":
            continue

        result.add(entry.stem.lower())

    return result


def has_underscored_variant(expected_stage_textures: Set[str], padded_name: str) -> bool:
    prefix = f"{padded_name}_"
    return any(texture.startswith(prefix) for texture in expected_stage_textures)


def collect_leftover_stage_data(
    dump_root: Path,
    slot_number_to_name: Dict[str, str],
    stage_to_textures: Dict[str, Set[str]],
) -> tuple[Set[str], Dict[str, Set[str]], List[str]]:
    leftover_stages: Set[str] = set()
    stage_to_available_textures: Dict[str, Set[str]] = {}
    padding_log_rows: List[str] = []

    for file_path in iter_dump_tga_files_excluding_final_rebuilt(dump_root):
        tri_folder = file_path.parent.name.lower()
        stage_folder = file_path.parent.parent.name if file_path.parent.parent else ""

        if not stage_folder:
            continue

        resolved_stage = resolve_stage_name(stage_folder, slot_number_to_name)
        if not resolved_stage:
            continue

        leftover_stages.add(resolved_stage)

        stem = file_path.stem.lower()
        candidates = {stem}

        if not stem.startswith("00"):
            padded = f"00{stem}"
            expected_stage_textures = stage_to_textures.get(resolved_stage, set())

            if padded in expected_stage_textures and not has_underscored_variant(expected_stage_textures, padded):
                candidates.add(padded)
                padding_log_rows.append(f"{padded},{resolved_stage},{tri_folder},{stem}")

        stage_to_available_textures.setdefault(resolved_stage, set()).update(candidates)

    return leftover_stages, stage_to_available_textures, padding_log_rows


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    if not DUMP_ROOT.is_dir():
        print(f"ERROR: Dump root does not exist: {DUMP_ROOT}")
        return 1

    if not TEXTURE_MAP_CSV.is_file():
        print(f"ERROR: Texture map CSV does not exist: {TEXTURE_MAP_CSV}")
        return 1

    if not SLOT_NUMBER_TO_NAME_CSV.is_file():
        print(f"ERROR: SLOT stage mapping CSV does not exist: {SLOT_NUMBER_TO_NAME_CSV}")
        return 1

    print(f"Loading SLOT mapping: {SLOT_NUMBER_TO_NAME_CSV}")
    slot_number_to_name = load_slot_number_to_name(SLOT_NUMBER_TO_NAME_CSV)
    print(f"Loaded {len(slot_number_to_name):,} SLOT stage mappings")

    print(f"Loading expected texture map: {TEXTURE_MAP_CSV}")
    texture_to_stages, stage_to_textures, issues = load_expected_texture_map(TEXTURE_MAP_CSV)
    print(f"Loaded {len(texture_to_stages):,} unique texture filenames from texture map")
    print(f"Loaded {len(stage_to_textures):,} unique stages from texture map")

    print(f"Scanning Final Rebuilt: {FINAL_REBUILT_DIR}")
    final_rebuilt_filenames = collect_final_rebuilt_filenames(FINAL_REBUILT_DIR)
    print(f"Found {len(final_rebuilt_filenames):,} rebuilt texture files")

    print(f"Scanning leftover dump files under: {DUMP_ROOT}")
    leftover_stages, stage_to_available_textures, padding_log_rows = collect_leftover_stage_data(
        DUMP_ROOT,
        slot_number_to_name,
        stage_to_textures,
    )
    print(f"Found {len(leftover_stages):,} stages with leftover files in dump")

    expected_filenames = set(texture_to_stages.keys())
    missing_filenames = sorted(expected_filenames - final_rebuilt_filenames, key=str.lower)
    missing_filenames_set = set(missing_filenames)

    all_missing_lines: List[str] = []
    for texture_filename in missing_filenames:
        stages = sorted(texture_to_stages.get(texture_filename, set()))
        stages_str = " | ".join(stages)
        all_missing_lines.append(
            f"MISSING | texture_filename={texture_filename} | stages={stages_str}"
        )

    leftover_stage_missing_lines: List[str] = []
    leftover_stages_with_missing = 0
    missing_texture_stage_pairs = 0

    for stage in sorted(leftover_stages):
        expected_stage_textures = stage_to_textures.get(stage, set())
        available_in_stage = stage_to_available_textures.get(stage, set())

        missing_for_stage = sorted(
            tex
            for tex in expected_stage_textures
            if tex in missing_filenames_set and tex not in available_in_stage
        )

        if not missing_for_stage:
            continue

        leftover_stages_with_missing += 1
        missing_texture_stage_pairs += len(missing_for_stage)

        leftover_stage_missing_lines.append(
            f"=== STAGE: {stage} ({len(missing_for_stage)} missing) ==="
        )
        leftover_stage_missing_lines.extend(missing_for_stage)
        leftover_stage_missing_lines.append("")

    ALL_MISSING_LOG.write_text(
        "\n".join(all_missing_lines) + ("\n" if all_missing_lines else ""),
        encoding="utf-8",
    )

    LEFTOVER_STAGE_MISSING_LOG.write_text(
        "\n".join(leftover_stage_missing_lines),
        encoding="utf-8",
    )

    padding_csv_lines: List[str] = ["texture_name,stage,tri_strcode,texture_strcode"]
    padding_csv_lines.extend(sorted(set(padding_log_rows), key=str.lower))
    PADDING_FIX_LOG.write_text(
        "\n".join(padding_csv_lines) + "\n",
        encoding="utf-8",
    )

    summary_lines: List[str] = []
    summary_lines.append(f"Dump root: {DUMP_ROOT}")
    summary_lines.append(f"Final rebuilt dir: {FINAL_REBUILT_DIR}")
    summary_lines.append(f"Texture map CSV: {TEXTURE_MAP_CSV}")
    summary_lines.append(f"SLOT mapping CSV: {SLOT_NUMBER_TO_NAME_CSV}")
    summary_lines.append("")
    summary_lines.append(f"Unique texture filenames in texture map: {len(expected_filenames):,}")
    summary_lines.append(f"Unique texture filenames in Final Rebuilt: {len(final_rebuilt_filenames):,}")
    summary_lines.append(f"Missing texture filenames from Final Rebuilt: {len(missing_filenames):,}")
    summary_lines.append(f"Stages with leftover files in dump: {len(leftover_stages):,}")
    summary_lines.append(f"Leftover stages with missing textures: {leftover_stages_with_missing:,}")
    summary_lines.append(f"Missing texture/stage pairs across leftover stages: {missing_texture_stage_pairs:,}")
    summary_lines.append(f"Files needing 00 padding: {len(set(padding_log_rows)):,}")
    summary_lines.append("")

    if issues:
        summary_lines.append("=== TEXTURE MAP LOAD ISSUES ===")
        summary_lines.extend(issues)
        summary_lines.append("")

    summary_lines.append("=== LEFTOVER STAGES IN DUMP ===")
    for stage in sorted(leftover_stages):
        summary_lines.append(stage)
    summary_lines.append("")

    summary_lines.append(f"All missing log: {ALL_MISSING_LOG}")
    summary_lines.append(f"Leftover-stage missing log: {LEFTOVER_STAGE_MISSING_LOG}")
    summary_lines.append(f"Padding-needed log: {PADDING_FIX_LOG}")

    SUMMARY_LOG.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print("\nDone.\n")
    print(f"Missing texture filenames from Final Rebuilt: {len(missing_filenames):,}")
    print(f"Leftover stages with missing textures: {leftover_stages_with_missing:,}")
    print(f"Files needing 00 padding: {len(set(padding_log_rows)):,}")
    print("")
    print(f"All missing log: {ALL_MISSING_LOG}")
    print(f"Leftover-stage missing log: {LEFTOVER_STAGE_MISSING_LOG}")
    print(f"Padding-needed log: {PADDING_FIX_LOG}")
    print(f"Summary log: {SUMMARY_LOG}")

    return 0


if __name__ == "__main__":
    sys.exit(main())