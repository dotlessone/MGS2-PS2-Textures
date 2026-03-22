from __future__ import annotations

import csv
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ==========================================================
# CONFIG
# ==========================================================
DUMP_ROOT = Path(r"C:\Development\Git\MGS3-PS2-Textures\u - dumped from subsistence\dump")
FINAL_REBUILT_DIR = Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Substance\US\TGA")

TEXTURE_MAP_CSV = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs2_texture_map.csv")
SLOT_NUMBER_TO_NAME_CSV = Path(r"C:\Development\Git\MGS-Tri-Dumper\mgs3\extracted\SLOT\slot_number_to_name.csv")

LOG_FILE = DUMP_ROOT / "_final_rebuild_log.txt"
AMBIGUOUS_LOG_FILE = DUMP_ROOT / "_final_rebuild_ambiguous_log.txt"
UNMATCHED_LOG_FILE = DUMP_ROOT / "_final_rebuild_unmatched_log.txt"


# ==========================================================
# DATA TYPES
# ==========================================================
@dataclass(frozen=True)
class SourceCandidate:
    source_path: Path
    raw_stage_folder: str
    resolved_stage_name: str
    tri_strcode: str
    texture_strcode: str
    target_texture_filename: str
    target_path: Path


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


def load_texture_map(csv_path: Path) -> Tuple[Dict[Tuple[str, str, str], Set[str]], List[str]]:
    mapping: Dict[Tuple[str, str, str], Set[str]] = {}
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

            texture_filename = row[0].strip()
            stage = normalize_token(row[1])
            tri_strcode = normalize_token(row[2])
            texture_strcode = normalize_token(row[3])

            if not texture_filename or not stage or not tri_strcode or not texture_strcode:
                issues.append(f"Line {row_index}: missing required field(s)")
                continue

            key = (stage, tri_strcode, texture_strcode)
            mapping.setdefault(key, set()).add(texture_filename)

    return mapping, issues


def iter_candidate_tga_files(root: Path) -> List[Path]:
    files: List[Path] = []

    for current_root, dirnames, filenames in os.walk(root):
        current_path = Path(current_root)

        dirnames[:] = [d for d in dirnames if d.lower() != "final rebuilt"]

        for filename in filenames:
            if filename.lower().endswith(".tga"):
                files.append(current_path / filename)

    return files


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


def parse_source_candidate(
    file_path: Path,
    slot_number_to_name: Dict[str, str],
    texture_map: Dict[Tuple[str, str, str], Set[str]],
) -> Tuple[Optional[SourceCandidate], Optional[str], Optional[str]]:
    parent = file_path.parent
    tri_folder = parent.name
    stage_folder = parent.parent.name if parent.parent else ""

    if not stage_folder:
        return None, f"Invalid path structure, missing stage folder: {file_path}", None

    if not tri_folder:
        return None, f"Invalid path structure, missing tri folder: {file_path}", None

    texture_strcode = normalize_token(file_path.stem)
    tri_strcode = normalize_token(tri_folder)
    resolved_stage_name = resolve_stage_name(stage_folder, slot_number_to_name)

    if not resolved_stage_name:
        return None, None, (
            f"Unmatched stage mapping | source={file_path} | "
            f"raw_stage={stage_folder} | tri={tri_strcode} | texture={texture_strcode}"
        )

    key = (resolved_stage_name, tri_strcode, texture_strcode)
    candidates = texture_map.get(key)

    if not candidates:
        return None, None, (
            f"No CSV match | source={file_path} | "
            f"raw_stage={stage_folder} | resolved_stage={resolved_stage_name} | "
            f"tri={tri_strcode} | texture={texture_strcode}"
        )

    if len(candidates) > 1:
        candidate_str = " | ".join(sorted(candidates))
        return None, (
            f"Ambiguous CSV match | source={file_path} | "
            f"raw_stage={stage_folder} | resolved_stage={resolved_stage_name} | "
            f"tri={tri_strcode} | texture={texture_strcode} | "
            f"candidates={candidate_str}"
        ), None

    texture_filename = next(iter(candidates))
    target_path = FINAL_REBUILT_DIR / f"{texture_filename}.tga"

    return (
        SourceCandidate(
            source_path=file_path,
            raw_stage_folder=stage_folder,
            resolved_stage_name=resolved_stage_name,
            tri_strcode=tri_strcode,
            texture_strcode=texture_strcode,
            target_texture_filename=texture_filename,
            target_path=target_path,
        ),
        None,
        None,
    )


# ==========================================================
# GROUP PROCESSING
# ==========================================================
def process_target_group(
    texture_filename: str,
    candidates: List[SourceCandidate],
) -> List[str]:
    lines: List[str] = []

    if not candidates:
        return lines

    sorted_candidates = sorted(candidates, key=lambda x: str(x.source_path).lower())
    target_path = sorted_candidates[0].target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.exists():
        lines.append(
            f"TARGET_ALREADY_EXISTS | target={target_path} | texture_filename={texture_filename} | "
            f"incoming_count={len(sorted_candidates)}"
        )

        for candidate in sorted_candidates:
            if candidate.source_path.exists():
                candidate.source_path.unlink()
                lines.append(
                    f"DELETE_DUPLICATE_TO_EXISTING_TARGET | source={candidate.source_path} | "
                    f"target={target_path} | raw_stage={candidate.raw_stage_folder} | "
                    f"resolved_stage={candidate.resolved_stage_name} | tri={candidate.tri_strcode} | "
                    f"texture={candidate.texture_strcode}"
                )

        return lines

    chosen = sorted_candidates[0]
    duplicates = sorted_candidates[1:]

    shutil.move(str(chosen.source_path), str(target_path))
    lines.append(
        f"MOVE_GROUP_PRIMARY | source={chosen.source_path} | target={target_path} | "
        f"duplicate_count={len(duplicates)} | raw_stage={chosen.raw_stage_folder} | "
        f"resolved_stage={chosen.resolved_stage_name} | tri={chosen.tri_strcode} | "
        f"texture={chosen.texture_strcode}"
    )

    for candidate in duplicates:
        if candidate.source_path.exists():
            candidate.source_path.unlink()
            lines.append(
                f"DELETE_GROUP_DUPLICATE | source={candidate.source_path} | "
                f"target={target_path} | raw_stage={candidate.raw_stage_folder} | "
                f"resolved_stage={candidate.resolved_stage_name} | tri={candidate.tri_strcode} | "
                f"texture={candidate.texture_strcode}"
            )

    return lines


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

    FINAL_REBUILT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading SLOT mapping: {SLOT_NUMBER_TO_NAME_CSV}")
    slot_number_to_name = load_slot_number_to_name(SLOT_NUMBER_TO_NAME_CSV)
    print(f"Loaded {len(slot_number_to_name):,} SLOT stage mappings")

    print(f"Loading texture map: {TEXTURE_MAP_CSV}")
    texture_map, texture_map_issues = load_texture_map(TEXTURE_MAP_CSV)
    print(f"Loaded {len(texture_map):,} unique texture map keys")

    if texture_map_issues:
        print(f"Texture map load issues: {len(texture_map_issues):,}")

    print(f"Scanning for .tga files under: {DUMP_ROOT}")
    candidate_files = iter_candidate_tga_files(DUMP_ROOT)
    print(f"Found {len(candidate_files):,} .tga files to inspect\n")

    source_candidates: List[SourceCandidate] = []
    ambiguous_lines: List[str] = []
    unmatched_lines: List[str] = []

    for file_path in candidate_files:
        candidate, ambiguous, unmatched = parse_source_candidate(
            file_path=file_path,
            slot_number_to_name=slot_number_to_name,
            texture_map=texture_map,
        )

        if candidate is not None:
            source_candidates.append(candidate)

        if ambiguous:
            ambiguous_lines.append(ambiguous)

        if unmatched:
            unmatched_lines.append(unmatched)

    print(f"Matched source candidates: {len(source_candidates):,}")
    print(f"Ambiguous matches: {len(ambiguous_lines):,}")
    print(f"Unmatched files: {len(unmatched_lines):,}\n")

    grouped_by_texture_filename: Dict[str, List[SourceCandidate]] = {}
    for candidate in source_candidates:
        grouped_by_texture_filename.setdefault(
            candidate.target_texture_filename,
            [],
        ).append(candidate)

    group_keys = sorted(grouped_by_texture_filename.keys())
    print(f"Grouped into {len(group_keys):,} target texture_filename buckets")

    results: List[str] = []
    processed_groups = 0
    total_groups = len(group_keys)

    for texture_filename in group_keys:
        processed_groups += 1
        group_lines = process_target_group(
            texture_filename=texture_filename,
            candidates=grouped_by_texture_filename[texture_filename],
        )
        results.extend(group_lines)

        if processed_groups % 100 == 0 or processed_groups == total_groups:
            print(f"[{processed_groups:,}/{total_groups:,}] target groups processed")

    log_lines: List[str] = []
    log_lines.append(f"Dump root: {DUMP_ROOT}")
    log_lines.append(f"Final rebuilt dir: {FINAL_REBUILT_DIR}")
    log_lines.append(f"Texture map CSV: {TEXTURE_MAP_CSV}")
    log_lines.append(f"SLOT mapping CSV: {SLOT_NUMBER_TO_NAME_CSV}")
    log_lines.append("")
    log_lines.append(f"Total scanned .tga files: {len(candidate_files):,}")
    log_lines.append(f"Matched source candidates: {len(source_candidates):,}")
    log_lines.append(f"Target groups: {len(group_keys):,}")
    log_lines.append(f"Ambiguous matches: {len(ambiguous_lines):,}")
    log_lines.append(f"Unmatched files: {len(unmatched_lines):,}")
    log_lines.append("")

    if texture_map_issues:
        log_lines.append("=== TEXTURE MAP LOAD ISSUES ===")
        log_lines.extend(texture_map_issues)
        log_lines.append("")

    log_lines.append("=== ACTIONS ===")
    log_lines.extend(results)
    LOG_FILE.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    AMBIGUOUS_LOG_FILE.write_text(
        "\n".join(sorted(ambiguous_lines)) + ("\n" if ambiguous_lines else ""),
        encoding="utf-8",
    )

    UNMATCHED_LOG_FILE.write_text(
        "\n".join(sorted(unmatched_lines)) + ("\n" if unmatched_lines else ""),
        encoding="utf-8",
    )

    move_count = sum(1 for line in results if line.startswith("MOVE_GROUP_PRIMARY | "))
    delete_group_duplicate_count = sum(1 for line in results if line.startswith("DELETE_GROUP_DUPLICATE | "))
    delete_existing_duplicate_count = sum(1 for line in results if line.startswith("DELETE_DUPLICATE_TO_EXISTING_TARGET | "))
    existing_target_count = sum(1 for line in results if line.startswith("TARGET_ALREADY_EXISTS | "))

    # ==========================================================
    # CLEANUP: REMOVE EMPTY FOLDERS
    # ==========================================================
    print("\nCleaning up empty folders...")

    removed_dirs = 0

    for current_root, dirnames, filenames in os.walk(DUMP_ROOT, topdown=False):
        current_path = Path(current_root)

        if current_path == FINAL_REBUILT_DIR:
            continue

        if FINAL_REBUILT_DIR in current_path.parents:
            continue

        try:
            if not any(current_path.iterdir()):
                current_path.rmdir()
                removed_dirs += 1
        except Exception:
            pass

    print(f"Removed empty folders: {removed_dirs:,}\n")

    print("Done.\n")
    print(f"Moved to Final Rebuilt: {move_count:,}")
    print(f"Deleted group duplicates: {delete_group_duplicate_count:,}")
    print(f"Deleted duplicates against existing targets: {delete_existing_duplicate_count:,}")
    print(f"Existing targets encountered: {existing_target_count:,}")
    print(f"Ambiguous CSV matches: {len(ambiguous_lines):,}")
    print(f"Unmatched files: {len(unmatched_lines):,}")
    print("")
    print(f"Main log: {LOG_FILE}")
    print(f"Ambiguous log: {AMBIGUOUS_LOG_FILE}")
    print(f"Unmatched log: {UNMATCHED_LOG_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())