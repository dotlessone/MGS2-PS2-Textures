from __future__ import annotations

import csv
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from PIL import Image


# ==========================================================
# CONFIG
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
TARGET_DIR = SCRIPT_DIR / "_win"

OUTPUT_CSV_WIN_ONLY = SCRIPT_DIR / "mgs2_mc_dimensions.csv"
OUTPUT_CSV_FULL_RECURSIVE = SCRIPT_DIR / "mgs2_mc_dimensions_including_override_folders.csv"

MISSING_PNG_LOG_WIN_ONLY = SCRIPT_DIR / "missing_matching_png_for_ctxr_win_only.txt"
MISSING_PNG_LOG_FULL_RECURSIVE = SCRIPT_DIR / "missing_matching_png_for_ctxr_full_recursive.txt"

PC_NONPOW2_MATCH_LOG_WIN_ONLY = SCRIPT_DIR / "pc_raw_dims_equal_ciel2_win_only.txt"
PC_NONPOW2_MATCH_LOG_FULL_RECURSIVE = SCRIPT_DIR / "pc_raw_dims_equal_ciel2_full_recursive.txt"

VERSION_DATES_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_ps2_sha1_version_dates.csv"
)

MC_TRI_DUMPED_METADATA_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

MANUAL_BP_REMADE_TXT = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_manually_identified_bp_remade.txt"
)

MC_VERSION_DATES_CSV = SCRIPT_DIR / "MC_Version_Dates.csv"
MC_TEXTURE_UPDATE_VERSIONS_CSV = SCRIPT_DIR / "MC_Texture_Update_Versions.csv"

MAX_WORKERS = max(4, os.cpu_count() or 1)
HASH_CHUNK_SIZE = 8 * 1024 * 1024

PC_ORIGIN_VERSION = "Substance (PC)"
PC_VERSION_UNIX_TIME = "1045766390"

HD_ORIGIN_VERSION = "HD Collection (PS3)"
HD_VERSION_UNIX_TIME = "1318534422"

CTRLTYPE_MC_VERSION = "1.2.0"


# ==========================================================
# HELPERS
# ==========================================================
def sha1_file(path: Path) -> str:
    digest = hashlib.sha1()

    with path.open("rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def format_alpha_levels(alpha_levels: List[int]) -> str:
    return "[" + ", ".join(str(v) for v in alpha_levels) + "]"


def get_png_metadata(png_path: Path) -> Tuple[int, int, List[int]]:
    with Image.open(png_path) as img:
        width, height = img.size

        if "A" in img.getbands():
            alpha = img.getchannel("A")
            alpha_levels = sorted(set(alpha.getdata()))
        else:
            alpha_levels = [255]

    return width, height, alpha_levels


def normalize_rel_dir(value: str) -> str:
    normalized = value.replace("\\", "/").strip()

    while "//" in normalized:
        normalized = normalized.replace("//", "/")

    normalized = normalized.strip("/")

    if not normalized:
        return ""

    return normalized + "/"


def parse_version_tuple(version: str) -> Tuple[int, ...]:
    parts = version.strip().split(".")
    out: List[int] = []

    for part in parts:
        part = part.strip()
        if not part:
            out.append(0)
            continue

        try:
            out.append(int(part))
        except ValueError as exc:
            raise ValueError(f"Invalid version number: '{version}'") from exc

    return tuple(out)


def find_ctxr_files_under_win() -> List[Path]:
    if not TARGET_DIR.is_dir():
        raise RuntimeError(f"_win directory not found: {TARGET_DIR}")

    ctxr_files = [p for p in TARGET_DIR.rglob("*.ctxr") if p.is_file()]
    ctxr_files.sort(key=lambda p: str(p).lower())
    return ctxr_files


def find_ctxr_files_full_recursive() -> List[Path]:
    ctxr_files = [p for p in SCRIPT_DIR.rglob("*.ctxr") if p.is_file()]
    ctxr_files.sort(key=lambda p: str(p).lower())
    return ctxr_files


def load_version_dates(csv_path: Path) -> Dict[str, Tuple[str, str]]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Version dates CSV not found: {csv_path}")

    version_map: Dict[str, Tuple[str, str]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"sha1", "game", "version", "first_seen_unix"}
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"Version dates CSV is missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            sha1_value = (row.get("sha1") or "").strip().lower()
            game = (row.get("game") or "").strip()
            version = (row.get("version") or "").strip()
            first_seen_unix = (row.get("first_seen_unix") or "").strip()

            if not sha1_value:
                continue

            origin_version = f"{game} {version}".strip()
            version_map[sha1_value] = (origin_version, first_seen_unix)

    return version_map


def load_manual_bp_remade_stems(txt_path: Path) -> Set[str]:
    if not txt_path.is_file():
        raise FileNotFoundError(f"Manual BP remade TXT not found: {txt_path}")

    stems: Set[str] = set()

    with txt_path.open("r", encoding="utf-8-sig", newline="") as f:
        for line in f:
            stem = line.strip().lower()

            if not stem:
                continue

            if stem.startswith("#"):
                continue

            stems.add(stem)

    return stems


def load_mc_tri_dumped_metadata(
    csv_path: Path,
) -> Dict[str, Dict[str, int]]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"MC tri-dumped metadata CSV not found: {csv_path}")

    metadata_map: Dict[str, Dict[str, int]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {
            "texture_name",
            "mc_tri_dumped_width",
            "mc_tri_height",
            "mc_tri_width_ciel2",
            "mc_tri_height_ciel2",
        }
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"MC tri-dumped metadata CSV is missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            texture_name = (row.get("texture_name") or "").strip().lower()
            raw_width_str = (row.get("mc_tri_dumped_width") or "").strip()
            raw_height_str = (row.get("mc_tri_height") or "").strip()
            ciel_width_str = (row.get("mc_tri_width_ciel2") or "").strip()
            ciel_height_str = (row.get("mc_tri_height_ciel2") or "").strip()

            if (
                not texture_name
                or not raw_width_str
                or not raw_height_str
                or not ciel_width_str
                or not ciel_height_str
            ):
                continue

            try:
                raw_width = int(raw_width_str)
                raw_height = int(raw_height_str)
                ciel_width = int(ciel_width_str)
                ciel_height = int(ciel_height_str)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid MC tri-dumped metadata dimensions for '{texture_name}': "
                    f"raw=({raw_width_str}, {raw_height_str}), "
                    f"ciel2=({ciel_width_str}, {ciel_height_str})"
                ) from exc

            metadata_map[texture_name] = {
                "mc_tri_dumped_width": raw_width,
                "mc_tri_height": raw_height,
                "mc_tri_width_ciel2": ciel_width,
                "mc_tri_height_ciel2": ciel_height,
            }

    return metadata_map


def load_mc_version_dates(csv_path: Path) -> Dict[str, str]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"MC version dates CSV not found: {csv_path}")

    version_date_map: Dict[str, str] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"version_number", "unix_hex_time"}
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"MC version dates CSV is missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            version_number = (row.get("version_number") or "").strip()
            unix_hex_time = (row.get("unix_hex_time") or "").strip()

            if not version_number:
                continue

            if not unix_hex_time:
                raise ValueError(
                    f"Missing unix_hex_time for MC version '{version_number}'"
                )

            try:
                unix_time = str(int(unix_hex_time, 16))
            except ValueError as exc:
                raise ValueError(
                    f"Invalid unix_hex_time '{unix_hex_time}' for version '{version_number}'"
                ) from exc

            version_date_map[version_number] = unix_time

    return version_date_map


def load_mc_texture_update_versions(
    csv_path: Path,
    mc_version_date_map: Dict[str, str],
) -> Dict[Tuple[str, str], Tuple[str, str]]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"MC texture update versions CSV not found: {csv_path}")

    latest_map: Dict[Tuple[str, str], Tuple[str, str]] = {}
    latest_version_tuples: Dict[Tuple[str, str], Tuple[int, ...]] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"file_name", "version_number", "relative_path"}
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"MC texture update versions CSV is missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            file_name = (row.get("file_name") or "").strip().lower()
            version_number = (row.get("version_number") or "").strip()
            relative_path = normalize_rel_dir(row.get("relative_path") or "")

            if not file_name and not version_number and not relative_path:
                continue

            if not file_name or not version_number:
                continue

            unix_time = mc_version_date_map.get(version_number)
            if unix_time is None:
                raise ValueError(
                    f"Version '{version_number}' from MC_Texture_Update_Versions.csv "
                    f"was not found in MC_Version_Dates.csv"
                )

            key = (file_name, relative_path)
            version_tuple = parse_version_tuple(version_number)
            existing_version_tuple = latest_version_tuples.get(key)

            if existing_version_tuple is None or version_tuple > existing_version_tuple:
                latest_version_tuples[key] = version_tuple
                latest_map[key] = (version_number, unix_time)

    return latest_map


def get_ctxr_relative_parent_dir(ctxr_path: Path) -> str:
    parent_relative = ctxr_path.parent.relative_to(SCRIPT_DIR).as_posix()
    return normalize_rel_dir(parent_relative)


def process_ctxr(
    ctxr_path: Path,
    version_map: Dict[str, Tuple[str, str]],
    tri_metadata_map: Dict[str, Dict[str, int]],
    manual_bp_remade_stems: Set[str],
    mc_update_map: Dict[Tuple[str, str], Tuple[str, str]],
    mc_version_date_map: Dict[str, str],
):
    png_path = ctxr_path.with_suffix(".png")

    if not png_path.is_file():
        return {
            "status": "missing_png",
            "ctxr_path": ctxr_path,
        }

    texture_name = ctxr_path.stem
    texture_name_lower = texture_name.lower()
    relative_parent_dir = get_ctxr_relative_parent_dir(ctxr_path)
    relative_parent_dir_lower = relative_parent_dir.lower()

    mc_ctxr_sha1 = sha1_file(ctxr_path)
    mc_resaved_sha1 = sha1_file(png_path)
    mc_width, mc_height, alpha_levels = get_png_metadata(png_path)

    origin_version = ""
    version_unix_time = ""
    bp_remade = False
    pc_equal_dims_log_entry = None

    tri_meta = tri_metadata_map.get(texture_name_lower)
    raw_width: Optional[int] = None
    raw_height: Optional[int] = None
    ciel_width: Optional[int] = None
    ciel_height: Optional[int] = None

    if tri_meta is not None:
        raw_width = tri_meta["mc_tri_dumped_width"]
        raw_height = tri_meta["mc_tri_height"]
        ciel_width = tri_meta["mc_tri_width_ciel2"]
        ciel_height = tri_meta["mc_tri_height_ciel2"]

    # Priority 1: Master Collection explicit texture/version map
    mc_update_info = mc_update_map.get((texture_name_lower, relative_parent_dir))
    if mc_update_info is not None:
        mc_version_number, mc_unix_time = mc_update_info
        origin_version = f"Master Collection - {mc_version_number}"
        version_unix_time = mc_unix_time
        bp_remade = True

    # Priority 2: SHA1 matches from PS2 version map
    if not origin_version:
        version_info: Optional[Tuple[str, str]] = version_map.get(mc_resaved_sha1.lower())
        if version_info is not None:
            origin_version, version_unix_time = version_info

    # Priority 3: PC inferred from ciel2 dimension match
    if (
        not origin_version
        and texture_name_lower not in manual_bp_remade_stems
        and tri_meta is not None
        and ciel_width is not None
        and ciel_height is not None
    ):
        if ciel_width == mc_width and ciel_height == mc_height:
            origin_version = PC_ORIGIN_VERSION
            version_unix_time = PC_VERSION_UNIX_TIME

            if raw_width == ciel_width and raw_height == ciel_height:
                pc_equal_dims_log_entry = texture_name

    # Priority 3.5: ctrltype_ folders are Master Collection 1.2.0
    if not origin_version and "ctrltype_" in relative_parent_dir_lower:
        ctrltype_unix_time = mc_version_date_map.get(CTRLTYPE_MC_VERSION)
        if ctrltype_unix_time is None:
            raise ValueError(
                f"Required MC version '{CTRLTYPE_MC_VERSION}' was not found in MC_Version_Dates.csv"
            )

        origin_version = f"Master Collection - {CTRLTYPE_MC_VERSION}"
        version_unix_time = ctrltype_unix_time
        bp_remade = True

    # Priority 4: everything else
    if not origin_version:
        origin_version = HD_ORIGIN_VERSION
        version_unix_time = HD_VERSION_UNIX_TIME
        bp_remade = True

    mismatch = False

    if pc_equal_dims_log_entry is not None:
        mismatch = True

    if raw_width is not None and mc_width < raw_width:
        mismatch = True

    if raw_height is not None and mc_height < raw_height:
        mismatch = True

    return {
        "status": "ok",
        "row": (
            texture_name,
            mc_width,
            mc_height,
            format_alpha_levels(alpha_levels),
            mc_resaved_sha1,
            mc_ctxr_sha1,
            origin_version,
            version_unix_time,
            str(bp_remade).lower(),
            "none",
            relative_parent_dir,
            str(mismatch).lower(),
        ),
        "pc_equal_dims_log_entry": pc_equal_dims_log_entry,
    }


def write_outputs(
    output_csv: Path,
    missing_png_log: Path,
    pc_nonpow2_match_log: Path,
    rows: List[Tuple],
    missing_pngs: List[str],
    pc_equal_dims_entries: List[str],
) -> None:
    rows.sort(key=lambda row: (row[0].lower(), row[10].lower()))
    missing_pngs.sort(key=str.lower)
    pc_equal_dims_entries.sort(key=str.lower)

    with output_csv.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "texture_name",
                "mc_width",
                "mc_height",
                "mc_alpha_levels",
                "mc_resaved_sha1",
                "mc_ctxr_sha1",
                "origin_version",
                "version_unix_time",
                "bp_remade",
                "region_specific",
                "relative_path",
                "mismatch",
            ]
        )
        writer.writerows(rows)

    if missing_pngs:
        with missing_png_log.open("w", encoding="utf-8", newline="\n") as f:
            for path in missing_pngs:
                f.write(path + "\n")
    else:
        if missing_png_log.exists():
            missing_png_log.unlink()

    if pc_equal_dims_entries:
        with pc_nonpow2_match_log.open("w", encoding="utf-8", newline="\n") as f:
            for entry in pc_equal_dims_entries:
                f.write(entry + "\n")
    else:
        if pc_nonpow2_match_log.exists():
            pc_nonpow2_match_log.unlink()


def process_dataset(
    label: str,
    ctxr_files: List[Path],
    version_map: Dict[str, Tuple[str, str]],
    tri_metadata_map: Dict[str, Dict[str, int]],
    manual_bp_remade_stems: Set[str],
    mc_update_map: Dict[Tuple[str, str], Tuple[str, str]],
    mc_version_date_map: Dict[str, str],
    output_csv: Path,
    missing_png_log: Path,
    pc_nonpow2_match_log: Path,
) -> int:
    if not ctxr_files:
        print(f"No .ctxr files found for dataset: {label}")

        if output_csv.exists():
            output_csv.unlink()

        if missing_png_log.exists():
            missing_png_log.unlink()

        if pc_nonpow2_match_log.exists():
            pc_nonpow2_match_log.unlink()

        return 0

    print(f"[{label}] Found {len(ctxr_files)} .ctxr files to process.")

    rows: List[Tuple] = []
    missing_pngs: List[str] = []
    pc_equal_dims_entries: List[str] = []
    failures = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                process_ctxr,
                ctxr_path,
                version_map,
                tri_metadata_map,
                manual_bp_remade_stems,
                mc_update_map,
                mc_version_date_map,
            )
            for ctxr_path in ctxr_files
        ]

        total = len(futures)

        for index, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()

                if result["status"] == "ok":
                    rows.append(result["row"])

                    pc_equal_dims_log_entry = result.get("pc_equal_dims_log_entry")
                    if pc_equal_dims_log_entry:
                        pc_equal_dims_entries.append(pc_equal_dims_log_entry)

                elif result["status"] == "missing_png":
                    missing_pngs.append(str(result["ctxr_path"]))
            except Exception as exc:
                failures += 1
                print(f"[{label}] Failed: {exc}")

            if index % 1000 == 0 or index == total:
                print(f"[{label}] Processed {index}/{total}")

    write_outputs(
        output_csv=output_csv,
        missing_png_log=missing_png_log,
        pc_nonpow2_match_log=pc_nonpow2_match_log,
        rows=rows,
        missing_pngs=missing_pngs,
        pc_equal_dims_entries=pc_equal_dims_entries,
    )

    print()
    print(f"[{label}] Wrote {len(rows)} rows to: {output_csv}")
    print(f"[{label}] Missing matching PNGs: {len(missing_pngs)}")
    print(f"[{label}] PC raw dims == ciel2 entries: {len(pc_equal_dims_entries)}")

    return failures


# ==========================================================
# MAIN
# ==========================================================
def main() -> int:
    version_map = load_version_dates(VERSION_DATES_CSV)
    print(f"Loaded {len(version_map)} SHA1 version mappings.")

    tri_metadata_map = load_mc_tri_dumped_metadata(MC_TRI_DUMPED_METADATA_CSV)
    print(f"Loaded {len(tri_metadata_map)} MC tri-dumped metadata mappings.")

    manual_bp_remade_stems = load_manual_bp_remade_stems(MANUAL_BP_REMADE_TXT)
    print(f"Loaded {len(manual_bp_remade_stems)} manually identified BP remade stems.")

    mc_version_date_map = load_mc_version_dates(MC_VERSION_DATES_CSV)
    print(f"Loaded {len(mc_version_date_map)} MC version date mappings.")

    mc_update_map = load_mc_texture_update_versions(
        MC_TEXTURE_UPDATE_VERSIONS_CSV,
        mc_version_date_map,
    )
    print(f"Loaded {len(mc_update_map)} MC texture update mappings.")

    win_only_ctxr_files = find_ctxr_files_under_win()
    full_recursive_ctxr_files = find_ctxr_files_full_recursive()

    failures = 0

    failures += process_dataset(
        label="WIN_ONLY",
        ctxr_files=win_only_ctxr_files,
        version_map=version_map,
        tri_metadata_map=tri_metadata_map,
        manual_bp_remade_stems=manual_bp_remade_stems,
        mc_update_map=mc_update_map,
        mc_version_date_map=mc_version_date_map,
        output_csv=OUTPUT_CSV_WIN_ONLY,
        missing_png_log=MISSING_PNG_LOG_WIN_ONLY,
        pc_nonpow2_match_log=PC_NONPOW2_MATCH_LOG_WIN_ONLY,
    )

    print()
    print("=" * 72)
    print()

    failures += process_dataset(
        label="FULL_RECURSIVE",
        ctxr_files=full_recursive_ctxr_files,
        version_map=version_map,
        tri_metadata_map=tri_metadata_map,
        manual_bp_remade_stems=manual_bp_remade_stems,
        mc_update_map=mc_update_map,
        mc_version_date_map=mc_version_date_map,
        output_csv=OUTPUT_CSV_FULL_RECURSIVE,
        missing_png_log=MISSING_PNG_LOG_FULL_RECURSIVE,
        pc_nonpow2_match_log=PC_NONPOW2_MATCH_LOG_FULL_RECURSIVE,
    )

    print()
    if failures:
        print(f"Completed with {failures} failure(s).")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())