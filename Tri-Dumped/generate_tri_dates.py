from __future__ import annotations

import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set, Tuple


REQUIRED_COMPILATION_DATES = "Compilation Dates.csv"
EARLIEST_OUTPUT_CSV = "sha1_earliest_versions.csv"
MISSING_MC_OUTPUT_CSV = "missing_mc_sha1s.csv"
MAX_WORKERS = max(4, os.cpu_count() or 4)

METADATA_CSV = Path(
    r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection\Metadata\mgs2_mc_tri_dumped_metadata.csv"
)

BLASTLIST = {
    Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\Master Collection").resolve(),
    Path(r"C:\Development\Git\MGS2-PS2-Textures\Tri-Dumped\todo").resolve(),
}

PRINT_LOADS = True


@dataclass(frozen=True)
class VersionInfo:
    game: str
    region: str
    compilation_dt_utc: datetime
    compilation_unix: int
    sha1_txt_path: Path


def parse_gmt_offset(value: str) -> timezone:
    value = value.strip()
    if len(value) != 6 or value[0] not in {"+", "-"} or value[3] != ":":
        raise ValueError(f"Invalid GMT_OFFSET: {value}")

    sign = 1 if value[0] == "+" else -1
    hours = int(value[1:3])
    minutes = int(value[4:6])

    delta = timedelta(hours=hours, minutes=minutes) * sign
    return timezone(delta)


def parse_compilation_datetime(date_value: str, time_value: str, gmt_offset: str) -> Tuple[datetime, int]:
    tz = parse_gmt_offset(gmt_offset)

    dt_local = datetime.strptime(
        f"{date_value} {time_value}",
        "%Y-%m-%d %H:%M:%S.%f"
    ).replace(tzinfo=tz)

    dt_utc = dt_local.astimezone(timezone.utc)
    unix_time = int(dt_utc.timestamp())

    return dt_utc, unix_time


def load_compilation_regions(csv_path: Path) -> List[Tuple[str, datetime, int]]:
    out: List[Tuple[str, datetime, int]] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        expected_columns = {"REGION", "DATE", "TIME", "GMT_OFFSET"}
        if reader.fieldnames is None:
            raise RuntimeError(f"CSV has no header: {csv_path}")

        missing_columns = expected_columns - set(reader.fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"CSV is missing required columns {sorted(missing_columns)}: {csv_path}"
            )

        for row_number, row in enumerate(reader, start=2):
            region = (row.get("REGION") or "").strip()
            date_value = (row.get("DATE") or "").strip()
            time_value = (row.get("TIME") or "").strip()
            gmt_offset = (row.get("GMT_OFFSET") or "").strip()

            if not region:
                raise RuntimeError(f"Blank REGION at {csv_path}:{row_number}")
            if not date_value:
                raise RuntimeError(f"Blank DATE for REGION '{region}' at {csv_path}:{row_number}")
            if not time_value:
                raise RuntimeError(f"Blank TIME for REGION '{region}' at {csv_path}:{row_number}")
            if not gmt_offset:
                raise RuntimeError(f"Blank GMT_OFFSET for REGION '{region}' at {csv_path}:{row_number}")

            dt_utc, unix_time = parse_compilation_datetime(
                date_value,
                time_value,
                gmt_offset,
            )

            out.append((region, dt_utc, unix_time))

    if not out:
        raise RuntimeError(f"No regions found in: {csv_path}")

    return out


def discover_versions(subfolder: Path) -> Tuple[Path, List[str], List[VersionInfo]]:
    errors: List[str] = []
    versions: List[VersionInfo] = []

    compilation_dates_path = subfolder / REQUIRED_COMPILATION_DATES
    if not compilation_dates_path.is_file():
        errors.append(f"Missing required file: {compilation_dates_path}")
        return subfolder, errors, versions

    try:
        regions = load_compilation_regions(compilation_dates_path)
    except Exception as exc:
        errors.append(f"Failed to parse {compilation_dates_path}: {exc}")
        return subfolder, errors, versions

    game_name = subfolder.name

    for region, dt_utc, unix_time in regions:
        region_folder = subfolder / region
        if not region_folder.is_dir():
            errors.append(f"Missing required region folder: {region_folder}")
            continue

        sha1_txt_path = region_folder / f"{game_name}_{region}_ALL_SHA1s.txt"
        if not sha1_txt_path.is_file():
            errors.append(f"Missing required file: {sha1_txt_path}")
            continue

        versions.append(
            VersionInfo(
                game=game_name,
                region=region,
                compilation_dt_utc=dt_utc,
                compilation_unix=unix_time,
                sha1_txt_path=sha1_txt_path,
            )
        )

    return subfolder, errors, versions


def load_sha1s(version: VersionInfo) -> Tuple[VersionInfo, List[str], Set[str]]:
    errors: List[str] = []
    sha1s: Set[str] = set()

    try:
        with version.sha1_txt_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                sha1 = raw_line.strip().lower()

                if not sha1:
                    continue

                if len(sha1) != 40 or any(ch not in "0123456789abcdef" for ch in sha1):
                    errors.append(
                        f"Invalid SHA1 in {version.sha1_txt_path}:{line_number}: {raw_line.rstrip()}"
                    )
                    continue

                sha1s.add(sha1)
    except Exception as exc:
        errors.append(f"Failed to read {version.sha1_txt_path}: {exc}")

    return version, errors, sha1s


def load_metadata_sha1s(csv_path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames is None:
            raise RuntimeError(f"No header in {csv_path}")

        required_columns = {"texture_name", "mc_tri_dumped_sha1"}
        missing_columns = required_columns - set(reader.fieldnames)
        if missing_columns:
            raise RuntimeError(
                f"Metadata CSV is missing required columns {sorted(missing_columns)}: {csv_path}"
            )

        for row_number, row in enumerate(reader, start=2):
            texture = (row.get("texture_name") or "").strip().lower()
            sha1 = (row.get("mc_tri_dumped_sha1") or "").strip().lower()

            if not texture and not sha1:
                continue

            if not texture:
                raise RuntimeError(f"Blank texture_name at {csv_path}:{row_number}")

            if not sha1:
                raise RuntimeError(f"Blank mc_tri_dumped_sha1 for '{texture}' at {csv_path}:{row_number}")

            if len(sha1) != 40 or any(ch not in "0123456789abcdef" for ch in sha1):
                raise RuntimeError(f"Invalid mc_tri_dumped_sha1 for '{texture}' at {csv_path}:{row_number}: {sha1}")

            out[sha1] = texture

    if not out:
        raise RuntimeError(f"No metadata SHA1s found in: {csv_path}")

    return out


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    earliest_output_csv_path = script_dir / EARLIEST_OUTPUT_CSV
    missing_mc_output_csv_path = script_dir / MISSING_MC_OUTPUT_CSV

    all_subfolders: List[Path] = sorted(
        path.resolve()
        for path in script_dir.iterdir()
        if path.is_dir()
    )

    subfolders: List[Path] = [
        path for path in all_subfolders
        if path not in BLASTLIST
    ]

    if not subfolders:
        raise RuntimeError(f"No non-blastlisted subfolders found in: {script_dir}")

    discovery_errors: Dict[Path, List[str]] = {}
    all_versions: List[VersionInfo] = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(subfolders))) as executor:
        futures = [executor.submit(discover_versions, subfolder) for subfolder in subfolders]

        for future in as_completed(futures):
            subfolder, errors, versions = future.result()
            if errors:
                discovery_errors[subfolder] = errors
            else:
                all_versions.extend(versions)

    if discovery_errors:
        lines: List[str] = ["Validation failed."]
        for subfolder in sorted(discovery_errors):
            lines.append("")
            lines.append(f"[{subfolder}]")
            lines.extend(discovery_errors[subfolder])
        raise FileNotFoundError("\n".join(lines))

    metadata_map = load_metadata_sha1s(METADATA_CSV)
    metadata_sha1s = set(metadata_map.keys())

    read_errors: List[str] = []
    earliest_by_sha1: Dict[str, VersionInfo] = {}
    all_loaded_sha1s: Set[str] = set()
    map_lock = Lock()

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(all_versions))) as executor:
        futures = [executor.submit(load_sha1s, version) for version in all_versions]

        for future in as_completed(futures):
            version, errors, sha1s = future.result()

            if errors:
                read_errors.extend(errors)
                continue

            if PRINT_LOADS:
                print(
                    f"SHA1s Loaded - Game: {version.game} "
                    f"Version: {version.region} "
                    f"Compilation Time: {version.compilation_unix}"
                )

            with map_lock:
                all_loaded_sha1s.update(sha1s)

                for sha1 in sha1s:
                    existing = earliest_by_sha1.get(sha1)
                    if existing is None or version.compilation_dt_utc < existing.compilation_dt_utc:
                        earliest_by_sha1[sha1] = version

    if read_errors:
        raise RuntimeError("\n".join(read_errors))

    earliest_rows = [
        (sha1, v.game, v.region, v.compilation_unix)
        for sha1, v in earliest_by_sha1.items()
    ]
    earliest_rows.sort()

    with earliest_output_csv_path.open("w", encoding="utf-8", newline="\n") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["sha1", "game", "version", "first_seen_unix"])
        writer.writerows(earliest_rows)

    missing_mc_sha1s = metadata_sha1s - all_loaded_sha1s

    print()
    print(f"Wrote {len(earliest_rows)} unique SHA1 rows to: {earliest_output_csv_path}")

    if missing_mc_sha1s:
        rows = [(metadata_map[s], s) for s in sorted(missing_mc_sha1s)]

        with missing_mc_output_csv_path.open("w", encoding="utf-8", newline="\n") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(["texture_name", "sha1"])
            writer.writerows(rows)

        print(f"Wrote {len(rows)} missing MC SHA1 rows to: {missing_mc_output_csv_path}")
    else:
        if missing_mc_output_csv_path.exists():
            missing_mc_output_csv_path.unlink()
        print("No missing MC SHA1s found.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise