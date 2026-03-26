from __future__ import annotations

import csv
import hashlib
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Optional, Set, Tuple


# ==========================================================
# CONFIG
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent

TEXTURE_MAPPINGS_CSV = SCRIPT_DIR / "mgs2_texture_strcode_mappings.csv"
TRI_MAPPINGS_CSV = SCRIPT_DIR / "mgs2_tri_mappings.csv"

# Dry mode is ON by default.
DRY_RUN = False

# Add the region folders you want to process here.
PROCESS_REGION_FOLDERS = [
    # "us",
    # "eu",
    "jp",
]


MANUAL_CONFLICT_SHA1_OVERRIDES: Dict[str, str] = {
    "ba34283c172431fa75f69d68824c7d23d92fb6c2": "0009bc34_b930b4499997afc4e26e408ea169f9c7",
    "eda27df6e2ba6d8b30a0606f3ec5e02b4bc5fb29": "act_telop5_alp_ovl.bmp_e350349959b1556776f7b5d2e07689fc",
    "fec0481213902971719ca44ae0962dee41bb8b22": "act_telop5_alp_ovl.bmp",
}

MAX_WORKERS = max(4, os.cpu_count() or 4)
SHA1_BUFFER_SIZE = 8 * 1024 * 1024
OPEN_CONFLICT_FOLDERS = True

PNG_SUFFIX = ".png"


# ==========================================================
# TYPES / DATA CLASSES
# ==========================================================
ComboKey = Tuple[str, str, str]       # (stage, tri_strcode, texture_strcode)
TriAliasKey = Tuple[str, str]         # (region_folder, tri_name)
StageTriKey = Tuple[str, str]         # (stage, tri_strcode)


@dataclass(frozen=True)
class ResolvedNestedFile:
    path: Path
    region_folder: str
    tri_name: str
    stage: str
    tri_strcode: str
    texture_strcode: str
    texture_name: str


@dataclass
class RegionState:
    region_root: Path
    dry_run: bool
    removed_paths: Set[Path] = field(default_factory=set)
    planned_root_sources: Dict[str, Path] = field(default_factory=dict)  # texture_name -> source path or planned destination source
    sha1_cache: Dict[Path, str] = field(default_factory=dict)
    sha1_cache_lock: Lock = field(default_factory=Lock)
    actions_taken: int = 0


# ==========================================================
# HELPERS
# ==========================================================
def pause_and_exit(message: str, exit_code: int = 1) -> None:
    print()
    print("ERROR")
    print(message)
    print()
    try:
        input("Press ENTER to exit...")
    except KeyboardInterrupt:
        pass
    raise SystemExit(exit_code)


def normalize(value: str) -> str:
    return value.strip()


def iter_filtered_lines(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for raw_line in f:
            stripped = raw_line.lstrip()

            if not stripped.strip():
                continue

            if stripped.startswith(";"):
                continue

            yield raw_line


def load_texture_mappings(path: Path) -> Tuple[Dict[ComboKey, str], Dict[ComboKey, List[str]]]:
    combo_to_names: Dict[ComboKey, Set[str]] = defaultdict(set)

    reader = csv.DictReader(iter_filtered_lines(path))
    required_columns = {"texture_name", "stage", "tri_strcode", "texture_strcode"}

    if reader.fieldnames is None:
        pause_and_exit(f"Failed to read header from {path}")

    missing = required_columns - set(reader.fieldnames)

    if missing:
        pause_and_exit(f"{path.name} is missing required columns: {', '.join(sorted(missing))}")

    for row in reader:
        texture_name = normalize(row["texture_name"])
        stage = normalize(row["stage"])
        tri_strcode = normalize(row["tri_strcode"])
        texture_strcode = normalize(row["texture_strcode"])

        if not texture_name or not stage or not tri_strcode or not texture_strcode:
            continue

        combo = (stage, tri_strcode, texture_strcode)
        combo_to_names[combo].add(texture_name)

    unique_map: Dict[ComboKey, str] = {}
    conflict_map: Dict[ComboKey, List[str]] = {}

    for combo, names in combo_to_names.items():
        sorted_names = sorted(names)

        if len(sorted_names) == 1:
            unique_map[combo] = sorted_names[0]
        else:
            conflict_map[combo] = sorted_names

    return unique_map, conflict_map


def load_tri_aliases(path: Path) -> Dict[TriAliasKey, Set[StageTriKey]]:
    alias_map: Dict[TriAliasKey, Set[StageTriKey]] = defaultdict(set)

    reader = csv.DictReader(iter_filtered_lines(path))
    required_columns = {"tri_name", "region_folder", "stage", "tri_strcode"}

    if reader.fieldnames is None:
        pause_and_exit(f"Failed to read header from {path}")

    missing = required_columns - set(reader.fieldnames)

    if missing:
        pause_and_exit(f"{path.name} is missing required columns: {', '.join(sorted(missing))}")

    for row in reader:
        tri_name = normalize(row["tri_name"])
        region_folder = normalize(row["region_folder"])
        stage = normalize(row["stage"])
        tri_strcode = normalize(row["tri_strcode"])

        if not tri_name or not region_folder or not stage or not tri_strcode:
            continue

        alias_map[(region_folder, tri_name)].add((stage, tri_strcode))

    return dict(alias_map)


def iter_region_pngs(region_root: Path) -> Iterable[Path]:
    yield from sorted(region_root.rglob(f"*{PNG_SUFFIX}"))


def is_direct_region_file(path: Path, region_root: Path) -> bool:
    return path.parent == region_root


def get_region_texture_dest(region_root: Path, texture_name: str) -> Path:
    return region_root / f"{texture_name}{PNG_SUFFIX}"


def safe_rel(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def open_folder_in_explorer(path: Path) -> None:
    if not OPEN_CONFLICT_FOLDERS:
        return

    if not hasattr(os, "startfile"):
        return

    try:
        os.startfile(str(path))
    except OSError:
        pass


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(SHA1_BUFFER_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()


def get_cached_sha1(state: RegionState, path: Path) -> str:
    with state.sha1_cache_lock:
        cached = state.sha1_cache.get(path)

    if cached is not None:
        return cached

    digest = sha1_of_file(path)

    with state.sha1_cache_lock:
        state.sha1_cache[path] = digest

    return digest


def batch_sha1(state: RegionState, paths: Iterable[Path]) -> Dict[Path, str]:
    unique_paths = sorted(set(paths))
    result: Dict[Path, str] = {}

    to_hash: List[Path] = []

    with state.sha1_cache_lock:
        for path in unique_paths:
            cached = state.sha1_cache.get(path)

            if cached is not None:
                result[path] = cached
            else:
                to_hash.append(path)

    if not to_hash:
        return result

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(sha1_of_file, path): path for path in to_hash}

        for future in as_completed(future_map):
            path = future_map[future]
            digest = future.result()
            result[path] = digest

    with state.sha1_cache_lock:
        for path, digest in result.items():
            state.sha1_cache[path] = digest

    return result


def resolve_nested_png(
    path: Path,
    region_root: Path,
    tri_alias_map: Dict[TriAliasKey, Set[StageTriKey]],
    unique_texture_map: Dict[ComboKey, str],
    conflict_texture_map: Dict[ComboKey, List[str]],
) -> Tuple[Optional[ResolvedNestedFile], Optional[ComboKey], Optional[str]]:
    if is_direct_region_file(path, region_root):
        return None, None, None

    rel = path.relative_to(region_root)
    parts = rel.parts

    if len(parts) < 2:
        return None, None, f"Unexpected nested layout: {rel}"

    region_folder = normalize(region_root.name)
    tri_name = normalize(parts[0])
    texture_strcode = path.stem

    alias_key = (region_folder, tri_name)
    alias_values = tri_alias_map.get(alias_key)

    if not alias_values:
        return None, None, f"No tri alias for region='{region_folder}', tri_name='{tri_name}'"

    matched_unique_combo: Optional[ComboKey] = None
    matched_conflict_combo: Optional[ComboKey] = None

    for stage, tri_strcode in sorted(alias_values):
        combo = (stage, tri_strcode, texture_strcode)

        if combo in conflict_texture_map:
            if matched_unique_combo is not None:
                return None, None, (
                    f"Both unique and conflicting mappings matched for "
                    f"region='{region_folder}', tri_name='{tri_name}', texture_strcode='{texture_strcode}'"
                )

            if matched_conflict_combo is not None and matched_conflict_combo != combo:
                return None, None, (
                    f"Multiple conflicting mappings matched for "
                    f"region='{region_folder}', tri_name='{tri_name}', texture_strcode='{texture_strcode}'"
                )

            matched_conflict_combo = combo
            continue

        if combo in unique_texture_map:
            if matched_conflict_combo is not None:
                return None, None, (
                    f"Both conflicting and unique mappings matched for "
                    f"region='{region_folder}', tri_name='{tri_name}', texture_strcode='{texture_strcode}'"
                )

            if matched_unique_combo is not None and matched_unique_combo != combo:
                return None, None, (
                    f"Ambiguous unique mapping match for "
                    f"region='{region_folder}', tri_name='{tri_name}', texture_strcode='{texture_strcode}'"
                )

            matched_unique_combo = combo

    if matched_conflict_combo is not None:
        return None, matched_conflict_combo, None

    if matched_unique_combo is None:
        return None, None, (
            f"No texture mapping match for region='{region_folder}', tri_name='{tri_name}', "
            f"texture_strcode='{texture_strcode}'"
        )

    stage, tri_strcode, _ = matched_unique_combo
    texture_name = unique_texture_map[matched_unique_combo]

    resolved = ResolvedNestedFile(
        path=path,
        region_folder=region_folder,
        tri_name=tri_name,
        stage=stage,
        tri_strcode=tri_strcode,
        texture_strcode=texture_strcode,
        texture_name=texture_name,
    )
    return resolved, None, None


def action_delete(state: RegionState, path: Path) -> None:
    if path in state.removed_paths:
        return

    if state.dry_run:
        print(f"[DRY] DELETE  {safe_rel(path, state.region_root)}")
        state.removed_paths.add(path)
        state.actions_taken += 1
        return

    path.unlink()
    print(f"DELETE       {safe_rel(path, state.region_root)}")
    state.removed_paths.add(path)
    state.actions_taken += 1


def action_move_to_root(state: RegionState, source: Path, texture_name: str) -> None:
    dest = get_region_texture_dest(state.region_root, texture_name)

    if state.dry_run:
        print(f"[DRY] MOVE    {safe_rel(source, state.region_root)} -> {texture_name}{PNG_SUFFIX}")
        state.removed_paths.add(source)
        state.planned_root_sources[texture_name] = source
        state.actions_taken += 1
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    source.rename(dest)
    print(f"MOVE         {safe_rel(source, state.region_root)} -> {texture_name}{PNG_SUFFIX}")
    state.removed_paths.add(source)
    state.planned_root_sources[texture_name] = dest
    state.actions_taken += 1


def region_root_source_for_texture(state: RegionState, texture_name: str) -> Optional[Path]:
    planned = state.planned_root_sources.get(texture_name)

    if planned is not None:
        return planned

    actual = get_region_texture_dest(state.region_root, texture_name)

    if actual.exists() and actual not in state.removed_paths:
        return actual

    return None


def consolidate_manual_selection(state: RegionState, source: Path, texture_name: str) -> None:
    existing_root_source = region_root_source_for_texture(state, texture_name)

    if existing_root_source is not None:
        sha_map = batch_sha1(state, [source, existing_root_source])

        if sha_map[source] != sha_map[existing_root_source]:
            pause_and_exit(
                f"Manual conflict resolution would collide with existing root texture '{texture_name}{PNG_SUFFIX}' "
                f"but SHA1 does not match.\n"
                f"Existing root source: {existing_root_source}\n"
                f"Selected source:      {source}"
            )

        action_delete(state, source)
        return

    action_move_to_root(state, source, texture_name)

def get_manual_conflict_override_texture_name(
    state: RegionState,
    source: Path,
    possible_names: List[str],
) -> Optional[str]:
    source_sha1 = get_cached_sha1(state, source).lower()
    forced_texture_name = MANUAL_CONFLICT_SHA1_OVERRIDES.get(source_sha1)

    if forced_texture_name is None:
        return None

    if forced_texture_name not in possible_names:
        pause_and_exit(
            f"Manual conflict SHA1 override matched '{source}', but forced texture_name "
            f"'{forced_texture_name}' is not a valid candidate for this file.\n"
            f"SHA1: {source_sha1}\n"
            f"Valid candidates: {', '.join(possible_names)}"
        )

    return forced_texture_name

def prompt_for_manual_conflict_resolution(
    state: RegionState,
    conflict_file: Path,
    combo: ComboKey,
    possible_names: List[str],
) -> None:
    stage, tri_strcode, texture_strcode = combo
    folder = conflict_file.parent

    print()
    print("==========================================================")
    print("MANUAL CONFLICT RESOLUTION REQUIRED")
    print("==========================================================")
    print(f"Region folder   : {state.region_root.name}")
    print(f"Folder          : {folder}")
    print(f"File            : {conflict_file.name}")
    print(f"Stage           : {stage}")
    print(f"Tri strcode     : {tri_strcode}")
    print(f"Texture strcode : {texture_strcode}")
    print("Possible texture_name values:")

    for index, texture_name in enumerate(possible_names, start=1):
        print(f"  {index}) {texture_name}")

    lookup_string = f"mgs2 /base /<{'|'.join(possible_names)}>.png"

    print()
    print("Lookup:")
    print(lookup_string)
    print()

    forced_texture_name = get_manual_conflict_override_texture_name(
        state=state,
        source=conflict_file,
        possible_names=possible_names,
    )

    if forced_texture_name is not None:
        print(f"Auto-resolved by SHA1 override: {forced_texture_name}")
        consolidate_manual_selection(state, conflict_file, forced_texture_name)
        return

    open_folder_in_explorer(folder)

    while True:
        try:
            raw = input("Select the correct texture_name number (or 'q' to quit): ").strip()
        except KeyboardInterrupt:
            print()
            pause_and_exit("Interrupted during manual conflict resolution.")

        if raw.lower() in {"q", "quit", "exit"}:
            pause_and_exit("User cancelled during manual conflict resolution.", exit_code=0)

        if not raw.isdigit():
            print("Invalid selection.")
            continue

        choice = int(raw)

        if not (1 <= choice <= len(possible_names)):
            print("Selection out of range.")
            continue

        selected_texture_name = possible_names[choice - 1]
        consolidate_manual_selection(state, conflict_file, selected_texture_name)
        return

def preflight_manual_conflicts(
    state: RegionState,
    tri_alias_map: Dict[TriAliasKey, Set[StageTriKey]],
    unique_texture_map: Dict[ComboKey, str],
    conflict_texture_map: Dict[ComboKey, List[str]],
) -> None:
    pending: List[Tuple[Path, ComboKey]] = []

    for path in iter_region_pngs(state.region_root):
        if path in state.removed_paths:
            continue

        resolved, conflict_combo, warning = resolve_nested_png(
            path=path,
            region_root=state.region_root,
            tri_alias_map=tri_alias_map,
            unique_texture_map=unique_texture_map,
            conflict_texture_map=conflict_texture_map,
        )

        if warning is not None:
            continue

        if conflict_combo is None:
            continue

        pending.append((path, conflict_combo))

    if not pending:
        print(f"[{state.region_root.name}] No manual conflicts found.")
        return

    print(f"[{state.region_root.name}] Found {len(pending)} manually resolved conflicting file(s).")

    for path, combo in pending:
        if path in state.removed_paths:
            continue

        possible_names = conflict_texture_map[combo]
        prompt_for_manual_conflict_resolution(state, path, combo, possible_names)


def collect_resolved_nested_groups(
    state: RegionState,
    tri_alias_map: Dict[TriAliasKey, Set[StageTriKey]],
    unique_texture_map: Dict[ComboKey, str],
    conflict_texture_map: Dict[ComboKey, List[str]],
) -> Tuple[Dict[str, List[ResolvedNestedFile]], List[str]]:
    groups: Dict[str, List[ResolvedNestedFile]] = defaultdict(list)
    warnings: List[str] = []

    for path in iter_region_pngs(state.region_root):
        if path in state.removed_paths:
            continue

        if is_direct_region_file(path, state.region_root):
            continue

        resolved, conflict_combo, warning = resolve_nested_png(
            path=path,
            region_root=state.region_root,
            tri_alias_map=tri_alias_map,
            unique_texture_map=unique_texture_map,
            conflict_texture_map=conflict_texture_map,
        )

        if conflict_combo is not None:
            stage, tri_strcode, texture_strcode = conflict_combo
            warnings.append(
                f"Unresolved conflict remains: {safe_rel(path, state.region_root)} "
                f"-> ({stage}, {tri_strcode}, {texture_strcode})"
            )
            continue

        if warning is not None:
            warnings.append(f"{safe_rel(path, state.region_root)}: {warning}")
            continue

        if resolved is None:
            continue

        groups[resolved.texture_name].append(resolved)

    for texture_name in groups:
        groups[texture_name].sort(key=lambda item: str(item.path).lower())

    return dict(sorted(groups.items())), warnings


def process_resolved_groups(state: RegionState, groups: Dict[str, List[ResolvedNestedFile]]) -> None:
    for texture_name, items in groups.items():
        nested_paths = [item.path for item in items]
        root_source = region_root_source_for_texture(state, texture_name)

        if root_source is not None:
            sha_map = batch_sha1(state, nested_paths + [root_source])
            root_sha1 = sha_map[root_source]

            mismatches = [path for path in nested_paths if sha_map[path] != root_sha1]

            if mismatches:
                pause_and_exit(
                    f"SHA1 mismatch for texture_name '{texture_name}'.\n"
                    f"Root source: {root_source}\n"
                    f"Mismatching nested file: {mismatches[0]}"
                )

            for path in nested_paths:
                action_delete(state, path)

            continue

        sha_map = batch_sha1(state, nested_paths)
        unique_sha1s = {sha_map[path] for path in nested_paths}

        if len(unique_sha1s) != 1:
            sorted_paths = "\n".join(f"  {path}" for path in nested_paths)
            pause_and_exit(
                f"Multiple files mapping to '{texture_name}' do not all share the same SHA1.\n"
                f"Files:\n{sorted_paths}"
            )

        keep_path = nested_paths[0]
        delete_paths = nested_paths[1:]

        action_move_to_root(state, keep_path, texture_name)

        for path in delete_paths:
            action_delete(state, path)


def validate_region_folder(region_root: Path) -> None:
    if not region_root.exists():
        pause_and_exit(f"Region folder does not exist: {region_root}")

    if not region_root.is_dir():
        pause_and_exit(f"Region path is not a folder: {region_root}")


# ==========================================================
# MAIN
# ==========================================================
def main() -> None:
    print("Loading CSV mappings...")

    unique_texture_map, conflict_texture_map = load_texture_mappings(TEXTURE_MAPPINGS_CSV)
    tri_alias_map = load_tri_aliases(TRI_MAPPINGS_CSV)

    print(f"Unique stage/tri_strcode/texture_strcode mappings   : {len(unique_texture_map)}")
    print(f"Conflicting stage/tri_strcode/texture_strcode combos: {len(conflict_texture_map)}")
    print(f"Tri alias buckets                                    : {len(tri_alias_map)}")
    print(f"Dry run                                              : {DRY_RUN}")
    print(f"Workers                                              : {MAX_WORKERS}")
    print()

    if not PROCESS_REGION_FOLDERS:
        pause_and_exit("PROCESS_REGION_FOLDERS is empty.")

    for region_name in PROCESS_REGION_FOLDERS:
        region_name = normalize(region_name)

        if not region_name:
            continue

        region_root = SCRIPT_DIR / region_name
        validate_region_folder(region_root)

        print("==========================================================")
        print(f"PROCESSING REGION: {region_name}")
        print("==========================================================")

        state = RegionState(region_root=region_root, dry_run=DRY_RUN)

        print()
        print("1) PREFLIGHT MANUAL CONFLICT RESOLUTION")
        preflight_manual_conflicts(
            state=state,
            tri_alias_map=tri_alias_map,
            unique_texture_map=unique_texture_map,
            conflict_texture_map=conflict_texture_map,
        )

        print()
        print("2) CONSOLIDATING REMAINING NESTED PNGS")
        groups, warnings = collect_resolved_nested_groups(
            state=state,
            tri_alias_map=tri_alias_map,
            unique_texture_map=unique_texture_map,
            conflict_texture_map=conflict_texture_map,
        )

        if warnings:
            print()
            print("Warnings")
            print("--------")
            for warning in warnings:
                print(warning)
            print()

        if not groups:
            print(f"[{region_name}] No resolvable nested PNGs found.")
        else:
            print(f"[{region_name}] Resolved texture groups: {len(groups)}")
            process_resolved_groups(state, groups)

        print()
        print(f"[{region_name}] Actions planned/executed: {state.actions_taken}")
        print()

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print()
        pause_and_exit("Interrupted by user.", exit_code=1)
    except Exception as exc:
        pause_and_exit(f"Unhandled exception: {exc}", exit_code=1)