from __future__ import annotations

import csv
import hashlib
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict
from tqdm import tqdm


def get_git_root() -> Path:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        raise SystemExit("Error: git is not installed or not on PATH.")

    if result.returncode != 0:
        raise SystemExit(f"Error: not inside a git repository.\n{result.stderr.strip()}")

    root = Path(result.stdout.strip())
    if not root.exists():
        raise SystemExit(f"Error: git root path does not exist: {root}")

    return root


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


from concurrent.futures import ThreadPoolExecutor, as_completed

def collect_tga_hashes(root: Path, label: str) -> Dict[str, str]:
    if not root.is_dir():
        raise SystemExit(f"Error: directory does not exist: {root}")

    files = [p for p in root.rglob("*.tga") if p.is_file()]
    result: Dict[str, str] = {}

    def worker(path: Path):
        return path.stem.lower(), sha1_file(path), path

    with ThreadPoolExecutor(max_workers=8) as exe:
        futures = [exe.submit(worker, p) for p in files]

        for f in tqdm(as_completed(futures), total=len(futures), desc=f"Hashing {label}", unit="file"):
            stem_lower, digest, path = f.result()

            if stem_lower in result and result[stem_lower] != digest:
                raise SystemExit(
                    f"Error: conflicting hashes for case-insensitive stem '{stem_lower}' in {root}\n"
                    f"Existing digest: {result[stem_lower]}\nNew from: {path} -> {digest}"
                )

            result[stem_lower] = digest

    return result


def get_unix_timestamp_jst(year: int, month: int, day: int, hour: int, minute: int, second: int) -> int:
    jst = timezone(timedelta(hours=9))
    dt = datetime(year, month, day, hour, minute, second, tzinfo=jst)
    return int(dt.timestamp())


def main() -> None:
    repo_root = get_git_root()

    sol_root = repo_root / "t - dumped from sons of liberty" / "dump" / "Final Rebuilt"
    subs_root = repo_root / "u - dumped from substance" / "dump" / "Final Rebuilt"
    csv_out = repo_root / "u - dumped from substance" / "mgs2_ps2_substance_version_dates.csv"

    print(f"Git root: {repo_root}")
    print(f"Sons of Liberty dir: {sol_root}")
    print(f"Substance dir:       {subs_root}")
    print(f"Output CSV:          {csv_out}")

    sol_hashes = collect_tga_hashes(sol_root, "Sons of Liberty")
    subs_hashes = collect_tga_hashes(subs_root, "Substance")

    print(f"Found {len(sol_hashes)} unique case-insensitive stems in Sons of Liberty Final Rebuilt.")
    print(f"Found {len(subs_hashes)} unique case-insensitive stems in Substance Final Rebuilt.")

    sol_unix = get_unix_timestamp_jst(2001, 9, 27, 22, 17, 38)
    subs_unix = get_unix_timestamp_jst(2002, 10, 24, 11, 31, 17)

    csv_out.parent.mkdir(parents=True, exist_ok=True)

    items = sorted(subs_hashes.items(), key=lambda x: x[0])

    with csv_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["stem", "tga_hash", "origin_date", "origin_version"])

        for stem_lower, subs_hash in tqdm(items, desc="Writing CSV", unit="file"):
            sol_hash = sol_hashes.get(stem_lower)

            if sol_hash is not None and sol_hash == subs_hash:
                origin_version = "sons of liberty"
                origin_date = sol_unix
            else:
                origin_version = "substance"
                origin_date = subs_unix

            writer.writerow([stem_lower, subs_hash, origin_date, origin_version])

    print("Done.")


if __name__ == "__main__":
    main()
