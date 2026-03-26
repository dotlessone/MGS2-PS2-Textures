import csv
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ==========================================================
# CONFIG
# ==========================================================
CSV_REL_PATH = Path(r"u - dumped from substance/mgs2_mc_dimensions.csv")
MAX_WORKERS = 8


# ==========================================================
# HELPERS
# ==========================================================
def get_git_root() -> Path:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL
        )
        return Path(out.decode().strip())
    except Exception:
        raise SystemExit("Not inside a git repo or cannot resolve git root.")


def is_pow2(v: int) -> bool:
    return v > 0 and (v & (v - 1)) == 0


def check_row(row):
    try:
        name = row["texture_name"].strip()
        w = int(row["mc_width"])
        h = int(row["mc_height"])
    except Exception:
        return None

    if not is_pow2(w) or not is_pow2(h):
        return name

    return None


# ==========================================================
# MAIN
# ==========================================================
def main():
    git_root = get_git_root()
    csv_path = git_root / CSV_REL_PATH

    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    out_path = csv_path.parent / "mgs2_mc_npots.txt"
    results = []

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(check_row, row) for row in rows]
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                results.append(r)

    results.sort()

    with out_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(results))

    print(f"Done. {len(results)} NPOT textures → {out_path}")


if __name__ == "__main__":
    main()
