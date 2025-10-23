import os
import csv
from collections import defaultdict, OrderedDict

def read_csv(path):
    """Read a CSV into a list of (header, rows_dict)."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            return [], []
        header = rows[0]
        data = rows[1:]
        return header, data

def main():
    folder = os.path.dirname(os.path.abspath(__file__))
    csv_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not csv_files:
        print("No CSV files found.")
        return

    print(f"[+] Found {len(csv_files)} CSV files. Merging on first column...")

    merged = defaultdict(OrderedDict)
    all_headers = ["key"]  # first column name placeholder
    header_maps = []

    # Read and normalize all CSVs
    for path in csv_files:
        header, rows = read_csv(path)
        if not header:
            continue
        key_col = header[0]
        rest_cols = header[1:]
        # Add unique columns
        for col in rest_cols:
            if col not in all_headers:
                all_headers.append(col)

        for row in rows:
            if not row:
                continue
            key = row[0]
            if key not in merged:
                merged[key] = {}
            for col, val in zip(header[1:], row[1:]):
                merged[key][col] = val

        print(f"  Merged: {os.path.basename(path)} ({len(rows)} rows)")

    output_path = os.path.join(folder, "merged_output.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(all_headers)
        for key in sorted(merged.keys(), key=str.lower):
            row = [key]
            for col in all_headers[1:]:
                row.append(merged[key].get(col, ""))
            writer.writerow(row)

    print(f"\n[+] Done. Wrote merged file: {output_path}")

if __name__ == "__main__":
    main()
