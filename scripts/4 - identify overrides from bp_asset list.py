import csv
from collections import defaultdict

def main():
    input_file = "bp_assets_mgs2.csv"
    output_file = "duplicate_hashes_log.txt"

    hash_map = defaultdict(list)

    with open(input_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 2:
                continue
            filename, hash_val = row[0].strip(), row[1].strip()
            hash_map[hash_val].append(filename)

    with open(output_file, "w", encoding="utf-8") as f:
        for hash_val, filenames in hash_map.items():
            if len(filenames) > 1:
                for filename in filenames:
                    f.write(f"{filename},{hash_val}\n")

    print(f"Done. Logged duplicate hashes to {output_file}")

if __name__ == "__main__":
    main()
