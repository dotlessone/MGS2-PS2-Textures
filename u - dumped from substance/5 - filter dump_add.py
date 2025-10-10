import os

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    current_path = os.path.join(base_dir, "current.txt")
    dump_add_path = os.path.join(base_dir, "dump_add.txt")
    output_path = os.path.join(base_dir, "dump_add_filtered.txt")

    # Load current.txt into a set for O(1) lookup
    with open(current_path, "r", encoding="utf-8") as f:
        current_lines = {line.strip() for line in f if line.strip()}

    # Filter dump_add.txt
    with open(dump_add_path, "r", encoding="utf-8") as f:
        dump_lines = [line.strip() for line in f if line.strip()]

    filtered = [line for line in dump_lines if line not in current_lines]

    # Write the results
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        for line in filtered:
            f.write(line + "\n")

    print(f"Done. {len(filtered)} lines written to {output_path}")

if __name__ == "__main__":
    main()
