from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Filter weather station data by temperature threshold and write results to a new CSV."
    )
    p.add_argument("--input", required=True, help="Path to input CSV file")
    p.add_argument("--output", required=True, help="Path to output CSV file")
    p.add_argument("--threshold", type=float, required=True, help="Temperature threshold (C)")
    p.add_argument("--column", default="temp_c", help="Column to filter on (default: temp_c)")
    p.add_argument(
        "--log",
        default="kata-01-files/logs/kata01.log",
        help="Log file path (default: kata-01-files/logs/kata01.log)",
    )
    return p.parse_args()


def append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"{ts} | {message}\n")


def read_csv_rows(input_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    return rows, fieldnames

def read_json_rows(input_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    import json
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, dict):
            rows = data.get("records", [])
        else:
            rows = data
        if rows:
            fieldnames = list(rows[0].keys())
        else:
            fieldnames = []
    return rows, fieldnames

def read_input_rows(input_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if input_path.suffix.lower() == ".json":
        return read_json_rows(input_path)
    else:
        return read_csv_rows(input_path)


def filter_rows(
    rows: list[dict[str, str]],
    column: str,
    threshold: float,
) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    for r in rows:
        raw = r.get(column, "")
        try:
            val = float(raw)
        except (ValueError, TypeError):
            continue
        if val >= threshold:
            kept.append(r)
    return kept


def write_csv_rows(output_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    log_path = Path(args.log)

    # Graceful missing input
    if not input_path.exists():
        msg = f"ERROR input file not found: {input_path}"
        print(msg)
        append_log(log_path, msg)
        return 2

    rows, fieldnames = read_input_rows(input_path)
    kept = filter_rows(rows, args.column, args.threshold)
    write_csv_rows(output_path, fieldnames, kept)

    msg = (
        f"read={len(rows)} wrote={len(kept)} "
        f"threshold={args.threshold} column={args.column} "
        f"input={input_path} output={output_path}"
    )
    append_log(log_path, msg)

    print(f"Read {len(rows)} rows; wrote {len(kept)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

