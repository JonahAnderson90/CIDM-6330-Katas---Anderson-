from pathlib import Path
import argparse
from datetime import datetime


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter weather station data by temperature threshold"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output CSV file",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        required=True,
        help="Temperature threshold (C)",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        timestamp = datetime.utcnow().isoformat()
        print(f"[{timestamp}] ERROR: Input file not found: {input_path}")
        return

    print("Arguments parsed successfully")
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print(f"Threshold: {args.threshold}")


if __name__ == "__main__":
    main()
