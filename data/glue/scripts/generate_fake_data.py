#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd


def build_fake_events(rows: int, seed: int) -> pd.DataFrame:
    random.seed(seed)
    now = datetime.now().replace(microsecond=0)
    event_types = ["claim", "update", "enrollment", "deletion"]

    data = []
    for i in range(1, rows + 1):
        data.append(
            {
                "event_id": i,
                "member_id": random.randint(1, 1_000_000),
                "event_type": random.choice(event_types),
                "amount": round(random.uniform(10.0, 5000.0), 2),
                "event_timestamp": now - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23)),
            }
        )

    df = pd.DataFrame(data)
    return df.astype(
        {
            "event_id": "int64",
            "member_id": "int64",
            "event_type": "string",
            "amount": "float64",
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate one fake parquet file")
    parser.add_argument("--rows", type=int, default=200, help="Number of rows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", default=None, help="Optional output parquet file path")
    args = parser.parse_args()

    if args.rows <= 0:
        raise ValueError("rows must be greater than 0")

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path("data/fake") / f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = build_fake_events(rows=args.rows, seed=args.seed)
    df.to_parquet(output_path, index=False, coerce_timestamps="us")

    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
