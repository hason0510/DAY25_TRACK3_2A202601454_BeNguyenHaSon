from __future__ import annotations

import argparse
import json
from pathlib import Path

from reliability_lab.chaos import load_queries, run_simulation
from reliability_lab.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--out", default="reports/metrics.json")
    parser.add_argument(
        "--requests",
        type=int,
        default=None,
        help="Override load_test.requests (per scenario).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Override load_test.concurrency (1 = sequential).",
    )
    parser.add_argument(
        "--details",
        default=None,
        help="Optional path for the per-scenario breakdown (defaults next to --out).",
    )
    args = parser.parse_args()
    config = load_config(args.config)
    if args.requests is not None:
        config.load_test.requests = args.requests
    if args.concurrency is not None:
        config.load_test.concurrency = args.concurrency
    metrics = run_simulation(config, load_queries())

    out_path = Path(args.out)
    metrics.write_json(out_path)
    csv_path = out_path.with_suffix(".csv")
    metrics.write_csv(csv_path)

    details_path = Path(args.details) if args.details else out_path.with_name(
        out_path.stem + "_scenarios.json"
    )
    details_path.parent.mkdir(parents=True, exist_ok=True)
    details_path.write_text(
        json.dumps(metrics.scenario_details, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"wrote {out_path}")
    print(f"wrote {csv_path}")
    print(f"wrote {details_path}")


if __name__ == "__main__":
    main()
