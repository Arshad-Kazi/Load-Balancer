import asyncio
import argparse
import json
import logging
import sys
from pathlib import Path

from lb import LoadBalancer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def parse_args():
    p = argparse.ArgumentParser(description="Round-Robin Load Balancer")
    p.add_argument(
        "--config",
        default="config/config.json",
        help="Path to JSON config file (default: config/config.json)",
    )
    return p.parse_args()


def load_config(path: str) -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        print(f"[ERROR] Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    with cfg_path.open() as f:
        return json.load(f)


def main():
    args = parse_args()
    config = load_config(args.config)
    lb = LoadBalancer(config)
    try:
        asyncio.run(lb.run())
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()