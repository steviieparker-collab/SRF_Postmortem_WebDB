#!/usr/bin/env python3
"""
SRF Postmortem — Unified CLI entry point.
"""
import argparse
import sys
from pathlib import Path

from src.core.config import load_config
from src.core.logger import get_logger, setup_logging
from src.orchestrator import SRFOrchestrator

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="SRF Postmortem — Integrated Event Monitoring & Web Viewer",
    )
    parser.add_argument("--config", default="config/config.yaml", help="Config path")
    parser.add_argument("--mode", choices=["batch", "monitor", "web", "full"], help="Override mode")
    parser.add_argument("--input", help="Input directory for batch mode")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")

    args = parser.parse_args()
    config = load_config(args.config)

    if args.mode:
        config.system.mode = args.mode

    if args.verbose:
        config.logging.level = "DEBUG"

    setup_logging(level=config.logging.level)

    orch = SRFOrchestrator(config)

    if args.mode == "web":
        orch.run_web_server()
    else:
        orch.run(mode=args.mode)


if __name__ == "__main__":
    main()
