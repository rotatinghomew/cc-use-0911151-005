from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from corridor_twin.twin_service import Application, run, verify
from corridor_twin.backtest.service import BacktestApplication
from corridor_twin.backtest.recommendation import EligibilityConfig
from corridor_twin.visibility.control_tower import (
    export_backtest_bundle,
    verify_backtest_bundle,
)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog='corridor_twin', description='中国—东盟跨境智慧物流走廊数字孪生')
    commands = root.add_subparsers(dest="command", required=True)
    execute = commands.add_parser("run")
    execute.add_argument("--fixture", required=True)
    execute.add_argument("--database", required=True)
    execute.add_argument("--output", required=True)
    check = commands.add_parser("verify")
    check.add_argument("--database", required=True)
    check.add_argument("--output", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--database", required=True)
    backtest = commands.add_parser("backtest")
    backtest.add_argument("--fixture", required=True, help="事件流夹具（含更正事件与异常旅程）")
    backtest.add_argument("--database", required=True)
    backtest.add_argument("--output", required=True)
    backtest.add_argument("--min-samples", type=int, default=3)
    backtest.add_argument("--max-mae-ratio", type=float, default=0.30)
    backtest.add_argument("--min-coverage", type=float, default=0.70)
    backtest.add_argument("--max-envelope-ratio", type=float, default=0.75)
    backtest_verify = commands.add_parser("backtest-verify")
    backtest_verify.add_argument("--output", required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "run":
        result = run(args.fixture, args.database, args.output)
    elif args.command == "verify":
        result = verify(args.database, args.output)
    elif args.command == "backtest":
        application = BacktestApplication(args.database, min_samples=args.min_samples)
        config = EligibilityConfig(
            min_samples=args.min_samples, max_mae_ratio=args.max_mae_ratio,
            min_coverage=args.min_coverage, max_envelope_ratio=args.max_envelope_ratio)
        result = application.run_backtest(args.fixture, config=config)
        result["bundle"] = export_backtest_bundle(args.output, result)
    elif args.command == "backtest-verify":
        errors = verify_backtest_bundle(args.output)
        result = {"ok": not errors, "errors": errors}
    else:
        result = Application(args.database).inspect()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
