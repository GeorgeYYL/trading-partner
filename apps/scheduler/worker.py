# Minimal runner so we can: python apps/scheduler/worker.py --symbol AAPL
import argparse
from apps.scheduler.flow_daily import run  # ✅ 改为绝对导入更稳

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="AAPL")
    args = p.parse_args()
    print(run(symbol=args.symbol))
