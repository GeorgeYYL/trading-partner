# thin wrapper for Makefile target; could be moved to Prefect deployments later
from apps.scheduler.flow_daily import run


if __name__ == "__main__":
    run(symbol="AAPL")