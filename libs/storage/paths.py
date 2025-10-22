from pathlib import Path


BASE = Path("/data")
RAW = BASE / "raw"
CLEAN = BASE / "clean"
RAW.mkdir(parents=True, exist_ok=True)
CLEAN.mkdir(parents=True, exist_ok=True)