"""
Start the Financial Market Intelligence Data Warehouse.
Run with: python start.py
"""

import os, sys, time, subprocess
from pathlib import Path

BASE   = Path(__file__).parent
VENV   = BASE / "venv"
PYTHON = str(VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
PIP    = str(VENV / ("Scripts/pip.exe"    if os.name == "nt" else "bin/pip"))


def load_env():
    env_file = BASE / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def run(*cmd):
    result = subprocess.run(list(cmd), cwd=BASE, shell=(os.name == "nt"))
    if result.returncode != 0:
        sys.exit(result.returncode)


def wait_for(name, check):
    print(f"  Waiting for {name}", end="", flush=True)
    for _ in range(30):
        r = subprocess.run(check, capture_output=True, cwd=BASE, shell=(os.name == "nt"))
        if r.returncode == 0:
            print(" — ready")
            return
        print(".", end="", flush=True)
        time.sleep(2)
    print(f"\n  ERROR: {name} did not start in time.")
    sys.exit(1)


def main():
    load_env()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY is not set in .env")
        sys.exit(1)

    # 1. Databases
    print("[1/5] Starting databases...")
    run("docker", "compose", "up", "-d")

    # 2. Python environment
    print("\n[2/5] Setting up Python environment...")
    if not VENV.exists():
        run(sys.executable, "-m", "venv", str(VENV))
    run(PIP, "install", "-q", "-r", "requirements.txt")

    # 3. Wait for databases to accept connections
    print("\n[3/5] Waiting for databases...")
    wait_for("MySQL", [PYTHON, "-c",
        "import mysql.connector; mysql.connector.connect("
        "host='localhost',port=3306,user='root',password='root',database='financial_market')"])
    wait_for("PostgreSQL", [PYTHON, "-c",
        "import psycopg2; psycopg2.connect("
        "host='localhost',port=5433,dbname='financial_warehouse',user='postgres',password='postgres')"])
    wait_for("MongoDB", [PYTHON, "-c",
        "from pymongo import MongoClient; "
        "MongoClient('mongodb://localhost:27017',serverSelectionTimeoutMS=2000).server_info()"])

    # 4. ETL
    print("\n[4/5] Running ETL pipelines...")
    print("  Stock prices → MySQL")
    run(PYTHON, "etl/etl_stocks.py")
    print("  News articles → MongoDB")
    run(PYTHON, "etl/etl_news.py")
    print("  Building warehouse in PostgreSQL")
    run(PYTHON, "etl/etl_warehouse.py")

    # 5. Dashboard
    print("\n[5/5] Launching dashboard...")
    subprocess.run([PYTHON, "-m", "streamlit", "run", "dashboard/app.py"], cwd=BASE)


if __name__ == "__main__":
    main()
