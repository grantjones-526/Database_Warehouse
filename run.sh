#!/bin/bash
set -e

echo "=== Financial Market Intelligence Data Warehouse ==="
echo ""

# Step 1: Start databases
echo "[1/7] Starting Docker containers..."
sudo docker compose up -d
echo "Waiting for databases to accept connections..."

# Wait for MySQL
until sudo docker compose exec -T mysql mysqladmin ping -uroot -proot --silent 2>/dev/null; do
    sleep 2
done
echo "  MySQL ready."

# Wait for PostgreSQL
until sudo docker compose exec -T postgres pg_isready -U postgres --silent 2>/dev/null; do
    sleep 2
done
echo "  PostgreSQL ready."

# Wait for MongoDB
until sudo docker compose exec -T mongo mongosh --eval "db.runCommand('ping').ok" --quiet 2>/dev/null; do
    sleep 2
done
echo "  MongoDB ready."

# Step 2: Set up virtual environment and install dependencies
echo ""
echo "[2/7] Setting up virtual environment and installing dependencies..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q -r requirements.txt

# Step 3: ETL Pipeline 1 - Stock prices → MySQL
echo ""
echo "[3/7] Running ETL Pipeline 1: Stock Prices → MySQL..."
python etl/etl_stocks.py

# Step 4: ETL Pipeline 2 - News → MongoDB
echo ""
echo "[4/7] Running ETL Pipeline 2: News Articles → MongoDB..."
python etl/etl_news.py

# Step 5: ETL Pipeline 3 - MySQL + MongoDB → PostgreSQL
echo ""
echo "[5/7] Running ETL Pipeline 3: Building Warehouse in PostgreSQL..."
python etl/etl_warehouse.py

# Step 6: Create Ollama model
echo ""
echo "[6/7] Creating Ollama model from Modelfile..."
ollama create financial-sql -f ollama/Modelfile

# Step 7: Launch dashboard
echo ""
echo "[7/7] Launching Streamlit dashboard..."
streamlit run dashboard/app.py
