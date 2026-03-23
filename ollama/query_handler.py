"""
Ollama Query Handler
Sends natural language questions to Ollama, validates the generated SQL,
and executes it against PostgreSQL.
"""

import re
import requests
import psycopg2

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "financial-sql"

ALLOWED_TABLES = {"fact_market_data", "dim_asset", "dim_date", "dim_sector"}

FORBIDDEN_KEYWORDS = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|EXEC)\b',
    re.IGNORECASE,
)


def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="financial_warehouse",
        user="postgres",
        password="postgres",
    )


def ollama_generate(question):
    """Send a question to Ollama and return the generated SQL."""
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": question,
            "stream": False,
        },
    )
    response.raise_for_status()
    return response.json().get("response", "").strip()


def validate_sql(sql):
    """
    Validate that generated SQL is a safe SELECT statement.
    Returns (is_valid, result_or_error).
    """
    sql = sql.strip().rstrip(";")

    # Strip markdown code fences if the model wrapped its output
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        sql = "\n".join(lines).strip()

    if not sql.upper().startswith("SELECT"):
        return False, "Only SELECT statements are permitted."

    if FORBIDDEN_KEYWORDS.search(sql):
        return False, "Statement contains forbidden keywords."

    # Check balanced parentheses
    if sql.count("(") != sql.count(")"):
        return False, "Syntax error: unbalanced parentheses."

    # Check that only known tables appear in FROM/JOIN clauses
    table_matches = re.findall(
        r'\bFROM\s+(\w+)|\bJOIN\s+(\w+)', sql, re.IGNORECASE
    )
    for match in table_matches:
        table = next(t for t in match if t)
        if table.lower() not in ALLOWED_TABLES:
            return False, f"Unknown table referenced: {table}"

    return True, sql


def run_nl_query(question):
    """
    Full pipeline: question → Ollama → validate → execute → results.
    Returns a list of dicts or an error dict.
    """
    raw_sql = ollama_generate(question)

    valid, result = validate_sql(raw_sql)
    if not valid:
        return [{"error": result, "generated_sql": raw_sql}]

    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(result)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return {
                "sql": result,
                "data": [dict(zip(cols, row)) for row in rows],
            }
    except psycopg2.Error as e:
        return {"error": f"Query execution failed: {e}", "generated_sql": result}
    finally:
        conn.close()


if __name__ == "__main__":
    # Quick test
    question = input("Ask a question: ")
    result = run_nl_query(question)
    print(result)
