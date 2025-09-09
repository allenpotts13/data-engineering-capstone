import os

import duckdb

DB_PATH = os.path.join(os.getcwd(), "data", "duckdb", "motorcycle_capstone.duckdb")
SCHEMAS = ["config", "bronze", "analysis"]
SETUP_SQL_DIR = os.path.join(os.getcwd(), "data", "sql", "setup")


def setup_duckdb():
    con = duckdb.connect(DB_PATH)
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for filename in os.listdir(SETUP_SQL_DIR):
        if filename.endswith(".sql"):
            file_path = os.path.join(SETUP_SQL_DIR, filename)
            print(f"Executing: {filename}")
            try:
                with open(file_path, "r") as f:
                    sql = f.read()
                    con.execute(sql)
            except Exception as e:
                print(f"Error in {filename}: {e}")
                raise
    con.close()
    print("DuckDB setup complete: schemas and tables created.")


if __name__ == "__main__":
    setup_duckdb()
