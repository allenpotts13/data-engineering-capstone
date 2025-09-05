import os

import duckdb

DUCKDB_PATH = os.path.join(os.getcwd(), "data", "duckdb", "motorcycle_capstone.duckdb")
SQL_DIR = os.path.join(os.getcwd(), "data", "sql")


def run_sql_scripts(db_path, sql_dir):
    con = duckdb.connect(db_path)
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql"):
            sql_path = os.path.join(sql_dir, filename)
            with open(sql_path, "r") as f:
                sql = f.read()
            print(f"Running {filename}...")
            con.execute(sql)
    con.close()


if __name__ == "__main__":
    run_sql_scripts(DUCKDB_PATH, SQL_DIR)
