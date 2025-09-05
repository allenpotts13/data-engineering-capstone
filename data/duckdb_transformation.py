import os

import duckdb
from src.utils.logger import setup_logger

DUCKDB_PATH = os.path.join(os.getcwd(), "data", "duckdb", "motorcycle_capstone.duckdb")
SQL_DIR = os.path.join(os.getcwd(), "data", "sql")

logger = setup_logger(__name__, log_file="src/logs/duckdb_transformation.log")


def run_sql_scripts(db_path, sql_dir):
    con = duckdb.connect(db_path)
    success = True
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql"):
            sql_path = os.path.join(sql_dir, filename)
            with open(sql_path, "r") as sql_file:
                sql = sql_file.read()
            logger.info(f"Running {filename}...")
            try:
                con.execute(sql)
                logger.info(f"Successfully ran {filename}.")
            except Exception as e:
                logger.error(f"Error running {filename}: {e}")
                success = False
    con.close()
    return success

def main():
    return run_sql_scripts(DUCKDB_PATH, SQL_DIR)

if __name__ == "__main__":
    main()
