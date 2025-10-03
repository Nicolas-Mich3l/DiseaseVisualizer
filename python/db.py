import duckdb
from pathlib import Path


class dbWrapper:
    def __init__(self, db_path="omop.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.load_from_csv("./data/")

    def load_from_csv(self, data_dir):
        data_path = Path(data_dir)

        for table in ["CDM_PERSON", "CDM_CONDITION_OCCURRENCE", "CDM_MEASUREMENT"]:
            file = data_path / f"{table}.csv"
            if file.exists():
                self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS '{table}' AS SELECT * FROM '{file}';
                """)
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"Loaded {count:,} rows into {table}")
            else:
                print(f"{file} not found, skipping...")

    def get_cohorts(self, disease_concept_id):
        pass


if __name__ == "__main__":
    db = dbWrapper("omop.db")
