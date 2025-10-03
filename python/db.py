import duckdb
from pathlib import Path


class dbWrapper:
    def __init__(self, db_path="omop.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.load_from_csv("./data/100k/")

    def load_from_csv(self, data_dir):
        data_path = Path(data_dir)

        for table in ["person", "condition_occurrence", "measurement"]:
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
        disease_cohort = self.conn.execute(
            """
            SELECT DISTINCT
                p.person_id,
                p.gender_concept_id,
                p.year_of_birth,
                CASE 
                    WHEN p.year_of_birth IS NULL THEN NULL
                    WHEN 2024 - p.year_of_birth < 20 THEN '<20'
                    WHEN 2024 - p.year_of_birth < 40 THEN '20-40'
                    WHEN 2024 - p.year_of_birth < 60 THEN '40-60'
                    ELSE '60+'
                END as age_group,
                CASE 
                    WHEN p.gender_concept_id = 8507 THEN 'Male'
                    WHEN p.gender_concept_id = 8532 THEN 'Female'
                    ELSE 'Unknown'
                END as gender
            FROM person p
            INNER JOIN condition_occurrence co 
                ON p.person_id = co.person_id
            WHERE co.condition_concept_id = ?
            """,
            [disease_concept_id],
        ).df()

        non_disease_cohort = self.conn.execute(
            """
            SELECT DISTINCT
                p.person_id,
                p.gender_concept_id,
                p.year_of_birth,
                CASE 
                    WHEN p.year_of_birth IS NULL THEN NULL
                    WHEN 2024 - p.year_of_birth < 20 THEN '<20'
                    WHEN 2024 - p.year_of_birth < 40 THEN '20-40'
                    WHEN 2024 - p.year_of_birth < 60 THEN '40-60'
                    ELSE '60+'
                END as age_group,
                CASE 
                    WHEN p.gender_concept_id = 8507 THEN 'Male'
                    WHEN p.gender_concept_id = 8532 THEN 'Female'
                    ELSE 'Unknown'
                END as gender
            FROM person p
            WHERE p.person_id NOT IN (
                SELECT DISTINCT person_id 
                FROM condition_occurrence 
                WHERE condition_concept_id = ?
            )
            """,
            [disease_concept_id],
        ).df()

        return disease_cohort, non_disease_cohort

    def getMeasurements(self, view, measurement_concept_id):
        placeholders = ",".join(["?"] * len(view))
        measurements = self.conn.execute(
            f"""
            SELECT 
                person_id,
                value_as_number,
                measurement_date
            FROM measurement
            WHERE person_id IN ({placeholders})
                AND measurement_concept_id = ?
            ORDER BY person_id, measurement_date DESC
            """,
            view + [measurement_concept_id],
        ).df()

        measurements = measurements.groupby("person_id").first().reset_index()

        return measurements[["person_id", "value_as_number"]]


if __name__ == "__main__":
    db = dbWrapper("omop.db")
    disease_cohort, healthy_cohort = db.get_cohorts(443392)

    measuremnent_id = 2212099

    print(db.getMeasurements(healthy_cohort["person_id"].tolist(), measuremnent_id))
    print(db.getMeasurements(disease_cohort["person_id"].tolist(), measuremnent_id))
