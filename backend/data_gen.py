import pandas as pd
import numpy as np
import sys
from pathlib import Path


def generate_lipid_values_by_cohort(
    data_dir,
    disease_concept_id,
    output_dir=None,
):
    """
    Generates distinct-ish Lipid Panel distributions for disease vs non-disease cohorts.

    Disease cohort (e.g., Diabetes patients):
        - Mean: 240 mg/dL (elevated cholesterol, common in diabetes)
        - Std Dev: 45 mg/dL
        - Range: 150-400 mg/dL

    Non-disease cohort (healthy):
        - Mean: 180 mg/dL (normal cholesterol)
        - Std Dev: 30 mg/dL
        - Range: 100-300 mg/dL

    Args:
        data_dir: Directory containing CSV files
        disease_concept_id: OMOP concept_id for the disease condition
        output_dir: Output directory (defaults to data_dir if None)

    Returns:
        Updated measurement DataFrame
    """

    LIPID_PANEL_CONCEPT_ID = 2212095

    data_path = Path(data_dir)

    # Disease cohort configuration (elevated lipids)
    disease_config = {
        "mean": 240,  # mg/dL - elevated cholesterol
        "std": 45,  # mg/dL - higher variation
        "min": 150,  # mg/dL
        "max": 400,  # mg/dL
        "name": "Disease Cohort (Elevated Lipids)",
    }

    # Non-disease cohort configuration (normal lipids)
    healthy_config = {
        "mean": 180,  # mg/dL - normal cholesterol
        "std": 30,  # mg/dL - lower variation
        "min": 100,  # mg/dL
        "max": 300,  # mg/dL
        "name": "Non-Disease Cohort (Normal Lipids)",
    }

    # Step 1: Read CSV files
    print("Step 1: Reading CSV files...")

    measurement_file = data_path / "measurement.csv"
    condition_file = data_path / "condition_occurrence.csv"
    person_file = data_path / "person.csv"

    if not measurement_file.exists():
        raise FileNotFoundError(f"measurement.csv not found in {data_dir}")
    if not condition_file.exists():
        raise FileNotFoundError(f"condition_occurrence.csv not found in {data_dir}")
    if not person_file.exists():
        raise FileNotFoundError(f"person.csv not found in {data_dir}")

    measurements = pd.read_csv(measurement_file)
    conditions = pd.read_csv(condition_file)
    persons = pd.read_csv(person_file)

    # grab cohorts
    disease_persons = conditions[
        conditions["condition_concept_id"] == disease_concept_id
    ]["person_id"].unique()

    all_persons = persons["person_id"].unique()
    healthy_persons = set(all_persons) - set(disease_persons)

    lipid_mask = measurements["measurement_concept_id"] == LIPID_PANEL_CONCEPT_ID
    lipid_measurements = measurements[lipid_mask].copy()

    if len(lipid_measurements) == 0:
        print(
            f" No lipid panel measurements found (concept_id={LIPID_PANEL_CONCEPT_ID})"
        )
        print("  Exiting without changes.")
        return measurements

    print(f" Found {len(lipid_measurements):,} lipid panel measurements")

    # Separate into disease and non-disease measurements
    disease_lipid_mask = lipid_measurements["person_id"].isin(disease_persons)
    healthy_lipid_mask = lipid_measurements["person_id"].isin(healthy_persons)

    disease_lipid_count = disease_lipid_mask.sum()
    healthy_lipid_count = healthy_lipid_mask.sum()

    # Generate disease cohort values (elevated lipids)
    if disease_lipid_count > 0:
        disease_values = np.random.normal(
            loc=disease_config["mean"],
            scale=disease_config["std"],
            size=disease_lipid_count,
        )
        disease_values = np.clip(
            disease_values, disease_config["min"], disease_config["max"]
        )

        # Update disease cohort measurements
        disease_indices = lipid_measurements[disease_lipid_mask].index
        measurements.loc[disease_indices, "value_as_number"] = disease_values

    # Generate healthy cohort values (normal lipids)
    if healthy_lipid_count > 0:
        healthy_values = np.random.normal(
            loc=healthy_config["mean"],
            scale=healthy_config["std"],
            size=healthy_lipid_count,
        )
        healthy_values = np.clip(
            healthy_values, healthy_config["min"], healthy_config["max"]
        )

        # Update healthy cohort measurements
        healthy_indices = lipid_measurements[healthy_lipid_mask].index
        measurements.loc[healthy_indices, "value_as_number"] = healthy_values

    if output_dir is None:
        output_dir = data_dir

    output_path = Path(output_dir)
    output_file = output_path / "measurement.csv"

    measurements.to_csv(output_file, index=False)
    return measurements


def main():
    """Entry point for command-line usage."""
    if len(sys.argv) < 2:
        print("Usage: python generate_lipid_values.py <data_dir> <output_dir>")
        print("  python generate_lipid_values.py ./data/100k/  ./data/100k_updated/")
        sys.exit(1)

    data_dir = sys.argv[1]
    disease_concept_id = int(sys.argv[2])

    # Check if data directory exists
    if not Path(data_dir).exists():
        print(f"Error: Data directory '{data_dir}' not found!")
        sys.exit(1)

    # Generate values
    try:
        generate_lipid_values_by_cohort(data_dir, disease_concept_id)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
