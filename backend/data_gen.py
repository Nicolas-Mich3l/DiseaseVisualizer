import pandas as pd
import numpy as np
import sys
from pathlib import Path


def generate_lipid_values(input_file, output_file=None):
    """
    Generate Gaussian-distributed values for Lipid Panel measurements.
    Returns:
        DataFrame with updated values
    """

    LIPID_PANEL_CONCEPT_ID = 2212095

    # Lipid panel configuration
    config = {
        "mean": 200,  # mg/dL - typical total cholesterol
        "std": 40,  # mg/dL - standard deviation
        "min": 100,  # mg/dL - minimum physiological value
        "max": 400,  # mg/dL - maximum physiological value
        "name": "Lipid Panel",
    }

    print(f"Reading CSV file: {input_file}")

    # Read the measurement CSV
    df = pd.read_csv(input_file)

    print(f"Total rows in CSV: {len(df):,}")

    # Find rows matching lipid panel concept_id
    lipid_mask = df["measurement_concept_id"] == LIPID_PANEL_CONCEPT_ID
    lipid_count = lipid_mask.sum()

    if lipid_count == 0:
        print(f"No rows found with measurement_concept_id = {LIPID_PANEL_CONCEPT_ID}")
        print("No changes made to the CSV.")
        return df

    print(f"Found {lipid_count:,} lipid panel measurements")

    # Generate Gaussian-distributed values
    print(
        f"Generating values: mean={config['mean']}, std={config['std']}, "
        f"range=[{config['min']}, {config['max']}]"
    )

    lipid_values = np.random.normal(
        loc=config["mean"], scale=config["std"], size=lipid_count
    )

    # Clip to physiological range
    lipid_values = np.clip(lipid_values, config["min"], config["max"])

    # Update the value_as_number column for lipid panel rows
    df.loc[lipid_mask, "value_as_number"] = lipid_values.astype(float)

    # Calculate and display statistics
    updated_values = df.loc[lipid_mask, "value_as_number"]

    print("\n" + "=" * 60)
    print(f"✓ Generated {config['name']} values successfully!")
    print("=" * 60)
    print(f"Count:          {len(updated_values):,}")
    print(f"Mean:           {updated_values.mean():.2f} mg/dL")
    print(f"Std Dev:        {updated_values.std():.2f} mg/dL")
    print(f"Min:            {updated_values.min():.2f} mg/dL")
    print(f"Max:            {updated_values.max():.2f} mg/dL")
    print(f"Median:         {updated_values.median():.2f} mg/dL")
    print(f"25th percentile: {updated_values.quantile(0.25):.2f} mg/dL")
    print(f"75th percentile: {updated_values.quantile(0.75):.2f} mg/dL")
    print("=" * 60)

    # Write to output file
    if output_file is None:
        output_file = input_file

    df.to_csv(output_file, index=False)

    return df


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_lipid_values.py <input_csv> [output_csv]")
        print("\nExample:")
        print("  python generate_lipid_values.py measurement.csv")
        print(
            "  python generate_lipid_values.py measurement.csv measurement_updated.csv"
        )
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    # Check if input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found!")
        sys.exit(1)

    # Generate values
    try:
        generate_lipid_values(input_file, output_file)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
