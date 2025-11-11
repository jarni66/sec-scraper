import os
import pandas as pd
import csv

sec_dir = "sec_forms"
out_file = "filling_check.csv"
validation_file = "validation_check.csv"


def filling_check():
    # --- Load existing cik-acsn pairs into a set (fast lookup) ---
    existing_pairs = set()
    if os.path.exists(out_file):
        df_existing = pd.read_csv(out_file, usecols=["cik", "acsn"])
        existing_pairs = set(zip(df_existing["cik"], df_existing["acsn"]))
        del df_existing  # free memory

    # --- Prepare CSV writer ---
    file_exists = os.path.exists(out_file)
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["cik", "acsn", "report_date", "num_rows"])
        if not file_exists:
            writer.writeheader()

        # --- Iterate SEC directory ---
        date_list = os.listdir(sec_dir)
        for report_date in date_list:
            file_list = os.listdir(f"{sec_dir}/{report_date}")
            for file in file_list:
                if file.endswith(".parquet"):
                    cik = file.split("_")[0]
                    acsn = "-".join(file.split("_")[1:4])

                    # Skip if already in CSV
                    if (cik, acsn) in existing_pairs:
                        continue

                    # Count rows
                    df = pd.read_parquet(f"{sec_dir}/{report_date}/{file}")
                    len_df = len(df)

                    writer.writerow({
                        "cik": cik,
                        "acsn": acsn,
                        "report_date": report_date,
                        "num_rows": len_df,
                    })
                    existing_pairs.add((cik, acsn))  # add to set immediately


def validate_cik_month(group):
    years = group['report_date'].dt.year.unique()
    min_year, max_year = years.min(), years.max()

    # middle years only
    middle_years = [y for y in years if y not in (min_year, max_year)]

    for year in middle_years:
        months = group[group['report_date'].dt.year == year]['report_date'].dt.month.unique()
        if len(months) < 4:
            return "Failed"
    return "Passed"


def validation_check():
    df = pd.read_csv(out_file, dtype=str)
    df['report_date'] = pd.to_datetime(df['report_date'], format="%Y_%m_%d")

    result = df.groupby("cik").apply(validate_cik_month).reset_index()
    result.columns = ["cik", "status"]
    result.to_csv(validation_file, index=False)

validation_check()