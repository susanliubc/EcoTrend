import pandas as pd
import pyarrow
import gcsfs

# GCS bucket folder
bucket_path = "gs://de-eco-trend-bucket/"

# List of your 10 Parquet files
parquet_files = [
    "SP500_data.parquet",
    "NASDAQ100_data.parquet",
    "DGS10_data.parquet",
    "VIXCLS_data.parquet",
    "EFFR_data.parquet",
    "CPIAUCSL_data.parquet",
    "PCEPI_data.parquet",
    "CIVPART_data.parquet",
    "INDPRO_data.parquet",
    "CSUSHPISA_data.parquet"
]

# Initialize GCS filesystem
fs = gcsfs.GCSFileSystem()

for file_name in parquet_files:
    gcs_input_path = bucket_path + file_name
    gcs_output_path = bucket_path + file_name.replace(".parquet", "_us.parquet")

    # Read Parquet into Pandas
    df = pd.read_parquet(gcs_input_path, engine="pyarrow", filesystem=fs)

    # Convert all datetime columns to microseconds precision
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].dt.floor('us')  # downcast from ns → us

    # Write fixed Parquet back to GCS
    df.to_parquet(gcs_output_path, engine="pyarrow", index=False, filesystem=fs)

    print(f"Converted {gcs_input_path} -> {gcs_output_path}")


