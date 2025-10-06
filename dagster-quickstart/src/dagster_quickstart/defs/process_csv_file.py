import pandas as pd
import dagster as dg

sample_data_file = "src/dagster_quickstart/defs/data/sample_data.csv"
processed_data_file = "src/dagster_quickstart/defs/data/processed_data.csv"


@dg.asset
def processed_data(context: dg.AssetExecutionContext):
    ## Read data from the CSV
    df = pd.read_csv(sample_data_file)

    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    df.to_csv(processed_data_file, index=False)
    
    # Emit metadata to Dagit UI
    context.add_output_metadata(
        {
            "row_count": len(df),
            "preview": dg.MetadataValue.md(df.head().to_markdown()),  # shows sample rows
            "file_path": processed_data_file,
        }
    )
    
    return "Data loaded successfully"