from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.blocks.notifications import SlackWebhook
from prefect.utilities.notifications import slack_notifier

# handler = slack_notifier(only_states=[Completed]) # we can call it early

slack_webhook_block = SlackWebhook.load("slack")

@task(retries=3, state_handlers=[slack_notifier])
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    slack_webhook_block.notify(f"Completed extraction from {str(dataset_url)}")
    return df


@task(log_prints=True, state_handlers=[slack_notifier])
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    slack_webhook_block.notify(f"Completed cleaning of dataframe")
    return df


@task(state_handlers=[slack_notifier])
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"/Users/manojkl/Documents/data-engineering-manojkl/HW-2/data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    slack_webhook_block.notify(f"Completed saving of files to {str(path)}")
    return path


@task(state_handlers=[slack_notifier])
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    slack_webhook_block.notify(f"Completed uploading of files to {str(path)}")
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    slack_webhook_block.notify(f"Started the process of etl_web_to_gcs")
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    slack_webhook_block.notify(f"Completed extract from url, cleaning, saving to local directory and writing to GCS bucket for {color}:{month}/{year}")

if __name__ == "__main__":
    etl_web_to_gcs()
