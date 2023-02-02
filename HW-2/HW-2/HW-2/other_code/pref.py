import pandas
import yfinance as yf
from datetime import timedelta
from prefect import flow, task

@task(retries=3, cache_expiration=timedelta(30))
def fetch_data(ticker):
    return yf.download(ticker)
    
@task
def save_data(stock_df):
    stock_df.to_parquet()

@flow
def pipeline(ticker):
    df = fetch_data(ticker)
    print(df.shape)
    save_data(df)

from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=pipeline,
    name="Python Deployment Example",
)

if __name__ == "__main__":
    deployment.apply()