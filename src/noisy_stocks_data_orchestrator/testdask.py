import dask.dataframe as dd

df = dd.read_csv(r"/workspace/datasets/stock_market_dataset/Stocks/a.us.txt")

print(df)
