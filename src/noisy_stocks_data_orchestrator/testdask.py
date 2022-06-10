from queue import Empty, SimpleQueue

import dask.dataframe as dd

df = dd.read_csv(r"/workspace/datasets/stock_market_dataset/Stocks/a.us.txt")

print(df)

a = SimpleQueue()
a.put(item=9)
a.put(item=8)

b = a.get()  # expect 8
c = a.get()  # expect 9
try:
    d = a.get(timeout=1)  # Will try to find queue entry for 1 sec
except Empty:
    pass
print(b, c)
