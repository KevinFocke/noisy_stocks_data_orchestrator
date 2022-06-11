from itertools import combinations
from queue import Empty, SimpleQueue

import dask.dataframe as dd

df = dd.read_csv(r"/workspace/datasets/stock_market_dataset/Stocks/a.us.txt")

print(df)


list_of_dicts = []
a = {"message": "hello", "thought": "greeting"}
b = {"message": "yes", "thought": "agreement"}
c = {"message": "no", "thought": "disagreement"}
d = {"message": "kinda", "thought": "unsure"}

list_of_dicts.append(a)
list_of_dicts.append(b)
list_of_dicts.append(c)
list_of_dicts.append(d)
print(list_of_dicts)

combs = combinations(list_of_dicts, 2)

for el in combs:
    print(el)
