import sys

import pandas as pd

df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")


print(f"Hello pipeline {sys.argv}")