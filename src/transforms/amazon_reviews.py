#!/usr/bin/env python

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

dataset = pq.ParquetDataset('../../input/amazon_pq/product_category=Video_DVD')
table = dataset.read()

df = table.to_pandas()
print(df.count())

# Dataset contains information from US and French markets, limit it to US
us_df = df[df.marketplace == "US"]
print(us_df.count())

# Review information
review_df = us_df[["product_id", "review_id", "review_headline", "review_body", "star_rating", "review_date"]].copy()
review_df.drop_duplicates(inplace=True)
review_df.set_index("review_id", inplace=True)
print(review_df.count())

review_table = pa.Table.from_pandas(review_df)
pq.write_table(review_table, '../../input/reviews.parquet')

# Movie information
movie_df = us_df[["product_id", "product_title"]].copy()
movie_df.drop_duplicates(inplace=True)
movie_df.set_index("product_id", inplace=True)
print(movie_df.count())

movie_table = pa.Table.from_pandas(movie_df)
pq.write_table(movie_table, '../../input/movies.parquet')
