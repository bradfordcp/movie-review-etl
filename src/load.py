#!/usr/bin/env python

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine

connection_string = "postgres://postgres:postgres@127.0.0.1:5432/etl_project"
engine = create_engine(connection_string)
engine.execute("TRUNCATE movies CASCADE")

movies_dataset = pq.ParquetDataset('../input/movies.parquet')
movies_table = movies_dataset.read()
movies_df = movies_table.to_pandas()
movies_df.to_sql("movies", engine, if_exists="append")

reviews_dataset = pq.ParquetDataset('../input/reviews.parquet')
reviews_table = reviews_dataset.read()
reviews_df = reviews_table.to_pandas()
reviews_df.to_sql("reviews", engine, if_exists="append")
