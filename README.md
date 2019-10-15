# Movie Review Sentiment Analysis ETL Pipeline

This repository represents an Extract, Transform, and Load (ETL) pipeline for sentiment analysis of Amazon Reviews. Data is first sourced from Amazon's own [Open Data Registry](https://registry.opendata.aws/amazon-reviews/) in the Parquet data format. From there the transformation pipelines filters the data down to the `Video DVD` category and `US` marketplace segment. Additionally the number of columns is reduced to match the target data shape. Next the data is written back out in to parquet format for consumption by the load process. This process reads in the intermediary parquet files and publishes them out to PostgreSQL via the `sqlalchemy` and `psycopg2` packages. While it is possible to perform the extract, transform, and load processes in a single script we have opted for separate scripts with data files cached in the `data` directory. This allows for modifications to be made to each phase of the pipeline without having to start processing over from scratch.

## Data Sources
* [Amazon Reviews](https://registry.opendata.aws/amazon-reviews/)
* Generated - [Sentiment Analysis](https://nlp.stanford.edu/sentiment/code.html)

## Pipeline
1. Download data files
   * `cd src && ./download_amazon.sh` - **2.90 GB** _note_ this requires an Amazon Web Services Access Key and Secret ID
2. Transformations / Generation
   * Amazon Reviews - `cd src && python extract_transform.py`
     1. Load source Parquet files (7,135,819 records)
     1. Limit to `US` market (6,166,026 records)
     1. Reviews
        1. Filter columns ("product_id", "review_id", "review_headline", "review_body", "star_rating", "review_date")
        1. Remove duplicates (5,069,140 records)
        1. Store reviews - `input/reviews.parquet`
     1. Movies
        1. Filter columns ("product_id", "product_title")
        1. Remove duplicates (297,919 records)
        1. Store movies - `input/movies.parquet`
   * Sentiment Analysis (Generation)
     1. Iterate over reduced review dataset
     1. Submit review title and text to analysis service (TBD)
     1. Store review id and sentiment scores - `input/sentiment.parquet`
3. Load
   * Schema - `schema/schema.sql`
   * Data - `src/load.py`
     * Movies
     * Reviews
     * Sentiment Analysis
