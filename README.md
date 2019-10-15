# Movie Review Sentiment Analysis ETL Pipeline

This repository represents an Extract, Transform, and Load (ETL) pipeline for sentiment analysis of Amazon Reviews. Data is first sourced from Amazon's own [Open Data Registry](https://registry.opendata.aws/amazon-reviews/)

## Data Sources
* [Amazon Reviews](https://registry.opendata.aws/amazon-reviews/)
* Generated - Sentiment Analysis

## Pipeline
1. Download data files
   * `cd src && ./download_amazon.sh` - **2.90 GB** _note_ this requires an Amazon Web Services Access Key and Secret ID
2. Transformations / Generation
   * Amazon Reviews - `cd src/transforms && python amazon_review.py`
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
     1. Store review id and sentiment scores
3. Load
   * Movies
   * Reviews
   * Sentiment Analysis
