# Movie Review Sentiment Analysis ETL Pipeline

_Description of processes / dataset_

## Data Sources
* IMDB - Movie Catalog
* IMDB - Reviews
* IMDB - Cast
* Generated - Sentiment Analysis

## Pipeline
1. Download data files
   * URL 1
   * URL 2
   * ...
2. Transformations
   * IMDB - Movies - `src/movie_transform.py`
     1. Remove columns ...
     2. Remove duplicates ...
3. Load
   * Movies
   * Reviews
     * Dataset downloads + generated sentiment analysis
   * Cast
