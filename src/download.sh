#!/usr/bin/env bash

mkdir ../input/amazon/
aws s3 cp --recursive s3://amazon-reviews-pds/parquet/product_category=Video_DVD/ ../input/amazon/
