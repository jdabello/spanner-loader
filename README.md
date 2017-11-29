# spanner-loader

This directory contains a python script that can be used to import data into [Cloud Spanner](https://cloud.google.com/spanner/). The script reads a gzipped csv file from a [Google Cloud Storage](https://cloud.google.com/gcs/) bucket and a schema file and then inserts the data into a specified Spanner table in batches.
