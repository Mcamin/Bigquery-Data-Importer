# Bigquery Data Importer 

## Requirements: 
- Google cloud bigquery installed
```
pip install google-cloud-bigquery
```
- 

This is a script that loads rows in json format from a txt file line by line and insert them in a Bigquery Table.
This script can be used to debug the issues with streaming data from a Pub/Sub Topic to Bigquery using Dataflow.

The variables to be adjusted are the following: 

```
filename = "./data.txt" # The files containing the rows to insert in json format
table_id = 'your_project_id.your_dataset_id.your_table_id' the project id . dataset id . table id 
error_filepath = "./errors.log" the error log file
skipped_filepath = "./skipped.log" the data file log containing the skipped rows due to an error 
```
