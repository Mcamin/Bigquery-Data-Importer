from google.cloud import bigquery
import logging
import json

error_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
data_formatter = logging.Formatter('%(message)s')

filename = "./data.txt"
table_id = 'your_project_id.your_dataset_id.your_table_id'
error_filepath = "./errors.log"
skipped_filepath = "./skipped.log"

def setup_logger(name, log_file, formatter, level=logging.INFO):
    """Setup Logger"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


err_logger = setup_logger("Errors", error_filepath, error_formatter, logging.ERROR)
data_logger = setup_logger("Skipped", skipped_filepath, data_formatter, logging.INFO, )


def load_and_parse_json(file):
    """
    Loads the requests
    :param file: the filepath
    :return: a list of dict
    """
    parsed_data = []
    with open(file, 'r') as f:
        for line in f:
            try:
                parsed_data.append(json.loads(json.loads(line)))
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")

    return parsed_data


def insert_into_bigquery_table(data, tbl_id, batch_size=1):
    """
    Inserts a row in a BigQuery table
    :param data: an object of type dict or a list of objects to insert
    :param tbl_id: The table id in the following format: project_id.dataset_id.table_id
    :param batch_size: the batch_size to use to insert the data
    """
    client = bigquery.Client()
    row_to_insert = data
    errors = []
    if isinstance(data, dict):
        row_to_insert = [data]
        errors = client.insert_rows_json(table=tbl_id, json_rows=row_to_insert)
    else:
        for i in range(0, len(row_to_insert), batch_size):
            batch = row_to_insert[i:i + batch_size]
            e = client.insert_rows_json(table=tbl_id, json_rows=batch)
            errors.extend(e)

    if len(errors) == 0:
        print("Data successfully inserted into BigQuery table.")
        return []
    else:
        print(f"Encountered errors while inserting data into BigQuery table.")
        for e in errors:
            err_logger.error(str(e))
            data_logger.info(str(data))
            print(e)
        return errors


def main(batch=False):
    """
    Main
    :param batch: whether to insert in rows in batches or not
    :return:
    """

    parsed_json_list = load_and_parse_json(filename)
    inserted = 0
    skipped = 0
    if batch:
        errors = insert_into_bigquery_table(parsed_json_list, table_id, batch_size=1000)
    else:
        for data in parsed_json_list:
            print("Inserting Data")
            print(data)
            errors = insert_into_bigquery_table(data, table_id)
            if len(errors) == 0:
                inserted += 1
            else:
                skipped += 1
            print(f"Inserted: {inserted}")
            print(f"Skipped: {skipped}")
    print("Done!")


if __name__ == '__main__':
    main()
