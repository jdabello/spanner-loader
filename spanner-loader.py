import re
import csv
import gzip
import uuid
import codecs
import argparse
import logging
from collections import OrderedDict
from google.oauth2 import service_account
from google.cloud import storage, spanner

apply_type = {
    'INTEGER': lambda x: int(x),
    'INT64': lambda x: int(x),
    'FLOAT64': lambda x: float(x),
    'TIMESTAMP': lambda x: get_timestamp_with_nanoseconds(x),
    'STRING': lambda x: str(x),
    'DATE': lambda x: get_date(x)
}

def unescaped_str(arg_str):
    return codecs.decode(str(arg_str), 'unicode_escape')

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))

def parse_schema(schema_file):
    """ Parse a local comma separated schema file and return a dict mapping of
    column name -> column type """

    with open(schema_file, 'r') as schema:
        col_mapping = OrderedDict()
        columns = schema.read().split(",")
        for column in columns:
            column = re.sub(r'[\n\t\s]*', '', column)
            col_name, col_type = column.split(":")
            col_mapping[col_name] = col_type

    return col_mapping

def get_timestamp_with_nanoseconds(timestamp_string):
    from google.cloud.spanner_v1._helpers import TimestampWithNanoseconds
    from datetime import datetime

    date_tmp = None
    if "." in timestamp_string:
        date_tmp = datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f')
    else:
        date_tmp = datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')
    timestamp=TimestampWithNanoseconds(date_tmp.year, date_tmp.month, date_tmp.day, date_tmp.hour, date_tmp.minute, date_tmp.second, date_tmp.microsecond)
    return timestamp

def get_date(date_string):
    from datetime import datetime
    date_tmp = datetime.strptime(date_string, "%Y-%m-%d").date()
    return date_tmp

def load_file(instance_id,
              database_id,
              table_id,
              batchsize,
              bucket_name,
              file_name,
              schema_file,
              delimiter,
              dry_run,
              verbose,
              debug,
              add_uuid,
              project_id=None,
              path_to_credentials=None):

    # Construct any arguments to be passed to the Spanner client init. These
    # are optional since in cases when gcloud SDK is configured, or when
    # pulling from instance metadata it is not necessary to manually specify
    client_args = {}
    if path_to_credentials:
        client_args['credentials'] = service_account.Credentials.from_service_account_file(path_to_credentials)

    if project_id:
        client_args['project'] = project_id

    # Build Spanner clients and set instance/database based on arguments
    # TODO (djrut): Add exception handling
    spanner_client = spanner.Client(**client_args)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Generate a unique local temporary file name to allow multiple invocations
    # of the tool from the same parent directory, and enable path to
    # multi-threaded loader in future
    local_file_name = ''.join(['.spanner_loader_tmp-', str(uuid.uuid4())])

    # TODO (djrut): Add exception handling
    download_blob(bucket_name, file_name, local_file_name)

    # Figure out the source and target column names based on the schema file
    # provided, and add a uuid if that option is enabled
    col_mapping = parse_schema(schema_file=schema_file)

    src_col = list(col_mapping.keys())

    if add_uuid:
        target_col = ['uuid'] + src_col
    else:
        target_col = src_col

    print('Detected {} columns in source schema: {}'
          .format(len(col_mapping), src_col))

    with gzip.open(local_file_name, "rt") as source_file:
        reader = csv.DictReader(source_file,
                                delimiter=delimiter,
                                fieldnames=src_col)
        row_cnt = 0
        batch_cnt = 0
        insert_cnt = 0
        row_batch = []

        for row in reader:
            target_row = []
            skip_row = False

            logging.info('Processing source row = {}'.format(row))

            print('Processing row {:,}'.format(row_cnt), end='\r')

            if add_uuid:
                target_row.append(str(uuid.uuid4()))

            for col_name, col_value in row.items():
                logging.info('Processing column: {} = {}'
                             .format(col_name, col_value))

                try:
                    target_cell = apply_type[col_mapping[col_name]](col_value)
                except ValueError as err:
                    logging.warning(('Bad field detected: col = {}, value = {} '
                                     'Skipping row...')
                                    .format(col_name, col_value))
                    skip_row = True
                    break
                else:
                    target_row.append(target_cell)

            if not skip_row:
                row_batch.append(target_row)
                batch_cnt += 1
                insert_cnt += 1

            if batch_cnt >= batchsize:
                if not dry_run:
                    with database.batch() as batch:
                      batch.insert(
                          table=table_id,
                          columns=target_col,
                          values=row_batch
                      )

                    logging.info('Inserted {} rows into table {}'
                                 .format(batch_cnt, table_id))
                else:
                    print('Dry-run batch = {}'
                          .format(row_batch))

                batch_cnt = 0
                row_batch = []

            row_cnt += 1

        print('Load job complete for source file {}. Inserted {} of {} rows'
              .format(file_name, insert_cnt, row_cnt))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=('Spanner batch loader utility'))

    parser.add_argument(
        '-V',
        '--verbose',
        default=False,
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '-D',
        '--debug',
        default=False,
        action='store_true',
        help='Enable debug logging'
    )

    parser.add_argument(
        '--dry-run',
        default=False,
        action='store_true',
        help='Perform dry-run without actually inserting rows'
    )

    parser.add_argument(
        '--add-uuid',
        default=False,
        action='store_true',
        help='Add a uuid column to target row'
    )

    parser.add_argument(
        '--instance_id',
        required=True,
        help='Your Cloud Spanner instance ID.'
    )

    parser.add_argument(
        '--database_id',
        required=True,
        help='Your Cloud Spanner database ID.'
    )

    parser.add_argument(
        '--table_id',
        required=True,
        help='Your table name'
    )

    parser.add_argument(
        '--batchsize',
        default=32,
        type=int,
        help='The number of rows to insert in a batch'
    )

    parser.add_argument(
        '--delimiter',
        type=unescaped_str,
        default=',',
        help='The delimiter used between columns in source file'
    )

    parser.add_argument(
        '--bucket_name',
        required=True,
        help='The name of the bucket containing the source file'
    )

    parser.add_argument(
        '--file_name',
        required=True,
        help='The csv input data file'
    )

    parser.add_argument(
        '--schema_file',
        required=True,
        help='The format file describing the input data file'
    )

    parser.add_argument(
        '--project_id',
        required=False,
        help='Your Google Cloud Platform project ID.'
    )

    parser.add_argument(
        '--path_to_credentials',
        required=False,
        help='Path to the json file with the credentials.'
    )

    args = parser.parse_args().__dict__

    # Setup logging levels based on verbosity setting
    FORMAT = '%(levelname)s: [%(filename)s/%(funcName)s] %(message)s'
    level = logging.WARNING

    # Mute certain overly verbose modules
    logging.getLogger("googleapiclient").setLevel(level)
    logging.getLogger("oauth2client").setLevel(level)

    if args['verbose']:
        level = logging.INFO

    if args['debug']:
        level = logging.DEBUG
        logging.getLogger("googleapiclient").setLevel(level)
        logging.getLogger("oauth2client").setLevel(level)

    logging.basicConfig(level=level, format=FORMAT)

    load_file(**args)
