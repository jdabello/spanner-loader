import google.auth.credentials
from google.oauth2 import service_account
from google.cloud import storage, spanner
import csv
import gzip
import argparse

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))

def parse_schema(schema):
	with open(schema, 'r') as schema:
		columns=schema.read().split(",")		
		typelist = []
		collist = []
		for column in columns:
			column_details=column.split(":")
			collist.append(column_details[0])
			typelist.append(column_details[1])

	return collist,typelist

def get_timestamp_with_nanoseconds(timestamp_string):
	from google.cloud.spanner_v1._helpers import TimestampWithNanoseconds
	from datetime import datetime
	date_tmp=None
	if("." in timestamp_string):
		date_tmp = datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f')
	else:
		date_tmp = datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')
	timestamp=TimestampWithNanoseconds(date_tmp.year, date_tmp.month, date_tmp.day, date_tmp.hour, date_tmp.minute, date_tmp.second, date_tmp.microsecond)
	return timestamp
	
def get_date(date_string):
	from datetime import datetime
	date_tmp=datetime.strptime(date_string, "%Y-%m-%d").date()
	return date_tmp
	
def load_file(project_id,path_to_credentials,instance_id,database_id,table_id, batchsize, bucket, file_name, schema):
	credentials = service_account.Credentials.from_service_account_file(path_to_credentials)
	spanner_client = spanner.Client(project=project_id, credentials=credentials)
	instance = spanner_client.instance(instance_id)
	database = instance.database(database_id)
	download_blob(bucket,file_name,'source_file.gz')
	schema_details = parse_schema(schema)
	collist=schema_details[0]
	typelist=schema_details[1]
	numcols = len(typelist)
	with gzip.open("source_file.gz", "rt") as file:
		reader = csv.reader(file, delimiter='|')
		alist = []
		irows = 0		
		for row in reader:
			for x in range(0,numcols):
				if 'INTEGER' in typelist[x]:
					row[x] = int(row[x])
				if 'TIMESTAMP' in typelist[x]:
					row[x] = get_timestamp_with_nanoseconds(row[x])
				if 'STRING' in typelist[x]:
					row[x] = row[x]
				if 'DATE' in typelist[x]:
					row[x] = get_date(row[x])
			alist.append(row)
			irows=irows+1
			if(irows>=int(batchsize)):
				with database.batch() as batch:
					batch.insert(
						table=table_id,
						columns=collist,
						values=alist
					)
				print 'inserted %s rows' % irows
				irows=0
				alist = []


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        '--project_id',
        help='Your Google Cloud Platform project ID.'
    )

    parser.add_argument(
        '--path_to_credentials',
        help='Path to the json file with the credentials.'
    )

    parser.add_argument(
        '--instance_id',
        help='Your Cloud Spanner instance ID.'
    )

    parser.add_argument(
        '--database_id',
        help='Your Cloud Spanner database ID.'
    )

    parser.add_argument(
        '--table_id',
        help='Your table name'
    )

    parser.add_argument(
		'--batchsize',
        help='The number of rows to insert in a batch'
    )

    parser.add_argument(
		'--bucket_name',
        help='The name of the bucket for the source file'
    )

    parser.add_argument(
		'--file_name',
        help='The csv input data file'
    )

    parser.add_argument(
        '--schema_file',
        help='The format file describing the input data file'
    )

    args = parser.parse_args()

    load_file(args.project_id,
              args.path_to_credentials,
              args.instance_id,
              args.database_id,
              args.table_id,
              args.batchsize,
              args.bucket_name,
              args.file_name,
              args.schema_file)
