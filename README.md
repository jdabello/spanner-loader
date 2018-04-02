# spanner-loader

This directory contains a python script that can be used to import data into [Cloud Spanner](https://cloud.google.com/spanner/). The script reads a gzipped csv file from a [Google Cloud Storage](https://cloud.google.com/gcs/) bucket and a local schema file, and then inserts the data into a specified Spanner table in batches.


## Table of Contents
1. [Create a Cloud Spanner Table](#spanner-table)
2. [Create a schema for your Spanner Table](#schema)
3. [Create a Service Account](#service-account)
4. [Usage](#usage)

## <a name="spanner-table"></a>1. Create a Cloud Spanner Table

Follow the steps on the [Spanner Quickstart](https://cloud.google.com/spanner/docs/quickstart-console) to create your spanner instance, database and table.

## <a name="schema"></a>2. Create a schema for your Spanner Table

Use the [sample.schema](sample.schema) to define the schema for the table that you are going to load. Use a colon ( : ) to specify the data type for each of your columns and a comma ( , ) to separate each of the columns on your table.

For example for a table with two STRING columns, named one and two, this would be the corresponding schema.
```bash
one:STRING,two:STRING
```

## <a name="service-account"></a>3. Create a Service Account (optional)

*Note: This step is not required in the event that you have configured appropriate account and project configuration using the gcloud SDK, or are running the tool from a GCE instance within the target project with a service account that has appropriate permissions for the Spanner instance being targeted. In these cases, the tool will pick-up the configuration from the environment automatically.*

Optionally, create a service account to be used by the spanner client library for authentication against your Spanner instance.

Follow the steps described in [Creating a Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) to create a Service Account for this purpose. Once you have created your service account follow the steps described in [Creating a Service Account Key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) to create a key for the service account you just created and finally follow [these steps](https://cloud.google.com/iam/docs/granting-roles-to-service-accounts) to grant permissions to the service account.

Make sure to use a role with read and write access to Spanner, like Cloud Spanner Database User for example. You can have more information on the Cloud Spanner Roles [here](https://cloud.google.com/iam/docs/understanding-roles#spanner_name_short_roles).

## <a name="usage"></a>4. Usage

Note: this tool requires Python 3

Install the requirements for the python script by executing the following command:

```bash
pip3 install -r requirements.txt
```

Execute the spanner-loader python script with the required arguments.

```bash
python spanner-loader.py --instance_id=[Your Cloud Spanner instance ID] --database_id=[Your Cloud Spanner database ID] --table_id=[Your table name] --batchsize=[The number of rows to insert in a batch] --bucket_name=[The name of the bucket for the source file] --file_name=[The csv input data file] --schema_file=[The format file describing the input data file]

Optional parameters:

--delimiter=[The delimiter used between columns in source file]
--project_id=[Your Google Cloud Project id]
--path_to_credentials=[Path to the json file with the credentials] 
```