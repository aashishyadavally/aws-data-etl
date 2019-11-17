import os
import shutil
import json
import boto3
import psycopg2
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext

# Initialize the spark session
spark = SparkSession.builder.appName('AWS-Take-Home').getOrCreate()
sql_context = SQLContext(spark.sparkContext)


def upload_csv_to_bucket(df, feature):
    """Uploads CSV to temporary S3 Bucket.

    Arguments:
        df (pyspark.DataFrame):
            DataFrame which needs to be written to CSV
        feature (str):
            Feature for which dataframe was created

    Returns:
        variables (dict):
            Dictionary of variables for connections
    """
    with open('variables.json', 'r') as variables_json:
        variables = json.load(variables_json)

    temps3dir = variables['etl']['temps3dir']

    df.coalesce(1).write.save(f"tmp_{feature}", format='csv', header=True)
    csv_file = [f for f in os.listdir(f"tmp_{feature}") if f.endswith('.csv')][0]
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(f'tmp_{feature}/{csv_file}', temps3dir, f'{feature}.csv')
    shutil.rmtree(f'tmp_{feature}')
    return variables


def load_to_redshift(variables, feature):
    """Loads data from temporary S3 Bucket to Redshift Data Warehouse.

    Arguments:
        variables (dict):
            Dictionary of connection variables retrieved from variables.json
        feature (str):
            Feature for which table is to be updated.
    """
    # Retrieving connection variables
    db = variables['etl']['jdbc']['db']
    port = variables['etl']['jdbc']['port']
    schema = variables['etl']['jdbc']['schema']
    table = variables['etl']['jdbc']['dbtable'][feature]
    user = variables['etl']['jdbc']['user_name']
    password = variables['etl']['jdbc']['password']
    host_url = variables['etl']['jdbc']['url']
    s3_bucket_name = variables['etl']['temps3dir']
    file_path = f"s3://{variables['etl']['temps3dir']}/{feature}.csv"
    aws_access_key_id = variables['access']['access_key']
    aws_secret_access_key = variables['access']['secret_access_key']

    # Trying connection to Redshift
    # NOTE: Ensure Redshift, S3, EC2 and EMR belong to same security group
    try:
        connection = psycopg2.connect(dbname=db, port=port, user=user,
                                      password=password, host=host_url)
        print("Connection Successful!")
    except psycopg2.OperationalError as e:
        print(f"Unable to connect to Redshift: {e}")

    cursor = connection.cursor()

    # Redshift Query:
    # COPY <schema>.<table> from '<bucket>/<file>.csv'
    # credentials
    # 'aws_access_key_id=<access-key>;aws_secret_access_key=<secret-access-key>'
    # CSV IGNOREHEADER 1; commit;

    sql="""COPY {}.{} from '{}'\
        credentials \
        'aws_access_key_id={};aws_secret_access_key={}' \
        CSV IGNOREHEADER 1; commit;""".format(schema, table, file_path,
                                              aws_access_key_id, aws_secret_access_key)

    # Trying to execute COPY command
    try:
        cursor.execute(sql)
        print("Copy Command executed successfully")
    except psycopg2.Error as e:
        print(f"Failed to execute copy command: {e}")
    connection.close()

    # Deleting temporary CSV files from temps3dir
    s3 = boto3.resource('s3')
    s3.Object(s3_bucket_name, f"{feature}.csv").delete()
