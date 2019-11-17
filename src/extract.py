import json
from pathlib import Path
import boto3


def get_bucket_links():
    """Returns list of bucket links from variables.json

    Returns:
        source_bucket, dest_bucket (tuple):
            S3 Bucket links of data-source and raw-data respectively.
    """
    path_to_variables = 'variables.json'
    with open(path_to_variables, "r") as variables_json:
        variables = json.load(variables_json)
    source_bucket = variables['etl']['source_bucket']
    dest_bucket = variables['etl']['raw_data_bucket']
    return source_bucket, dest_bucket


def get_s3_diff(source, destination):
    """Gets list of files which are missing in raw-data source bucket.

    Arguments:
        source (str):
            S3 Bucket link of data-source
        destination (str):
            S3 Bucket link of raw-data

    Returns:
        (list)
            List of NETCDF files present in S3 Bucket of data-source and not
            present in raw-data S3 Bucket.
    """
    s3 = boto3.resource('s3')
    try:
        source_bucket = s3.Bucket(source)
        source_contents = [object.key for object in source_bucket.objects.all()]
    except KeyError:
        source_contents = []

    try:
        dest_bucket = s3.Bucket(destination)
        dest_contents = [object.key for object in dest_bucket.objects.all()]
        # Removing non-NETCDF files
        dest_contents = [file for file in dest_contents if file.endswith('.nc')]
    except KeyError:
        dest_contents = []

    return list(set(source_contents) - set(dest_contents))


def copy_from_source():
    """Compares raw-data bucket with data source, and downloads missing files.

    Returns:
        copied_files (list):
            List of files copied from data-source S3 Bucket to raw-data S3 Bucket.
    """
    source, destination = get_bucket_links()
    copy_files = get_s3_diff(source, destination)

    s3 = boto3.resource('s3')
    copied_files = []

    for file in copy_files:
        copy_source = {'Bucket': source, 'Key': file}
        s3.meta.client.copy(copy_source, destination, file)
        copied_file = Path(destination) / file
        assert str(copied_file).endswith('.nc')
        copied_files.append(file)
    return copied_files
