import os
import json
import subprocess
from datetime import datetime, timezone
import numpy as np
import iris
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('AWS-Take-Home').getOrCreate()


def get_bucket_links():
    """Gets links of raw-data S3 bucket from variables.json

    Returns:
        raw_data_bucket(str):
            S3 Bucket links of raw-data.
    """
    path_to_variables = 'variables.json'
    with open(path_to_variables, "r") as variables_json:
        variables = json.load(variables_json)
    raw_data_bucket = variables['etl']['raw_data_bucket']
    return raw_data_bucket


def utc_timestamp(hours_since_first_epoch):
    """Construct a timestamp of the format "%Y-%m-%d %H:%M:%S"
    for the given epoch.

    Arguments:
        hours_since_first_epoch (int):
            Epoch for reftime

    Returns:
        ts (str)
            Timestamp of the format "%Y-%m-%d %H:%M:%S"
    """
    epoch = hours_since_first_epoch * 60 * 60
    ts = datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")
    return ts


def create_feature_dataframe(data, feature, feature_dtype_mapping,
                             feature_index_mapping, dim_set):
    """Creates :class: ``pySpark.DataFrame`` for user-selected feature from the
    :class: ``iris.CubeList`` data.

    Arguments:
        data (iris.Cube):
            Iris data cube corresponding to the feature
        feature (str):
            Name of feature for which dataframe is to be created
        feature_dtype_mapping (dict):
            Maps feature to it's corresponding pyspark.sql.type for
            extending to schema
        feature_index_mapping (dict):
            Maps feature to it's corresponding parameter index
        dim_set (int):
            Type of dimension set the feature belongs to

    Returns:
        df (pyspark.DataFrame):
            Pyspark DataFrame corresponding to the feature
    """
    rows = []

    if dim_set == 1:
        schema = StructType([
            StructField("time", StringType(), True),
            StructField("grid_latitude", FloatType(), True),
            StructField("grid_longitude", FloatType(), True),
            StructField(feature, feature_dtype_mapping[feature], True),
        ])
    elif dim_set == 2:
        schema = StructType([
            StructField("time", StringType(), True),
            StructField("pressure", FloatType(), True),
            StructField("grid_latitude", FloatType(), True),
            StructField("grid_longitude", FloatType(), True),
            StructField(feature, feature_dtype_mapping[feature], True),
        ])

    feature_data = data.extract(feature)[feature_index_mapping[feature]]
    times = feature_data.coord('time').points
    latitudes = feature_data.coord('grid_latitude').points
    longitudes = feature_data.coord('grid_longitude').points

    if dim_set == 2:
        pressures = feature_data.coord('pressure').points

    # :func: ``data`` of :class: ``iris.Cube`` returns a numpy masked array
    feature_data = feature_data.data
    # Replacing missing values in numpy masked array with fill_value=1e+20
    np.ma.set_fill_value(feature_data, -999)
    feature_data = feature_data.filled()

    if dim_set == 1:
        for i, time in enumerate(times):
            time = utc_timestamp(time)
            for j, latitude in enumerate(latitudes):
                for k, longitude in enumerate(longitudes):
                    try:
                        rows.append([time, latitude.item(), longitude.item(), feature_data[i][j][k].item()])
                    except:
                        pass

    elif dim_set == 2:
        for i, time in enumerate(times):
            time = utc_timestamp(time)
            for j, pressure in enumerate(pressures):
                for k, latitude in enumerate(latitudes):
                    for l, longitude in enumerate(longitudes):
                        try:
                            rows.append([time, pressure.item(), latitude.item(), longitude.item(), feature_data[i][j][k][l].item()])
                        except:
                            pass

    df = spark.createDataFrame(rows, schema)
    return df


def process_netcdf(file_name):
    """Explore and extract features of NETCDF file using IRIS.

    Arguments:
        file_name (str):
            Name of NETCDF file

    Returns:
        dfs (list):
            List of pyspark dataframes for all features
    """
    s3 = boto3.resource('s3')
    raw_data_bucket = get_bucket_links()
    # Downloads NETCDF file into local bucket
    s3.Bucket(raw_data_bucket).download_file(file_name, 'tmp.nc')
    # Loading data into memory using NETCDF
    data = iris.load('tmp.nc')

    # Data loaded from the NETCDF files can have one of the following
    # set of dimensions:
    # 1. time, grid_latitude, grid_longitude
    # 2. time, pressure, grid_latitude, grid_longitude

    # ``features`` dictionary represents  feature with it's corresponding
    # dimension set, i.e, '1' or '2'.
    # Based on this dimension set, Pyspark dataframe is created for each
    # feature.

    features = {
        'dew_point_temperature': 1,
        'air_temperature': 1,
        'wind_speed_of_gust': 1,
    }
    
    # Every feature in the features list is mapped to it's corresponding
    # datatype, so as to create a schema for Pyspark DataFrame.
    feature_dtype_mapping = {
        'dew_point_temperature': FloatType(),
        'air_temperature': FloatType(),
        'wind_speed_of_gust': FloatType(),
    }

    # Certain features have multiple parameters.
    # For example, there are four parameters of 'air_temperature'.
    # This dictionary maps the feature with the index of it's
    # corresponding parameter.
    feature_index_mapping = {
        'dew_point_temperature': 0,
        'air_temperature': 2,
        'wind_speed_of_gust': 0,
    }

    # To add more features to be retrieved from the NETCDF file, the user
    # needs to add the feature to the ``feature`` dictionary, it's corresponding
    # data-type mapping for Pyspark Scheme in ``feature_dtype_mapping``,
    # and index of parameter in ``feature_index_mapping``.

    # Every point on a regular grid is a function of latitude, longitude
    # and when appropriate, altitude.
    # 'reference_time' in the MOGREPS-UK Dataset indicates that the forecast
    # was started from 0th time, i.e, epoch of the timestamp.

    # There are four attributes of 'air temperature' in the dataset, three for
    # the ground level, and one for particular pressure levels in atmosphere.

    # For this project, I chose to retrieve the forecast for an exact reftime.

    # Creating Pyspark DataFrame for formatted data for user-selected features.

    dfs = []

    for feature, dim_set in features.items():
        df = create_feature_dataframe(data, feature, feature_dtype_mapping,
                                      feature_index_mapping, dim_set)

        df = df.withColumn("year", year(col("time").cast("timestamp")))\
               .withColumn("month", month(col("time").cast("timestamp")))\
               .withColumn("day", dayofmonth(col("time").cast("timestamp")))\
               .repartition(1000)

        df = df.sort(asc('time')).coalesce(1)
        dfs.append([df, feature])

    # Deleting the NETCDF file from driver node
    os.remove('tmp.nc')
    return dfs
