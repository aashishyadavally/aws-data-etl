from extract import copy_from_source
from transform import process_netcdf
from load import upload_csv_to_bucket, load_to_redshift


if __name__ == '__main__':
    copied_files = copy_from_source()
    for copied_file in copied_files:
        dfs = process_netcdf(copied_file)
        for [df, feature] in dfs:
            variables = upload_csv_to_bucket(df, feature)
            load_to_redshift(variables, feature)