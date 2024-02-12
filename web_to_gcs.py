import os
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage
import requests
import logging
import subprocess

"""
Pre-reqs:
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./influential-bit-412922-648c0711dbe7.json"
# services = ['fhv','green','yellow']
# init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "week3-data-ware-house")

yellow_taxi_dtypes = {
    'VendorID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'RatecodeID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'payment_type': pd.Int64Dtype(),
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'improvement_surcharge': float,
    'tip_amount': float,
    'tolls_amount': float,
    'total_amount': float,
    'congestion_surcharge': float,
    'Airport_fee': float
    # 'ehail_fee': float,
    # 'trip_type': float,
}

# native date parsing
parse_yellow_taxi_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

# table_schema_green = pa.schema(
#     [
#         ('VendorID',pa.string()),
#         ('lpep_pickup_datetime',pa.timestamp('s')),
#         ('lpep_dropoff_datetime',pa.timestamp('s')),
#         ('store_and_fwd_flag',pa.string()),
#         ('RatecodeID',pa.int64()),
#         ('PULocationID',pa.int64()),
#         ('DOLocationID',pa.int64()),
#         ('passenger_count',pa.int64()),
#         ('trip_distance',pa.float64()),
#         ('fare_amount',pa.float64()),
#         ('extra',pa.float64()),
#         ('mta_tax',pa.float64()),
#         ('tip_amount',pa.float64()),
#         ('tolls_amount',pa.float64()),
#         ('ehail_fee',pa.float64()),
#         ('improvement_surcharge',pa.float64()),
#         ('total_amount',pa.float64()),
#         ('payment_type',pa.int64()),
#         ('trip_type',pa.int64()),
#         ('congestion_surcharge',pa.float64()),
#     ]
# )

# table_schema_yellow = pa.schema(
#     [
#         ('VendorID', pa.string()),
#         ('tpep_pickup_datetime', pa.timestamp('s')),
#         ('tpep_dropoff_datetime', pa.timestamp('s')),
#         ('passenger_count', pa.int64()),
#         ('trip_distance', pa.float64()),
#         ('RatecodeID', pa.string()),
#         ('store_and_fwd_flag', pa.string()),
#         ('PULocationID', pa.int64()),
#         ('DOLocationID', pa.int64()),
#         ('payment_type', pa.int64()),
#         ('fare_amount',pa.float64()),
#         ('extra',pa.float64()),
#         ('mta_tax', pa.float64()),
#         ('tip_amount', pa.float64()),
#         ('tolls_amount', pa.float64()),
#         ('improvement_surcharge', pa.float64()),
#         ('total_amount', pa.float64()),
#         ('congestion_surcharge', pa.float64())
#     ]
# )


# def format_to_parquet(src_file, service):
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)

#     if service == 'yellow':
#         table = table.cast(table_schema_yellow)
#     elif service == 'green':
#         table = table.cast(table_schema_green)

#     pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # # csv file_name
        # file_name_gz = f"{service}_tripdata_{year}-{month}.csv.gz"

        # # download it using requests via a pandas df
        # request_url = f"{init_url}{service}/{file_name_gz}"
        # r = requests.get(request_url)
        # open(file_name_gz, 'wb').write(r.content)
        # print(f"Local: {file_name_gz}")

        # # read it back into a parquet file
        # df = pd.read_csv(file_name_gz, sep=",", compression='gzip', dtype=yellow_taxi_dtypes, parse_dates=parse_yellow_taxi_dates)
        # file_name_parquet = file_name_gz.replace('.csv.gz', '.parquet')
        # df.to_parquet(file_name_parquet, engine='pyarrow')
        # print(f"Parquet: {file_name_parquet}")

        # # upload it to gcs
        # upload_to_gcs(BUCKET, f"{service}/{file_name_parquet}", file_name_parquet)
        # print(f"GCS: {service}/{file_name_parquet}")

        # os.system(f"gzip -d {file_name_gz}")
        # # os.system(f"rm {file_name_gz}.*")

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # request url for week 3 homework
        request_url = f'{init_url}{file_name}'
        print(request_url)

        #request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        df = pq.read_table(file_name)

        #df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
web_to_gcs('2022', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
# web_to_gcs('2019', 'fhv')
# web_to_gcs('2020', 'fhv')
