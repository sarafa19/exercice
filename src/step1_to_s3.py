import os
import json
import boto3
from botocore.exceptions import ClientError


def upload_data_to_s3(bucket_name):

    s3 = boto3.client('s3', 
                    endpoint_url='http://localhost:4566',
                    aws_access_key_id='root',
                    aws_secret_access_key='root',
                    region_name='us-east-1',
                    )

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket '{bucket_name}'.")

    file_path = "./exercice/data/News_Category_Dataset_v3.json"
    object_name = os.path.basename(file_path) 
    s3.upload_file(file_path, bucket_name, object_name)
    print(f"Fichier '{file_path}' uploadé dans le bucket '{bucket_name}' sous le nom '{object_name}'.")

    #os.remove(combined_json_path)
    #print(f"Deleted local file: {combined_json_path}")


if __name__ == "__main__":
    import argparse
    print('Script started')

    import sys
    sys.argv = [
        "step1_to_s3.py",
        "--bucket_name", "raw", 
    ]

    parser = argparse.ArgumentParser(description="Upload data")
    parser.add_argument("--bucket_name", type=str, required=True, help="Name of the S3 bucket")
    args = parser.parse_args()

    upload_data_to_s3(args.bucket_name)