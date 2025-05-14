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
    temp_cleaned_path = "./exercice/data/cleaned_news.json"

    # Lire ligne par ligne et convertir en liste JSON
    combined_data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line:
                try:
                    combined_data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    # Sauvegarder la liste en JSON bien formé
    with open(temp_cleaned_path, 'w', encoding='utf-8') as outfile:
        json.dump(combined_data, outfile, indent=2)

    # Nom du fichier à envoyer dans S3
    object_name = "cleaned_news.json"
    s3.upload_file(temp_cleaned_path, bucket_name, object_name)

    print(f"Fichier nettoyé '{temp_cleaned_path}' uploadé dans le bucket '{bucket_name}' sous le nom '{object_name}'.")

    # Nettoyage local (optionnel)
    os.remove(temp_cleaned_path)
    print(f"Fichier temporaire local supprimé : {temp_cleaned_path}")


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
