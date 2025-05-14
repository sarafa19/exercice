import json
import boto3
import os
import pymysql
from io import BytesIO

LOCALSTACK_ENDPOINT = "http://localhost:4566"
AWS_ACCESS_KEY = "root"
AWS_SECRET_KEY = "root"
REGION_NAME = "us-east-1"

MYSQL_CONFIG = {
    "host": "localhost", 
    "user": "root",
    "password": "root",
    "database": "staging",
    "port": 3307
}

BUCKET_NAME = "raw"
OBJECT_KEY = "cleaned_news.json"  


def download_and_clean_data():

    s3 = boto3.client(
        's3',
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION_NAME
    )

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    data = obj['Body'].read().decode('utf-8')

    print("Contenu brut JSON téléchargé depuis S3 :")
    print(data[:200])

    records = []
    for line in data.splitlines():
        try:
            record = json.loads(data)
            records.append(record)
        except json.JSONDecodeError:
            print("Erreur de lecture JSON.")
        continue

    cleaned = []
    for r in records:
        if all(k in r for k in ["headline", "short_description", "category", "link", "date"]):
            cleaned.append(r)

    print(f"{len(cleaned)} lignes valides conservées après nettoyage.")
    return cleaned


def insert_into_mysql(cleaned_data):
    connection = pymysql.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO news_staging (headline, short_description, category, link, date)
        VALUES (%s, %s, %s, %s, %s)
    """

    for row in cleaned_data:
        try:
            cursor.execute(insert_query, (
                row["headline"],
                row["short_description"],
                row["category"],
                row["link"],
                row["date"]
            ))
        except Exception as e:
            print("Erreur lors de l'insertion :", e)

    connection.commit()
    print(f"{cursor.rowcount} lignes insérées dans la table staging.")
    cursor.close()
    connection.close()


if __name__ == "__main__":
    print("Début du script Étape 2 : nettoyage et insertion")
    cleaned_data = download_and_clean_data()

    if cleaned_data:
        insert_into_mysql(cleaned_data)
