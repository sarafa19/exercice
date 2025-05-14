import os
import json
import boto3
import pymysql
from botocore.exceptions import ClientError

def download_file_from_s3(bucket_name, file_name, download_path):
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='root',
        aws_secret_access_key='root',
        region_name='us-east-1',
    )
    
    try:
        s3.download_file(bucket_name, file_name, download_path)
        print(f" Fichier téléchargé depuis S3 : {download_path}")
    except ClientError as e:
        print(f" Erreur lors du téléchargement : {e}")
        raise

def create_database_if_not_exists(host, user, password, db_name, port=3307):
    conn = pymysql.connect(host=host, user=user, password=password, port=port)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    cursor.close()
    conn.close()
    print(f" Base de données '{db_name}' vérifiée/créée.")


def clean_data(file_path):
    cleaned_rows = []

    with open(file_path, 'r') as f:
        for line in f:
            try:
                item = json.loads(line)
               
                if all(k in item and item[k] for k in ['link', 'headline', 'category', 'date']):
                    cleaned_rows.append(item)
            except json.JSONDecodeError:
                continue  
    
    print(f" {len(cleaned_rows)} lignes valides trouvées.")
    return cleaned_rows

def insert_into_mysql(data, host, user, password, db, port=3307):
    print(f"Tentative de connexion à MySQL sur {host}:{port} avec l'utilisateur '{user}'...")
    conn = pymysql.connect(host=host, user=user, password=password, database=db, port=port)
    print(" Connexion réussie.")
    cursor = conn.cursor()
  
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_staging (
            id INT AUTO_INCREMENT PRIMARY KEY,
            link TEXT,
            headline TEXT,
            category VARCHAR(255),
            short_description TEXT,
            authors TEXT,
            date DATE
        );
    """)
    print("Table `news_staging` vérifiée/créée avec succès.")

    print(f"Début de l'insertion des {len(data)} éléments...")
    print(f"Début de l'insertion des {len(data)} éléments...")
    for i, item in enumerate(data, start=1):
            try:
                print(f"Insertion de l'élément {i} : {item['headline']}...")
                cursor.execute("""
                    INSERT INTO news_staging (link, headline, category, short_description, authors, date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    item['link'],
                    item['headline'],
                    item['category'],
                    item.get('short_description', ''),
                    item.get('authors', ''),
                    item['date']
                ))
            except Exception as e:
                print(f" Erreur lors de l'insertion de l'élément {i} : {e}")
    
    conn.commit()
    print(f"Insertion terminée. {len(data)} éléments insérés dans `news_staging`.")
    cursor.close()
    conn.close()
    print(" Insertion terminée dans la table `news_staging`.")

if __name__ == "__main__":
    import argparse

    import sys
    sys.argv = [
        "step2_to_sql.py",
        "--bucket", "raw",
        "--file_name", "News_Category_Dataset_v3.json",
    ]

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket_name", required=True)
    parser.add_argument("--file_name", required=True)
    parser.add_argument("--mysql_host", default="localhost")
    parser.add_argument("--mysql_user", default="root")
    parser.add_argument("--mysql_password", default="root")
    parser.add_argument("--mysql_db", default="staging")
    args = parser.parse_args()

    local_path = "/tmp/news_data.json"

    download_file_from_s3(args.bucket_name, args.file_name, local_path)
    cleaned_data = clean_data(local_path)
    create_database_if_not_exists(args.mysql_host, args.mysql_user, args.mysql_password, args.mysql_db)
    insert_into_mysql(cleaned_data, args.mysql_host, args.mysql_user, args.mysql_password, args.mysql_db)
