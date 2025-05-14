import pymysql
import pymongo
from transformers import DistilBertTokenizer, DistilBertModel
import torch

def extract_data_from_mysql(host, user, password, db, port=3307):
    print("Connexion à MySQL pour extraction des données...")
    conn = pymysql.connect(host=host, user=user, password=password, database=db, port=port)
    cursor = conn.cursor()

    cursor.execute("SELECT headline, category FROM news_staging")
    data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    print(f"{len(data)} éléments extraits de MySQL.")
    return data


def tokenize_text(texts):
    print("Initialisation du tokenizer DistilBERT...")
    tokenizer = DistilBertTokenizer.from_pretrained("distilbert-base-uncased")
    model = DistilBertModel.from_pretrained("distilbert-base-uncased")
    
    print("Tokenisation des titres...")
    tokenized_texts = []
    
    for text in texts:
        tokens = tokenizer(text, padding=True, truncation=True, return_tensors="pt")
        outputs = model(**tokens)
        embeddings = outputs.last_hidden_state.mean(dim=1).detach().numpy()
        tokenized_texts.append(embeddings)

    print(f"Tokenisation terminée pour {len(texts)} textes.")
    return tokenized_texts


def insert_into_mongodb(data, tokenized_data, mongo_uri, db_name="news_db"):
    print("Connexion à MongoDB...")
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    collection = db.curated_news

    print(f"Insertion des documents dans MongoDB...")
    documents = []
    for i, (headline, category) in enumerate(data):
        enriched_document = {
            "headline": headline,
            "category": category,
            "tokenized_representation": tokenized_data[i].tolist()  # Conversion en liste pour insertion
        }
        documents.append(enriched_document)
    
    collection.insert_many(documents)
    print(f"{len(documents)} documents insérés dans la collection 'curated_news'.")


def process_pipeline(mysql_host, mysql_user, mysql_password, mysql_db, mongo_uri):
    print("1. Extraction des données depuis MySQL")
    data = extract_data_from_mysql(mysql_host, mysql_user, mysql_password, mysql_db)

    print(" 2. Tokenisation des titres avec DistilBERT")
    headlines = [item[0] for item in data]  
    tokenized_data = tokenize_text(headlines)

    insert_into_mongodb(data, tokenized_data, mongo_uri)

mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'root'
mysql_db = 'staging'
mongo_uri = "mongodb://localhost:27017/"


if __name__ == "__main__":
    process_pipeline(mysql_host, mysql_user, mysql_password, mysql_db, mongo_uri)
