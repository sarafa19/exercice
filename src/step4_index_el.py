import pymongo
from elasticsearch import Elasticsearch
import time

def create_elasticsearch_index(es_host, index_name="news"):
    es = Elasticsearch([es_host])

    index_body = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "category": {"type": "keyword"},
                "tokens": {"type": "dense_vector", "dims": 768}, 
                "timestamp": {"type": "date"}
            }
        }
    }
    

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_body)
        print(f"Index '{index_name}' créé dans Elasticsearch.")
    else:
        print(f"L'index '{index_name}' existe déjà.")


def extract_from_mongodb(mongo_uri, db_name="news_db", collection_name="curated_news"):
    print(f"Connexion à MongoDB et extraction des documents de la collection '{collection_name}'...")
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    documents = list(collection.find())
    print(f"{len(documents)} documents extraits de MongoDB.")
    return documents


def insert_into_elasticsearch(es_host, documents, index_name="news"):
    es = Elasticsearch([es_host])
    
    for doc in documents:
        
        es_doc = {
            "title": doc["headline"], 
            "category": doc["category"], 
            "tokens": doc["tokenized_representation"],
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")  
        }
        
 
        es.index(index=index_name, document=es_doc)
        print(f"Document inséré avec ID : {doc['_id']}")

# Fonction principale
def process_indexing_pipeline(mongo_uri, es_host, mongo_db_name="news_db", mongo_collection_name="curated_news", index_name="news"):
    print("1. Créer l'index dans Elasticsearch")
    create_elasticsearch_index(es_host, index_name)
    
    print("2. Extraire les données depuis MongoDB")
    documents = extract_from_mongodb(mongo_uri, mongo_db_name, mongo_collection_name)
    
    print("3. Insérer les documents dans Elasticsearch")
    insert_into_elasticsearch(es_host, documents, index_name)


mongo_uri = "mongodb://localhost:27017/"
es_host = "http://localhost:9200"  


if __name__ == "__main__":
    process_indexing_pipeline(mongo_uri, es_host)
