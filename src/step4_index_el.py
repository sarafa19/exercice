import pymongo
import logging
from elasticsearch import Elasticsearch
import time


logging.basicConfig(
    filename="pipeline_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


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
        logging.info(f"Index '{index_name}' créé dans Elasticsearch.")
    else:
        logging.info(f"L'index '{index_name}' existe déjà.")

def extract_from_mongodb(mongo_uri, db_name="news_db", collection_name="curated_news"):
    logging.info(f"Connexion à MongoDB et extraction des documents de la collection '{collection_name}'...")
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    documents = list(collection.find())
    logging.info(f"{len(documents)} documents extraits de MongoDB.")
    return documents

def insert_into_elasticsearch(es_host, documents, index_name="news"):
    es = Elasticsearch([es_host])
    processed_count = 0
    missing_fields_count = 0

    for doc in documents:
        try:
            missing_fields = []
            if not doc.get("headline"):
                missing_fields.append("headline")
            if not doc.get("category"):
                missing_fields.append("category")
            if not doc.get("tokenized_representation"):
                missing_fields.append("tokenized_representation")

            if missing_fields:
                missing_fields_count += 1
                logging.warning(f"Document {doc['_id']} a des champs manquants : {', '.join(missing_fields)}")
                continue  

 
            es_doc = {
                "title": doc["headline"], 
                "category": doc["category"], 
                "tokens": doc["tokenized_representation"],
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            }


            es.index(index=index_name, document=es_doc)
            processed_count += 1
            logging.info(f"Document {doc['_id']} inséré dans Elasticsearch.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion du document {doc['_id']}: {e}")
    
    logging.info(f"Insertion terminée. {processed_count} documents insérés, {missing_fields_count} documents ignorés pour cause de champs manquants.")


def process_indexing_pipeline(mongo_uri, es_host, mongo_db_name="news_db", mongo_collection_name="curated_news", index_name="news"):
    try:
        print("1. Créer l'index dans Elasticsearch")
        create_elasticsearch_index(es_host, index_name)
        
        print("2.  Extraire les données depuis MongoDB")
        documents = extract_from_mongodb(mongo_uri, mongo_db_name, mongo_collection_name)
        
        print("# Insérer les documents dans Elasticsearch")
        insert_into_elasticsearch(es_host, documents, index_name)

        logging.info("Pipeline terminé avec succès.")
    except Exception as e:
        logging.error(f"Erreur dans le pipeline : {e}")


mongo_uri = "mongodb://localhost:27017/"
es_host = "http://localhost:9200"  


if __name__ == "__main__":
    process_indexing_pipeline(mongo_uri, es_host)
