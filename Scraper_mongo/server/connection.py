from pymongo import MongoClient
from decouple import config

# Conectando com o banco de dados em servidor
host = config("MONGO_HOST")
client = MongoClient(host)

db = client.greyDB