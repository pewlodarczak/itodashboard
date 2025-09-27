'''
pip install pandas pymongo pyyaml
'''
import pandas as pd
from pymongo import MongoClient
import yaml

# Load the CSV
df = pd.read_csv("iot_telemetry_data.csv")

# Convert timestamp from float seconds to datetime
df['ts'] = pd.to_datetime(df['ts'], unit='s')

# Convert data types
df['co'] = df['co'].astype(float)
df['humidity'] = df['humidity'].astype(float)
df['light'] = df['light'].astype(str).str.lower() == 'true'
df['lpg'] = df['lpg'].astype(float)
df['motion'] = df['motion'].astype(str).str.lower() == 'true'
df['smoke'] = df['smoke'].astype(float)
df['temp'] = df['temp'].astype(float)

# Convert to dictionary
records = df.to_dict(orient='records')

def load_config(path="config.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)

config = load_config()

uri = config["mongodb"]["uri"]
database_name = config["mongodb"]["database"]
collection_name = config["mongodb"]["collection"]

# Connect to MongoDB Atlas
client = MongoClient(uri)
db = client[database_name]
collection = db[collection_name]

# Insert data
collection.insert_many(records)
print("Import complete.")
