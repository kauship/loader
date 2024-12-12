import xml.etree.ElementTree as ET
from multiprocessing import Pool, cpu_count
from pymongo import MongoClient
import json
import os

# MongoDB Setup
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "mydb"
COLLECTION_NAME = "mycollection"

def parse_and_convert(file_path, start, end):
    """Parse part of the XML file and convert to JSON."""
    context = ET.iterparse(file_path, events=("start", "end"))
    messages = []
    for event, elem in context:
        if event == "end" and elem.tag == "Message":  # Replace with your XML tag
            messages.append(elem.attrib)  # Convert element to JSON-compatible dict
            elem.clear()  # Free memory

        if len(messages) >= 1000:  # Batch size for processing
            save_as_json(messages)
            messages = []
    
    if messages:
        save_as_json(messages)

def save_as_json(messages):
    """Save batch of messages as a JSON file."""
    filename = f"output_{os.getpid()}.json"
    with open(filename, "w") as f:
        json.dump(messages, f)

def upload_to_mongo(file_path):
    """Upload JSON data into MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    with open(file_path, "r") as f:
        data = json.load(f)
        collection.insert_many(data)

def process_file(file_path):
    """Main ETL function."""
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // cpu_count()
    
    with Pool(cpu_count()) as pool:
        offsets = [(file_path, i, i + chunk_size) for i in range(0, file_size, chunk_size)]
        pool.starmap(parse_and_convert, offsets)
    
    json_files = [f for f in os.listdir() if f.startswith("output_") and f.endswith(".json")]
    with Pool(cpu_count()) as pool:
        pool.map(upload_to_mongo, json_files)

# Run the pipeline
process_file("large_file.xml")
