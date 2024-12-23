import xml.etree.ElementTree as ET
import json
import os
import time
import logging
from multiprocessing import Pool

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

def element_to_dict(elem):
    """
    Convert an XML element (with potential nested structure) to a dictionary.
    """
    node = {**elem.attrib}  # Include element's attributes
    for child in elem:
        child_dict = element_to_dict(child)  # Recursive call for children
        if child.tag in node:
            if not isinstance(node[child.tag], list):
                node[child.tag] = [node[child.tag]]
            node[child.tag].append(child_dict)
        else:
            node[child.tag] = child_dict

    # Include text content
    if elem.text and elem.text.strip():
        if node:
            node["_text"] = elem.text.strip()
        else:
            return elem.text.strip()

    return node

def process_chunk(chunk, output_dir, chunk_id):
    """
    Process a single chunk of XML data and write it to a JSON file.
    """
    messages = []
    for message in chunk:
        # Parse the <Message> element into a dictionary
        parsed_message = element_to_dict(message)
        messages.append(parsed_message)

    # Write the chunk to a JSON file
    output_file = os.path.join(output_dir, f"output_{chunk_id}.json")
    with open(output_file, "w") as f:
        json.dump(messages, f, indent=2)

    logging.info(f"Chunk {chunk_id} processed with {len(messages)} messages.")
    return chunk_id, len(messages)

def split_xml(file_path, chunk_size):
    """
    Split the XML file into chunks of <Message> elements.
    """
    logging.info("Splitting XML into chunks.")
    context = ET.iterparse(file_path, events=("start", "end"))
    current_chunk = []
    chunks = []

    # Iterate over XML and group <Message> elements into chunks
    for event, elem in context:
        if event == "end" and elem.tag == "instrument":
            # Convert element to a dictionary *before* clearing it
            current_chunk.append(elem)
            
            if len(current_chunk) >= chunk_size:
                chunks.append(current_chunk)
                current_chunk = []
            
            elem.clear()  # Free memory after element is fully processed

    # Add the remaining chunk if any
    if current_chunk:
        chunks.append(current_chunk)

    logging.info(f"Split XML into {len(chunks)} chunks.")
    return chunks

def parallel_parse_and_convert(file_path, chunk_size, output_dir, num_workers):
    """
    Parse XML and convert to JSON using parallel processing.
    """
    os.makedirs(output_dir, exist_ok=True)
    start_time = time.time()

    # Split XML into chunks
    chunks = split_xml(file_path, chunk_size)

    # Use multiprocessing to process chunks in parallel
    with Pool(num_workers) as pool:
        tasks = [
            (chunk, output_dir, i + 1) for i, chunk in enumerate(chunks)
        ]
        results = pool.starmap(process_chunk, tasks)

    total_messages = sum(count for _, count in results)
    end_time = time.time()

    logging.info(f"Total chunks processed: {len(results)}")
    logging.info(f"Total messages processed: {total_messages}")
    logging.info(f"Total processing time: {end_time - start_time:.2f}s")

# Main execution
if __name__ == "__main__":
    input_file = "large_file.xml"  # Path to your XML file
    output_directory = "json_output"  # Directory to save JSON files
    chunk_size = 100  # Number of messages per chunk
    num_workers = 4  # Number of parallel processes

    logging.info("Starting parallel ETL process.")
    parallel_parse_and_convert(input_file, chunk_size, output_directory, num_workers)
    logging.info("Parallel ETL process completed successfully.")
