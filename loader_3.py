import xml.etree.ElementTree as ET
import json
import os
import time
import logging

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

def element_to_dict(elem):
    """
    Convert an XML element (with potential nested structure) to a dictionary.
    """
    node = {**elem.attrib}  # Base dictionary with attributes, if any

    for child in elem:
        child_dict = element_to_dict(child)  # Recursive call
        if child.tag in node:
            # Handle multiple children with the same tag as a list
            if not isinstance(node[child.tag], list):
                node[child.tag] = [node[child.tag]]
            node[child.tag].append(child_dict)
        else:
            node[child.tag] = child_dict

    # Add text content if available
    if elem.text and elem.text.strip():
        if node:
            node["_text"] = elem.text.strip()
        else:
            return elem.text.strip()

    return node

def parse_and_convert(file_path, chunk_size, output_dir):
    """
    Parse XML and convert to JSON, including nested fields.
    Logs the time taken for processing.
    """
    logging.info("Starting XML parsing and JSON conversion.")
    start_time = time.time()

    context = ET.iterparse(file_path, events=("start", "end"))
    messages = []
    file_count = 0

    # Track times for operations
    parsing_start_time = time.time()
    total_parsing_time = 0
    total_writing_time = 0

    for event, elem in context:
        if event == "end" and elem.tag == "Message":
            # Parse and convert the element to a dictionary
            message_start_time = time.time()
            message = element_to_dict(elem)
            messages.append(message)
            elem.clear()  # Free memory
            message_time = time.time() - message_start_time
            total_parsing_time += message_time

        # Write JSON after processing a chunk
        if len(messages) >= chunk_size:
            file_count += 1
            chunk_start_time = time.time()
            save_as_json(messages, output_dir, file_count)
            chunk_time = time.time() - chunk_start_time
            total_writing_time += chunk_time

            logging.info(
                f"Chunk {file_count}: Processed {len(messages)} messages in {chunk_time:.2f}s."
            )
            messages = []

    # Save remaining messages
    if messages:
        file_count += 1
        chunk_start_time = time.time()
        save_as_json(messages, output_dir, file_count)
        chunk_time = time.time() - chunk_start_time
        total_writing_time += chunk_time

        logging.info(
            f"Final chunk {file_count}: Processed {len(messages)} messages in {chunk_time:.2f}s."
        )

    end_time = time.time()
    logging.info("Parsing and conversion completed.")

    # Summary statistics
    logging.info(f"Total chunks processed: {file_count}")
    logging.info(f"Total parsing time: {total_parsing_time:.2f}s")
    logging.info(f"Total writing time: {total_writing_time:.2f}s")
    logging.info(f"Total processing time: {end_time - start_time:.2f}s")

def save_as_json(messages, output_dir, file_count):
    """
    Save messages as a JSON file and log the operation time.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"output_{file_count}.json")
    with open(filename, "w") as f:
        json.dump(messages, f, indent=2)

# Main execution
if __name__ == "__main__":
    input_file = "large_file.xml"  # Path to your XML file
    output_directory = "json_output"  # Directory to save JSON files
    chunk_size = 1000  # Number of messages per JSON file

    logging.info("Starting the ETL process.")
    parse_and_convert(input_file, chunk_size, output_directory)
    logging.info("ETL process completed successfully.")
