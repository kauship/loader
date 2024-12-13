import xml.etree.ElementTree as ET
import json
import os
import logging
import asyncio
from multiprocessing import Pool, cpu_count
from aiofiles import open as aio_open

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

def element_to_dict(elem):
    """
    Convert an XML element to a dictionary, including nested structures and attributes.
    """
    node = {**elem.attrib}  # Add element's attributes
    for child in elem:
        child_dict = element_to_dict(child)
        if child.tag in node:
            if not isinstance(node[child.tag], list):
                node[child.tag] = [node[child.tag]]
            node[child.tag].append(child_dict)
        else:
            node[child.tag] = child_dict

    if elem.text and elem.text.strip():
        node["_text"] = elem.text.strip()
    return node

async def write_json(output_dir, chunk_id, messages):
    """
    Asynchronously write JSON to a file.
    """
    output_file = os.path.join(output_dir, f"output_{chunk_id}.json")
    async with aio_open(output_file, "w") as f:
        await f.write(json.dumps(messages, indent=2))
    logging.info(f"Written chunk {chunk_id} with {len(messages)} messages.")

def parse_and_convert(file_path, start_tag, chunk_size, chunk_id):
    """
    Parse a segment of the XML file and convert it to JSON.
    """
    context = ET.iterparse(file_path, events=("end",))
    messages = []
    count = 0

    for event, elem in context:
        if elem.tag == start_tag:
            message_dict = element_to_dict(elem)
            messages.append(message_dict)
            count += 1

            if count % chunk_size == 0:
                yield messages
                messages = []

            elem.clear()

    # Yield remaining messages
    if messages:
        yield messages

def process_chunk(file_path, start_tag, output_dir, chunk_size, chunk_id):
    """
    Process a chunk of the XML file, parse, and write to JSON.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Parse the XML chunk and get messages
    for messages in parse_and_convert(file_path, start_tag, chunk_size, chunk_id):
        loop.run_until_complete(write_json(output_dir, chunk_id, messages))
        chunk_id += 1

    loop.close()

def parallel_process(file_path, output_dir, start_tag, chunk_size, num_workers):
    """
    Run parallel processing on XML file.
    """
    os.makedirs(output_dir, exist_ok=True)
    with Pool(num_workers) as pool:
        pool.starmap(
            process_chunk,
            [
                (file_path, start_tag, output_dir, chunk_size, i)
                for i in range(num_workers)
            ],
        )

if __name__ == "__main__":
    input_file = "large_file.xml"  # Replace with your file
    output_directory = "json_output"
    start_tag = "Message"  # Root tag of messages
    chunk_size = 100
    num_workers = cpu_count()

    logging.info("Starting parallel processing.")
    parallel_process(input_file, output_directory, start_tag, chunk_size, num_workers)
    logging.info("Processing complete.")
