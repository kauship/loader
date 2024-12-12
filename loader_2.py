import xml.etree.ElementTree as ET
import json
import os
from multiprocessing import Pool, cpu_count

def parse_and_convert(file_path, chunk_size, output_dir):
    """Parse part of the XML file and save JSON files."""
    context = ET.iterparse(file_path, events=("start", "end"))
    messages = []
    file_count = 0

    for event, elem in context:
        if event == "end" and elem.tag == "Message":  # Replace with your XML tag
            messages.append(elem.attrib)  # Convert XML element to a dictionary
            elem.clear()  # Free memory

        # Write to JSON after reaching the chunk size
        if len(messages) >= chunk_size:
            file_count += 1
            save_as_json(messages, output_dir, file_count)
            messages = []

    # Save any remaining messages
    if messages:
        file_count += 1
        save_as_json(messages, output_dir, file_count)

def save_as_json(messages, output_dir, file_count):
    """Save batch of messages as a JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"output_{file_count}.json")
    with open(filename, "w") as f:
        json.dump(messages, f, indent=2)  # Pretty-print for readability

def process_file_in_parallel(file_path, chunk_size, output_dir):
    """Process the XML file in parallel to generate JSON files."""
    file_size = os.path.getsize(file_path)
    num_cores = cpu_count()
    chunk_offsets = [(file_path, chunk_size, output_dir)] * num_cores

    with Pool(num_cores) as pool:
        pool.starmap(parse_and_convert, chunk_offsets)

# Main execution
if __name__ == "__main__":
    input_file = "large_file.xml"  # Path to the large XML file
    output_directory = "json_output"  # Directory to save JSON files
    chunk_size = 1000  # Number of messages per JSON file

    parse_and_convert(input_file, chunk_size, output_directory)
