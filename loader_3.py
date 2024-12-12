import xml.etree.ElementTree as ET
import json
import os

def element_to_dict(elem):
    """
    Convert an XML element (with potential nested structure) to a dictionary.
    """
    # Base dictionary with attributes, if any
    node = {**elem.attrib}  

    # Process child elements
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
        if node:  # Combine text with attributes or children
            node["_text"] = elem.text.strip()
        else:
            return elem.text.strip()

    return node

def parse_and_convert(file_path, chunk_size, output_dir):
    """Parse XML and convert to JSON, including nested fields."""
    context = ET.iterparse(file_path, events=("start", "end"))
    messages = []
    file_count = 0

    for event, elem in context:
        if event == "end" and elem.tag == "Message":  # Replace 'Message' with your XML tag
            message = element_to_dict(elem)  # Convert the element to a dictionary
            messages.append(message)
            elem.clear()  # Free memory

        # Write JSON after processing a chunk
        if len(messages) >= chunk_size:
            file_count += 1
            save_as_json(messages, output_dir, file_count)
            messages = []

    # Save remaining messages
    if messages:
        file_count += 1
        save_as_json(messages, output_dir, file_count)

def save_as_json(messages, output_dir, file_count):
    """Save messages as JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"output_{file_count}.json")
    with open(filename, "w") as f:
        json.dump(messages, f, indent=2)

# Main execution
if __name__ == "__main__":
    input_file = "large_file.xml"  # Path to your XML file
    output_directory = "json_output"  # Directory to save JSON files
    chunk_size = 1000  # Number of messages per JSON file

    parse_and_convert(input_file, chunk_size, output_directory)
