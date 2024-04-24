
import json 
from datetime import datetime
import os 

def save_data_as_json(data, target_folder="/app/raw_files"):
    """
    This function saves the provided data to a JSON file in the specified target folder. 
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    filename = f"{timestamp}.json"
    filepath = os.path.join(target_folder, filename)
    os.makedirs(target_folder, exist_ok=True)

    with open(filepath, "w") as file:
        json.dump(data, file, indent=3)

    print(f"Data saved as {filepath}")


def read_json(file_path):
    """
    Read a JSON file and return the data as a Python dictionary or list.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict or list: The data from the JSON file.
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"No such file: {file_path}")
    except json.JSONDecodeError:
        print(f"File is not a valid JSON: {file_path}")

def check_filename(filename):
    """This function checks that the file name follows the defined timestamp format
    """
    timestamp_format = "%Y-%m-%d %H:%M"  
    timestamp_str = filename.split('.')[0]
    try:
        if datetime.strptime(timestamp_str, timestamp_format) :
            return True
    except Exception:
        pass