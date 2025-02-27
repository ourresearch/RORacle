import json
import csv
import os
from pathlib import Path
from collections import defaultdict

# Define file paths
DATA_DIR = Path(__file__).parent.parent / "data"
ORG_CSV_FILE = DATA_DIR / "ror_organizations.csv"
NAME_TO_IDS_FILE = DATA_DIR / "ror_names_to_ids.csv"

def create_name_to_ids_mapping():
    """
    Create a CSV file mapping each unique name to a list of ROR IDs.
    The CSV has two columns: name and ids, with ids being a pipe-separated list.
    Names are sorted by length (longest first).
    """
    print("Creating name-to-ids mapping from existing ROR organizations data...")
    
    # Dictionary to map names to ROR IDs
    name_to_ids = defaultdict(list)
    
    # Read the existing CSV file
    with open(ORG_CSV_FILE, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ror_id = row['id']
            
            # Process acronyms
            if row['acronyms']:
                for acronym in row['acronyms'].split(';'):
                    name_to_ids[acronym].append(ror_id)
            
            # Process names
            if row['names']:
                for name in row['names'].split(';'):
                    name_to_ids[name].append(ror_id)
    
    print(f"Found {len(name_to_ids)} unique names across all ROR records.")
    
    # Sort names by length (longest first)
    sorted_names = sorted(name_to_ids.keys(), key=len, reverse=True)
    
    # Write to CSV
    with open(NAME_TO_IDS_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'ids'])  # Header
        
        for name in sorted_names:
            # Join the IDs with pipe separators
            ids_pipe_separated = "|".join(name_to_ids[name])
            writer.writerow([name, ids_pipe_separated])
    
    # Print statistics
    names_size = os.path.getsize(NAME_TO_IDS_FILE) / (1024 * 1024)  # MB
    print(f"\nNames to IDs CSV size: {names_size:.1f} MB")
    
    # Print sample of first few rows
    print("\nSample of first 5 rows in name-to-ids CSV file:")
    with open(NAME_TO_IDS_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i <= 5:  # header + 4 rows
                print(line.strip())

def process_ror_data():
    """
    Original function to process ROR JSON data into a CSV.
    This function assumes the original JSON file is available.
    Currently not in use.
    """
    print("This function requires the original ROR JSON data file, which is not available.")
    print("Please use create_name_to_ids_mapping() instead.")

if __name__ == "__main__":
    create_name_to_ids_mapping()
