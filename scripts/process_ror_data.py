import json
import csv
import os
from pathlib import Path

INPUT_FILE = "/Users/jasonpriem/Downloads/v1.59-2025-01-23-ror-data/v1.59-2025-01-23-ror-data_schema_v2.json"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "ror_organizations.csv"

def extract_names_by_type(names, target_type):
    """Extract name values of a specific type, removing duplicates."""
    values = set()
    for name in names:
        if "types" in name and target_type in name["types"]:
            values.add(name["value"])
    return ";".join(sorted(values))

def process_ror_data():
    print("Reading and processing ROR data...")
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    
    print(f"Processing {len(data)} organizations...")
    
    # Open CSV file for writing
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            'id', 'acronyms', 'names', 'country_name', 
            'country_subdivision_name', 'location_name'
        ])
        writer.writeheader()
        
        for item in data:
            # Extract and clean ID
            raw_id = item['id']
            clean_id = raw_id.replace('https://ror.org/', '')
            
            # Process names
            acronyms = extract_names_by_type(item['names'], 'acronym')
            
            # Get all non-acronym names
            other_names = set()
            for name in item['names']:
                if 'types' not in name or 'acronym' not in name['types']:
                    other_names.add(name['value'])
            names = ";".join(sorted(other_names))
            
            # Process location (using first location if multiple exist)
            location_data = {
                'country_name': '',
                'country_subdivision_name': '',
                'location_name': ''
            }
            
            if item.get('locations') and len(item['locations']) > 0:
                geonames = item['locations'][0].get('geonames_details', {})
                location_data = {
                    'country_name': geonames.get('country_name', ''),
                    'country_subdivision_name': geonames.get('country_subdivision_name', ''),
                    'location_name': geonames.get('name', '')
                }
            
            # Write record to CSV
            writer.writerow({
                'id': clean_id,
                'acronyms': acronyms,
                'names': names,
                'country_name': location_data['country_name'],
                'country_subdivision_name': location_data['country_subdivision_name'],
                'location_name': location_data['location_name']
            })
    
    # Print statistics
    original_size = os.path.getsize(INPUT_FILE) / (1024 * 1024)  # MB
    new_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)  # MB
    print(f"\nFile size comparison:")
    print(f"Original JSON size: {original_size:.1f} MB")
    print(f"New CSV size: {new_size:.1f} MB")
    print(f"Size reduction: {((original_size - new_size) / original_size * 100):.1f}%")
    
    # Print sample of first few rows
    print("\nSample of first 3 rows in new CSV file:")
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i <= 3:  # header + 2 rows
                print(line.strip())

if __name__ == "__main__":
    process_ror_data()
