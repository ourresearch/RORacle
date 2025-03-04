import csv
import json
import ast
import os
import sys
import requests
from typing import List, Dict

# Add the parent directory to sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

# Import the utility functions
from roracle.ror_utils import extract_ror_ids_from_google_sheet_labels, download_google_sheet_tests

class ROR_record:
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name
        }

class TestCase:
    def __init__(self, id: int, affiliation_string: str, ror_records: List[ROR_record]):
        self.id = id
        self.affiliation_string = affiliation_string
        self.ror_records = ror_records
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "affiliation_string": self.affiliation_string,
            "ror_records": [record.to_dict() for record in self.ror_records]
        }

def process_google_sheet():
    """Process the Google Sheet CSV and generate a JSON file with test cases."""
    # First, download the latest test cases from Google Sheets
    csv_path = download_google_sheet_tests()
    output_path = os.path.join(project_root, 'data', 'test_cases.json')

    test_cases = []
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Extract ROR IDs from the labels column
            ror_ids = extract_ror_ids_from_google_sheet_labels(row['labels'])
            
            # Skip if there are no valid ROR IDs (use -1 to indicate no matches expected)
            if len(ror_ids) == 1 and ror_ids[0] == "-1":
                continue
            
            # Create ROR records
            ror_records = []
            
            # The Google Sheet format already has full URLs, so just extract the name if available
            for ror_id in ror_ids:
                # For now, we don't have names in the CSV, so set to empty string
                # This could be enhanced later if names are added back to the Google Sheet
                ror_records.append(ROR_record(id=ror_id, name=""))
            
            # Create and add the test case with the actual ID from the sheet
            test_case = TestCase(
                id=int(row['id']),
                affiliation_string=row['affiliation_string'],
                ror_records=ror_records
            )
            test_cases.append(test_case)
    
    # Write to JSON
    with open(output_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(
            [test_case.to_dict() for test_case in test_cases],
            jsonfile,
            indent=2
        )
    
    print(f"Processed {len(test_cases)} test cases and saved to {output_path}")

if __name__ == "__main__":
    process_google_sheet()
