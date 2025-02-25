import pandas as pd
import json
import ast
import os
from typing import List, Dict

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
    def __init__(self, affiliation_string: str, ror_records: List[ROR_record]):
        self.affiliation_string = affiliation_string
        self.ror_records = ror_records
    
    def to_dict(self) -> Dict:
        return {
            "affiliation_string": self.affiliation_string,
            "ror_records": [record.to_dict() for record in self.ror_records]
        }

def clean_labels_name(labels_name_str: str) -> List[str]:
    try:
        # Convert string representation of list to actual list
        names = ast.literal_eval(labels_name_str)
        # Clean up each name by removing the ID prefix if it exists
        cleaned_names = [name.split(' - ')[1] if ' - ' in name else name for name in names]
        return cleaned_names
    except:
        return []

def process_csv():
    # Get the absolute paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    input_path = os.path.join(project_root, 'data', 'openalex_gold_w_ror_ids.csv')
    output_path = os.path.join(project_root, 'data', 'test_cases.json')

    # Read the CSV file
    df = pd.read_csv(input_path)
    test_cases = []

    for _, row in df.iterrows():
        # Get institution names
        names = clean_labels_name(row['labels_name'])
        
        # Get ROR IDs
        ror_ids = []
        if pd.notna(row['ror_id']) and row['ror_id'] != '':
            ror_ids = [id.strip() for id in row['ror_id'].split(';')]
        
        # Validate lengths match
        if len(names) != len(ror_ids):
            print(f"Error: Mismatch in number of names and IDs for affiliation: {row['affiliation_string']}")
            print(f"Names: {names}")
            print(f"IDs: {ror_ids}")
            continue
            
        # Create ROR records
        ror_records = [ROR_record(id=id, name=name) 
                      for name, id in zip(names, ror_ids)]
        
        # Create TestCase
        test_case = TestCase(
            affiliation_string=row['affiliation_string'],
            ror_records=ror_records
        )
        
        test_cases.append(test_case)
    
    # Convert to JSON and save
    json_data = [tc.to_dict() for tc in test_cases]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    process_csv()
