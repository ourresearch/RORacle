import csv
import os
import ast
from typing import Dict, List, Optional

# Importing here to avoid circular imports
# This will only be used for type annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .ror_matcher import RORRecord

# Global dictionary to cache ROR ID -> names mapping
ror_id_to_names = {}

def load_ror_names():
    """Load ROR IDs and names from the CSV file into a dictionary."""
    global ror_id_to_names
    
    # Skip if already loaded
    if ror_id_to_names:
        return ror_id_to_names
        
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    csv_path = os.path.join(project_root, 'data', 'ror_organizations.csv')
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Get the ROR ID
            ror_id = row['id']
            
            # Split names on semicolon and add main name
            names = [n.strip() for n in row['names'].split(';') if n.strip()]
            
            # Add acronyms if any
            acronyms = [a.strip() for a in row['acronyms'].split(';') if a.strip()]
            
            # Combine all names
            all_names = names + acronyms
            
            # Add to dictionary with both versions of the ID
            # Version with prefix
            full_id = f"https://ror.org/{ror_id}"
            ror_id_to_names[full_id] = all_names
            
            # Also add the version without prefix
            ror_id_to_names[ror_id] = all_names
            
    return ror_id_to_names

def create_ror_record(ror_id: str, location: Optional[str] = None) -> 'RORRecord':
    """
    Factory method to create a RORRecord from just an ID.
    
    Args:
        ror_id: The ROR ID (with or without https://ror.org/ prefix)
        location: Optional location string
        
    Returns:
        A RORRecord with names populated from the CSV file
    """
    from .ror_matcher import RORRecord
    
    # Ensure the names dictionary is loaded
    names_dict = load_ror_names()
    
    # Get names for this ROR ID
    names = names_dict.get(ror_id, [])
    
    # Create and return the RORRecord
    return RORRecord(id=ror_id, names=names, location=location)

def extract_ror_ids_from_labels(labels_str: str) -> List[str]:
    """
    Extract ROR IDs from labels string in insti_bench.tsv format.
    The format is a string representation of a list with elements like:
    '056jjra10 - Jewish General Hospital'
    
    Args:
        labels_str: String representation of labels from insti_bench.tsv
        
    Returns:
        List of ROR IDs extracted from the labels
    """
    try:
        # Use ast.literal_eval to safely parse the string representation of a list
        labels = ast.literal_eval(labels_str)
        
        # Extract IDs from each label
        ror_ids = []
        for label in labels:
            # Special case for "-1"
            if label == "-1" or label.startswith("-1 "):
                ror_ids.append("-1")
                continue
                
            # Extract ID - it's the part before " - "
            if " - " in label:
                ror_id = label.split(" - ")[0].strip()
                ror_ids.append(ror_id)
                
        return ror_ids
    except (SyntaxError, ValueError) as e:
        # If parsing fails, log the error and return an empty list
        print(f"Error parsing labels: {e} for string: {labels_str}")
        return []
