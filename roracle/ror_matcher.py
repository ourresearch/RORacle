from typing import List, Dict
from dataclasses import dataclass
from .ror_data_manager import ror_data, normalize_text
from .ror_utils import load_ror_names

@dataclass
class RORRecord:
    id: str
    names: List[str] = None
    location: str = None

    def __post_init__(self):
        # Add ROR URL prefix if not already present
        if not self.id.startswith('https://ror.org/'):
            self.id = f'https://ror.org/{self.id}'
        # Initialize empty list for names if None
        if self.names is None:
            self.names = []

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "names": self.names,
            "location": self.location
        }

def is_standalone_word(text: str, word: str) -> bool:
    """Check if word appears as a complete word/phrase in text."""
    idx = text.find(word)
    if idx == -1:
        return False
        
    # Check boundaries
    text_len = len(text)
    word_len = len(word)
    end_idx = idx + word_len
    
    # Check start boundary
    if idx > 0 and text[idx-1] != ' ':
        return False
        
    # Check end boundary
    if end_idx < text_len and text[end_idx] != ' ':
        return False
        
    return True

def find_ror_records(affiliation_string: str) -> List[RORRecord]:
    """
    Find ROR records for a given affiliation string using the following approach:
    1. Normalize the input string
    2. Try to match institution names from longest to shortest
    3. For non-unique matches, check for location matches
    4. Remove matched text and continue searching
    5. For all-uppercase alternate names, verify case-sensitive match
    """
    # Load ROR names
    ror_names = load_ror_names()
    
    results = []
    # Store both normalized and original affiliation string
    original_affiliation = affiliation_string
    remaining_text = ' ' + normalize_text(affiliation_string) + ' '  # Add spaces for boundary checking
    
    for name in ror_data.sorted_names:
        # Skip very short names (less than 3 characters) to prevent spurious matches
        if len(name) < 3:
            continue
            
        # Check if name exists as a whole word/phrase
        search_name = ' ' + name + ' '
        if search_name in remaining_text:
            institutions = ror_data.name_to_institutions[name]
            is_unique = len(institutions) == 1
            
            if is_unique:
                # Unique match - add it and remove the text
                inst = institutions[0]
                
                # Check if this is an all-uppercase alternate name that requires case-sensitive matching
                skip_match = False
                if name in inst.original_alternate_names:
                    original_name = inst.original_alternate_names[name]
                    # If the original name is all uppercase (like "LABS")
                    if original_name.isupper() and len(original_name) > 1:
                        # Check if it exists in the original affiliation string
                        if original_name not in original_affiliation:
                            # Skip this match if the uppercase version doesn't appear in the original text
                            skip_match = True
                
                if not skip_match:
                    # Create location string by joining country, subdivision, and location with semicolons
                    location_parts = [
                        inst.location.country,
                        inst.location.country_subdivision_name,
                        inst.location.location_name
                    ]
                    location_string = ';'.join([part for part in location_parts if part])
                    
                    # Get all names for this institution from the loaded names
                    all_names = ror_names.get(inst.id, [])
                    
                    record = RORRecord(
                        id=inst.id, 
                        names=all_names,
                        location=location_string
                    )
                    results.append(record)
                    remaining_text = remaining_text.replace(search_name, ' ')
                
            elif len(institutions) > 1:
                # Non-unique match - check for location matches
                for inst in institutions:
                    # Check if this is an all-uppercase alternate name that requires case-sensitive matching
                    skip_match = False
                    if name in inst.original_alternate_names:
                        original_name = inst.original_alternate_names[name]
                        # If the original name is all uppercase (like "LABS")
                        if original_name.isupper() and len(original_name) > 1:
                            # Check if it exists in the original affiliation string
                            if original_name not in original_affiliation:
                                # Skip this match if the uppercase version doesn't appear in the original text
                                skip_match = True
                    
                    if not skip_match and inst.has_location_match(remaining_text):
                        # Create location string by joining country, subdivision, and location with semicolons
                        location_parts = [
                            inst.location.country,
                            inst.location.country_subdivision_name,
                            inst.location.location_name
                        ]
                        location_string = ';'.join([part for part in location_parts if part])
                        
                        # Get all names for this institution from the loaded names
                        all_names = ror_names.get(inst.id, [])
                        
                        record = RORRecord(
                            id=inst.id, 
                            names=all_names,
                            location=location_string
                        )
                        results.append(record)
                        remaining_text = remaining_text.replace(search_name, ' ')
                        break  # Take only the first location match
            
            # Normalize whitespace after removal
            remaining_text = ' '.join(remaining_text.split())
            remaining_text = ' ' + remaining_text + ' '  # Add spaces back for next iteration
            
            if remaining_text.strip() == '':
                break  # Nothing left to match
    
    return results
