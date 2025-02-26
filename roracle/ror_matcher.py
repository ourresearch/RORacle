from typing import List, Dict
from dataclasses import dataclass
from .ror_data_manager import ror_data, normalize_text

@dataclass
class RORRecord:
    id: str
    name: str
    alternate_names: List[str] = None
    matching_name: str = None
    is_matching_name_unique: bool = None
    location: str = None

    def __post_init__(self):
        # Add ROR URL prefix if not already present
        if not self.id.startswith('https://ror.org/'):
            self.id = f'https://ror.org/{self.id}'
        # Initialize empty list for alternate_names if None
        if self.alternate_names is None:
            self.alternate_names = []

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "alternate_names": self.alternate_names,
            "matching_name": self.matching_name,
            "is_matching_name_unique": self.is_matching_name_unique,
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
    """
    results = []
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
                # Create location string by joining country, subdivision, and location with semicolons
                location_parts = [
                    inst.location.country,
                    inst.location.country_subdivision_name,
                    inst.location.location_name
                ]
                location_string = ';'.join([part for part in location_parts if part])
                
                record = RORRecord(
                    id=inst.id, 
                    name=inst.name,
                    alternate_names=inst.alternate_names,
                    matching_name=name,
                    is_matching_name_unique=True,
                    location=location_string
                )
                results.append(record)
                remaining_text = remaining_text.replace(search_name, ' ')
                
            elif len(institutions) > 1:
                # Non-unique match - check for location matches
                for inst in institutions:
                    if inst.has_location_match(remaining_text):
                        # Create location string by joining country, subdivision, and location with semicolons
                        location_parts = [
                            inst.location.country,
                            inst.location.country_subdivision_name,
                            inst.location.location_name
                        ]
                        location_string = ';'.join([part for part in location_parts if part])
                        
                        record = RORRecord(
                            id=inst.id, 
                            name=inst.name,
                            alternate_names=inst.alternate_names,
                            matching_name=name,
                            is_matching_name_unique=False,
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
