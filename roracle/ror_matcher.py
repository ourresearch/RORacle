from typing import List, Dict, Tuple
from dataclasses import dataclass
from .ror_data_manager import ror_data, normalize_text
from .ror_utils import load_ror_names
import time
import logging

logger = logging.getLogger(__name__)

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
    Find ROR records for a given affiliation string using Trie-based string matching:
    1. Use a custom Trie to find all possible name matches
    2. Sort matches by position and length (prioritizing longer matches)
    3. Greedily select non-overlapping matches
    4. For non-unique matches, check for location matches
    5. For all-uppercase alternate names, verify case-sensitive match
    """
    start_time = time.time()
    
    # Load ROR names
    ror_names = load_ror_names()
    
    results = []
    # Store both normalized and original affiliation string
    original_affiliation = affiliation_string
    normalized_text_with_boundaries = ' ' + normalize_text(affiliation_string) + ' '  # Add spaces for boundary checking
    
    # Find all possible matches using Trie
    all_matches = ror_data.trie.search_all(normalized_text_with_boundaries)
    
    # Filter matches to ensure they have proper word boundaries
    word_boundary_matches = []
    for start_pos, end_pos, name in all_matches:
        # Check if this is a whole word/phrase with proper boundaries
        is_word = True
        if start_pos > 0 and normalized_text_with_boundaries[start_pos-1] != ' ':
            is_word = False
        if end_pos + 1 < len(normalized_text_with_boundaries) and normalized_text_with_boundaries[end_pos+1] != ' ':
            is_word = False
            
        if is_word:
            word_boundary_matches.append((start_pos, end_pos, name))
    
    # Sort matches by start position and then by length (descending)
    word_boundary_matches.sort(key=lambda x: (x[0], -len(x[2])))
    
    trie_time = time.time() - start_time
    logger.debug(f"Trie matching completed in {trie_time:.6f} seconds. Found {len(word_boundary_matches)} potential matches.")
    
    # Create a list to track which positions in the string have been matched
    text_positions = [False] * len(normalized_text_with_boundaries)
    
    # Process matches to handle overlaps and maintain existing logic
    for match_start, match_end, name in word_boundary_matches:
        # Check if any part of this match overlaps with a previously matched portion
        overlap = False
        for i in range(match_start, match_end + 1):
            if text_positions[i]:
                overlap = True
                break
                
        if not overlap:
            institutions = ror_data.name_to_institutions[name]
            is_unique = len(institutions) == 1
            
            if is_unique:
                # Unique match - add it and mark this portion of text as matched
                inst = institutions[0]
                
                # Check for uppercase alternate names (maintaining existing logic)
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
                    
                    # Mark the matched positions as used
                    for i in range(match_start, match_end + 1):
                        text_positions[i] = True
                
            elif len(institutions) > 1:
                # Non-unique match - check for location matches (maintaining existing logic)
                for inst in institutions:
                    # Check for uppercase alternate names
                    skip_match = False
                    if name in inst.original_alternate_names:
                        original_name = inst.original_alternate_names[name]
                        if original_name.isupper() and len(original_name) > 1:
                            if original_name not in original_affiliation:
                                skip_match = True
                    
                    if not skip_match and inst.has_location_match(normalized_text_with_boundaries):
                        # Create location string
                        location_parts = [
                            inst.location.country,
                            inst.location.country_subdivision_name,
                            inst.location.location_name
                        ]
                        location_string = ';'.join([part for part in location_parts if part])
                        
                        # Get all names for this institution
                        all_names = ror_names.get(inst.id, [])
                        
                        record = RORRecord(
                            id=inst.id, 
                            names=all_names,
                            location=location_string
                        )
                        results.append(record)
                        
                        # Mark the matched positions as used
                        for i in range(match_start, match_end + 1):
                            text_positions[i] = True
                        
                        break  # Take only the first location match
    
    processing_time = time.time() - start_time
    logger.debug(f"Total processing time: {processing_time:.6f} seconds. Found {len(results)} records.")
    
    return results
