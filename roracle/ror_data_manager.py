import csv
import os
import string
from typing import Dict, List, Set, Tuple, Any
from dataclasses import dataclass
import re
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple Trie implementation
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.value = None

class Trie:
    def __init__(self):
        self.root = TrieNode()
        
    def insert(self, word: str, value: Any = None):
        """Insert a word into the trie with optional value"""
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True
        node.value = value if value is not None else word
        
    def search_all(self, text: str):
        """Find all occurrences of trie words in the text"""
        matches = []
        n = len(text)
        
        # Check each position in the text as a potential start
        for start_pos in range(n):
            node = self.root
            # Try to match word starting at current position
            for i in range(start_pos, n):
                char = text[i]
                # If character is not in trie, break
                if char not in node.children:
                    break
                # Move to next node
                node = node.children[char]
                # If this is a word ending, we found a match
                if node.is_end_of_word:
                    end_pos = i
                    value = node.value
                    matches.append((start_pos, end_pos, value))
                    
        return matches

@dataclass
class RORLocation:
    country: str
    country_subdivision_name: str
    location_name: str

@dataclass
class RORInstitution:
    id: str
    name: str
    location: RORLocation
    alternate_names: List[str]
    original_alternate_names: Dict[str, str] = None  # Map from normalized to original

    def has_location_match(self, text: str) -> bool:
        """Check if any location information matches the given text"""
        normalized_text = normalize_text(text)
        location_terms = {
            normalize_text(self.location.country),
            normalize_text(self.location.country_subdivision_name),
            normalize_text(self.location.location_name)
        }
        # Remove empty strings from location terms
        location_terms = {t for t in location_terms if t}
        return any(term in normalized_text for term in location_terms)

class RORDataManager:
    def __init__(self):
        self.name_to_institutions: Dict[str, List[RORInstitution]] = {}
        self.sorted_names: List[str] = []
        self.trie = Trie()  # Replace automaton with trie
        logger.info("Starting RORDataManager initialization...")
        start_time = time.time()
        self._load_data()
        logger.info(f"RORDataManager initialization completed in {time.time() - start_time:.2f} seconds")
        logger.info(f"Total unique institution names: {len(self.name_to_institutions)}")

    def _load_data(self):
        """Load ROR data from CSV and build lookup dictionary"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        csv_path = os.path.join(project_root, 'data', 'ror_organizations.csv')

        # Time CSV reading
        csv_start = time.time()
        logger.info("Reading CSV file...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        csv_time = time.time() - csv_start
        logger.info(f"CSV read completed in {csv_time:.2f} seconds. Found {len(rows)} organizations.")

        # Time institution processing
        proc_start = time.time()
        logger.info("Processing institutions and building name mappings...")
        name_count = 0
        for row in rows:
            # Split names on semicolon
            names = [n.strip() for n in row['names'].split(';') if n.strip()]
            main_name = names[0] if names else ""
            
            # Get acronyms if any
            acronyms = [a.strip() for a in row['acronyms'].split(';') if a.strip()]
            
            # Create institution
            institution = RORInstitution(
                id=row['id'],
                name=main_name,
                location=RORLocation(
                    country=row['country_name'],
                    country_subdivision_name=row['country_subdivision_name'],
                    location_name=row['location_name']
                ),
                alternate_names=[normalize_text(n) for n in names[1:] + acronyms],  # Additional names plus acronyms
                original_alternate_names={normalize_text(n): n for n in names[1:] + acronyms}
            )
            
            # Add main name
            self._add_name_mapping(institution.name, institution)
            name_count += 1
            
            # Add alternate names
            for alt_name in institution.alternate_names:
                self._add_name_mapping(alt_name, institution)
                name_count += 1
        
        proc_time = time.time() - proc_start
        logger.info(f"Institution processing completed in {proc_time:.2f} seconds")
        logger.info(f"Processed {name_count} total names (including alternates)")
        
        # Time name sorting
        sort_start = time.time()
        logger.info("Sorting names by length...")
        self.sorted_names = sorted(self.name_to_institutions.keys(), 
                                 key=lambda x: (-len(x), x))
        sort_time = time.time() - sort_start
        logger.info(f"Name sorting completed in {sort_time:.2f} seconds")
        
        # Build Trie
        trie_start = time.time()
        logger.info("Building Trie for efficient string matching...")
        self._build_trie()
        trie_time = time.time() - trie_start
        logger.info(f"Trie building completed in {trie_time:.2f} seconds")

    def _add_name_mapping(self, name: str, institution: RORInstitution):
        """Add a name->institution mapping to our lookup dictionary"""
        normalized_name = normalize_text(name)
        if normalized_name:  # Only add if we have a non-empty string after normalization
            if normalized_name not in self.name_to_institutions:
                self.name_to_institutions[normalized_name] = []
            self.name_to_institutions[normalized_name].append(institution)
            
    def _build_trie(self):
        """Build Trie for efficient string matching"""
        # Add all normalized institution names to the trie
        # Skip very short names (less than 3 characters) to prevent spurious matches
        count = 0
        for name in self.name_to_institutions.keys():
            if len(name) >= 3:
                self.trie.insert(name, name)
                count += 1
        
        logger.info(f"Built trie with {count} patterns")

def normalize_text(text: str) -> str:
    """
    Normalize text by:
    - Converting to lowercase
    - Removing punctuation
    - Normalizing whitespace
    """
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove punctuation (except hyphens between words)
    text = re.sub(r'[^\w\s-]', ' ', text)
    
    # Replace hyphens with spaces
    text = text.replace('-', ' ')
    
    # Normalize whitespace
    text = ' '.join(text.split())
    
    return text

# Create a singleton instance
logger.info("Creating RORDataManager singleton instance...")
ror_data = RORDataManager()
