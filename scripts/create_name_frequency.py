#!/usr/bin/env python3
"""
Script to create frequency data for ROR names or place names
based on their occurrence in sample_from_prod.csv

Usage:
    python create_name_frequency.py [mode] [limit]
    
    mode: Optional, either 'names' (default) or 'places'
    limit: Optional integer to limit the number of sample affiliations processed
           Useful for testing with a smaller dataset
"""

import csv
import re
import sys
import time
import unicodedata
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

# Define file paths
DATA_DIR = Path(__file__).parent.parent / "data"
NAMES_TO_IDS_FILE = DATA_DIR / "ror_names_to_ids.csv"
ROR_ORGS_FILE = DATA_DIR / "ror_organizations.csv"
SAMPLE_FROM_PROD_FILE = DATA_DIR / "sample_from_prod.csv"
NAME_FREQ_FILE = DATA_DIR / "ror_name_freq.csv"
PLACE_FREQ_FILE = DATA_DIR / "place_name_freq.csv"

# Common place name abbreviations mapping
PLACE_ABBREVIATIONS = {
    "United States": ["USA", "U.S.A.", "U.S.", "United States of America"],
    "United Kingdom": ["UK", "U.K.", "Britain", "Great Britain"],
    "Australia": ["AUS", "Aussie"],
    "Canada": ["CAN"],
    "New Zealand": ["NZ"],
    "China": ["PRC", "People's Republic of China"],
    "Russia": ["Russian Federation"],
    "Germany": ["Deutschland"],
    "France": ["République Française"],
    "Japan": ["Nippon"],
    "Spain": ["España"],
    "Brazil": ["Brasil"],
    "India": ["Bharat"]
}

def normalize_text(text):
    """
    Normalize text by:
    1. Replacing all whitespace sequences with a single space
    2. Removing diacritical marks from Latin-based characters
    3. Preserving capitalization of ALL CAPS words, lowercasing others
    
    Args:
        text (str): The text to normalize
        
    Returns:
        str: Normalized text
    """
    if not text:
        return ""
    
    # Replace all whitespace sequences with a single space
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove diacritical marks from Latin-based characters
    # This will convert characters like "é" to "e", "ü" to "u", etc.
    nfkd_form = unicodedata.normalize('NFKD', text)
    text = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    
    # Split into words
    words = text.split()
    
    # Process each word: keep ALL CAPS words, lowercase others
    normalized_words = []
    for word in words:
        # Check if word is ALL CAPS (allowing non-alpha chars)
        if re.match(r'^[A-Z0-9\W]*[A-Z]+[A-Z0-9\W]*$', word) and not re.match(r'^[^a-zA-Z]*$', word):
            # It's ALL CAPS with at least one letter, keep it as is
            normalized_words.append(word)
        else:
            # Not ALL CAPS, convert to lowercase
            normalized_words.append(word.lower())
    
    return ' '.join(normalized_words)


def expand_place_name_with_abbreviations(place_names):
    """
    Expands the list of place names with common abbreviations
    
    Args:
        place_names (dict): Dictionary mapping original place names to normalized names
        
    Returns:
        dict: Expanded dictionary with abbreviations included
    """
    expanded = place_names.copy()
    
    # For each common abbreviation, add it to the expanded list
    for official_name, abbrevs in PLACE_ABBREVIATIONS.items():
        # Only expand if we have the official name
        if official_name in place_names:
            normalized_name = place_names[official_name]
            # Add all abbreviations with the same normalized name
            for abbrev in abbrevs:
                expanded[abbrev] = normalized_name
    
    return expanded


def load_place_names():
    """
    Loads place names from the ROR organizations CSV file
    
    Returns:
        dict: Dictionary mapping place name to its type (country, subdivision, location)
        dict: Dictionary mapping original place names to normalized place names
    """
    place_names = {}
    place_types = {}
    
    with open(ROR_ORGS_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Extract place names and types
            if row['country_name']:
                place_names[row['country_name']] = normalize_text(row['country_name'])
                place_types[row['country_name']] = 'country'
                
            if row['country_subdivision_name']:
                place_names[row['country_subdivision_name']] = normalize_text(row['country_subdivision_name'])
                place_types[row['country_subdivision_name']] = 'subdivision'
                
            if row['location_name']:
                place_names[row['location_name']] = normalize_text(row['location_name'])
                place_types[row['location_name']] = 'location'
    
    # Expand with abbreviations
    expanded_place_names = expand_place_name_with_abbreviations(place_names)
    
    # Update place_types with the abbreviations
    for abbrev, normalized in expanded_place_names.items():
        if abbrev not in place_types:
            # Find the type of the official name that corresponds to this abbreviation
            for official, abbrevs in PLACE_ABBREVIATIONS.items():
                if abbrev in abbrevs and official in place_types:
                    place_types[abbrev] = place_types[official]
                    break
    
    return place_types, expanded_place_names


def create_name_frequency_csv(mode='names', sample_limit=None):
    """
    Create a CSV mapping ROR names or place names to their frequency in sample_from_prod.csv
    
    Args:
        mode (str): Either 'names' or 'places' 
        sample_limit (int, optional): Limit the number of sample affiliations to process.
                                     Useful for testing with a smaller dataset.
    """
    total_start_time = time.time()
    
    if mode == 'names':
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Running in NAME mode")
        output_file = NAME_FREQ_FILE
    else:  # places mode
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Running in PLACE mode")
        output_file = PLACE_FREQ_FILE
    
    if sample_limit:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Running with sample limit: {sample_limit}")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Creating frequency CSV...")
    
    # Load either ROR names or place names depending on the mode
    if mode == 'names':
        # Load ROR names from ror_names_to_ids.csv
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading ROR names from CSV...")
        load_start = time.time()
        ror_names = []
        with open(NAMES_TO_IDS_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if row and row[0] and len(row[0]) >= 4:  # Ensure name isn't empty and has a minimum length
                    ror_names.append(row[0])
        load_time = time.time() - load_start
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Loaded {len(ror_names)} ROR names in {load_time:.2f} seconds")
        
        # Normalize ROR names
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Normalizing ROR names...")
        norm_start = time.time()
        normalized_names = {name: normalize_text(name) for name in ror_names}
        norm_time = time.time() - norm_start
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Normalized {len(normalized_names)} names in {norm_time:.2f} seconds")
        
        # We don't need types for organization names
        name_types = {name: None for name in ror_names}
    
    else:  # places mode
        # Load place names from ror_organizations.csv
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading place names from CSV...")
        load_start = time.time()
        
        # Load place names and their types
        name_types, normalized_names = load_place_names()
        
        # Get the list of original place names
        place_names = list(normalized_names.keys())
        
        load_time = time.time() - load_start
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Loaded {len(place_names)} place names in {load_time:.2f} seconds")
    
    # Create mapping from normalized name back to original name(s)
    norm_to_original = defaultdict(list)
    for original, normalized in normalized_names.items():
        norm_to_original[normalized].append(original)
    
    # Load and normalize affiliations from sample_from_prod.csv
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading sample affiliations...")
    aff_start = time.time()
    sample_affiliations = []
    with open(SAMPLE_FROM_PROD_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            # Apply limit if specified
            if sample_limit and i >= sample_limit:
                break
                
            if "original_affiliation" in row and row["original_affiliation"]:
                affiliation = row["original_affiliation"]
                normalized = normalize_text(affiliation)
                sample_affiliations.append(normalized)
                
            # Show progress for large datasets
            if i > 0 and i % 10000 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {i} sample affiliations...")
    
    aff_time = time.time() - aff_start
    limit_str = f"first {sample_limit} of " if sample_limit else ""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Loaded {len(sample_affiliations)} sample affiliations ({limit_str}total) in {aff_time:.2f} seconds")
    
    # Count frequencies of normalized names in normalized sample affiliations
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Counting name frequencies in sample affiliations...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] This may take some time for large datasets...")
    count_start = time.time()
    name_counts = Counter()
    
    # Get unique normalized names for processing
    unique_norm_names = set(normalized_names.values())
    total_names = len(unique_norm_names)
    
    # For each normalized name, count its occurrences in sample affiliations
    for i, norm_name in enumerate(unique_norm_names):
        # Skip very short names (less than 4 chars)
        if len(norm_name) < 4:
            continue
            
        # Pad normalized name with spaces for word boundary matching
        space_padded_name = f" {norm_name} "
        
        # Count matches in sample affiliations (padded with spaces for word boundary matching)
        count = 0
        for affiliation in sample_affiliations:
            # Pad the affiliation with spaces for word boundary matching
            space_padded_affiliation = f" {affiliation} "
            if space_padded_name in space_padded_affiliation:
                count += 1
                
        if count > 0:
            name_counts[norm_name] = count
        
        # Show progress every 1% or 1000 names, whichever is smaller
        progress_interval = min(1000, max(1, total_names // 100))
        if i > 0 and i % progress_interval == 0:
            elapsed = time.time() - count_start
            percent_done = (i / total_names) * 100
            est_total = elapsed / (i / total_names)
            est_remaining = est_total - elapsed
            
            # Format times as minutes:seconds
            elapsed_min, elapsed_sec = divmod(int(elapsed), 60)
            remaining_min, remaining_sec = divmod(int(est_remaining), 60)
            
            print(f"{percent_done:.0f}% done in {elapsed_min}:{elapsed_sec:02d} ({remaining_min}:{remaining_sec:02d} to go)")
    
    count_time = time.time() - count_start
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Finished counting frequencies in {count_time:.2f} seconds")
    
    # Create a list of (original_name, normalized_name, frequency, type) tuples
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Creating result rows...")
    result_rows = []
    
    # Group by normalized name
    normalized_to_originals = defaultdict(list)
    normalized_to_type = {}
    normalized_to_frequency = {}

    # Use the appropriate list of names based on mode
    if mode == 'names':
        target_names = ror_names
    else:
        target_names = place_names

    for original_name in target_names:
        normalized = normalized_names[original_name]
        frequency = name_counts[normalized]
        
        # Store the frequency
        normalized_to_frequency[normalized] = frequency
        
        # Store the original name
        normalized_to_originals[normalized].append(original_name)
        
        # Store the type (for place names)
        if mode == 'places' and original_name in name_types:
            place_type = name_types.get(original_name, 'unknown')
            # If we already have a type for this normalized name, only update if it's a country
            # (country is more important than subdivision or location)
            if normalized not in normalized_to_type or place_type == 'country':
                normalized_to_type[normalized] = place_type

    # Create one row per normalized name
    for normalized, frequency in normalized_to_frequency.items():
        if frequency > 0:  # Only include names that were found
            # Get all original forms
            originals = normalized_to_originals[normalized]
            
            # Choose a representative original form (the first one)
            representative = originals[0] if originals else normalized
            
            # For place names, include the type
            if mode == 'places':
                place_type = normalized_to_type.get(normalized, 'unknown')
                result_rows.append((representative, normalized, frequency, place_type))
            else:
                # Just name and frequency for organization names
                result_rows.append((representative, normalized, frequency))

    # Sort by frequency (highest first) and then by name length (longest first)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Sorting results...")
    result_rows.sort(key=lambda x: (-x[2], -len(x[0])))
    
    # Write to CSV
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Writing results to CSV...")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if mode == 'places':
            writer.writerow(['name', 'normalized_name', 'frequency', 'type'])
        else:
            writer.writerow(['name', 'normalized_name', 'frequency'])
            
        writer.writerows(result_rows)
    
    # Print statistics
    total_matches = sum(name_counts.values())
    unique_matches = sum(1 for count in name_counts.values() if count > 0)
    
    total_time = time.time() - total_start_time
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {unique_matches} unique {'place' if mode == 'places' else 'ROR'} names in sample affiliations")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total name occurrences: {total_matches}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Output written to {output_file}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total execution time: {total_time:.2f} seconds")
    
    # Show a sample of the first few rows
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Sample of first 5 rows in {'place' if mode == 'places' else 'name'} frequency CSV file:")
    with open(output_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        print(','.join(header))
        for i, row in enumerate(reader):
            if i >= 5:
                break
            print(','.join(row))


if __name__ == "__main__":
    # Parse command line arguments
    sample_limit = None
    mode = 'names'  # Default mode
    
    if len(sys.argv) > 1:
        # Check if the first argument is a mode or a limit
        if sys.argv[1].lower() in ['names', 'places']:
            mode = sys.argv[1].lower()
            # Check if a limit is also provided
            if len(sys.argv) > 2 and sys.argv[2].isdigit():
                sample_limit = int(sys.argv[2])
        elif sys.argv[1].isdigit():
            # The first argument is a limit
            sample_limit = int(sys.argv[1])
    
    # Run the script with specified mode and limit
    create_name_frequency_csv(mode=mode, sample_limit=sample_limit)
