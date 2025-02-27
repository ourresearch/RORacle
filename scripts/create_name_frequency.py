#!/usr/bin/env python3
"""
Script to create ror_name_freq.csv with frequency data for ROR names
based on their occurrence in sample_from_prod.csv

Usage:
    python create_name_frequency.py [limit]
    
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
SAMPLE_FROM_PROD_FILE = DATA_DIR / "sample_from_prod.csv"
NAME_FREQ_FILE = DATA_DIR / "ror_name_freq.csv"


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


def create_name_frequency_csv(sample_limit=None):
    """
    Create a CSV mapping ROR names to their frequency in sample_from_prod.csv
    
    Args:
        sample_limit (int, optional): Limit the number of sample affiliations to process.
                                     Useful for testing with a smaller dataset.
    """
    total_start_time = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Creating name frequency CSV...")
    
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
    normalized_ror_names = {name: normalize_text(name) for name in ror_names}
    norm_time = time.time() - norm_start
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Normalized {len(normalized_ror_names)} names in {norm_time:.2f} seconds")
    
    # Create mapping from normalized name back to original name(s)
    norm_to_original = defaultdict(list)
    for original, normalized in normalized_ror_names.items():
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
    
    # Count frequencies of normalized ROR names in normalized sample affiliations
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Counting name frequencies in sample affiliations...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] This may take some time for large datasets...")
    count_start = time.time()
    ror_name_counts = Counter()
    
    # Get unique normalized names for processing
    unique_norm_names = set(normalized_ror_names.values())
    total_names = len(unique_norm_names)
    
    # For each normalized ROR name, count its occurrences in sample affiliations
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
            ror_name_counts[norm_name] = count
        
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
    
    # Create a list of (original_name, normalized_name, frequency) tuples
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Creating result rows...")
    result_rows = []
    for original_name in ror_names:
        normalized = normalized_ror_names[original_name]
        frequency = ror_name_counts[normalized]
        result_rows.append((original_name, normalized, frequency))
    
    # Sort by frequency (highest first) and then by name length (longest first)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Sorting results...")
    result_rows.sort(key=lambda x: (-x[2], -len(x[0])))
    
    # Write to CSV
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Writing results to CSV...")
    with open(NAME_FREQ_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['name', 'normalized_name', 'frequency'])
        writer.writerows(result_rows)
    
    # Print statistics
    total_matches = sum(ror_name_counts.values())
    unique_matches = sum(1 for count in ror_name_counts.values() if count > 0)
    
    total_time = time.time() - total_start_time
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {unique_matches} unique ROR names in sample affiliations")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total name occurrences: {total_matches}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Output written to {NAME_FREQ_FILE}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total execution time: {total_time:.2f} seconds")
    
    # Print sample of the output
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Sample of first 5 rows in name frequency CSV file:")
    with open(NAME_FREQ_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i <= 5:  # header + 4 rows
                print(line.strip())


if __name__ == "__main__":
    # Check if a limit argument was provided
    sample_limit = None
    if len(sys.argv) > 1:
        try:
            sample_limit = int(sys.argv[1])
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Running with sample limit: {sample_limit}")
        except ValueError:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Invalid limit argument: {sys.argv[1]}. Using all samples.")
    
    create_name_frequency_csv(sample_limit)
