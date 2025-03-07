#!/usr/bin/env python3
"""
Script to count the frequency of institution IDs in the sample_from_prod.csv file.

This script creates a new CSV file with two columns:
1. OpenAlex ID (with 'I' prefix added)
2. Count of occurrences

The script excludes the ID '-1' which indicates no match was found.
It also excludes 'null' values.

Usage:
    python count_institution_ids.py [output_filename]
    
    output_filename: Optional, name of the output CSV file
                    Default: sample_from_prod_id_counts.csv
"""

import csv
import sys
import re
from pathlib import Path
from collections import Counter

# Define file paths
DATA_DIR = Path(__file__).parent.parent / "data"
SAMPLE_FROM_PROD_FILE = DATA_DIR / "sample_from_prod.csv"

def count_institution_ids(output_filename="sample_from_prod_id_counts.csv"):
    """
    Count the frequency of institution IDs in the sample_from_prod.csv file
    and write the results to a new CSV file.
    
    Args:
        output_filename (str): Name of the output CSV file
    """
    print(f"Counting institution IDs from {SAMPLE_FROM_PROD_FILE}...")
    
    # Initialize counter for institution IDs
    id_counter = Counter()
    
    # Regular expression to extract IDs from the affiliation_ids column
    # The format is typically [123456789] or [123456789,987654321]
    id_pattern = re.compile(r'\[([^\]]+)\]')
    
    # Read the sample_from_prod.csv file
    with open(SAMPLE_FROM_PROD_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Process each row
        for i, row in enumerate(reader):
            if "affiliation_ids" in row and row["affiliation_ids"]:
                # Extract IDs from the affiliation_ids column
                match = id_pattern.search(row["affiliation_ids"])
                if match:
                    # Split by comma if multiple IDs
                    ids = [id.strip() for id in match.group(1).split(',')]
                    
                    # Count each ID
                    for inst_id in ids:
                        # Skip -1 and null as they're not real IDs
                        if inst_id != '-1' and inst_id.lower() != 'null':
                            id_counter[inst_id] += 1
            
            # Show progress for large datasets
            if i > 0 and i % 10000 == 0:
                print(f"Processed {i} rows...")
    
    # Create the output file path
    output_path = DATA_DIR / output_filename
    
    # Write the results to the output CSV file
    print(f"Writing results to {output_path}...")
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(['openalex_id', 'count'])
        
        # Write data rows, sorted by count (highest first)
        for inst_id, count in sorted(id_counter.items(), key=lambda x: -x[1]):
            # Add 'I' prefix to the OpenAlex ID
            openalex_id = f"I{inst_id}"
            writer.writerow([openalex_id, count])
    
    # Print statistics
    total_ids = sum(id_counter.values())
    unique_ids = len(id_counter)
    
    print(f"Found {unique_ids} unique institution IDs")
    print(f"Total ID occurrences: {total_ids}")
    print(f"Output written to {output_path}")
    
    # Show a sample of the first few rows
    print("\nSample of first 5 rows in the output file:")
    with open(output_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        print(','.join(header))
        for i, row in enumerate(reader):
            if i >= 5:
                break
            print(','.join(row))

if __name__ == "__main__":
    # Parse command line arguments
    output_filename = "sample_from_prod_id_counts.csv"
    
    if len(sys.argv) > 1:
        output_filename = sys.argv[1]
    
    # Run the script
    count_institution_ids(output_filename)
