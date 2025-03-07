#!/usr/bin/env python3
"""
Script to convert OpenAlex IDs to ROR IDs in the sample_from_prod_id_counts.csv file
and create a new file sample_from_prod_ror_counts.csv with ROR IDs, institution names,
and counts.

This script:
1. Reads the sample_from_prod_id_counts.csv file
2. Filters out entries with count ≤ 1
3. Converts OpenAlex IDs to ROR IDs using the OpenAlex API
4. Adds institution display names
5. Creates a new CSV with ROR IDs, counts, and institution names
"""

import csv
import json
import requests
import time
import re
from pathlib import Path
from collections import defaultdict

# Define file paths
DATA_DIR = Path(__file__).parent.parent / "data"
INPUT_FILE = DATA_DIR / "sample_from_prod_id_counts.csv"
OUTPUT_FILE = DATA_DIR / "sample_from_prod_ror_counts.csv"
LOG_FILE = DATA_DIR / "logs/ror_counts_conversion_log.txt"

# Maximum number of IDs to include in a single batch request
BATCH_SIZE = 50

# Time to pause between API calls (in seconds)
API_PAUSE = 1.0

# Maximum number of retries for failed batches
MAX_RETRIES = 3

# Backoff factor for retries (in seconds)
RETRY_BACKOFF = 2.0

# Pattern for valid OpenAlex IDs (starts with I followed by numeric)
VALID_OPENALEX_ID_PATTERN = re.compile(r'^I\d+$')

def is_valid_openalex_id(openalex_id):
    """
    Check if an OpenAlex ID is valid for API queries.
    
    Args:
        openalex_id (str): OpenAlex ID to check (with 'I' prefix)
        
    Returns:
        bool: True if the ID is valid, False otherwise
    """
    return bool(VALID_OPENALEX_ID_PATTERN.match(openalex_id))

def get_ror_and_names_for_openalex_batch(openalex_ids, log_file, retry_count=0):
    """
    Query the OpenAlex API to get ROR IDs and display names for a batch of OpenAlex IDs.
    
    Args:
        openalex_ids (list): List of OpenAlex IDs (with the 'I' prefix)
        log_file: File handle for logging
        retry_count: Current retry attempt
        
    Returns:
        dict: Dictionary mapping OpenAlex IDs to tuples of (ROR ID, display name)
    """
    # Filter out invalid OpenAlex IDs
    valid_ids = [id for id in openalex_ids if is_valid_openalex_id(id)]
    
    # If there are no valid IDs in this batch, return an empty mapping
    if not valid_ids:
        log_file.write(f"No valid OpenAlex IDs in batch, skipping API call\n")
        return {}
    
    # Format the IDs for the API query
    formatted_ids = '|'.join(valid_ids)
    url = f"https://api.openalex.org/institutions?select=id,ror,display_name&filter=ids.openalex:{formatted_ids}&per_page={BATCH_SIZE}"
    
    try:
        # Add a pause to avoid hitting rate limits
        # Increase pause time for retries using exponential backoff
        pause_time = API_PAUSE * (RETRY_BACKOFF ** retry_count)
        time.sleep(pause_time)
        
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        # Create a mapping from OpenAlex ID to (ROR ID, display name)
        result_mapping = {}
        
        # Track which IDs were found in the response
        found_ids = set()
        
        for item in data.get('results', []):
            # Extract the OpenAlex ID
            openalex_id = item.get('id', '').replace('https://openalex.org/', '')
            found_ids.add(openalex_id)
            
            # Extract the ROR ID (removing the https://ror.org/ prefix if present)
            ror_id = item.get('ror')
            display_name = item.get('display_name', '')
            
            if ror_id:
                ror_id = ror_id.replace('https://ror.org/', '')
                result_mapping[openalex_id] = (ror_id, display_name)
            else:
                log_file.write(f"OpenAlex ID {openalex_id} has no ROR ID in API response\n")
        
        # Log IDs that were not found in the response
        missing_ids = set(valid_ids) - found_ids
        if missing_ids:
            log_file.write(f"The following OpenAlex IDs were not found in the API response: {', '.join(missing_ids)}\n")
            log_file.write(f"API URL: {url}\n")
        
        return result_mapping
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching data for batch: {e}\nAPI URL: {url}\nIDs: {', '.join(valid_ids)}\n"
        print(error_msg)
        log_file.write(error_msg)
        
        # Retry logic for HTTP errors
        if retry_count < MAX_RETRIES:
            retry_count += 1
            log_file.write(f"Retrying batch (attempt {retry_count}/{MAX_RETRIES}) after {API_PAUSE * (RETRY_BACKOFF ** retry_count):.1f} seconds...\n")
            print(f"Retrying batch (attempt {retry_count}/{MAX_RETRIES})...")
            
            # If batch is large, try splitting it into smaller batches
            if len(valid_ids) > 10:
                log_file.write(f"Splitting batch into smaller batches for retry...\n")
                print(f"Splitting batch into smaller batches for retry...")
                
                mid = len(valid_ids) // 2
                first_half = valid_ids[:mid]
                second_half = valid_ids[mid:]
                
                # Process each half separately
                first_results = get_ror_and_names_for_openalex_batch(first_half, log_file, retry_count)
                second_results = get_ror_and_names_for_openalex_batch(second_half, log_file, retry_count)
                
                # Combine results
                combined_results = {**first_results, **second_results}
                return combined_results
            else:
                # For small batches, just retry the whole batch
                return get_ror_and_names_for_openalex_batch(valid_ids, log_file, retry_count)
        
        return {}
    except (KeyError, json.JSONDecodeError) as e:
        error_msg = f"Error parsing response for batch: {e}\nAPI URL: {url}\nIDs: {', '.join(valid_ids)}\n"
        print(error_msg)
        log_file.write(error_msg)
        return {}

def process_in_batches(openalex_ids_with_counts, log_file):
    """
    Process OpenAlex IDs in batches to get their corresponding ROR IDs and display names.
    
    Args:
        openalex_ids_with_counts (list): List of tuples (OpenAlex ID, count)
        log_file: File handle for logging
        
    Returns:
        list: List of tuples (OpenAlex ID, count, ROR ID, display name)
    """
    # Initialize results list
    results = []
    
    # Prepare batches
    batches = []
    current_batch = []
    
    for openalex_id, count in openalex_ids_with_counts:
        current_batch.append(openalex_id)
        
        if len(current_batch) >= BATCH_SIZE:
            batches.append(current_batch)
            current_batch = []
    
    # Add the last batch if not empty
    if current_batch:
        batches.append(current_batch)
    
    total_batches = len(batches)
    print(f"Processing {len(openalex_ids_with_counts)} OpenAlex IDs in {total_batches} batches...")
    log_file.write(f"Processing {len(openalex_ids_with_counts)} OpenAlex IDs in {total_batches} batches...\n")
    
    # Create a mapping from OpenAlex ID to count for easy lookup
    id_to_count = {id: count for id, count in openalex_ids_with_counts}
    
    # Process each batch
    for i, batch in enumerate(batches):
        print(f"Processing batch {i+1}/{total_batches} ({len(batch)} IDs)...")
        log_file.write(f"Processing batch {i+1}/{total_batches} ({len(batch)} IDs)...\n")
        
        # Get ROR IDs and display names for this batch
        batch_results = get_ror_and_names_for_openalex_batch(batch, log_file)
        
        # Add results to the list
        for openalex_id, (ror_id, display_name) in batch_results.items():
            count = id_to_count.get(openalex_id, 0)
            results.append((openalex_id, count, ror_id, display_name))
    
    return results

def main():
    """Main function to process the file"""
    # Create logs directory if it doesn't exist
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    
    with open(LOG_FILE, 'w', encoding='utf-8') as log_file:
        start_time = time.time()
        log_file.write(f"Starting conversion at {time.ctime()}\n")
        print(f"Starting conversion...")
        
        # Read the input CSV file
        print(f"Reading input file: {INPUT_FILE}")
        log_file.write(f"Reading input file: {INPUT_FILE}\n")
        
        openalex_ids_with_counts = []
        
        with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                openalex_id = row.get('openalex_id', '')
                count = int(row.get('count', 0))
                
                # Skip entries with count <= 1
                if count > 1:
                    openalex_ids_with_counts.append((openalex_id, count))
        
        # Process the OpenAlex IDs in batches
        print(f"Found {len(openalex_ids_with_counts)} OpenAlex IDs with count > 1")
        log_file.write(f"Found {len(openalex_ids_with_counts)} OpenAlex IDs with count > 1\n")
        
        results = process_in_batches(openalex_ids_with_counts, log_file)
        
        # Write the results to the output CSV file
        print(f"Writing output file: {OUTPUT_FILE}")
        log_file.write(f"Writing output file: {OUTPUT_FILE}\n")
        
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ror_id', 'count', 'display_name'])
            
            # Sort by count (highest first)
            results.sort(key=lambda x: -x[1])
            
            # Write the rows
            for _, count, ror_id, display_name in results:
                writer.writerow([ror_id, count, display_name])
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"Conversion completed in {elapsed_time:.2f} seconds")
        print(f"Converted {len(results)} OpenAlex IDs to ROR IDs")
        print(f"Results written to {OUTPUT_FILE}")
        
        log_file.write(f"Conversion completed in {elapsed_time:.2f} seconds\n")
        log_file.write(f"Converted {len(results)} OpenAlex IDs to ROR IDs\n")
        log_file.write(f"Results written to {OUTPUT_FILE}\n")

if __name__ == "__main__":
    main()
