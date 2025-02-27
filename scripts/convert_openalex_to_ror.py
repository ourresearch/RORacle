#!/usr/bin/env python3
"""
Script to convert OpenAlex IDs to ROR IDs in the institution benchmark file.
This script reads from insti_bench_openalex_ids.tsv and creates a new file insti_bench.tsv
with ROR IDs instead of OpenAlex IDs.

The insti_bench_openalex_ids.tsv file comes from here: https://docs.google.com/spreadsheets/d/1yHVn0mybvgM3Hx4_4LnOY3agXNzuHVN5iHfjroD3S34/edit?gid=0#gid=0 
"""

import csv
import json
import ast
import requests
import time
import re
from pathlib import Path
from collections import defaultdict

# Define file paths
INPUT_FILE = Path("data/insti_bench_openalex_ids.tsv")
OUTPUT_FILE = Path("data/insti_bench.tsv")
LOG_FILE = Path("data/logs/conversion_log.txt")

# Maximum number of IDs to include in a single batch request
BATCH_SIZE = 50

# Time to pause between API calls (in seconds)
API_PAUSE = 1.0

# Maximum number of retries for failed batches
MAX_RETRIES = 3

# Backoff factor for retries (in seconds)
RETRY_BACKOFF = 2.0

# Pattern for valid OpenAlex IDs (numeric and not -1)
VALID_OPENALEX_ID_PATTERN = re.compile(r'^(?!-1$)\d+$')

def is_valid_openalex_id(openalex_id):
    """
    Check if an OpenAlex ID is valid for API queries.
    
    Args:
        openalex_id (str): OpenAlex ID to check
        
    Returns:
        bool: True if the ID is valid, False otherwise
    """
    return bool(VALID_OPENALEX_ID_PATTERN.match(openalex_id))

def extract_openalex_ids_from_labels(all_rows):
    """
    Extract all unique OpenAlex IDs from the labels column of all rows.
    
    Args:
        all_rows (list): List of rows from the TSV file
        
    Returns:
        dict: Dictionary mapping OpenAlex IDs to their original positions in the data
    """
    # Dictionary to store all unique OpenAlex IDs and their positions
    openalex_id_positions = defaultdict(list)
    
    for row_idx, row in enumerate(all_rows):
        if row_idx == 0:  # Skip header
            continue
            
        if len(row) < 2:
            continue
            
        try:
            # Parse the labels column
            labels = ast.literal_eval(row[1])
            
            for label_idx, label in enumerate(labels):
                # Split the label into ID and name
                parts = label.split(' - ', 1)
                if len(parts) != 2:
                    continue
                    
                openalex_id, name = parts
                # Store the position (row_idx, label_idx) for this OpenAlex ID
                openalex_id_positions[openalex_id].append((row_idx, label_idx))
                
        except (SyntaxError, ValueError) as e:
            print(f"Error parsing labels in row {row_idx}: {e}")
    
    return openalex_id_positions

def get_ror_ids_for_openalex_batch(openalex_ids, log_file, retry_count=0):
    """
    Query the OpenAlex API to get ROR IDs for a batch of OpenAlex IDs.
    
    Args:
        openalex_ids (list): List of OpenAlex IDs (without the 'I' prefix)
        log_file: File handle for logging
        retry_count: Current retry attempt
        
    Returns:
        dict: Dictionary mapping OpenAlex IDs to their corresponding ROR IDs
    """
    # Filter out invalid OpenAlex IDs
    valid_ids = [id for id in openalex_ids if is_valid_openalex_id(id)]
    
    # If there are no valid IDs in this batch, return an empty mapping
    if not valid_ids:
        log_file.write(f"No valid OpenAlex IDs in batch, skipping API call\n")
        return {}
    
    # Format the IDs for the API query
    formatted_ids = '|'.join([f"I{id}" for id in valid_ids])
    url = f"https://api.openalex.org/institutions?select=id,ror&filter=ids.openalex:{formatted_ids}&per_page={BATCH_SIZE}"
    
    try:
        # Add a pause to avoid hitting rate limits
        # Increase pause time for retries using exponential backoff
        pause_time = API_PAUSE * (RETRY_BACKOFF ** retry_count)
        time.sleep(pause_time)
        
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        # Create a mapping from OpenAlex ID to ROR ID
        ror_mapping = {}
        
        # Track which IDs were found in the response
        found_ids = set()
        
        for item in data.get('results', []):
            # Extract the OpenAlex ID (removing the 'I' prefix)
            openalex_id = item.get('id', '').replace('https://openalex.org/I', '')
            found_ids.add(openalex_id)
            
            # Extract the ROR ID (removing the https://ror.org/ prefix if present)
            ror_id = item.get('ror')
            if ror_id:
                ror_id = ror_id.replace('https://ror.org/', '')
                ror_mapping[openalex_id] = ror_id
            else:
                log_file.write(f"OpenAlex ID {openalex_id} has no ROR ID in API response\n")
        
        # Log IDs that were not found in the response
        missing_ids = set(valid_ids) - found_ids
        if missing_ids:
            log_file.write(f"The following OpenAlex IDs were not found in the API response: {', '.join(missing_ids)}\n")
            log_file.write(f"API URL: {url}\n")
        
        return ror_mapping
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching ROR IDs for batch: {e}\nAPI URL: {url}\nIDs: {', '.join(valid_ids)}\n"
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
                first_results = get_ror_ids_for_openalex_batch(first_half, log_file, retry_count)
                second_results = get_ror_ids_for_openalex_batch(second_half, log_file, retry_count)
                
                # Combine results
                combined_results = {**first_results, **second_results}
                return combined_results
            else:
                # For small batches, just retry the whole batch
                return get_ror_ids_for_openalex_batch(valid_ids, log_file, retry_count)
        
        return {}
    except (KeyError, json.JSONDecodeError) as e:
        error_msg = f"Error parsing response for batch: {e}\nAPI URL: {url}\nIDs: {', '.join(valid_ids)}\n"
        print(error_msg)
        log_file.write(error_msg)
        return {}

def process_in_batches(openalex_id_positions, log_file):
    """
    Process OpenAlex IDs in batches to get their corresponding ROR IDs.
    
    Args:
        openalex_id_positions (dict): Dictionary mapping OpenAlex IDs to their positions
        log_file: File handle for logging
        
    Returns:
        dict: Dictionary mapping OpenAlex IDs to their corresponding ROR IDs
    """
    all_openalex_ids = list(openalex_id_positions.keys())
    ror_mapping = {}
    
    # Log invalid IDs before processing
    invalid_ids = [id for id in all_openalex_ids if not is_valid_openalex_id(id)]
    if invalid_ids:
        log_file.write(f"Found {len(invalid_ids)} invalid OpenAlex IDs that will be skipped: {', '.join(invalid_ids)}\n\n")
        print(f"Found {len(invalid_ids)} invalid OpenAlex IDs that will be skipped")
    
    # Process in batches
    for i in range(0, len(all_openalex_ids), BATCH_SIZE):
        batch = all_openalex_ids[i:i+BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE + 1} of {(len(all_openalex_ids) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} IDs)")
        log_file.write(f"Processing batch {i//BATCH_SIZE + 1} of {(len(all_openalex_ids) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} IDs)\n")
        
        # Count valid IDs in this batch
        valid_ids_in_batch = [id for id in batch if is_valid_openalex_id(id)]
        if len(valid_ids_in_batch) < len(batch):
            log_file.write(f"  Batch contains {len(batch) - len(valid_ids_in_batch)} invalid IDs that will be skipped\n")
        
        batch_mapping = get_ror_ids_for_openalex_batch(batch, log_file)
        ror_mapping.update(batch_mapping)
        
        # Print the number of successful mappings in this batch
        success_count = sum(1 for id in batch if id in batch_mapping and batch_mapping[id])
        print(f"  Found ROR IDs for {success_count} out of {len(valid_ids_in_batch)} valid OpenAlex IDs in this batch")
        log_file.write(f"  Found ROR IDs for {success_count} out of {len(valid_ids_in_batch)} valid OpenAlex IDs in this batch\n")
        
        # Log IDs that couldn't be mapped
        unmapped_ids = [id for id in valid_ids_in_batch if id not in batch_mapping or not batch_mapping[id]]
        if unmapped_ids:
            log_file.write(f"  Could not map the following OpenAlex IDs: {', '.join(unmapped_ids)}\n")
            
            # Try individual requests for unmapped IDs to debug
            for unmapped_id in unmapped_ids:
                log_file.write(f"  Trying individual request for OpenAlex ID: {unmapped_id}\n")
                try:
                    time.sleep(API_PAUSE)
                    individual_url = f"https://api.openalex.org/institutions?select=id,ror&filter=ids.openalex:I{unmapped_id}"
                    response = requests.get(individual_url)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get('meta', {}).get('count', 0) > 0 and data.get('results'):
                        result = data['results'][0]
                        ror_id = result.get('ror')
                        if ror_id:
                            ror_id = ror_id.replace('https://ror.org/', '')
                            ror_mapping[unmapped_id] = ror_id
                            log_file.write(f"    Success! Found ROR ID {ror_id} for OpenAlex ID {unmapped_id}\n")
                        else:
                            log_file.write(f"    No ROR ID found for OpenAlex ID {unmapped_id} in individual request\n")
                    else:
                        log_file.write(f"    No results found for OpenAlex ID {unmapped_id} in individual request\n")
                        
                except Exception as e:
                    log_file.write(f"    Error in individual request for OpenAlex ID {unmapped_id}: {e}\n")
    
    return ror_mapping

def update_labels_with_ror_ids(all_rows, openalex_id_positions, ror_mapping, log_file):
    """
    Update the labels in all rows with ROR IDs.
    
    Args:
        all_rows (list): List of rows from the TSV file
        openalex_id_positions (dict): Dictionary mapping OpenAlex IDs to their positions
        ror_mapping (dict): Dictionary mapping OpenAlex IDs to their corresponding ROR IDs
        log_file: File handle for logging
        
    Returns:
        list: Updated rows with ROR IDs
    """
    # Create a copy of the rows to modify
    updated_rows = [row.copy() for row in all_rows]
    
    # Keep track of how many IDs were successfully converted
    converted_count = 0
    not_found_count = 0
    invalid_count = 0
    
    # Update each position with the corresponding ROR ID
    for openalex_id, positions in openalex_id_positions.items():
        ror_id = ror_mapping.get(openalex_id)
        
        for row_idx, label_idx in positions:
            try:
                # Parse the labels column
                labels = ast.literal_eval(updated_rows[row_idx][1])
                
                # Split the label into ID and name
                parts = labels[label_idx].split(' - ', 1)
                if len(parts) != 2:
                    continue
                
                _, name = parts
                
                if ror_id:
                    # Replace the OpenAlex ID with the ROR ID
                    labels[label_idx] = f"{ror_id} - {name}"
                    converted_count += 1
                else:
                    # Keep the original if ROR ID not found
                    if is_valid_openalex_id(openalex_id):
                        not_found_count += 1
                        log_file.write(f"No ROR ID found for OpenAlex ID {openalex_id} ({name}) in row {row_idx}\n")
                    else:
                        invalid_count += 1
                
                # Update the row with the modified labels
                updated_rows[row_idx][1] = str(labels)
                
            except (IndexError, SyntaxError, ValueError) as e:
                error_msg = f"Error updating labels in row {row_idx}: {e}\n"
                print(error_msg)
                log_file.write(error_msg)
    
    print(f"Converted {converted_count} OpenAlex IDs to ROR IDs")
    print(f"Could not find ROR IDs for {not_found_count} valid OpenAlex IDs")
    print(f"Skipped {invalid_count} invalid OpenAlex IDs")
    log_file.write(f"Converted {converted_count} OpenAlex IDs to ROR IDs\n")
    log_file.write(f"Could not find ROR IDs for {not_found_count} valid OpenAlex IDs\n")
    log_file.write(f"Skipped {invalid_count} invalid OpenAlex IDs\n")
    
    return updated_rows

def main():
    """Main function to process the file"""
    print(f"Converting OpenAlex IDs to ROR IDs...")
    print(f"Reading from: {INPUT_FILE}")
    print(f"Writing to: {OUTPUT_FILE}")
    print(f"Logging to: {LOG_FILE}")
    
    # Create output directory if it doesn't exist
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Create logs directory if it doesn't exist
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Open log file
    with open(LOG_FILE, 'w', encoding='utf-8') as log_file:
        log_file.write(f"Conversion started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Reading from: {INPUT_FILE}\n")
        log_file.write(f"Writing to: {OUTPUT_FILE}\n")
        log_file.write(f"Batch size: {BATCH_SIZE}\n")
        log_file.write(f"API pause: {API_PAUSE} seconds\n")
        log_file.write(f"Max retries: {MAX_RETRIES}\n")
        log_file.write(f"Retry backoff: {RETRY_BACKOFF} seconds\n\n")
        
        # Read all rows from the input file
        with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile, delimiter='\t')
            all_rows = list(reader)
        
        print(f"Read {len(all_rows)} rows from input file")
        log_file.write(f"Read {len(all_rows)} rows from input file\n")
        
        # Extract all unique OpenAlex IDs and their positions
        print("Extracting OpenAlex IDs from labels...")
        log_file.write("Extracting OpenAlex IDs from labels...\n")
        openalex_id_positions = extract_openalex_ids_from_labels(all_rows)
        print(f"Found {len(openalex_id_positions)} unique OpenAlex IDs")
        log_file.write(f"Found {len(openalex_id_positions)} unique OpenAlex IDs\n\n")
        
        # Process OpenAlex IDs in batches to get ROR IDs
        print("Fetching ROR IDs in batches...")
        log_file.write("Fetching ROR IDs in batches...\n")
        ror_mapping = process_in_batches(openalex_id_positions, log_file)
        log_file.write("\n")
        
        # Update the labels with ROR IDs
        print("Updating labels with ROR IDs...")
        log_file.write("Updating labels with ROR IDs...\n")
        updated_rows = update_labels_with_ror_ids(all_rows, openalex_id_positions, ror_mapping, log_file)
        log_file.write("\n")
        
        # Write the updated rows to the output file
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter='\t')
            writer.writerows(updated_rows)
        
        print(f"Conversion complete! Output written to {OUTPUT_FILE}")
        log_file.write(f"Conversion completed at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Output written to {OUTPUT_FILE}\n")

if __name__ == "__main__":
    main()
