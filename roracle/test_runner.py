import json
import time
import random
import csv
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
import os
from .ror_matcher import find_ror_records, RORRecord
from .ror_utils import load_ror_names, create_ror_record, extract_ror_ids_from_labels, extract_ror_ids_from_google_sheet_labels, get_test_cases_from_google_sheet

def _load_test_cases():
    """
    Load test cases directly from Google Sheets.
    Does not cache results or save to disk.
    
    Returns:
        List of test case dictionaries
    """
    # Fetch test cases directly from Google Sheets
    return get_test_cases_from_google_sheet()

def compare_records(produced_records: List[RORRecord], expected_records: List[RORRecord]) -> tuple[List[RORRecord], List[RORRecord], List[RORRecord]]:
    """Compare produced and expected records, returning (matches, under_matches, over_matches)"""
    produced_ids = {r.id for r in produced_records}
    expected_ids = {r.id for r in expected_records}
    
    # Find matching records
    matching_ids = produced_ids & expected_ids
    
    # Use the produced records for matches, since they already have names populated from find_ror_records
    matches = [r for r in produced_records if r.id in matching_ids]
    
    # Find under_matches (in expected but not in produced)
    under_matches = [r for r in expected_records if r.id not in produced_ids]
    
    # Find over_matches (in produced but not in expected)
    over_matches = [r for r in produced_records if r.id not in expected_ids]
    
    return matches, under_matches, over_matches

def run_test_by_id(test_id: int) -> Dict:
    """
    Run a single test case by ID and return the result.
    
    Args:
        test_id: ID of the test case to run
        
    Returns:
        Dict with test result or error message
    """
    # Load test cases (will use cached version if already loaded)
    test_cases = _load_test_cases()
    
    # Find the test case with the matching ID
    matching_test_cases = [tc for tc in test_cases if int(tc["id"]) == test_id]
    
    if not matching_test_cases:
        return {
            "error": f"Invalid test ID: {test_id}. Test ID not found in test cases."
        }
    
    # Get the test case
    test_case = matching_test_cases[0]
    
    try:
        # Time this individual test
        test_start = time.time()
        
        # Get affiliation, dataset_name, and expected records from the CSV
        affiliation = test_case["affiliation_string"]
        dataset_name = test_case["dataset_name"]
        
        # Extract ROR IDs from the labels column using new format
        ror_ids = extract_ror_ids_from_google_sheet_labels(test_case["labels"])
        
        # Special case: if the only ROR ID is "-1" or labels is empty, it means no matches are expected
        no_matches_expected = len(ror_ids) == 1 and ror_ids[0] == "-1"
        
        # Create expected records using the factory function, but only if not the special "-1" case
        expected_records = []
        if not no_matches_expected:
            expected_records = [
                create_ror_record(ror_id)
                for ror_id in ror_ids
            ]
        
        # Get produced records
        produced_records = find_ror_records(affiliation)
        
        # Calculate elapsed time
        elapsed = time.time() - test_start
        
        # Compare produced and expected records
        matches, under_matches, over_matches = compare_records(produced_records, expected_records)
        
        # Determine if the test is passing
        # For the special "-1" case, the test passes if no records were produced
        if no_matches_expected:
            is_passing = len(produced_records) == 0
        else:
            is_passing = len(under_matches) == 0 and len(over_matches) == 0
        
        # Create test result using the actual ID from the sheet
        actual_id = int(test_case["id"])
        result = TestResult(
            id=actual_id,
            is_passing=is_passing,
            affiliation=affiliation,
            matches=matches,
            under_matches=under_matches,
            over_matches=over_matches,
            elapsed=elapsed,
            dataset_name=dataset_name,
            no_matches_expected=no_matches_expected
        )
        
        return {
            "meta": {
                "elapsed": round(elapsed, 3)
            },
            "test_id": actual_id,
            "is_passing": result.is_passing,
            "affiliation": result.affiliation,
            "dataset_name": result.dataset_name,
            "matches": [record.to_dict() for record in result.matches],
            "under_matches": [record.to_dict() for record in result.under_matches],
            "over_matches": [record.to_dict() for record in result.over_matches],
            "no_matches_expected": result.no_matches_expected
        }
    except Exception as e:
        # Handle any errors
        return {
            "error": f"Error running test {test_id}: {str(e)}"
        }

def run_tests(limit: Optional[int] = None, sample: Optional[Union[bool, int]] = None, dataset_name: Optional[str] = None) -> Dict:
    """
    Run tests and return a summary of results.
    
    Args:
        limit: Optional maximum number of tests to run
        sample: If True, randomizes test order. If int, uses it as random seed.
        dataset_name: Optional filter to only run tests from a specific dataset
    
    Returns:
        Dict with meta information and test results
    """
    # Load test cases (will use cached version if already loaded)
    print("Loading test cases from Google Sheets...")
    test_cases = _load_test_cases()
    print(f"Loaded {len(test_cases)} test cases.")
    
    # Filter by dataset_name if specified
    if dataset_name:
        filtered_test_cases = [tc for tc in test_cases if tc.get("dataset_name") == dataset_name]
    else:
        filtered_test_cases = test_cases.copy()  # Make a copy to avoid modifying the global cache
    
    # Handle sampling if requested
    # First get the test IDs to ensure consistent ordering regardless of other data
    test_ids = [int(tc["id"]) for tc in filtered_test_cases]
    
    # If sample is provided, use it to shuffle the ids consistently
    if sample:
        print(f"Using sample seed: {sample}")
        # Create a new random generator with the specified seed
        rng = random.Random(sample if isinstance(sample, int) else None)
        
        # Create a list of (index, id) pairs, shuffle it, and then reorder test_cases
        id_with_indices = list(enumerate(test_ids))
        rng.shuffle(id_with_indices)
        
        # Extract the reordered indices
        shuffled_indices = [idx for idx, _ in id_with_indices]
        
        # Reorder the test cases according to the shuffled indices
        filtered_test_cases = [filtered_test_cases[i] for i in shuffled_indices]
        test_ids = [test_ids[i] for i in shuffled_indices]
    
    # Apply limit if specified
    if limit and limit < len(filtered_test_cases):
        filtered_test_cases = filtered_test_cases[:limit]
        test_ids = test_ids[:limit]
    
    # Run tests
    results = []
    passing = 0
    failing = 0
    errors = 0
    
    # For calculating overall metrics
    total_matches = 0
    total_under_matches = 0
    total_over_matches = 0
    
    # Record times for stats
    start_time = time.time()
    all_times = []
    
    total_tests = len(test_ids)
    print(f"Running {total_tests} tests...")
    
    for i, test_id in enumerate(test_ids):
        # Show progress
        if i % 100 == 0 or i == total_tests - 1:
            print(f"Progress: {i+1}/{total_tests} tests completed ({((i+1)/total_tests)*100:.1f}%)")
        
        # Run the test
        test_start = time.time()
        result = run_test_by_id(test_id)
        test_elapsed = time.time() - test_start
        all_times.append(test_elapsed)
        
        # Check for error
        if "error" in result:
            errors += 1
        else:
            # Add to passing or failing count
            if result["is_passing"]:
                passing += 1
            else:
                failing += 1
                
            # Add to metrics
            total_matches += len(result["matches"])
            total_under_matches += len(result["under_matches"])
            total_over_matches += len(result["over_matches"])
        
        # Add to results
        results.append(result)
    
    # Calculate total elapsed time
    total_elapsed = time.time() - start_time
    
    # Calculate percentages
    total_completed_tests = passing + failing
    pass_percentage = (passing / total_completed_tests * 100) if total_completed_tests > 0 else 0
    fail_percentage = (failing / total_completed_tests * 100) if total_completed_tests > 0 else 0
    
    # Calculate time stats
    avg_time = sum(all_times) / len(all_times) if all_times else 0
    
    print(f"Test run completed in {total_elapsed:.2f} seconds.")
    print(f"Results: {passing} passing, {failing} failing, {errors} errors")
    
    # Calculate performance metrics
    precision = total_matches / (total_matches + total_over_matches) if (total_matches + total_over_matches) > 0 else 0
    recall = total_matches / (total_matches + total_under_matches) if (total_matches + total_under_matches) > 0 else 0
    f_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    # Separate tests into passing, failing, and error
    passing_results = [r for r in results if "is_passing" in r and r["is_passing"]]
    failing_results = [r for r in results if "is_passing" in r and not r["is_passing"]]
    error_results = [r for r in results if "error" in r]
    
    return {
        "meta": {
            "total_tests": len(results),
            "passing": passing,
            "failing": failing,
            "errors": errors,
            "pass_rate_percent": round(pass_percentage, 2),
            "total_elapsed": round(total_elapsed, 3),
            "avg_time_per_test": round(avg_time, 3),
            "performance": {
                "precision": round(precision, 3),
                "recall": round(recall, 3),
                "f_score": round(f_score, 3)
            }
        },
        "passing_tests": passing_results,
        "failing_tests": failing_results,
        "error_tests": error_results
    }

@dataclass
class TestResult:
    id: int
    is_passing: bool
    affiliation: str
    matches: List[RORRecord]
    under_matches: List[RORRecord]
    over_matches: List[RORRecord]
    elapsed: float
    dataset_name: str = None
    no_matches_expected: bool = False
    
    def to_dict(self):
        return {
            "id": self.id,
            "is_passing": self.is_passing,
            "affiliation": self.affiliation,
            "matches": [record.to_dict() for record in self.matches],
            "under_matches": [record.to_dict() for record in self.under_matches],
            "over_matches": [record.to_dict() for record in self.over_matches],
            "elapsed": round(self.elapsed, 3),
            "dataset_name": self.dataset_name,
            "no_matches_expected": self.no_matches_expected
        }
