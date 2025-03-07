import json
import time
import random
import csv
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
import os
from .ror_matcher import find_ror_records, RORRecord
from .ror_utils import load_ror_names, create_ror_record, extract_ror_ids_from_labels, extract_ror_ids_from_google_sheet_labels, get_test_cases_from_google_sheet

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

def run_test_by_id(test_id: int, test_cases: List[Dict]) -> Dict:
    """
    Run a single test case by ID and return the result.
    
    Args:
        test_id: ID of the test case to run
        test_cases: List of test cases to search through
        
    Returns:
        Dict with test result or error message
    """
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

def run_tests(test_cases: List[Dict], limit: Optional[int] = None, sample: Optional[Union[bool, int]] = None, dataset_name: Optional[str] = None) -> Dict:
    """
    Run tests and return a summary of results.
    
    Args:
        test_cases: List of test cases to run
        limit: Deprecated parameter, no longer used
        sample: Deprecated parameter, no longer used
        dataset_name: Optional filter to only run tests from a specific dataset
    
    Returns:
        Dict with meta information and test results
    """
    # Filter by dataset_name if specified
    if dataset_name:
        filtered_test_cases = [tc for tc in test_cases if tc.get("dataset_name") == dataset_name]
    else:
        filtered_test_cases = test_cases.copy()  # Make a copy to avoid modifying the input
    
    # Get the test IDs to ensure consistent ordering regardless of other data
    test_ids = [int(tc["id"]) for tc in filtered_test_cases]
        
    # Track start time for the whole test run
    overall_start = time.time()
    
    # Initialize lists for passing, failing, and error tests
    passing_tests = []
    failing_tests = []
    error_tests = []
    
    # Counter for progress tracking
    total_tests = len(filtered_test_cases)
    completed = 0
    
    # For calculating overall metrics
    total_matches = 0
    total_under_matches = 0
    total_over_matches = 0
    
    # Run each test
    for test_case in filtered_test_cases:
        # Get the test ID
        test_id = int(test_case["id"])
        
        # Run the test
        result = run_test_by_id(test_id, test_cases)
        
        # Increment the counter and print progress
        completed += 1
        print(f"Progress: {completed}/{total_tests} tests completed ({completed/total_tests*100:.1f}%)")
        
        # Check for errors
        if "error" in result:
            error_tests.append({
                "test_id": test_id,
                "error": result["error"]
            })
            continue
            
        # Add to passing or failing tests
        if result["is_passing"]:
            passing_tests.append(result)
            # Add to metrics
            total_matches += len(result["matches"])
            total_under_matches += len(result["under_matches"])
            total_over_matches += len(result["over_matches"])
        else:
            failing_tests.append(result)
            # Add to metrics
            total_matches += len(result["matches"])
            total_under_matches += len(result["under_matches"])
            total_over_matches += len(result["over_matches"])
    
    # Calculate overall elapsed time
    overall_elapsed = time.time() - overall_start
    
    # Calculate percentages
    total_completed_tests = len(passing_tests) + len(failing_tests)
    pass_percentage = (len(passing_tests) / total_completed_tests * 100) if total_completed_tests > 0 else 0
    
    # Calculate performance metrics
    precision = total_matches / (total_matches + total_over_matches) if (total_matches + total_over_matches) > 0 else 0
    recall = total_matches / (total_matches + total_under_matches) if (total_matches + total_under_matches) > 0 else 0
    f_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    # Prepare results
    print(f"Test run completed in {overall_elapsed:.2f} seconds.")
    print(f"Results: {len(passing_tests)} passing, {len(failing_tests)} failing, {len(error_tests)} errors")
    
    return {
        "meta": {
            "total_tests": total_tests,
            "passing": len(passing_tests),
            "failing": len(failing_tests),
            "errors": len(error_tests),
            "pass_rate_percent": round(pass_percentage, 2),
            "total_elapsed": round(overall_elapsed, 2),
            "performance": {
                "precision": round(precision, 3),
                "recall": round(recall, 3),
                "f_score": round(f_score, 3)
            }
        },
        "passing_tests": passing_tests,
        "failing_tests": failing_tests,
        "error_tests": error_tests
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
