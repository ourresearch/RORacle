import json
import time
import random
import csv
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
import os
from .ror_matcher import find_ror_records, RORRecord
from .ror_utils import load_ror_names, create_ror_record, extract_ror_ids_from_labels

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
    # Load test cases from insti_bench.tsv
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    tsv_path = os.path.join(project_root, 'data', 'insti_bench.tsv')
    
    # Read the TSV file and convert to a list of test cases
    test_cases = []
    with open(tsv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            test_cases.append(row)
    
    # Check if test_id is valid
    if test_id < 0 or test_id >= len(test_cases):
        return {
            "error": f"Invalid test ID: {test_id}. Valid IDs range from 0 to {len(test_cases) - 1}."
        }
    
    # Get the test case
    test_case = test_cases[test_id]
    
    try:
        # Time this individual test
        test_start = time.time()
        
        # Get affiliation, dataset_name, and expected records from the TSV
        affiliation = test_case["affiliation_string"]
        dataset_name = test_case["dataset_name"]
        
        # Extract ROR IDs from the labels column
        ror_ids = extract_ror_ids_from_labels(test_case["labels"])
        
        # Create expected records using the factory function
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
        
        # Create test result
        result = TestResult(
            id=test_id,
            is_passing=len(under_matches) == 0 and len(over_matches) == 0,
            affiliation=affiliation,
            matches=matches,
            under_matches=under_matches,
            over_matches=over_matches,
            elapsed=elapsed,
            dataset_name=dataset_name
        )
        
        return {
            "meta": {
                "elapsed": round(elapsed, 3)
            },
            "test_id": test_id,
            "is_passing": result.is_passing,
            "affiliation": result.affiliation,
            "dataset_name": result.dataset_name,
            "matches": [record.to_dict() for record in result.matches],
            "under_matches": [record.to_dict() for record in result.under_matches],
            "over_matches": [record.to_dict() for record in result.over_matches]
        }
    except Exception as e:
        # Handle any errors
        return {
            "error": f"Error running test {test_id}: {str(e)}"
        }

def run_tests(limit: Optional[int] = None, sample: Optional[Union[bool, int]] = None) -> Dict:
    """
    Run tests and return a summary of results.
    
    Args:
        limit: Optional maximum number of tests to run
        sample: If True, randomizes test order. If int, uses it as random seed.
    
    Returns:
        Dict with meta information and test results
    """
    # Load test cases from insti_bench.tsv
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    tsv_path = os.path.join(project_root, 'data', 'insti_bench.tsv')
    
    # Read the TSV file
    test_cases = []
    with open(tsv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            test_cases.append(row)
    
    # Generate test indices
    test_indices = list(range(len(test_cases)))
    
    # Handle sampling if requested
    if sample:
        if isinstance(sample, int):
            # Use as random seed
            random.seed(sample)
        random.shuffle(test_indices)
    
    # Apply limit if specified
    if limit and limit < len(test_indices):
        test_indices = test_indices[:limit]
    
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
    
    for i, test_idx in enumerate(test_indices):
        # Run the test
        test_start = time.time()
        result = run_test_by_id(test_idx)
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
            
            # Update metrics for precision/recall calculations
            total_matches += len(result["matches"])
            total_under_matches += len(result["under_matches"])
            total_over_matches += len(result["over_matches"])
                
        # Add to results
        results.append(result)
        
    # Calculate overall elapsed time and stats
    total_elapsed = time.time() - start_time
    avg_time = sum(all_times) / len(all_times) if all_times else 0
    
    # Calculate passing percentage
    test_count = passing + failing
    pass_rate = (passing / test_count) * 100 if test_count > 0 else 0
    
    # Calculate overall performance metrics
    precision = total_matches / (total_matches + total_over_matches) if (total_matches + total_over_matches) > 0 else 0
    recall = total_matches / (total_matches + total_under_matches) if (total_matches + total_under_matches) > 0 else 0
    f_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    # Calculate overall metrics for passing tests
    passing_results = [r for r in results if "is_passing" in r and r["is_passing"]]
    failing_results = [r for r in results if "is_passing" in r and not r["is_passing"]]
    error_results = [r for r in results if "error" in r]
    
    # Return comprehensive results
    return {
        "meta": {
            "total_tests": len(results),
            "passing": passing,
            "failing": failing,
            "errors": errors,
            "pass_rate_percent": round(pass_rate, 2),
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
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "is_passing": self.is_passing,
            "affiliation": self.affiliation,
            "matches": [r.to_dict() for r in self.matches],
            "under_matches": [r.to_dict() for r in self.under_matches],
            "over_matches": [r.to_dict() for r in self.over_matches],
            "elapsed": round(self.elapsed, 3),
            "dataset_name": self.dataset_name
        }
