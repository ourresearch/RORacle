import json
import time
import random
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
import os
from .ror_matcher import find_ror_records, RORRecord
from .ror_utils import load_ror_names, create_ror_record

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
    # Load test cases
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    test_cases_path = os.path.join(project_root, 'data', 'test_cases.json')
    
    with open(test_cases_path, 'r') as f:
        test_cases = json.load(f)
    
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
        
        # Get affiliation and expected records
        affiliation = test_case["affiliation_string"]
        
        # Create expected records with names from the CSV
        expected_records = [
            create_ror_record(r["id"])
            for r in test_case["ror_records"]
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
            elapsed=elapsed
        )
        
        # Calculate performance metrics for this test
        precision = len(matches) / (len(matches) + len(over_matches)) if (len(matches) + len(over_matches)) > 0 else 0
        recall = len(matches) / (len(matches) + len(under_matches)) if (len(matches) + len(under_matches)) > 0 else 0
        f_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        return {
            "meta": {
                "elapsed": round(elapsed, 3),
                "performance": {
                    "precision": round(precision, 3),
                    "recall": round(recall, 3),
                    "f_score": round(f_score, 3)
                }
            },
            "result": result.to_dict()
        }
    except Exception as e:
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
    # Load test cases
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    test_cases_path = os.path.join(project_root, 'data', 'test_cases.json')
    
    with open(test_cases_path, 'r') as f:
        test_cases = json.load(f)
    
    # Handle sampling - do this BEFORE applying the limit
    seed = None
    indices = None
    if sample is not None:
        # Create a list of (index, test_case) tuples to preserve original indices
        indexed_test_cases = list(enumerate(test_cases))
        
        if isinstance(sample, bool) and sample:
            # Generate a random seed
            seed = random.randint(0, 2**32 - 1)
        elif isinstance(sample, int):
            # Use the provided integer as seed
            seed = sample
        
        # Set the random seed
        if seed is not None:
            random.seed(seed)
            # Shuffle the indexed test cases
            random.shuffle(indexed_test_cases)
            # Unpack the shuffled indexed test cases
            indices, test_cases = zip(*indexed_test_cases)
            indices = list(indices)
            test_cases = list(test_cases)
    
    # Apply limit AFTER shuffling if specified
    if limit is not None and limit < len(test_cases):
        test_cases = test_cases[:limit]
        if indices is not None:
            indices = indices[:limit]
    
    # Initialize metrics
    start_time = time.time()
    results_passing = []
    results_failing = []
    num_error = 0
    
    # Track performance metrics
    total_matches = 0
    total_under_matches = 0
    total_over_matches = 0
    
    # Run each test
    for i, test_case in enumerate(test_cases):
        try:
            # Time this individual test
            test_start = time.time()
            
            # Get affiliation and expected records
            affiliation = test_case["affiliation_string"]
            expected_records = [
                create_ror_record(r["id"])
                for r in test_case["ror_records"]
            ]
            
            # Get produced records
            produced_records = find_ror_records(affiliation)
            
            # Calculate elapsed time
            elapsed = time.time() - test_start
            
            # Compare produced and expected records
            matches, under_matches, over_matches = compare_records(produced_records, expected_records)
            
            # Update metrics
            total_matches += len(matches)
            total_under_matches += len(under_matches)
            total_over_matches += len(over_matches)
            
            # Get the original index if we shuffled, otherwise use the current index
            original_index = indices[i] if sample is not None else i
            
            # Create test result
            result = TestResult(
                id=original_index,
                is_passing=len(under_matches) == 0 and len(over_matches) == 0,
                affiliation=affiliation,
                matches=matches,
                under_matches=under_matches,
                over_matches=over_matches,
                elapsed=elapsed
            )
            
            # Add to appropriate list
            if result.is_passing:
                results_passing.append(result)
            else:
                results_failing.append(result)
                
        except Exception as e:
            num_error += 1
            print(f"Error running test for affiliation '{affiliation}': {str(e)}")
    
    # Calculate timing stats
    total_elapsed = time.time() - start_time
    avg_time = sum([r.elapsed for r in results_passing + results_failing]) / len(results_passing + results_failing) if results_passing + results_failing else 0
    max_time = max([r.elapsed for r in results_passing + results_failing]) if results_passing + results_failing else 0
    min_time = min([r.elapsed for r in results_passing + results_failing]) if results_passing + results_failing else 0
    
    # Calculate performance metrics
    precision = total_matches / (total_matches + total_over_matches) if (total_matches + total_over_matches) > 0 else 0
    recall = total_matches / (total_matches + total_under_matches) if (total_matches + total_under_matches) > 0 else 0
    f_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    return {
        "meta": {
            "num_passing": len(results_passing),
            "num_failing": len(results_failing),
            "num_error": num_error,
            "num_total": len(test_cases),
            "elapsed": round(total_elapsed, 3),
            "elapsed_per_test": round(avg_time, 3),
            "elapsed_min": round(min_time, 3),
            "elapsed_max": round(max_time, 3),
            "seed": seed,
            "performance": {
                "precision": round(precision, 2),
                "recall": round(recall, 2),
                "f_score": round(f_score, 2)
            }
        },
        "results_failing": [r.to_dict() for r in results_failing],
        "results_passing": [r.to_dict() for r in results_passing]
    }

@dataclass
class TestResult:
    id: int
    is_passing: bool
    affiliation: str
    matches: List[RORRecord]
    under_matches: List[RORRecord]
    over_matches: List[RORRecord]
    elapsed: float  # time taken for this test

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "is_passing": self.is_passing,
            "affiliation": self.affiliation,
            "matches": [r.to_dict() for r in self.matches],
            "under_matches": [r.to_dict() for r in self.under_matches],
            "over_matches": [r.to_dict() for r in self.over_matches],
            "elapsed": round(self.elapsed, 3)
        }
