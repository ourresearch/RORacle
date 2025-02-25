import json
import time
from typing import List, Dict, Set
from dataclasses import dataclass, asdict
import os
from .ror_matcher import find_ror_records, RORRecord

@dataclass
class TestResult:
    is_passing: bool
    affiliation: str
    matches: List[RORRecord]
    missing_records: List[RORRecord]
    wrong_records: List[RORRecord]

    def to_dict(self) -> Dict:
        return {
            "is_passing": self.is_passing,
            "affiliation": self.affiliation,
            "matches": [r.to_dict() for r in self.matches],
            "missing_records": [r.to_dict() for r in self.missing_records],
            "wrong_records": [r.to_dict() for r in self.wrong_records]
        }

def compare_records(produced_records: List[RORRecord], expected_records: List[RORRecord]) -> tuple[List[RORRecord], List[RORRecord], List[RORRecord]]:
    """Compare produced and expected records, returning (matches, missing, wrong)"""
    produced_ids = {r.id for r in produced_records}
    expected_ids = {r.id for r in expected_records}
    
    # Find matching records
    matching_ids = produced_ids & expected_ids
    matches = [r for r in produced_records if r.id in matching_ids]
    
    # Find missing records (in expected but not in produced)
    missing = [r for r in expected_records if r.id not in produced_ids]
    
    # Find wrong records (in produced but not in expected)
    wrong = [r for r in produced_records if r.id not in expected_ids]
    
    return matches, missing, wrong

def run_tests() -> Dict:
    """Run all tests and return results summary"""
    # Load test cases
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    test_cases_path = os.path.join(project_root, 'data', 'test_cases.json')
    
    with open(test_cases_path, 'r') as f:
        test_cases = json.load(f)
    
    start_time = time.time()
    results_passing = []
    results_failing = []
    num_error = 0
    
    # Run each test
    for test_case in test_cases:
        try:
            # Get actual results from our matcher
            affiliation = test_case['affiliation_string']
            produced_records = find_ror_records(affiliation)
            
            # Convert test case records to RORRecord objects
            expected_records = [
                RORRecord(id=r['id'], name=r['name']) 
                for r in test_case['ror_records']
            ]
            
            # Compare results
            matches, missing, wrong = compare_records(produced_records, expected_records)
            
            # Create test result
            result = TestResult(
                is_passing=len(missing) == 0 and len(wrong) == 0,
                affiliation=affiliation,
                matches=matches,
                missing_records=missing,
                wrong_records=wrong
            )
            
            # Add to appropriate list
            if result.is_passing:
                results_passing.append(result)
            else:
                results_failing.append(result)
                
        except Exception as e:
            num_error += 1
            print(f"Error running test for affiliation '{affiliation}': {str(e)}")
    
    # Calculate timing
    elapsed = time.time() - start_time
    num_tests = len(test_cases)
    
    return {
        "meta": {
            "num_passing": len(results_passing),
            "num_failing": len(results_failing),
            "num_error": num_error,
            "elapsed": round(elapsed, 3),
            "elapsed_per_test": round(elapsed / num_tests, 3) if num_tests > 0 else 0
        },
        "results_failing": [r.to_dict() for r in results_failing],
        "results_passing": [r.to_dict() for r in results_passing]
    }
