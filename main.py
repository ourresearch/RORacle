from fastapi import FastAPI
from roracle.ror_matcher import find_ror_records
from roracle.test_runner import run_tests, run_test_by_id
from typing import Optional, Union

app = FastAPI(title="RORacle API")

@app.get("/")
async def root():
    return {
        "msg": "Don't panic"
    }

@app.get("/ror-records")
async def get_ror_records(affiliation: str):
    """
    Get ROR records for a given affiliation string.
    """
    records = find_ror_records(affiliation)
    return {
        "meta": {
            "total_results": len(records)
        },
        "results": [record.to_dict() for record in records]
    }

@app.get("/tests")
async def run_test_suite(limit: Optional[int] = None, sample: Optional[Union[bool, int]] = None, dataset_name: Optional[str] = None):
    """
    Run test suite and return results.
    
    Args:
        limit: Optional maximum number of tests to run
        sample: If True, randomizes test order. If int, uses it as random seed.
        dataset_name: Optional filter to only run tests from a specific dataset
    """
    return run_tests(limit=limit, sample=sample, dataset_name=dataset_name)

@app.get("/tests/{test_id}")
async def run_single_test(test_id: int):
    """
    Run a single test case by ID and return the result.
    
    Args:
        test_id: ID of the test case to run
    """
    return run_test_by_id(test_id=test_id)
