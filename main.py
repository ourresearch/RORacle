from fastapi import FastAPI
from roracle.ror_matcher import find_ror_records
from roracle.test_runner import run_tests
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
async def run_test_suite(limit: Optional[int] = None, sample: Optional[Union[bool, int]] = None):
    """
    Run test suite and return results.
    
    Args:
        limit: Optional maximum number of tests to run
        sample: If True, randomizes test order. If int, uses it as random seed.
    """
    return run_tests(limit=limit, sample=sample)
