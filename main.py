from fastapi import FastAPI, HTTPException
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
    try:
        return run_tests(limit=limit, sample=sample, dataset_name=dataset_name)
    except Exception as e:
        # Raise a more helpful HTTPException when something goes wrong
        raise HTTPException(
            status_code=500,
            detail=f"Error running tests: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
        )

@app.get("/tests/{test_id}")
async def run_single_test(test_id: int):
    """
    Run a single test case by ID and return the result.
    
    Args:
        test_id: ID of the test case to run
    """
    try:
        result = run_test_by_id(test_id=test_id)
        if "error" in result:
            raise HTTPException(
                status_code=404 if "not found" in result["error"].lower() else 500,
                detail=result["error"]
            )
        return result
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Raise a more helpful HTTPException for other errors
        raise HTTPException(
            status_code=500,
            detail=f"Error running test {test_id}: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
        )
