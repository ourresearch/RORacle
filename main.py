from fastapi import FastAPI, HTTPException
from roracle.ror_matcher import find_ror_records
from roracle.test_runner import run_tests, run_test_by_id
from roracle.ror_utils import get_test_cases_from_google_sheet
from typing import Optional, Union, List
import logging
import time

logger = logging.getLogger(__name__)

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
        # Fetch test cases from Google Sheets once at the beginning
        print("Loading test cases from Google Sheets...")
        test_cases = get_test_cases_from_google_sheet()
        print(f"Loaded {len(test_cases)} test cases.")
        
        # Run the test suite with the fetched test cases
        result = run_tests(test_cases, limit, sample, dataset_name)
        
        return result
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error running test suite: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
        )

@app.get("/tests/{test_id}")
async def run_single_test(test_id: int):
    """
    Run a single test case by ID and return the result.
    
    Args:
        test_id: ID of the test case to run
    """
    try:
        # Fetch test cases from Google Sheets once at the beginning
        print("Loading test cases from Google Sheets...")
        test_cases = get_test_cases_from_google_sheet()
        print(f"Loaded {len(test_cases)} test cases.")
        
        # Run the single test with the fetched test cases
        result = run_test_by_id(test_id, test_cases)
        
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
        logger.error(f"Error running test {test_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error running test {test_id}: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
        )

@app.get("/benchmark")
async def benchmark_ror_records(affiliations: List[str]):
    """
    Benchmark the performance of find_ror_records with multiple affiliation strings.
    
    Args:
        affiliations: List of affiliation strings to process
    """
    results = []
    total_start_time = time.time()
    
    for affiliation in affiliations:
        start_time = time.time()
        records = find_ror_records(affiliation)
        end_time = time.time()
        
        results.append({
            "affiliation": affiliation,
            "execution_time": end_time - start_time,
            "record_count": len(records),
            "records": [record.to_dict() for record in records]
        })
    
    total_time = time.time() - total_start_time
    
    return {
        "meta": {
            "total_affiliations": len(affiliations),
            "total_execution_time": total_time,
            "average_execution_time": total_time / len(affiliations) if affiliations else 0
        },
        "results": results
    }
