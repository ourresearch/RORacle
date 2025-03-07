from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from roracle.ror_matcher import find_ror_records
from roracle.test_runner import run_tests, run_test_by_id
from roracle.ror_utils import get_test_cases_from_google_sheet
from typing import Optional, Union, List
import logging
import time

logger = logging.getLogger(__name__)

# Initialize the FastAPI application
app = FastAPI(title="RORacle API")

# Add CORS middleware to allow cross-origin requests from any domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

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
async def run_test_suite(dataset_name: Optional[str] = None):
    """
    Run test suite and return results.
    
    Args:
        dataset_name: Optional filter to only run tests from a specific dataset
    """
    try:
        # Fetch test cases from Google Sheets once at the beginning
        print("Loading test cases from Google Sheets...")
        test_cases = get_test_cases_from_google_sheet()
        print(f"Loaded {len(test_cases)} test cases.")
        
        # Run the test suite with the fetched test cases
        result = run_tests(test_cases, None, None, dataset_name)
        
        return result
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error running test suite: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
        )

@app.get("/tests/datasets")
async def list_datasets():
    """
    List all available datasets with test counts.
    
    Returns:
        Dict with dataset names and their test counts
    """
    try:
        # Fetch test cases from Google Sheets
        print("Loading test cases from Google Sheets to list datasets...")
        test_cases = get_test_cases_from_google_sheet()
        print(f"Loaded {len(test_cases)} total test cases.")
        
        # Group test cases by dataset_name and count them
        datasets = {}
        for test_case in test_cases:
            dataset_name = test_case.get("dataset_name", "unknown")
            if dataset_name not in datasets:
                datasets[dataset_name] = 0
            datasets[dataset_name] += 1
        
        # Format the response
        dataset_list = [
            {"name": name, "count": count} 
            for name, count in datasets.items()
        ]
        
        # Sort alphabetically by name
        dataset_list.sort(key=lambda x: x["name"])
        
        return {
            "meta": {
                "total_datasets": len(dataset_list),
                "total_tests": len(test_cases)
            },
            "results": dataset_list
        }
    except Exception as e:
        logger.error(f"Error listing datasets: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error listing datasets: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
        )

@app.get("/tests/datasets/{dataset_name}")
async def run_dataset_tests(dataset_name: str):
    """
    Run all tests for a specific dataset and return results.
    
    Args:
        dataset_name: Name of the dataset to run tests for
    """
    try:
        # Fetch test cases from Google Sheets once at the beginning
        print(f"Loading test cases from Google Sheets for dataset: {dataset_name}...")
        test_cases = get_test_cases_from_google_sheet()
        print(f"Loaded {len(test_cases)} total test cases.")
        
        # Run the test suite with the fetched test cases for the specified dataset
        result = run_tests(test_cases, None, None, dataset_name)
        
        return result
    except Exception as e:
        logger.error(f"Error running tests for dataset {dataset_name}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error running tests for dataset {dataset_name}: {str(e)}. This may happen if the Google Sheet is unavailable. Please ensure you have internet access and the Google Sheet URL is correct."
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
