from fastapi import FastAPI
from roracle.ror_matcher import find_ror_records
from roracle.test_runner import run_tests

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
async def run_test_suite():
    """
    Run all tests from test_cases.json and return results.
    """
    return run_tests()
