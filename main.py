from fastapi import FastAPI
from roracle.ror_matcher import find_ror_records

app = FastAPI(title="RORacle API")

@app.get("/")
async def root():
    return {
        "msg": "Don't panic"
    }

@app.get("/affiliations")
async def get_ror_records(affiliation: str):
    """
    Get ROR records for a given affiliation string.
    """
    records = find_ror_records(affiliation)
    return {"ror_records": [record.to_dict() for record in records]}
