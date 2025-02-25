from typing import List, Dict

class RORRecord:
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name
        }

def find_ror_records(affiliation_string: str) -> List[RORRecord]:
    """
    Mock function that returns a sample ROR record for any affiliation string.
    In the future, this will be replaced with actual matching logic.
    """
    # Return a sample record for now
    sample_record = RORRecord(
        id="https://ror.org/02ex6cf31",
        name="Brookhaven National Laboratory"
    )
    return [sample_record]
