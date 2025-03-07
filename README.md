# RORacle API

RORacle is an API service that takes affiliation strings from scholarly papers and returns lists of ROR IDs that represent the institutions mentioned. Basically it’s a ROR-backed NER service. 


## Approach

Instead of trying to identify and isolate entities from the affiliation strings, we take the opposite approach: we start with the list of alternate institution names provided by ROR. We iterate through this list and try each one to see if we find a match in the affiliation string. If we do, we add this ROR to our list of results.

Since affiliations can have multiple ROR IDs associated with them, we check every single ROR alternate name, for every string. Every time we find a match, we add it to the list to return. 

There are a few gotchas with this approach. 

First, not all the alternate names found in ROR are unique. For example, MIT could refer to Massachusetts Institute of Technology or Mumbai Institute of Technology. 

So, for non-unique name strings, we require some kind of geographical information as well as a name match. For example, "MIT, India" will correctly generate Mumbai Institute of Technology. "MIT" by itself will generate no match at all.

Second, some name strings are subsets of other name strings. For example, "Harvard" is a subset of "Harvard Medical School". So if we take the naive approach, the affiliation string "Harvard Medical School, MA, USA" would generate two matches. We don't want this. Instead, we want to generate only one match for "Harvard Medical School" and no matches for "Harvard".

To handle this, we sort the list of alternate names by length from longest to shortest. Then as we check the affiliation string for each name in turn, if we get a hit we remove that string from the affiliation string. 

So in the example above, we would get a hit on Harvard Medical School. The input string would be truncated to "MA, USA" and there would be no more matches for any other institution.



## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the development server:
```bash
uvicorn main:app --reload
```

The API will be available at http://localhost:8000

## API Documentation

When running locally, view the interactive API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
