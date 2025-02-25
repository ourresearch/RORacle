from fastapi import FastAPI

app = FastAPI(title="RORacle API")

@app.get("/")
async def root():
    return {
        "msg": "Don't panic"
    }

