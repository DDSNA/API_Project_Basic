from fastapi import FastAPI
import httpx
from pythonping import ping

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/prun/update/database")
async def update_database(request_type: str = "update db"):
    link_prun_db_cloudfunction_update = "https://us-central1-prun-409500.cloudfunctions.net/prun_orders"
    async with httpx.AsyncClient(timeout=720.30) as client:
        response = await client.post(link_prun_db_cloudfunction_update, json={"type": request_type})
        response_json = response.json()
    return {
        "message": "Sending request...",
    }


@app.post("/ping/otherwebsite")
async def ping(website: str = "eve.danserban.ro"):
    try:
        r = await httpx.get('https://' + website)
        return {"status": r.status_code}
    except Exception as e:
        return {"error": str(e)}

@app.get("/database_call")
async def database_call()


