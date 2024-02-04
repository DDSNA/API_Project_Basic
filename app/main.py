from datetime import datetime
from io import StringIO
import pandas as pd

from fastapi import FastAPI, HTTPException, WebSocketException
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
    try:
        async with httpx.AsyncClient(timeout=720.30) as client:
            response = await client.post(link_prun_db_cloudfunction_update, json={"type": request_type})
            if response.headers['Content-Type'] == 'application/json':
                response_json = response.json()
            else:
                raise HTTPException(status_code=500, detail="Response is not JSON")
    except HTTPException as e:
        raise HTTPException(status_code=504,
                            detail="There is an issue with the cloud function call - may not be reachable")


@app.get("/ping/otherwebsite")
async def ping(website: str = "eve.danserban.ro"):
    try:
        r = httpx.get('https://' + website)
        return {"status": r.status_code}
    except Exception as e:
        return {"error": str(e)}


@app.get("/prun_bids")
async def save_current_prun_orders_volume():
    api_csv_list = ['/csv/buildings',
                    '/csv/buildingcosts',
                    '/csv/buildingworkforces',
                    '/csv/buildingrecipes',
                    '/csv/materials',
                    '/csv/prices',
                    '/csv/orders',
                    '/csv/bids',
                    '/csv/recipeinputs',
                    '/csv/recipeoutputs',
                    '/csv/planets',
                    '/csv/planetresources',
                    '/csv/planetproductionfees',
                    '/csv/planetdetail',
                    '/csv/systems',
                    '/csv/systemlinks',
                    '/csv/systemplanets',
                    '/csv/burnrate',
                    '/csv/workforce']

    async def download_csv():
        for api_root in api_csv_list:
            api_link = "https://rest.fnar.net"
            called_api_link = f"{api_link}{api_root}"
            async with httpx.AsyncClient() as client:
                result = await client.get(called_api_link)

            if result.status_code == 200:
                api_name = api_root.replace('/csv/', '')

                # only proceed if the api works!
                current_time = datetime.now().strftime("%d-m-%Y-%H-%M")
                data = StringIO(result.text)
                destination_filename= f"{current_time}-{api_name}.csv"
                dataframe = pd.read_csv(data)
                print(dataframe.head())
                #TODO: continue saving locally the files that are needed
    await download_csv()
