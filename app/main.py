# Standard library imports
import os
from datetime import datetime
from io import StringIO
import logging
import psycopg2

# Third-party imports
import pandas as pd
import sqlalchemy
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocketException, status

app = FastAPI()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("log.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("Logging started")

@app.get("/")
async def root():
    logger.info(f" request / endpoint!")
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


@app.get("/prun_update_all", status_code=status.HTTP_202_ACCEPTED)
async def save_current_prun_orders_volume():
    """
    This function helps in saving ALL current prun orders in a PostgreSQL database. Check with administrator for an
    export or api endpoint for accessing that data.
    """
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
        load_dotenv(dotenv_path=".env")
        user = os.getenv("DB_INSERT_USERNAME")
        password = os.getenv("DB_INSERT_USERNAME_PASSWORD")
        hostname = os.getenv("DB_HOSTNAME")
        database = os.getenv("DB_INSERT_DATABASE")
        port = os.getenv("DB_PORT")

        logger.info(f"Connecting to {hostname}:{port} with: {user} and {password}")
        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}:{port}/{database}')
        for api_root in api_csv_list:
            api_link = "https://rest.fnar.net"
            called_api_link = f"{api_link}{api_root}"
            logger.info(f"API link: {called_api_link}")
            async with httpx.AsyncClient(timeout=360) as client:
                result = await client.get(called_api_link)

            if result.status_code == 200:
                api_name = api_root.replace('/csv/', '')

                # only proceed if the api works!
                current_time = datetime.now().strftime("%d-%m-%Y-%H-%M")
                data = StringIO(result.text)
                destination_filename = f"{current_time}-{api_name}.csv"
                dataframe = pd.read_csv(data)
                print(dataframe.head())
                # serialize into string for easier archivation and later parsing down the road
                dataframe_string = dataframe.to_string()
                data = [[destination_filename, dataframe_string]]
                dataframe = pd.DataFrame(data, columns=['csv_filename', 'csv_content'])
                print(dataframe)
                print(dataframe.shape)
                try:
                    dataframe.to_sql(name="temporary_csv_hold", con=engine, schema="prun", if_exists="append", index=False)
                except Exception as error:
                    logger.error(f"Error while uploading to database for {destination_filename}, {error}")
                    pass
            else:
                logger.error(f"Investigate error during API call (non-200 answer from source) {called_api_link}")

            return
    try:
        await download_csv()
        return status.HTTP_201_CREATED
    except HTTPException as e:
        logger.error(f"Error downloading file, {e} occured!")
        raise HTTPException(status_code=500, detail=str(e))
    except WebSocketException as e:
        logger.error(f"Error downloading file, {e} occured!")
        raise WebSocketException(code=501, reason=str(e))
    except Exception as e:
        logger.error(f"Encountered exception {e}")
        raise Exception
