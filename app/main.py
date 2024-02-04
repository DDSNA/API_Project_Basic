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
from fastapi import FastAPI, HTTPException, WebSocketException, status, Response
from fastapi.openapi.utils import get_openapi


app = FastAPI()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("log.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("Logging started")


### OPEN API SCHEMA CUSTOMIZATION
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="DDSNA Data Engineering Portofolio API",
        version="0.1.8",
        summary="""Thank you for visiting my portofolio project.
                The schema for my little PrUn ML project. PostgreSQL saving and processing included (and planned)!""",
        description="""
        As mentioned the schema is for personal use. If you wish to access the database feel free to reach out
        to the development team (email here!). The code is not opensource at the moment.
        Currently the data is saved as a string from a dataframe for all csv endpoints of https://doc.fnar.net.
        Only through their graceful contribution were my skills honed and possible at all, so I thank them and you should too!""",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/")
async def root():
    logger.info(f" request / endpoint!")
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/prun/update/database")
async def update_database(request_json: str = '{"type":"update db"}',
                          cloud_function_url: str = "https://us-central1-prun-409500.cloudfunctions.net/prun_orders"
                          ):
    """
    This function was retired due to budget restrictions, but it is functional if provided a cloud function as
    an argument for calling
    :param request_json: custom key, json formatted string
    :param cloud_function_url:
    :return:
    """
    link_prun_db_cloudfunction_update = cloud_function_url
    try:
        async with httpx.AsyncClient(timeout=720.30) as client:

            response = await client.post(link_prun_db_cloudfunction_update, json=request_json)
            if response.headers['Content-Type'] == 'application/json':
                response_json = response.json()
                return response_json
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


@app.post("/new-update")
async def new_update(title:str = "Dick raiser is coming", message:str = "For your ass"):
    """
    When a new update rolls out, inform the discord bot
    :param update:
                    title: str
                    message: str
                    updateTime: datetime
    :return:
    """
    webhook_url = ("https://discord.com/api/webhooks/1203826362918375544/UWV5Rkp4E-Yar2znY"
                   "-l50At_QQ_WSMEHrhO4Woyoc47A7g5LpmgbHInL0lyyuA3lOLOw")
    headers = {"Content-Type": "application/json"}
    updateTime = datetime.now().isoformat()
    update = {"title": title, "message": message, "updateTime": updateTime}
    async with httpx.AsyncClient() as client:
        response = await client.post(webhook_url, headers=headers, json=update)
    return response.status_code


@app.get("/prun_update_all", status_code=status.HTTP_202_ACCEPTED)
async def save_current_prun_orders_volume(response: Response):
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
                dataframe_string = dataframe.to_html()
                data = [[destination_filename, dataframe_string]]
                dataframe = pd.DataFrame(data, columns=['csv_filename', 'csv_content'])
                print(dataframe)
                print(dataframe.shape)
                try:
                    dataframe.to_sql(name="temporary_csv_hold", con=engine, schema="prun", if_exists="append",
                                     index=False)
                except Exception as error:
                    logger.error(f"Error while uploading to database for {destination_filename}, {error}")
                    pass
            else:
                logger.error(f"Investigate error during API call (non-200 answer from source) {called_api_link}")

    try:
        await download_csv()
        return status.HTTP_200_OK
    except HTTPException as e:
        logger.error(f"Error downloading file, {e} occured!")
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        raise HTTPException(status_code=500, detail=str(e))
    except WebSocketException as e:
        logger.error(f"Error downloading file, {e} occured!")
        response.status_code = status.HTTP_502_BAD_GATEWAY
        raise WebSocketException(code=502, reason=str(e))
    except Exception as e:
        logger.error(f"Encountered unknown exception {e}")
        response.status_code = status.HttpStatus.INTERNAL_SERVER_ERROR
        raise Exception
