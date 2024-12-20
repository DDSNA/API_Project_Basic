# Standard library imports
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
import logging

import psycopg2
from typing import Annotated

# Third-party imports
import pandas as pd
import sqlalchemy
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocketException, status, Response, Depends
from fastapi.responses import RedirectResponse
from fastapi.openapi.utils import get_openapi
from fastapi.security import OAuth2PasswordBearer
# import redis

# routes imports, routes is a fast api module that should contain a file named tables.py where a router is defined
from routes import tables, users, reports

app = FastAPI()

app.include_router(tables.router)
app.include_router(users.router)
app.include_router(reports.router)

# security
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
# logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("log.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("Logging started")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#TODO: Update this to internal railway network when releasing

# rd = redis.Redis(host='localhost', port=6379, db=0)


### OPEN API SCHEMA CUSTOMIZATION
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="DDSNA Data Engineering Portofolio API",
        version="0.2.1",
        summary="Thank you for visiting my portofolio project.",
        description="""
        Prosperous Universe is an industrial space trading and manufacturing game where you work with other players to create an economy from the groud up at a galactic level.
        As mentioned the schema is for personal use. If you wish to access the database feel free to reach out
        to the development team (email here!). The code is not opensource at the moment.
        Currently the data is saved as a string from a dataframe for all csv endpoints of https://doc.fnar.net.
        Only through their graceful contribution were my skills honed and possible at all, so I thank them and you should check out their work if you can!""",
        routes=app.routes,
        contact={'name': 'Maintainer', 'email': 'hello@danserban.ro', 'url': 'https://danserban.ro'}
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://upload.wikimedia.org/wikiversity/en/8/8c/FastAPI_logo.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse('/docs')


@app.get("/prun/update/database", tags=['not functional', 'gcp'])
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
                logger.error(msg="Error from cloud function")
                raise HTTPException(status_code=500, detail="Response is not JSON")
    except HTTPException as e:
        logger.error(msg=f"Error with {e}")
        raise HTTPException(status_code=504,
                            detail="There is an issue with the cloud function call - may not be reachable")


@app.get("/ping/otherwebsite", status_code=status.HTTP_200_OK, tags=['prun', 'debug'], deprecated=True)
async def ping(website: str = "eve.danserban.ro"):
    try:
        r = httpx.get('https://' + website)
        return {"status": r.status_code}
    except Exception as e:
        return {"error": str(e)}


@app.post("/new-update", tags=['functional', 'prun'])
async def new_update(title: str = "Update regarding database!",
                     message: str = "The database is currently running a new "
                                    "update entry"):
    """
    When a new update rolls out, inform the discord bot
    :param title: Title of discord post
    :param message: Body of discord post
    :return:
    """
    webhook_url = ("https://discord.com/api/webhooks/1203826362918375544/UWV5Rkp4E-Yar2znY"
                   "-l50At_QQ_WSMEHrhO4Woyoc47A7g5LpmgbHInL0lyyuA3lOLOw")
    headers = {"Content-Type": "application/json"}
    updateTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = message + "\n" + updateTime
    update = {
        "embeds":
            [
                {"title": title, "description": message}
            ]
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(webhook_url, headers=headers, json=update)
    return response.status_code


@app.get("/prun_update_all", status_code=status.HTTP_202_ACCEPTED, tags=['functional', 'prun'], deprecated=True)
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
                logger.info(msg="Succesful API connection")
                api_name = api_root.replace('/csv/', '')

                # only proceed if the api works!
                current_time = datetime.now().strftime("%d-%m-%Y-%H-%M")
                data = StringIO(result.text)
                destination_filename = f"{current_time}-{api_name}.csv"
                dataframe = pd.read_csv(data)
                pandas_type_dataframe = dataframe
                # optional but in use now for my own purposes
                timezone_gmt_plus_two = timezone(timedelta(hours=+2))
                pandas_type_dataframe["collection_timestamp"] = datetime.now(tz=timezone_gmt_plus_two)
                print(dataframe.head())
                # serialize into string for easier archivation and later parsing down the road
                # currently keeping it in html because CSV had massive problems
                dataframe_string = dataframe.to_html()
                data = [
                    [destination_filename,
                     dataframe_string]
                ]
                dataframe = pd.DataFrame(data,
                                         columns=['csv_filename', 'csv_content']
                                         )
                print(dataframe)
                print(dataframe.shape)
                try:

                    logger.info(msg="Dataframe uploaded to sql")
                    pandas_type_dataframe.to_sql(
                        name=f"temporary_df_hold_{api_name}",
                        con=engine, schema="prun_data",
                        if_exists="append",
                        index=False
                    )
                    logger.info(msg=f"Dataframe uploaded to proper table for api {api_name}")

                except Exception as error:
                    logger.error(f"Error while uploading to database for {destination_filename}, {error}")
                    pass
            else:
                logger.error(f"Investigate error during API call (non-200 answer from source) {called_api_link}")

    try:
        await download_csv()
        async with httpx.AsyncClient() as client:
            result = await client.post('https://apiprojectbasic-production.up.railway.app/new-update?title=Update'
                                       '%20regarding%20database%21'
                                       '&message=The%20database%20is%20currently%20running%20a%20new%20update%20entry')
            logger.info(msg=f"{result.status_code}")
        return status.HTTP_200_OK

    except HTTPException as e:
        logger.error(f"Error downloading file, {e} occured!")
        async with httpx.AsyncClient() as client:
            result = await client.post(
                'https://apiprojectbasic-production.up.railway.app/new-update?title=Update%20regarding%20database%21'
                '&message=The%20database%20tried%20running%20a%20new%20update%20entry%20and'
                '%20failed')
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        logger.info(result.status_code)
        raise HTTPException(status_code=500, detail=str(e))
    except WebSocketException as e:
        logger.error(f"Error downloading file, {e} occured!")
        async with httpx.AsyncClient() as client:
            result = await client.post('https://apiprojectbasic-production.up.railway.app/new-update?title=Update'
                                       '%20regarding%20database%21'
                                       '&message=The%20database%20tried%20running%20a%20new%20update%20entry%20and%20failed')
        response.status_code = status.HTTP_502_BAD_GATEWAY
        raise WebSocketException(code=502, reason=str(e))
    except Exception as e:
        logger.error(f"Encountered unknown exception {e}")
        async with httpx.AsyncClient() as client:
            result = await client.post('https://apiprojectbasic-production.up.railway.app/new-update?title=Update'
                                       '%20regarding%20database%21'
                                       '&message=The%20database%20tried%20running%20a%20new%20update%20entry%20and%20failed')
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        raise Exception

# Database querying gets
# @app.get("/prun/company_list", tags=['azure', 'not functional'])
# async def get_company_list(token: Annotated[str, Depends(oauth2_scheme)]):
#     return {"token": token}
#
#     file_extension = file.content_type
#     credential = DefaultAzureCredential()
#     storage_url = os.getenv("AZURE_STORAGE_BLOB_URL")
#     blob_client = BlobClient(
#         storage_url,
#         container_name="blob-container-01",
#         blob_name=f"sample-blob-{str(uuid.uuid4())[0:5]}.{file_extension}",
#         credential=credential,
#     )
#
#     try:
#         contents = await file.read()
#         with open(f"app/temp/{file.filename}", "wb") as file_binary:
#             file_binary.write(contents)
#     except Exception as e:
#         logger.error(f"Error while uploading file to azure storage: {e} (saving file locally)")
#         return {"error": str(e)}
#
#     try:
#         file_loc = f"app/temp/{file.filename}"
#         with open(file_loc, "r") as data:
#             logger.info(f"Taking file from: {file_loc}")
#             blob_client.upload_blob(data)
#
#             print(f"Uploaded {file.filename} to {blob_client.url}")
#             return {"filename": file.filename}
#     except Exception as e:
#         logger.error(f"Error while uploading file to azure storage: {e} (uploading)")
#         data.close()
#         return {"error": str(e)}
