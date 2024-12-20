import sqlalchemy
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRouter
from sqlalchemy.engine import create_engine
from sqlalchemy import text, MetaData, Select, Table, select, Column, Integer, String, ForeignKey, Sequence
from sqlalchemy.exc import IntegrityError, NoSuchTableError
import os
import plotly.express as px

import logging
import sys

#logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)



router = APIRouter()
mode = os.environ.get("MODE")
#TODO: Improve this part of the code, it currently only handles dev mode but it should also find a way to hide away thse variables at code run
if mode == "dev":
    sql_alchemy_postgres_user = os.environ.get("PG_USER")
    sql_alchemy_postgres_password = os.environ.get("PG_PASSWORD")
    sql_alchemy_postgres_host = os.environ.get("PG_HOST")
    sql_alchemy_postgres_port = os.environ.get("PG_PORT")
    sql_alchemy_postgres_db = os.environ.get("PG_DATABASE")
    sql_alchemy_postgres_schema = os.environ.get("PG_SCHEMA")
    logger.error(f"{sql_alchemy_postgres_db}")
    logging.error(f"{sql_alchemy_postgres_db}")
else:
    pass


@router.get("/tables/{table_name}", tags=['functional', 'prun'])
async def get_table(table_name: str):
    """
    Function in charge of getting tables. Use the table name to get the table.
    ---
    Consider using the function below for a complete list of tables.

    `Example` : "Average Prices (All)" or _fact_companies_summary_dated


    """
    try:
        logger.info(f"{sql_alchemy_postgres_db}, "
                     f"{sql_alchemy_postgres_host}, "
                     f"{sql_alchemy_postgres_port}, "
                     f"{sql_alchemy_postgres_schema},"
                     f"{sql_alchemy_postgres_db},"
                     f"{sql_alchemy_postgres_user}")
        engine = create_engine(
            f"postgresql://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{sql_alchemy_postgres_host}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            if len(table_name) > 38:
                logger.info(f"Table name: {table_name}, with length of {len(table_name)}")
                print(len(table_name))
                raise HTTPException(status_code=400, detail="Table name too long")
            else:
                pass
            requested_table_loader = Table(table_name, MetaData(), autoload_with=engine, schema='prun_data')
            table = connection.execute(statement=select(requested_table_loader))

            with open(f"{table_name}.csv", "w") as file:
                # WRITE THE HEADER TO THE FILE TOO
                header = table.keys()
                file.write(",".join(header) + "\n")
                for row in table:
                    file.write(",".join([str(cell) for cell in row]) + "\n")
            table = open(f"{table_name}.csv", "r")
            return StreamingResponse(table, media_type="text/csv")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Table not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/table-list/{refresh}", tags=["functional", "prun"])
async def get_list_tables():
    try:
        logger.info(f"{sql_alchemy_postgres_db},"
                     f"{sql_alchemy_postgres_host},"
                     f"{sql_alchemy_postgres_port},"
                     f"{sql_alchemy_postgres_schema},"
                     f"{sql_alchemy_postgres_db},"
                     f"{sql_alchemy_postgres_user}")
        engine = create_engine(
            f"postgresql://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{sql_alchemy_postgres_host}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            metadata = MetaData()
            # call procedure to refresh table due to limited rights of user
            stmt = text("CALL prun_data.refresh_acc_cloud_accesible_tables();")
            result = connection.execute(stmt)

            # actual returned csv request
            table_selector = Table('acc_cloud_accesible_tables', metadata, autoload_with=engine, schema='prun_data')
            table = connection.execute(statement=select(table_selector))
            with open("table_list.csv", "w") as file:
                for row in table:
                    file.write(",".join([str(cell) for cell in row]) + "\n")
            table = open("table_list.csv", "r")
            # reconsider if streaming response is really necessary
            return StreamingResponse(table, media_type="text/csv")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Table list not found")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail="Table not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tables/visual-price/{item_ticker}")
async def get_visual_price_item(item_ticker: str,
                                data_focus: str = "bids"):
    item_ticker = item_ticker.upper()

    return None
