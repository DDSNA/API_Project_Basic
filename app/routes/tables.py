from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRouter
from sqlalchemy.engine import create_engine
from sqlalchemy import text
import os
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
mode = os.environ.get("MODE")
if mode == "dev":
    sql_alchemy_postgres_user = os.environ.get("PG_USER")
    sql_alchemy_postgres_password = os.environ.get("PG_PASSWORD")
    sql_alchemy_postgres_host = os.environ.get("PG_HOST")
    sql_alchemy_postgres_port = os.environ.get("PG_PORT")
    sql_alchemy_postgres_db = os.environ.get("PG_DATABASE")
    sql_alchemy_postgres_schema = os.environ.get("PG_SCHEMA")
else:
    pass


@router.get("/tables/{table_name}", tags=['functional', 'prun'])
async def get_table(table_name: str):
    """
    Function in charge of getting tables. Tables with spaces need to have quotes inserted.

    ==Example==: "Average Prices (All)" or _fact_companies_summary_dated


    :param table_name:
    :return:
    """
    try:
        logging.info(f"{sql_alchemy_postgres_db}, "
                     f"{sql_alchemy_postgres_host}, "
                     f"{sql_alchemy_postgres_port}, "
                     f"{sql_alchemy_postgres_schema},"
                     f"{sql_alchemy_postgres_db},"
                     f"{sql_alchemy_postgres_user}")
        engine = create_engine(
            f"postgresql://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{sql_alchemy_postgres_host}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            table = connection.execute(text(f"SELECT * FROM {sql_alchemy_postgres_schema}.{table_name}"))
            with open(f"{table_name}.csv", "w") as file:
                for row in table:
                    file.write(",".join([str(cell) for cell in row]) + "\n")
            table = open(f"{table_name}.csv", "r")
            return StreamingResponse(table, media_type="text/csv")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Table not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/list", tags=["functional", "prun"])
async def get_list_tables():
    try:
        logging.info(f"{sql_alchemy_postgres_db}, "
                     f"{sql_alchemy_postgres_host}, "
                     f"{sql_alchemy_postgres_port}, "
                     f"{sql_alchemy_postgres_schema},"
                     f"{sql_alchemy_postgres_db},"
                     f"{sql_alchemy_postgres_user}")
        engine = create_engine(
            f"postgresql://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{sql_alchemy_postgres_host}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            stmt = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='prun_data' "
            table = connection.execute(text(stmt))
            with open(f"table_list.csv", "w") as file:
                for row in table:
                    file.write(",".join([str(cell) for cell in row]) + "\n")
            table = open(f"table_list.csv", "r")
            return StreamingResponse(table, media_type="text/csv")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Table list not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
