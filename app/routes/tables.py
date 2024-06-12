from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRouter
from sqlalchemy.engine import create_engine
from sqlalchemy import text
import os
import logging
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

@router.get("/tables/{table_name}")
async def get_table(table_name: str):
    try:
        logging.info(f"{sql_alchemy_postgres_db}, "
                     f"{sql_alchemy_postgres_host}, "
                     f"{sql_alchemy_postgres_port}, "
                     f"{sql_alchemy_postgres_schema},"
                     f"{sql_alchemy_postgres_db},"
                     f"{sql_alchemy_postgres_user}")
        engine = create_engine(f"postgresql://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{sql_alchemy_postgres_host}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            table = connection.execute(text(f"SELECT * FROM {sql_alchemy_postgres_schema}.{table_name}"))
            with open (f"{table_name}.csv", "w") as file:
                for row in table:
                    file.write(",".join([str(cell) for cell in row]) + "\n")
            table = open(f"{table_name}.csv", "r")
            return StreamingResponse(table, media_type="text/csv")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Table not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))