import datetime

import pandas as pd
import sqlalchemy
from fastapi import HTTPException
from fastapi.responses import FileResponse
from fastapi.routing import APIRouter
from sqlalchemy.engine import create_engine
from sqlalchemy import text, MetaData, Select, Table, select, Column, Integer, String, ForeignKey, Sequence
from sqlalchemy.exc import IntegrityError, NoSuchTableError
from pathlib import Path
import os

import seaborn as sns
import matplotlib.pyplot as plt
import logging
import sys

#logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s-%(lineno)d')
handler.setFormatter(formatter)
logger.addHandler(handler)

router = APIRouter()


def cleanup_processed_files():
    """
    Cleanup processed files
    :return:
    """
    try:
        for file in os.listdir('./processed'):
            os.remove(f'./processed/{file}')
        logging.info(f"Cleaned up processed files at {datetime.datetime.now()}")
        return True
    except Exception as e:
        logging.error(f"Error in cleanup: {e}")
        return False


def load_data(filename) -> pd.DataFrame:
    """
    Load data from a file, must contain file extension
    EX: file.csv
    :param filename:
    :return:
    """
    logging.info(f"Loading data from {filename} at {datetime.datetime.now()}")
    file_type = filename.split('.')[-1]
    logging.info(f"During loading, file type is {file_type}, the current directory is {os.getcwd()}")
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), f'../{file_type}/{filename}'))
    logging.info(f"Data path: {data_path}")
    logging.warning(f"Checking if path exists {os.path.exists(data_path)}, at {os.getcwd()}")
    data = pd.read_csv(data_path)
    print(data.head())
    print(data.info())
    logging.info(f"Data finished loading from {filename} at {datetime.datetime.now()}")
    return data


def create_plots(array: list, array_tickers: list):
    """
    Create plots for the data, based on the processed tables located in ./processed

    Example:
        array = ["temporary_df_hold_bids.csv", "temporary_df_hold_orders.csv"]

        array_tickers = ['H2O', 'LST', "O", "FEO", "FE", "COF", "NS", "PT", "OVE"]
    :param array:
    :param array_tickers:
    :return:
    """
    try:
        for df_name in array:
            logging.warning(
                f"Processing {df_name} at {datetime.datetime.now()}, checking if path exists {os.path.exists(f'./processed')}")
            if os.path.exists(f"./processed"):
                logging.info(f"Processed directory exists at {datetime.datetime.now()}")
                pass
            else:
                logging.warning(f"Creating processed directory at {datetime.datetime.now()}")
                # use os.path.abspath(os.path.join(os.path.dirname(__file__), f'{file_type}'))
                os.mkdir(f"./processed")
                os.mkdir(os.path.abspath(os.path.join(os.path.dirname(__file__), f'processed')))
            for material_ticker_filter in array_tickers:
                logging.info(f"Processing {df_name} at {datetime.datetime.now()} for ticker {material_ticker_filter}")
                df: pd.DataFrame = load_data(f"{df_name}")
                df.describe()
                print(f"Printing {material_ticker_filter} data")
                df = df[df['MaterialTicker'].str.fullmatch(material_ticker_filter) == True]
                df['Total Cost'] = df['ItemCount'] * df['ItemCost']
                df['Date'] = pd.to_datetime(df['collection_timestamp']).dt.date

                df['Suspected duplicate'] = df.duplicated(
                    subset=['MaterialTicker', 'ExchangeCode', 'ItemCost', 'ItemCount', 'CompanyName', 'Date'],
                    keep="first")
                df.to_csv(f'./processed/{material_ticker_filter}-{df_name}-with-suspected-duplicates.csv')
                print(df)
                df.drop(df[df['Suspected duplicate'] == True].index, inplace=True)
                df.drop(df[df['ItemCost'] < 0].index, inplace=True)
                df.drop(df[df['ItemCount'] < 0].index, inplace=True)

                grouped: pd.Series = df.groupby(['Date', 'ExchangeCode', 'MaterialTicker'])['ItemCount'].sum()
                grouped: pd.DataFrame = grouped.to_frame()
                logger.info(f"{os.curdir}")
                grouped.to_csv(f'./processed/{material_ticker_filter}-{df_name}-simplified_grouped.csv')
                df['Total Available'] = grouped['ItemCount'].sum()
                logging.info(f"Grouped data for {material_ticker_filter} at {datetime.datetime.now()}")
                logging.info(f"Plotting for {material_ticker_filter} at {datetime.datetime.now()}")

                plt.clf()
                # Create a new figure instance for the next plot
                plt.figure(figsize=(20, 10), dpi=120)

                # Reapply the plot settings for the new figure
                sns.lineplot(x='Date',
                             y='ItemCost',
                             data=df,
                             hue='ExchangeCode',
                             style='ExchangeCode',
                             markers=True,
                             dashes=False,
                             sizes=(1, 5),
                             palette='viridis')
                plt.title(f"Product analysis {material_ticker_filter}", fontsize=20, color='gray', fontweight='bold')
                plt.xticks(rotation=90)  # Rotate x-axis labels again if needed
                print("Applying annotations")
                plt.annotate(
                    f"Source: {str(df_name)}",
                    xy=(0.9, 1.11),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )

                plt.annotate(
                    f"Mean: {round(df['ItemCost'].mean(), 2)}",
                    xy=(0.9, 1.02),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Min: {round(df['ItemCost'].min(), 2)}",
                    xy=(0.9, 1.08),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Max: {round(df['ItemCost'].max(), 2)}",
                    xy=(0.9, 1.05),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )

                print("Saving plot with material ticker filter")
                plt.savefig(f'processed/{material_ticker_filter}-{df_name}.png')
                print("Showing plot")
                # plt.show()

                plt.clf()

                plt.figure(figsize=(20, 10), dpi=120)
                print("Dataframe: ")
                print(df)
                print("Grouped: ")
                print(grouped)
                # Reapply the plot settings for the new figure
                sns.lineplot(x='Date',
                             y='ItemCount',
                             data=grouped,
                             hue='ExchangeCode',
                             style='ExchangeCode',
                             markers=True,
                             dashes=False,
                             sizes=(1, 30))

                plt.title(f"Item Availability Daily for {material_ticker_filter}, split by Market Exchange",
                          fontsize=20, color='gray',
                          fontweight='bold')
                plt.xticks(rotation=90)

                plt.annotate(
                    f"Source: {str(df_name)}",
                    xy=(0.9, 1.11),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )

                plt.annotate(
                    f"Mean: {round(df['ItemCost'].mean(), 2)}",
                    xy=(0.9, 1.02),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Min: {round(df['ItemCost'].min(), 2)}",
                    xy=(0.9, 1.08),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Max: {round(df['ItemCost'].max(), 2)}",
                    xy=(0.9, 1.05),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                # Rotate x-axis labels again if needed
                print("Saving plot with material ticker filter")
                plt.savefig(f'processed/{material_ticker_filter}-{df_name}.png')
                print("Showing plot")
                logging.info(f"Finished plotting for {material_ticker_filter} at {datetime.datetime.now()}")
                # plt.show()
            del df
            plt.clf()

        return True
    except Exception as e:
        logging.error(f"Error in plotting: {e}")
        return False


@router.get("/reports/initialize", tags=['functional', 'prun'], status_code=200, include_in_schema=False)
async def initialize_tables(refresh: bool = False):
    """
    Not functional yet - in testing
    :return:
    """
    try:
        mode = os.environ.get("MODE")
        sql_alchemy_postgres_user = os.environ.get("PG_USER")
        sql_alchemy_postgres_password = os.environ.get("PG_PASSWORD")
        sql_alchemy_postgres_host = os.environ.get("PG_INTERNAL_DOMAIN")
        sql_alchemy_postgres_port = os.environ.get("PG_INTERNAL_PORT")
        sql_alchemy_postgres_db = os.environ.get("PG_DATABASE")
        sql_alchemy_postgres_schema = os.environ.get("PG_SCHEMA")
        logger.warning(f"Current working directory: {os.getcwd()}")

    except Exception as e:
        logger.error(f"Error getting environment variables: {e}")
        logger.warning(f"Current working directory: {os.getcwd()}")
        raise HTTPException(status_code=500, detail="Error getting environment variables")
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{sql_alchemy_postgres_host}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'prun_data'"
            tables = pd.read_sql(query, engine)
            tables_list = ["temporary_df_hold_bids", "temporary_df_hold_orders"]
            # tables_list :list[str] = [""]
            logger.warning(f"{tables_list}")
            tables_list.sort()

            preferred_file_types = ['csv', 'parquet']
            for file_type in preferred_file_types:
                if os.path.exists(os.path.abspath(os.path.join(os.path.dirname(__file__), f'{file_type}'))):
                    logging.info(f"Directory {file_type} exists, returning {os.path.exists(f'./{file_type}')}")
                    pass
                else:
                    logging.warning(f"Creating directory {file_type}")
                    os.mkdir(f"{file_type}")
                    os.mkdir(os.path.abspath(os.path.join(os.path.dirname(__file__), f'{file_type}')))
            logging.info(f"Reading tables: {tables_list}")
            for table_name in tables_list:
                try:
                    logging.info(f"Reading table: {table_name}")
                    df = pd.DataFrame(pd.read_sql(f'SELECT * FROM prun_data."{table_name}";', engine))
                    logging.info(f"Table {table_name} read")
                    df.to_csv(f"./csv/{table_name}.csv", index=False)
                    df.to_parquet(f"./parquet/{table_name}.parquet", index=False, engine='pyarrow')
                except Exception as e:
                    logging.error(f"Error reading table: {table_name} with error: {e}")
                    continue

        logging.info("Successfuly initialized data")
    except Exception as e:
        logger.error(f"Error reading tables: {e}")
        raise HTTPException(status_code=500, detail="Error reading tables")

    return {"message": "Data initialized successfully"}


# noinspection PyPackageRequirements
@router.get("/reports/{item_ticker}", tags=['functional', 'prun'], status_code=200)
async def get_visual_report(item_ticker: str,
                            refresh: bool,
                            data_focus: str = "bids"):
    """
    Function in charge of getting tables. Use the ticker name to get the the appropriate image
    :param item_ticker:
    :param refresh:
    :param data_focus:
    :return:
    """
    try:
        mode = os.environ.get("MODE")
        sql_alchemy_postgres_user = os.environ.get("PG_USER")
        sql_alchemy_postgres_password = os.environ.get("PG_PASSWORD")
        sql_alchemy_postgres_host = os.environ.get("PG_INTERNAL_DOMAIN")
        sql_alchemy_postgres_port = os.environ.get("PG_INTERNAL_PORT")
        sql_alchemy_postgres_db = os.environ.get("PG_DATABASE")
        sql_alchemy_postgres_schema = os.environ.get("PG_SCHEMA")
        logger.warning(f"Current working directory: {os.getcwd()}")
    except Exception as e:
        logger.error(f"Error getting environment variables: {e}")
        logger.warning(f"Current working directory: {os.getcwd()}")
        raise HTTPException(status_code=500, detail="Error getting environment variables")
    item_ticker = item_ticker.upper()
    current_dir = Path.cwd()
    for item in current_dir.iterdir():
        logger.info(item.name)

    try:
        if refresh:
            logger.info(f"Current working directory: {os.getcwd()} before delivering image")
            cleanup_processed_files()
            create_plots([f"temporary_df_hold_{data_focus}.csv"], [item_ticker])
            return FileResponse(f"./processed/{item_ticker}-temporary_df_hold_{data_focus}.csv.png",
                                media_type="image/png")
    except Exception as e:
        logger.error(f"Error reading image: {e}")
        cleanup_processed_files()
        raise HTTPException(status_code=500, detail=f"Error reading image for {item_ticker}")
    try:
        # TODO: CHECK WHY THE IMAGES FOLDER IS NOT USED FOR PNG GENERATION THEN REPLACE PROCESSED WITH IMAGES PATH
        data_focus = data_focus.lower()
        logger.warning(f"Data focus: {data_focus}")
        try:
            os.mkdir(f"./images")
        except Exception as e:
            logger.warning(f"{e}, ./images exists")
            pass
        if os.path.exists(f"./processed/{item_ticker}-temporary_df_hold_{data_focus}.csv.png"):
            logger.info(
                f"Path status of path is {os.path.exists(f'./processed/{item_ticker}-temporary_df_hold_{data_focus}.csv.png')}")
            logger.warning(f"{Path.cwd()}")
            logger.warning(f"{Path(f"Path already existed for {data_focus} {item_ticker}, therefore")}")
            return FileResponse(f"./processed/{item_ticker}-temporary_df_hold_{data_focus}.csv.png",
                                media_type="image/png")
        else:
            create_plots([f"temporary_df_hold_{data_focus}.csv"], [item_ticker])
    except Exception as e:
        logger.error(f"Error reading image: {e}")
        logger.warning(f"Current working directory: {os.getcwd()}")
        logger.warning(f"")
        cleanup_processed_files()
        raise HTTPException(status_code=500, detail=f"Error reading image for {item_ticker}")
