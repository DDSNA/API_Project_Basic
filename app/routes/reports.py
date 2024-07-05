import datetime

import pandas as pd
import sqlalchemy
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRouter
from sqlalchemy.engine import create_engine
from sqlalchemy import text, MetaData, Select, Table, select, Column, Integer, String, ForeignKey, Sequence
from sqlalchemy.exc import IntegrityError, NoSuchTableError
import os

import seaborn as sns
import matplotlib.pyplot as plt
import logging
import sys

#logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

internal_address_db = os.getenv("INTERNAL_ADDRESS_DB")

router = APIRouter()


@router.get("/reports/{item_ticker}", tags=['functional', 'prun'])
async def get_visual_report(item_ticker: str,
                            refresh: bool,
                            data_focus: str):
    """
    Function in charge of getting tables. Use the ticker name to get the the appropriate image
    :param item_ticker:
    :param refresh:
    :param data_focus:
    :return:
    """
    try:
        mode = os.environ.get("MODE")
        if mode == "dev":
            sql_alchemy_postgres_user = os.environ.get("PG_USER")
            sql_alchemy_postgres_password = os.environ.get("PG_PASSWORD")
            sql_alchemy_postgres_host = os.environ.get("PG_HOST")
            sql_alchemy_postgres_port = os.environ.get("PG_PORT")
            sql_alchemy_postgres_db = os.environ.get("PG_DATABASE")
            sql_alchemy_postgres_schema = os.environ.get("PG_SCHEMA")
        else:
            logger.info("Production mode")
            pass
    except Exception as e:
        logger.error(f"Error getting environment variables: {e}")
        raise HTTPException(status_code=500, detail="Error getting environment variables")

    try:
        engine = create_engine(
            f"postgresql://{sql_alchemy_postgres_user}:{sql_alchemy_postgres_password}@{internal_address_db}:{sql_alchemy_postgres_port}/{sql_alchemy_postgres_db}")
        with engine.connect() as connection:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'prun_data'"
            tables = pd.read_sql(query, engine)
            tables_list = tables['table_name'].tolist()
            tables_list.sort()

            preffered_file_types = ['csv', 'parquet']
            for file_type in preffered_file_types:
                if os.path.exists(f"./{file_type}"):
                    pass
                else:
                    os.mkdir(f"{file_type}")
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
    except Exception as e:
        logger.error(f"Error reading tables: {e}")
        raise HTTPException(status_code=500, detail="Error reading tables")

    try:
        if os.path.exists(f"./images/{item_ticker}-temporary_df_hold_{data_focus}_csv.png"):
            return StreamingResponse(f"./images/{item_ticker}-temporary_df_hold_{data_focus}_csv.png", media_type="image/png")
        else:
            create_plots([f"temporary_df_hold_{data_focus}.csv"], [item_ticker])
            pass
    except Exception as e:
        logger.error(f"Error reading image: {e}")
        cleanup_processed_files()
        raise HTTPException(status_code=500, detail=f"Error reading image for {item_ticker}")
    try:
        if refresh == True:
            cleanup_processed_files()
            create_plots([f"temporary_df_hold_{data_focus}.csv"], [item_ticker])
        return StreamingResponse(f"./images/{item_ticker}-temporary_df_hold_{data_focus}_csv.png", media_type="image/png")
    except Exception as e:
        logger.error(f"Error reading image: {e}")
        cleanup_processed_files()
        raise HTTPException(status_code=500, detail=f"Error reading image for {item_ticker}")
def load_data(filename) -> pd.DataFrame:
    """
    Load data from a file, must contain file extension
    EX: file.csv
    :param filename:
    :return:
    """
    logging.info(f"Loading data from {filename} at {datetime.datetime.now()}")
    file_type = filename.split('.')[-1]
    data_path = os.path.join(os.path.dirname(__file__), f'./{file_type}/{filename}')
    print(data_path)
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

            if os.path.exists(f"./processed"):
                pass
            else:
                os.mkdir(f"./processed")
            for material_ticker_filter in array_tickers:
                logging.info(f"Processing {df_name} at {datetime.datetime.now()} for ticker {material_ticker_filter}")
                df: pd.DataFrame = load_data(f"{df_name}")
                df.describe()
                print(f"Printing {material_ticker_filter} data")
                df = df[df['MaterialTicker'].str.fullmatch(material_ticker_filter) == True]
                df['Total Cost'] = df['ItemCount'] * df['ItemCost']
                df['Date'] = pd.to_datetime(df['collection_timestamp']).dt.date

                df['Suspected duplicate'] = df.duplicated(
                    subset=['MaterialTicker', 'ExchangeCode', 'ItemCost', 'ItemCount', 'CompanyName', 'Date'], keep="first")
                df.to_csv(f'processed/{material_ticker_filter}-{df_name}-with-suspected-duplicates.csv')
                print(df)
                df.drop(df[df['Suspected duplicate'] == True].index, inplace=True)
                df.drop(df[df['ItemCost'] < 0].index, inplace=True)
                df.drop(df[df['ItemCount'] < 0].index, inplace=True)

                grouped :pd.Series= df.groupby(['Date', 'ExchangeCode', 'MaterialTicker'])['ItemCount'].sum()
                grouped :pd.DataFrame= grouped.to_frame()
                grouped.to_csv(f'processed/{material_ticker_filter}-{df_name}-simplified_grouped.csv')
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

                plt.title(f"Item Availability Daily for {material_ticker_filter}, split by Market Exchange", fontsize=20, color='gray',
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
