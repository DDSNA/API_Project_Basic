import json

import pandas as pd


def basic_pandas(link) -> list[pd.DataFrame]:
    df_json = {}
    df = pd.read_html(link)
    return df
