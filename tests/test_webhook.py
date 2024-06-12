import httpx
import pandas as pd
from app.core.public_functions import basic_pandas

import pytest

def test_pandas():
    result = basic_pandas("https://developer.mozilla.org/en-US/docs/Learn/HTML/Tables/Basics")[0]
    assert isinstance(result, pd.DataFrame)



