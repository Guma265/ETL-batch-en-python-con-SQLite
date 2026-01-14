import pandas as pd
from etl_sqlite.transform import transform_dataframe


def test_valid_row():
    df = pd.DataFrame({
        "nombre": ["Ana"],
        "edad": [30],
        "ciudad": ["CDMX"],
    })

    valid, rejected = transform_dataframe(df)

    assert len(valid) == 1
    assert len(rejected) == 0
