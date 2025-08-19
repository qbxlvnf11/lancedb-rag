import numpy as np
import pandas as pd
import pyarrow as pa

def parsing_df(df):

    fields = []
    for col, dtype in df.dtypes.items():
        if col == 'vector':
            list_size = df['vector'].iloc[0].shape[0]
            fields.append(pa.field(col, pa.list_(pa.float32(), list_size)))
        elif dtype == 'object':
            fields.append(pa.field(col, pa.string()))
        elif dtype == 'int64':
                fields.append(pa.field(col, pa.int64()))
        else:
            fields.append(pa.field(col, pa.from_numpy_dtype(dtype)))
    
    schema = pa.schema(fields)
    tbl = pa.Table.from_pandas(df, schema=schema)

    return tbl