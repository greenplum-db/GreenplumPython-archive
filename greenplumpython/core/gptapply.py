import pandas as pd
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.core.dataframe_wrapper import DataFrameWrapper
from greenplumpython.core.gptable_metadata import GPTableMetadata
import inspect
import random
import string

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_func"+''.join(random.choice(letters) for i in range(stringLength))

def randomStringType(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_type"+''.join(random.choice(letters) for i in range(stringLength))

def gptapply(X: DataFrameWrapper, index, gp_database: GPDatabase, py_func, output_meta: GPTableMetadata  = None, runtime_id = None, runtime_type = "plcontainer", **kwargs):    
    # Get Function body
    udt_body = inspect.getsource(py_func)
    # Get extra arugments for py_func
    # Build SQL
    query_sql = ""
    out_df = gp_database.execute_query(query_sql)
    if output_meta is not None:
        out_df_wrapper = DataFrameWrapper(None, output_meta)
    else:
        out_df_wrapper = DataFrameWrapper(out_df, None)
    return out_df_wrapper