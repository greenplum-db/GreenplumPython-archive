from pandas.io.sql import DataFrame

from greenplumpython.core.gptable_metadata import GPTableMetadata


class DataFrameWrapper:
    def __init__(self, dataframe, table_metadata):
        self.pd_dataframe = dataframe
        self.table_metadata = table_metadata
