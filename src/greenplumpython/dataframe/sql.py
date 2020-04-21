import pandas as pd
from .database import GPDatabase

def _sqlalchemy_con(con):
    con_alchemy = pd.io.sql._engine_builder(con)
    if not pd.io.sql._is_sqlalchemy_connectable(con):
        raise NotImplementedError(
            "read_sql_table only supported for SQLAlchemy connectable."
        )
    return con_alchemy

def get_dataframe_from_table(table_name, con):
    """
    put table into DataFrame
    """
    df = pd.read_sql_table(table_name, con)
    return df

def get_dataframe_wrapper_from_table(table_name, con, index_col=None, schema=None):
    con_alchemy = _sqlalchemy_con(con)
    import sqlalchemy
    from sqlalchemy.schema import MetaData

    meta = MetaData(con, schema=schema)
    try:
        meta.reflect(only=[table_name], views=True)
    except sqlalchemy.exc.InvalidRequestError as err:
        raise ValueError(f"Table {table_name} not found") from err

    db = GPDatabase(con_alchemy, meta=meta)
    table = db.read_table(table_name,
        index_col=index_col,
    )
    return table

def get_dataframe_from_sql(query, con):
    df = pd.read_sql_query(query, con)
    return df

def get_dataframe_wrapper_from_sql(query, con):
    con_alchemy = _sqlalchemy_con(con)
    db = GPDatabase(con_alchemy, meta=None)
    return db.read_query(query)