from pg import connect
import sqlalchemy
import pandas as pd


class GPConnection(object):
    def __init__(self):
        self.connection_pool = dict()
        self.max_connectid = 1

    def _sqlalchemy_connection_convert(self, conn):
        conn_alchemy = pd.io.sql._engine_builder(conn)
        if not pd.io.sql._is_sqlalchemy_connectable(conn):
            raise NotImplementedError("read_sql_table only supported for SQLAlchemy connectable.")
        return conn_alchemy

    def connect(self, host: str, port: int, database: str, user: str, password: str):
        url = "postgresql+pygresql://%s:%s@%s:%d/%s" % (user, password, host, port, database)
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        conn_alchemy = self._sqlalchemy_connection_convert(conn)
        conn_pairs = (conn, conn_alchemy)
        self.connection_pool[self.max_connectid] = conn_pairs
        self.max_connectid += 1
        return conn_alchemy

    def get_connection(self, conn_id: int):
        if conn_id in self.connection_pool.keys():
            return self.connection_pool[conn_id][1]
        else:
            raise ValueError("conn id does not exist in GPConnection object")

    def list_connections(self):
        return self.connection_pool.copy()

    def close(self, conn_id: int):
        if conn_id in self.connection_pool.keys():
            self.connection_pool[conn_id][0].close()
            del self.connection_pool[conn_id]
        else:
            raise ValueError("conn id does not exist in GPConnection object")
