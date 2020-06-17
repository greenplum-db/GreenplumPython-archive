from pg import connect
import sqlalchemy
class GPConnection(object):

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.connection_pool = None
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def get_connection(self):
        url = "postgresql+pygresql://%s:%s@%s:%d/%s" % (self.user, self.password, self.host, self.port, self.database)
        engine = sqlalchemy.create_engine(url)
        #connection = connect(url)
        return engine.connect()
