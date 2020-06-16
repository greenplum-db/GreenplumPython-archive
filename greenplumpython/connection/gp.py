from pg import connect
import sqlalchemy
class GPConnection(object):

    def __init__(self, host: str, database: str, user: str, password: str):
        self.connection_pool = None
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def get_connection(self):
        #TODO port
        url = "postgresql+pygresql://%s:%s@%s:6000/%s" % (self.user, self.password, self.host, self.database)
        engine = sqlalchemy.create_engine(url)
        #connection = connect(url)
        return engine.connect()
