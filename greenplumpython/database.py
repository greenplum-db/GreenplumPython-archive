class Database:
    def __init__(self, conf: dict = {}):
        self._conf = conf
        self._connect()

    def close(self):
        raise NotImplementedError()

    def _execute(self, name: str):
        raise NotImplementedError()

    def _connect(self):
        raise NotImplementedError()
