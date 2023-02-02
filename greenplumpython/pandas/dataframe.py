import greenplumpython.dataframe as gp

class DataFrame:
    def __init__(self, data, columns) -> None:
        self._proxy= gp.DataFrame(query="SELECT xxx")