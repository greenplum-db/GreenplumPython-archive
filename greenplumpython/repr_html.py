# FIXME: class from tabulate, need to modify it in our own way
class JupyterHTMLStr(str):
    """Wrap the string with a _repr_html_ method so that Jupyter
    displays the HTML table"""

    def _repr_html_(self):
        return self
