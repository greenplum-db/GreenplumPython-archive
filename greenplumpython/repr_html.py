# FIXME : Class from tabulate, need to modify it
class JupyterHTMLStr(str):
    """Wrap the string with a _repr_html_ method so that Jupyter
    displays the HTML table"""

    def _repr_html_(self):
        return self

    def html_code(self):
        """Get access to html code"""
        return self
