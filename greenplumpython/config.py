"""Global configurations for GreenplumPython."""

print_sql: bool = False
"""
Enable this to display the SQL query sent by GreenplumPython to Database behind each command.
"""

use_pickler: bool = True
"""
Use pickler such as dill to serialize UDFs. Source code will be used if set to False.
"""