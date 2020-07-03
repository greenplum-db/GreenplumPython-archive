import os
host = os.getenv('GPHOST')
if host is None or host is '':
    host = "localhost"
port = os.getenv('PGPORT')
if port is None or port is '':
    port = 5432
else:
    port = int(port)
db = os.getenv('GPDATABASE')
if db is None or db is '':
    db = "gppython"
user = os.getenv('GPUSER')
if user is None or user is '':
    user = "gpadmin"
password = os.getenv('GPPASSWORD')
