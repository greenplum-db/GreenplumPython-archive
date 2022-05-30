import greenplumpython as gp


def test_db():
    db = gp.database(host="localhost", dbname="gpadmin")
    result = db.execute("SELECT version()")
    for row in result:
        assert "PostgreSQL" in row["version"]
    db.close()
