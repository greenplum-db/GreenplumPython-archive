import pytest

import greenplumpython as gp
import greenplumpython.experimental.embedding as _
from tests import db


@pytest.mark.requires_pgvector
def test_embedding_query_string(db: gp.Database):
    content = ["I have a dog.", "I like eating apples."]
    t = (
        db.create_dataframe(columns={"id": range(len(content)), "content": content})
        .save_as(
            table_name="doc",
            temp=True,
            column_names=["id", "content"],
            distribution_key={"id"},
            distribution_type="hash",
            drop_if_exists=True,
            drop_cascade=True,
        )
        .check_unique(columns={"id"})
    )
    t.embedding().create_index(column="content", model_name="all-MiniLM-L6-v2")

    # For search, don't use the DataFrame returned by create_index(),
    # but get a new clean DataFrame from table in database.
    tp = db.create_dataframe("doc", schema="pg_temp")
    assert t != tp
    df = tp.embedding().search(column="content", query="apple", top_k=1)
    assert len(list(df)) == 1
    for row in df:
        assert row["content"] == "I like eating apples."
