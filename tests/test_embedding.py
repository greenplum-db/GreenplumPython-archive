import pytest

import greenplumpython as gp
import greenplumpython.experimental.embedding as _
from tests import db


def search_embeddings(t: gp.DataFrame):
    results = t.embedding().search(column="content", query="apple", top_k=1)
    assert len(list(results)) == 1
    row = next(iter(results))
    assert row["content"] == "I like eating apples."


@pytest.mark.requires_pgvector
def test_embedding_query_text(db: gp.Database):
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
    t = t.embedding().create_index(column="content", model_name="all-MiniLM-L6-v2")
    search_embeddings(t)

    # Ensure that a new DataFrame created from table in database can also be
    # searched.
    search_embeddings(db.create_dataframe("doc", schema="pg_temp"))


@pytest.mark.requires_pgvector
def test_embedding_multi_col_unique(db: gp.Database):
    content = ["I have a dog.", "I like eating apples."]
    columns = {"id": range(len(content)), "id2": [1] * len(content), "content": content}
    t = (
        db.create_dataframe(columns=columns)
        .save_as(
            temp=True,
            column_names=list(columns.keys()),
            distribution_key={"id"},
            distribution_type="hash",
            drop_if_exists=True,
            drop_cascade=True,
        )
        .check_unique(columns={"id", "id2"})
    )
    t = t.embedding().create_index(column="content", model_name="all-MiniLM-L6-v2")
    print(
        "reloptions =",
        db._execute(
            f"SELECT reloptions FROM pg_class WHERE oid = '{t._qualified_table_name}'::regclass"
        ),
    )
    search_embeddings(t)
