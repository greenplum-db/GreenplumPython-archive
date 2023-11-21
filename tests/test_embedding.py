import _pytest
import pytest

import greenplumpython as gp
from tests import db as _


@pytest.mark.requires_pgvector
def test_embedding_query_string(db):
    content = ["I have a dog.", "I like eating apples."]
    t = (
        db.create_dataframe(columns={"id": range(len(content)), "content": content})
        .save_as(
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
    df = t.embedding().search(column="content", query="apple", top_k=1)
    assert len(list(df)) == 1
    for row in df:
        assert row["content"] == "I like eating apples."
