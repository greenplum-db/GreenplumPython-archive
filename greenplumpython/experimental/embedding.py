from collections import abc
from typing import Any
from uuid import uuid4

import greenplumpython as gp

_vector_type = gp.type_("vector", modifier=384)


@gp.create_function
def _generate_embedding(content: str, model_name: str) -> _vector_type:
    import sys

    SD = globals().get("SD", sys.modules["plpy"]._SD)
    if "model" not in SD:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(model_name)
        SD["model"] = model
    else:
        model = SD["model"]

    # Sentences are encoded by calling model.encode()
    emb = model.encode(content, normalize_embeddings=True)
    return emb.tolist()


class Embedding:
    def __init__(self, dataframe: gp.DataFrame) -> None:
        self._dataframe = dataframe

    def create_index(self, column: str, model: str) -> gp.DataFrame:
        """
        Generate embeddings and create index for a column of unstructured data.
        This include
        - texts,
        - images, or
        - videos, etc.

        This enables searching unstructured data based on semantic similarity,
        That is, whether they mean or contain similar things.

        For better efficiency, the generated embeddings is stored in a
        column-oriented approach, i.e., separated from the input DataFrame. The
        input DataFrame must have a **unique key** to identify the tuples in the
        search results.
        """

        assert self._dataframe.unique_key is not None, "Unique key is required to create index."

        embedding_col_name = "_emb_" + uuid4().hex
        embedding_df_cols = list(self._dataframe.unique_key) + [embedding_col_name]
        embedding_df: gp.DataFrame = (
            self._dataframe.assign(
                **{
                    embedding_col_name: lambda t: _vector_type(
                        _generate_embedding(t[column], model)
                    )
                },
            )[embedding_df_cols]
            .save_as(
                column_names=embedding_df_cols,
                distribution_key=self._dataframe.unique_key,
                distribution_type="hash",
            )
            .check_unique(self._dataframe.unique_key)
            .create_index(columns={embedding_col_name}, method="ivfflat")
        )
        assert self._dataframe._db is not None
        self._dataframe._db._execute(
            f"""
            DO $$
            BEGIN
                SET LOCAL allow_system_table_mods TO ON;

                WITH embedding_info AS (
                    SELECT attrelid, attnum, '{model}' AS model
                    FROM pg_attribute
                    WHERE 
                        attrelid = '{self._dataframe._qualified_table_name}'::regclass::oid AND
                        attname = '{column}'
                )
                UPDATE pg_class
                SET reloptions = array_append(
                    reloptions, 
                    format('_pygp_emb_%s=%s', attnum::text, to_json(embedding_info))
                )
                FROM embedding_info;

                WITH embedding_info AS (
                    SELECT attrelid, attnum, '{model}' AS model
                    FROM pg_attribute
                    WHERE 
                        attrelid = '{self._dataframe._qualified_table_name}'::regclass::oid AND
                        attname = '{column}'
                )
                INSERT INTO pg_depend
                SELECT
                    'pg_class'::regclass::oid AS classid,
                    '{embedding_df._qualified_table_name}'::regclass::oid AS objid,
                    0::oid AS objsubid,
                    'pg_class'::regclass::oid AS refclassid,
                    embedding_info.attrelid AS refobjid,
                    embedding_info.attnum AS refobjsubid,
                    'n' AS deptype
                FROM embedding_info;
            END;
            $$;
            """,
            has_results=False
        )
        return self._dataframe

    def search(self, column: str, query: Any, top_k: int) -> gp.DataFrame:
        assert self._dataframe._db is not None
        embdedding_info = self._dataframe._db._execute(
            f"""
            WITH embedding_oid AS (
                SELECT attrelid, attnum
                FROM pg_attribute
                WHERE 
                    attrelid = '{self._dataframe._qualified_table_name}'::regclass::oid AND
                    attname = '{column}'
            ), reloptions AS (
                SELECT unnest(reloptions) AS option
                FROM pg_class, embedding_oid
                WHERE pg_class.oid = attrelid
            ), embedding_info_json AS (
                SELECT split_part(option, '=', 2)::json AS val
                FROM reloptions, embedding_oid
                WHERE option LIKE format('_pygp_emb_%s=%%', attnum)
            ), embedding_info AS (
                SELECT * 
                FROM embedding_info_json, json_to_record(val) AS (attnum int4, attrelid oid, model text)
            )
            SELECT nspname, relname, model
            FROM embedding_info, pg_class, pg_namespace
            WHERE 
                pg_class.oid = attrelid AND
                relnamespace = pg_namespace.oid;
            """
        )
        # assert isinstance(embdedding_info, abc.Mapping)
        embedding_table_name = None
        for row in embdedding_info:
            embedding_table_name = f'"{row["nspname"]}"."{row["relname"]}"'
            model = row["model"]
            break
        assert embedding_table_name is not None
        embedding_df = self._dataframe._db.create_dataframe(embedding_table_name)
        assert self._dataframe.unique_key is not None
        distance = gp.operator("<#>")
        return self._dataframe.join(
            embedding_df.assign(
                distance=lambda t: distance(t["_emb_"], _generate_embedding(query, model))
            ).order_by("distance")[:top_k],
            how="inner",
            on=self._dataframe.unique_key,
            self_columns={"*"},
            other_columns={},
        )


def _embedding(dataframe: gp.DataFrame) -> Embedding:
    return Embedding(dataframe=dataframe)


setattr(gp.DataFrame, "embedding", _embedding)
