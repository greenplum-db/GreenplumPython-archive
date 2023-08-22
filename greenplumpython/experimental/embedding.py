from collections import abc
from typing import Any, Callable, cast
from uuid import uuid4

import greenplumpython as gp
from greenplumpython.func import FunctionExpr
from greenplumpython.row import Row


@gp.create_function
def _generate_embedding(content: str, model_name: str) -> gp.type_("vector", modifier=384):  # type: ignore reportUnknownParameterType
    import sys

    import sentence_transformers.SentenceTransformer as SentenceTransformer  # type: ignore reportMissingImports

    SD = globals().get("SD") if globals().get("SD") is not None else sys.modules["plpy"]._SD
    if "model" not in SD:
        model = SentenceTransformer(model_name)  # type: ignore reportUnknownVariableType
        SD["model"] = model  # type: ignore reportOptionalSubscript
    else:
        model = SD["model"]  # type: ignore reportOptionalSubscript

    # Sentences are encoded by calling model.encode()
    emb = model.encode(content, normalize_embeddings=True)  # type: ignore reportUnknownVariableType
    return emb.tolist()  # type: ignore reportUnknownVariableType


class Embedding:
    """
    Embeddings provide a compact and meaningful representation of objects in a numerical vector space.
    They capture the semantic relationships between objects.

    This class enables users to search unstructured data based on semantic similarity and to leverage the power of
    the vector index scan.
    """

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

        Args:
            column: name of column to create index on.
            model: name of model to generate embedding.

        Returns:
            Dataframe with target column indexed based on embeddings.

        """

        assert self._dataframe.unique_key is not None, "Unique key is required to create index."

        embedding_col_name = "_emb_" + uuid4().hex
        embedding_df_cols = list(self._dataframe.unique_key) + [embedding_col_name]
        embedding_df: gp.DataFrame = (
            self._dataframe.assign(
                **{
                    embedding_col_name: cast(
                        Callable[[gp.DataFrame], FunctionExpr],
                        lambda t: _generate_embedding(t[column], model),  # type: ignore reportUnknownLambdaType
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
                    SELECT '{embedding_df._qualified_table_name}'::regclass::oid AS embedding_relid, attnum, '{model}' AS model
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
                FROM embedding_info
                WHERE oid = '{self._dataframe._qualified_table_name}'::regclass::oid;

                WITH embedding_info AS (
                    SELECT '{embedding_df._qualified_table_name}'::regclass::oid AS embedding_relid, attnum, '{model}' AS model
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
                    '{self._dataframe._qualified_table_name}'::regclass::oid AS refobjid,
                    embedding_info.attnum AS refobjsubid,
                    'n' AS deptype
                FROM embedding_info;
            END;
            $$;
            """,
            has_results=False,
        )
        return self._dataframe

    def search(self, column: str, query: Any, top_k: int) -> gp.DataFrame:
        """
        Searche unstructured data based on semantic similarity on embeddings.

        Args:
            column: name of column to search
            query: content to be searched
            top_k: number of most similar results requested

        Returns:
            Dataframe with the top k most similar results in the `column` of `query`.

        See :ref:`embedding-example` for more details.
        """
        assert self._dataframe._db is not None
        embdedding_info = self._dataframe._db._execute(
            f"""
            WITH indexed_col_info AS (
                SELECT attrelid, attnum
                FROM pg_attribute
                WHERE 
                    attrelid = '{self._dataframe._qualified_table_name}'::regclass::oid AND
                    attname = '{column}'
            ), reloptions AS (
                SELECT unnest(reloptions) AS option
                FROM pg_class, indexed_col_info
                WHERE pg_class.oid = attrelid
            ), embedding_info_json AS (
                SELECT split_part(option, '=', 2)::json AS val
                FROM reloptions, indexed_col_info
                WHERE option LIKE format('_pygp_emb_%s=%%', attnum)
            ), embedding_info AS (
                SELECT * 
                FROM embedding_info_json, json_to_record(val) AS (attnum int4, embedding_relid oid, model text)
            )
            SELECT nspname, relname, attname, model
            FROM embedding_info, pg_class, pg_namespace, pg_attribute
            WHERE 
                pg_class.oid = embedding_relid AND
                relnamespace = pg_namespace.oid AND
                embedding_relid = attrelid AND
                pg_attribute.attnum = 2;
            """
        )
        assert isinstance(embdedding_info, abc.Mapping[str, Any])
        row: Row = embdedding_info[0]
        schema: str = row["nspname"]
        embedding_table_name: str = row["relname"]
        model = row["model"]
        embedding_col_name = row["attname"]
        embedding_df = self._dataframe._db.create_dataframe(
            table_name=embedding_table_name, schema=schema
        )
        assert self._dataframe.unique_key is not None
        distance = gp.operator("<->")  # L2 distance is the default operator class in pgvector
        return self._dataframe.join(
            embedding_df.assign(
                distance=lambda t: distance(
                    embedding_df[embedding_col_name], _generate_embedding(query, model)
                )
            ).order_by("distance")[:top_k],
            how="inner",
            on=self._dataframe.unique_key,
            self_columns={"*"},
            other_columns={},
        )


def _embedding(dataframe: gp.DataFrame) -> Embedding:
    return Embedding(dataframe=dataframe)


setattr(gp.DataFrame, "embedding", _embedding)
