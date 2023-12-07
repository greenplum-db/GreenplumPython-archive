from typing import Any, Callable, Literal, Optional, cast
from uuid import uuid4

import greenplumpython as gp
from greenplumpython.row import Row
from greenplumpython.type import TypeCast, _serialize_to_expr


@gp.create_function
def _record_dependency(
    base_table_oid: gp.type_("oid"), embedding_table_oid: gp.type_("oid")  # type: ignore reportUnknownParameterType
) -> None:
    import ctypes

    class ObjectAddress(ctypes.Structure):
        _fields_ = [
            ("classId", ctypes.c_uint32),
            ("objectId", ctypes.c_uint32),
            ("objectSubId", ctypes.c_uint32),
        ]

    postgres = ctypes.CDLL(None)
    recordDependencyOn = postgres["recordDependencyOn"]
    recordDependencyOn.argtypes = [
        ctypes.POINTER(ObjectAddress),
        ctypes.POINTER(ObjectAddress),
        ctypes.c_char,
    ]
    recordDependencyOn.restype = None

    RELATION_RELATION_ID = 1259
    base_table_addr = ObjectAddress(RELATION_RELATION_ID, base_table_oid, 0)
    embedding_table_addr = ObjectAddress(RELATION_RELATION_ID, embedding_table_oid, 0)

    DEPENDENCY_NORMAL = ctypes.c_char(ord("n"))
    recordDependencyOn(
        ctypes.byref(embedding_table_addr), ctypes.byref(base_table_addr), DEPENDENCY_NORMAL
    )


@gp.create_function
def create_embedding(content: str, model_name: str) -> gp.type_("vector"):  # type: ignore reportUnknownParameterType
    import sys

    import sentence_transformers  # type: ignore reportMissingImports

    sd_ = globals().get("SD")
    if sd_ is None:
        sd_ = sys.modules["plpy"]._SD
    if "model" not in sd_:
        import torch  # pyright: ignore [reportMissingImports, reportUnknownVariableType]

        # Limit the degree of parallelism, otherwise the task may not complete.
        # FIXME: This number should be set according to the resources available.
        torch.set_num_threads(4)
        model = sentence_transformers.SentenceTransformer(model_name)  # type: ignore reportUnknownVariableType
        sd_["model"] = model  # type: ignore reportOptionalSubscript
    else:
        model = sd_["model"]  # type: ignore reportOptionalSubscript

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

    def create_index(
        self,
        column: str,
        model_name: str,
        embedding_dimension: Optional[int] = None,
        method: Optional[Literal["ivfflat", "hnsw"]] = "hnsw",
    ) -> gp.DataFrame:
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
            model_name: name of model to generate embedding.
            embedding_dimension: dimension of the embedding.
            method: name of the index access method (i.e. index type) in `pgvector <https://github.com/pgvector/pgvector>`_.

        Returns:
            Dataframe with target column indexed based on embeddings.

        Example:
            Please refer to :ref:`tutorial-embedding` for more details.

        """

        assert self._dataframe.unique_key is not None, "Unique key is required to create index."
        if embedding_dimension is None:
            try:
                import sentence_transformers  # type: ignore reportMissingImports

                model = sentence_transformers.SentenceTransformer(model_name)  # type: ignore reportUnknownVariableType
                embedding_dimension: int = model[1].word_embedding_dimension  # From models.Pooling
            except:
                raise NotImplementedError(
                    "Model '{model_name}' doesn't provide embedding dimension information"
                )

        embedding_col_name = "_emb_" + uuid4().hex
        embedding_df_cols = list(self._dataframe.unique_key) + [embedding_col_name]
        embedding_df: gp.DataFrame = (
            self._dataframe.assign(
                **{
                    embedding_col_name: cast(
                        Callable[[gp.DataFrame], TypeCast],
                        # FIXME: Modifier must be adapted to all types of model.
                        # Can this be done with transformers.AutoConfig?
                        lambda t: gp.type_("vector", modifier=embedding_dimension)(create_embedding(t[column], model_name)),  # type: ignore reportUnknownLambdaType
                    )
                },
            )[embedding_df_cols]
            .save_as(
                column_names=embedding_df_cols,
                distribution_key=self._dataframe.unique_key,
                distribution_type="hash",
            )
            .check_unique(self._dataframe.unique_key)
        )
        if method is not None:
            assert method in ["ivfflat", "hnsw"]
            embedding_df = embedding_df.create_index(
                columns={embedding_col_name: "vector_l2_ops"}, method=method
            )
        assert self._dataframe._db is not None
        _record_dependency._create_in_db(self._dataframe._db)
        query_col_names = _serialize_to_expr(
            list(self._dataframe.unique_key) + [column], self._dataframe._db
        )
        sql_add_relationship = f"""
            DO $$
            BEGIN
                SET LOCAL allow_system_table_mods TO ON;
                
                WITH attnum_map AS (
                    SELECT attname, attnum FROM pg_attribute
                    WHERE 
                        attrelid = '{self._dataframe._qualified_table_name}'::regclass::oid AND
                        EXISTS (
                            SELECT FROM unnest({query_col_names}) AS query
                            WHERE attname = query
                        )
                ), embedding_info AS (
                    SELECT 
                        '{embedding_df._qualified_table_name}'::regclass::oid AS embedding_relid,
                        attnum AS content_attnum,
                        {len(self._dataframe._unique_key) + 1} AS embedding_attnum,
                        '{model_name}' AS model,
                        ARRAY(SELECT attnum FROM attnum_map WHERE attname != '{column}') AS unique_key
                    FROM attnum_map
                    WHERE attname = '{column}'
                )
                UPDATE pg_class
                SET reloptions = array_append(
                    reloptions,
                    format('_pygp_emb_%s=%s', content_attnum::text, to_json(embedding_info))
                )
                FROM embedding_info
                WHERE oid = '{self._dataframe._qualified_table_name}'::regclass::oid;

                PERFORM
                    {_record_dependency._qualified_name_str}(
                        '{self._dataframe._qualified_table_name}'::regclass::oid,
                        '{embedding_df._qualified_table_name}'::regclass::oid
                    );

                IF version() LIKE '%Greenplum%' THEN
                    PERFORM
                        {_record_dependency._qualified_name_str}(
                            '{self._dataframe._qualified_table_name}'::regclass::oid,
                            '{embedding_df._qualified_table_name}'::regclass::oid
                        )
                    FROM gp_dist_random('gp_id');
                END IF;
            END;
            $$;
            """
        self._dataframe._db._execute(sql_add_relationship, has_results=False)
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

        Example:
            Please refer to :ref:`tutorial-embedding` for more details.
        """
        assert self._dataframe._db is not None
        embdedding_info = self._dataframe._db._execute(
            f"""
            WITH indexed_col_info AS (
                SELECT attrelid, attnum AS content_attnum
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
                WHERE option LIKE format('_pygp_emb_%s=%%', content_attnum)
            ), embedding_info AS (
                SELECT * 
                FROM embedding_info_json, json_to_record(val) AS (
                    embedding_attnum int4, embedding_relid oid, model text, unique_key int[]
                )
            ), unique_key_names AS (
                SELECT ARRAY(
                    SELECT attname FROM pg_attribute
                    WHERE attrelid = embedding_relid AND attnum = ANY(unique_key)
                ) AS val
                FROM embedding_info
            )
            SELECT nspname, relname, attname, model, unique_key_names.val AS unique_key
            FROM embedding_info, pg_class, pg_namespace, pg_attribute, unique_key_names
            WHERE 
                pg_class.oid = embedding_relid AND
                relnamespace = pg_namespace.oid AND
                embedding_relid = attrelid AND
                embedding_attnum = attnum;
            """
        )
        row: Row = embdedding_info[0]  # type: ignore reportUnknownVariableType
        schema: str = row["nspname"]  # type: ignore reportUnknownVariableType
        embedding_table_name: str = row["relname"]  # type: ignore reportUnknownVariableType
        model = row["model"]  # type: ignore reportUnknownVariableType
        embedding_col_name = row["attname"]  # type: ignore reportUnknownVariableType
        embedding_df = self._dataframe._db.create_dataframe(
            table_name=embedding_table_name, schema=schema  # type: ignore reportUnknownArgumentType
        )
        unique_key: list[str] = row["unique_key"]  # type: ignore reportUnknownVariableType
        assert embedding_df is not None
        distance = gp.operator("<->")  # L2 distance is the default operator class in pgvector
        return self._dataframe.join(
            embedding_df.assign(
                distance=lambda t: distance(t[embedding_col_name], create_embedding(query, model))
            ).order_by("distance")[:top_k],
            how="inner",
            on=unique_key,  # type: ignore reportUnknownArgumentType
            self_columns={"*"},
            other_columns={},
        )


def _embedding(dataframe: gp.DataFrame) -> Embedding:
    return Embedding(dataframe=dataframe)


setattr(gp.DataFrame, "embedding", _embedding)
