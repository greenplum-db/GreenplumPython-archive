import base64
import inspect
import io
import pathlib
import tarfile
import uuid
from typing import get_type_hints

import psycopg2

import greenplumpython as gp
from greenplumpython.func import NormalFunction

_CHUNK_SIZE = 256 * 1024 * 1024  # Must be much < 1 GB


@gp.create_function
def _dump_file_chunk(tmp_archive_name: str, chunk_base64: str) -> int:
    tmp_archive_base = pathlib.Path("/") / "tmp" / tmp_archive_name
    tmp_archive_path = tmp_archive_base / f"{tmp_archive_name}.tar.gz"
    with open(tmp_archive_path, "ab") as tmp_archive:
        tmp_archive.write(base64.b64decode(chunk_base64))
    return 0


@gp.create_function
def _extract_files(tmp_archive_name: str) -> list[str]:
    tmp_archive_base = pathlib.Path("/") / "tmp" / tmp_archive_name
    tmp_archive_path = tmp_archive_base / f"{tmp_archive_name}.tar.gz"
    extracted_dir = tmp_archive_base / "extracted"
    if not extracted_dir.exists():
        with tarfile.open(tmp_archive_path, "r:gz") as tmp_archive:
            extracted_dir.mkdir()
            tmp_archive.extractall(str(extracted_dir))
        tmp_archive_path.unlink()
    for path in extracted_dir.rglob("*"):
        if path.is_file() and not path.is_symlink():
            yield str(path.resolve())


@classmethod
def _from_files(_, files: list[str], parser: NormalFunction, db: gp.Database) -> gp.DataFrame:
    tmp_archive_name = f"tar_{uuid.uuid4().hex}"
    tmp_archive_base = pathlib.Path("/") / "tmp" / tmp_archive_name
    tmp_archive_base.mkdir()
    tmp_archive_path = tmp_archive_base / f"{tmp_archive_name}.tar.gz"
    with tarfile.open(tmp_archive_path, "w:gz") as tmp_archive:
        for file_path in files:
            tmp_archive.add(pathlib.Path(file_path))
    with psycopg2.connect(db._conn.dsn, options="-c gp_session_role=utility") as util_conn:
        with util_conn.cursor() as cursor:
            cursor.execute(f"CREATE TEMP TABLE {tmp_archive_name} (id serial, chunk_base64 text);")
            with open(tmp_archive_path, "rb") as tmp_archive:
                while True:
                    chunk = tmp_archive.read(_CHUNK_SIZE)
                    if len(chunk) == 0:
                        break
                    chunk_base64 = base64.b64encode(chunk)
                    cursor.copy_expert(
                        f"COPY {tmp_archive_name} (chunk_base64) FROM STDIN",
                        io.BytesIO(chunk_base64),
                    )
            util_conn.commit()
            cursor.execute(_dump_file_chunk._serialize(db))  # type: ignore reportUnknownArgumentType
            cursor.execute(
                f"""
                SELECT {_dump_file_chunk._qualified_name_str}('{tmp_archive_name}', chunk_base64)
                FROM "{tmp_archive_name}"
                ORDER BY id;
                """
            )
    func_sig = inspect.signature(parser.unwrap())
    result_members = get_type_hints(func_sig.return_annotation)
    return db.apply(
        lambda: parser(_extract_files(tmp_archive_name)), expand=len(result_members) == 0
    )


setattr(gp.DataFrame, "from_files", _from_files)
