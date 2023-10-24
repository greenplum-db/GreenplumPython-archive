import base64
import inspect
import io
import pathlib
import sys
import tarfile
import tempfile
import uuid
from typing import Any, get_type_hints

import psycopg2
import psycopg2.extensions

import greenplumpython as gp
from greenplumpython.func import NormalFunction

_CHUNK_SIZE = 256 * 1024 * 1024  # Must be much < 1 GB


@gp.create_function
def _dump_file_chunk(tmp_dir_handle: str, chunk_base64: str) -> str:
    try:
        _gd = globals()["GD"]  # type: ignore reportUnknownVariableType
    except KeyError:
        _gd = sys.modules["plpy"]._GD
    if tmp_dir_handle not in _gd:
        server_tmp_dir = tempfile.TemporaryDirectory(prefix="pygp.srv.")
        _gd[tmp_dir_handle] = server_tmp_dir  # Pin to GD for later UDFs
    else:
        server_tmp_dir = _gd[tmp_dir_handle]  # type: ignore reportUnknownVariableType

    server_tmp_dir_path: pathlib.Path = pathlib.Path(server_tmp_dir.name)  # type: ignore reportUnknownVariableType
    server_tmp_dir_path.mkdir(parents=True, exist_ok=True)
    tmp_archive_path = server_tmp_dir_path / f"{tmp_dir_handle}.tar.gz"
    with open(tmp_archive_path, "ab") as tmp_archive:
        tmp_archive.write(base64.b64decode(chunk_base64))
    return server_tmp_dir.name


@gp.create_function
def _extract_files(server_tmp_dir: str, tmp_dir_handle: str, returning: str) -> list[str]:
    server_tmp_dir_path: pathlib.Path = pathlib.Path(server_tmp_dir)
    tmp_archive_path = server_tmp_dir_path / f"{tmp_dir_handle}.tar.gz"
    extracted_root = server_tmp_dir_path / "extracted"
    if not extracted_root.exists():
        with tarfile.open(tmp_archive_path, "r:gz") as tmp_archive:
            extracted_root.mkdir()
            tmp_archive.extractall(str(extracted_root))
        tmp_archive_path.unlink()
    if returning == "root":
        yield str(extracted_root.resolve())
    else:
        assert returning == "files"
        for path in extracted_root.rglob("*"):
            if path.is_file() and not path.is_symlink():
                yield str(path.resolve())


def _remove_tmp_dir(conn: psycopg2.extensions.connection, db: gp.Database, tmp_dir_handle: str):
    @gp.create_function
    def udf(tmp_dir_handle: str) -> None:
        try:
            _gd = globals()["GD"]  # type: ignore reportUnknownVariableType
        except KeyError:
            _gd = sys.modules["plpy"]._GD
        _gd[tmp_dir_handle].cleanup()

    with conn.cursor() as cursor:
        cursor.execute(udf._serialize(db))
        cursor.execute(f"SELECT {udf._qualified_name_str}('{tmp_dir_handle}');")


def _archive_and_upload(
    util_conn: psycopg2.extensions.connection,
    tmp_dir_handle: str,
    files: list[str],
    db: gp.Database,
) -> str:
    with tempfile.TemporaryDirectory(prefix="pygp.cln.") as local_tmp_dir:
        local_tmp_dir_path: pathlib.Path = pathlib.Path(local_tmp_dir)
        tmp_archive_path = local_tmp_dir_path / f"{tmp_dir_handle}.tar.gz"
        with tarfile.open(tmp_archive_path, "w:gz") as tmp_archive:
            for file_path in files:
                tmp_archive.add(pathlib.Path(file_path))
        with util_conn.cursor() as cursor:
            cursor.execute(f"CREATE TEMP TABLE {tmp_dir_handle} (id serial, text_base64 text);")
            with open(tmp_archive_path, "rb") as tmp_archive:
                while True:
                    chunk = tmp_archive.read(_CHUNK_SIZE)
                    if len(chunk) == 0:
                        break
                    chunk_base64 = base64.b64encode(chunk)
                    cursor.copy_expert(
                        f"COPY {tmp_dir_handle} (text_base64) FROM STDIN",
                        io.BytesIO(chunk_base64),
                    )
            util_conn.commit()
            cursor.execute(_dump_file_chunk._serialize(db))
            cursor.execute(
                f"""
                SELECT {_dump_file_chunk._qualified_name_str}('{tmp_dir_handle}', text_base64)
                FROM "{tmp_dir_handle}"
                ORDER BY id;
                """
            )
            return cursor.fetchall()[0][0]


@classmethod
def _from_files(_, files: list[str], parser: NormalFunction, db: gp.Database) -> gp.DataFrame:
    tmp_dir_handle = f"__pygp_tar_{uuid.uuid4().hex}"
    server_options = "-c gp_session_role=utility" if db._is_variant("greenplum") else None
    with psycopg2.connect(db._dsn, options=server_options) as util_conn:  # type: ignore reportUnknownVariableType
        server_tmp_dir = _archive_and_upload(util_conn, tmp_dir_handle, files, db)  # type: ignore reportUnknownArgumentType
        func_sig = inspect.signature(parser.unwrap())
        result_members = get_type_hints(func_sig.return_annotation)
        df = db.apply(
            lambda: parser(_extract_files(server_tmp_dir, tmp_dir_handle, "files")),
            expand=len(result_members) == 0,
        )
        # _remove_tmp_dir(util_conn, db, tmp_dir_handle)  # Cannot remove now since the returning DataFrame depends on it.
        return df


setattr(gp.DataFrame, "from_files", _from_files)

import subprocess


@gp.create_function
def _install_on_server(server_tmp_dir: str, local_tmp_dir: str, requirements: str) -> str:
    assert sys.executable, "Python executable is required to install packages."
    server_tmp_dir_path: pathlib.Path = pathlib.Path(server_tmp_dir)
    local_tmp_dir_path = pathlib.Path(local_tmp_dir)
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-index",
        "--requirement",
        "/dev/stdin",
        "--find-links",
        str(
            server_tmp_dir_path
            / "extracted"
            / local_tmp_dir_path.relative_to(local_tmp_dir_path.root)
        ),
    ]
    try:
        output = subprocess.check_output(
            cmd, text=True, stderr=subprocess.STDOUT, input=requirements
        )
        return output
    except subprocess.CalledProcessError as e:
        raise Exception(e.stdout)


def _install_packages(db: gp.Database, requirements: str):
    tmp_dir_handle = f"__pygp_tar_{uuid.uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix="pygp.cln.") as local_pkg_dir:
        local_tmp_dir_path = pathlib.Path(local_pkg_dir)
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--requirement",
            "/dev/stdin",
            "--dest",
            str(local_tmp_dir_path),
        ]
        try:
            subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, input=requirements)
        except subprocess.CalledProcessError as e:
            raise e from Exception(e.stdout)
        server_options = "-c gp_session_role=utility" if db._is_variant("greenplum") else None
        with psycopg2.connect(db._dsn, options=server_options) as util_conn:  # type: ignore reportUnknownVariableType
            server_tmp_dir = _archive_and_upload(util_conn, tmp_dir_handle, [local_pkg_dir], db)  # type: ignore reportUnknownArgumentType
            extracted = db.apply(lambda: _extract_files(server_tmp_dir, tmp_dir_handle, "root"))
            assert len(list(extracted)) == 1
            installed = extracted.apply(
                lambda _: _install_on_server(server_tmp_dir, local_pkg_dir, requirements)
            )
            assert len(list(installed)) == 1
            _remove_tmp_dir(util_conn, db, tmp_dir_handle)  # type: ignore reportUnknownArgumentType


setattr(gp.Database, "install_packages", _install_packages)
