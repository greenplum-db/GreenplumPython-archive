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
    tmp_archive_base.mkdir(parents=True, exist_ok=True)
    tmp_archive_path = tmp_archive_base / f"{tmp_archive_name}.tar.gz"
    with open(tmp_archive_path, "ab") as tmp_archive:
        tmp_archive.write(base64.b64decode(chunk_base64))
    return 0


@gp.create_function
def _extract_files(tmp_archive_name: str, returning: str) -> list[str]:
    tmp_archive_base = pathlib.Path("/") / "tmp" / tmp_archive_name
    tmp_archive_path = tmp_archive_base / f"{tmp_archive_name}.tar.gz"
    extracted_root = tmp_archive_base / "extracted"
    if not extracted_root.exists():
        with tarfile.open(tmp_archive_path, "r:gz") as tmp_archive:
            extracted_root.mkdir()
            tmp_archive.extractall(str(extracted_root))
        tmp_archive_path.unlink()
    if returning == "root":
        yield str(extracted_root)
    else:
        assert returning == "files"
        for path in extracted_root.rglob("*"):
            if path.is_file() and not path.is_symlink():
                yield str(path)


def _archive_and_upload(tmp_archive_name: str, files: list[str], db: gp.Database):
    tmp_archive_base = pathlib.Path("/") / "tmp" / tmp_archive_name
    tmp_archive_base.mkdir(exist_ok=True)
    tmp_archive_path = tmp_archive_base / f"{tmp_archive_name}.tar.gz"
    with tarfile.open(tmp_archive_path, "w:gz") as tmp_archive:
        for file_path in files:
            tmp_archive.add(pathlib.Path(file_path))
    server_options = "-c gp_session_role=utility" if db._is_variant("greenplum") else None
    with psycopg2.connect(db._dsn, options=server_options) as util_conn:  # type: ignore reportUnknownVariableType
        with util_conn.cursor() as cursor:  # type: ignore reportUnknownVariableType
            cursor.execute(f"CREATE TEMP TABLE {tmp_archive_name} (id serial, text_base64 text);")
            with open(tmp_archive_path, "rb") as tmp_archive:
                while True:
                    chunk = tmp_archive.read(_CHUNK_SIZE)
                    if len(chunk) == 0:
                        break
                    chunk_base64 = base64.b64encode(chunk)
                    cursor.copy_expert(
                        f"COPY {tmp_archive_name} (text_base64) FROM STDIN",
                        io.BytesIO(chunk_base64),
                    )
            util_conn.commit()
            cursor.execute(_dump_file_chunk._serialize(db))  # type: ignore reportUnknownArgumentType
            cursor.execute(
                f"""
                SELECT {_dump_file_chunk._qualified_name_str}('{tmp_archive_name}', text_base64)
                FROM "{tmp_archive_name}"
                ORDER BY id;
                """
            )


@classmethod
def _from_files(_, files: list[str], parser: NormalFunction, db: gp.Database) -> gp.DataFrame:
    tmp_archive_name = f"tar_{uuid.uuid4().hex}"
    _archive_and_upload(tmp_archive_name, files, db)
    return db.apply(
        lambda: parser(_extract_files(tmp_archive_name, "files")),
        expand=True,
    )


setattr(gp.DataFrame, "from_files", _from_files)


import subprocess as sp
import sys


@gp.create_function
def _install_on_server(pkg_dir: str, requirements: str) -> str:
    import subprocess as sp
    import sys

    assert sys.executable, "Python executable is required to install packages."
    try:
        exec_version = sp.check_output([sys.executable, "--version"], text=True, stderr=sp.STDOUT)
    except sp.CalledProcessError as e:
        raise Exception(e.stdout)

    lib_version = f"Python {sys.version_info.major}.{sys.version_info.minor}."
    assert exec_version.startswith(
        lib_version
    ), f"Python major and minor versions mismatch (executable {exec_version}, library {lib_version})"
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-index",
        "--requirement",
        "/dev/stdin",
        "--find-links",
        pkg_dir,
    ]
    try:
        output = sp.check_output(cmd, text=True, stderr=sp.STDOUT, input=requirements)
        return output
    except sp.CalledProcessError as e:
        raise Exception(e.stdout)


def _install_packages(db: gp.Database, requirements: str):
    tmp_archive_name = f"tar_{uuid.uuid4().hex}"
    # FIXME: Windows client is not supported yet.
    local_dir = pathlib.Path("/") / "tmp" / tmp_archive_name / "pip"
    local_dir.mkdir(parents=True)
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "download",
        "--requirement",
        "/dev/stdin",
        "--dest",
        local_dir,
    ]
    try:
        sp.check_output(cmd, text=True, stderr=sp.STDOUT, input=requirements)
    except sp.CalledProcessError as e:
        raise e from Exception(e.stdout)
    _archive_and_upload(tmp_archive_name, [local_dir], db)
    extracted = db.apply(lambda: _extract_files(tmp_archive_name, "root"), column_name="cache_dir")
    assert len(list(extracted)) == 1
    server_dir = (
        pathlib.Path("/")
        / "tmp"
        / tmp_archive_name
        / "extracted"
        / local_dir.relative_to(local_dir.root)
    )
    installed = extracted.apply(
        lambda _: _install_on_server(server_dir.as_uri(), requirements), column_name="result"
    )
    assert len(list(installed)) == 1


setattr(gp.Database, "install_packages", _install_packages)
