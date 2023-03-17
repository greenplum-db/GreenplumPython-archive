# read the contents of your README file
from pathlib import Path

import setuptools

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setuptools.setup(
    name="greenplum-python",
    install_requires=["psycopg2-binary==2.9.5", "dill==0.3.6"],
    packages=setuptools.find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
)
