import setuptools

setuptools.setup(
    name="greenplum-python",
    install_requires=["psycopg2==2.9.3"],
    packages=setuptools.find_packages(),
)
