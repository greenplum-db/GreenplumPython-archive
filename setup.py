import setuptools

setuptools.setup(
    name="greenplum-python",
    install_requires=["psycopg2-binary==2.9.5", "dill==0.3.6"],
    packages=setuptools.find_packages(),
)
