import setuptools

setuptools.setup(
    name="greenplum-python",
    install_requires=["psycopg2==2.9.3", "dill==0.3.6"],
    packages=setuptools.find_packages(),
)
