import setuptools

setuptools.setup(
    name="greenplum-python",
    # TODO: Maybe remove cloudpickle and add fallbacks.
    install_requires=["psycopg2==2.9.3", "cloudpickle==2.2.1"],
    packages=setuptools.find_packages(),
)
