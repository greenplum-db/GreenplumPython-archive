import setuptools
import versioneer 

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="greenplum-python", # Replace with your own username
    version=versioneer.get_version(),
    author="Greenplum Python",
    author_email="greenplum-python@vmware.com",
    description="Python interface for Greenplum",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/greenplum-db/GreenplumPython",
    packages=setuptools.find_packages(include=["greenplumpython", "greenplumpython.*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
