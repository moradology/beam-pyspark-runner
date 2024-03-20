from setuptools import setup, find_packages

setup(
    name='beam_pyspark_runner',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        "apache-beam==2.53.0",
        "pyspark==3.5.1"
    ],
)