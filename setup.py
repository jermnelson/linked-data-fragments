__author__ = "Jeremy Nelson"

from setuptools import setup, find_packages

setup(
    name="ldf_server",
    version="0.0.1",
    author="Jeremy Nelson",
    license="GNU AFFERO",
    packages=find_packages(exclude=['tests'])
)
