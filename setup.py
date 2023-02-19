

from setuptools import setup
from setuptools import find_packages

# setup the environment
setup(
    name="usajobs-api-ingesiton",
    version="0.1",
    description="USAJOBS Ingestion Pipeline",
    package=find_packages(),
    author="João Nisa",
    url="",
    include_package_data=True,
    zip_safe=False
)