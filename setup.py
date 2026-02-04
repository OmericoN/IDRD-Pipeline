from setuptools import setup, find_packages

setup(
    name="idrd_pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)
