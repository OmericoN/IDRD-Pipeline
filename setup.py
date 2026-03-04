from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = [
        line.strip() for line in f
        if line.strip() and not line.startswith("#")
    ]
setup(
    name="idrd_pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    install_requires=requirements,
    package_dir={"": "src"},
)
