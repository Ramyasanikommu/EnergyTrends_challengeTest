from setuptools import find_packages, setup

NAME = "energy-trends"
DESCRIPTION = "Library to download energy trends source_data, clean and report stats"
REQUIRES_PYTHON = ">=3.7.0"

REQUIREMENTS = ["pandas", "beautifulsoup4", "requests"]
DEV_REQUIREMENTS = ["pytest", "pytest-cov"]

setup(
    name=NAME,
    description=DESCRIPTION,
    version='0.1.0',
    author='Ramya Sanikommu',
    python_requires=REQUIRES_PYTHON,
    install_requires=REQUIREMENTS,
    extras_require={"dev": DEV_REQUIREMENTS},
    packages=find_packages(exclude=["test.*"]),
    include_package_data=True,
    )
