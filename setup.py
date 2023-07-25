from setuptools import find_packages, setup

# read the contents of the README file
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='promg',
    version='0.1.0a1',
    description='Pyhton library to build Event Knowledge Graphs',
    author='A. Swevels, D.Fahland',
    python_requires='>=3.7',
    install_requires=['neo4j', 'numpy', 'pandas', 'tabulate', 'tqdm'],
    license='GPL 3.0',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(
        where='src',
        include=['promg*']),
    package_dir={"": "src"}
)
