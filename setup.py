from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="floorer",
    version="0.1.6",
    author="Rodrigo Vera",
    author_email="rvabrilot@gmail.com",
    description="A small library to manage pandas DataFrames in Parquet files using PyArrow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rvabrilot/floorer",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'pyarrow',
    ],
)
