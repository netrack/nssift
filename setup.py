from setuptools import setup, find_packages

setup(
    name="dnstun",
    version="0.0.1",
    description="The ditributed DNS tracer.",
    packages=find_packages(),
    license="MIT",
    install_requires=[
        "pyspark>=2.3.0",
        "matplotlib>=2.2.0",
    ],
    entry_points={
        "console_scripts": [
            "dnstun = dnstun.app.main:main"],
    },
)
