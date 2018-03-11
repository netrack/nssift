import nssift
import setuptools


setuptools.setup(
    name="nssift",
    version=nssift.__version__,
    description="The distributed DNS traffic scatterer.",
    packages=setuptools.find_packages(),
    license="MIT",
    install_requires=[
        "pyspark>=2.3.0",
        "matplotlib>=2.2.0",
    ],
    entry_points={
        "console_scripts": [
            "nssift = nssift.main:main"
        ],
    },
)
