from setuptools import setup, find_packages

setup(
    name="dnstun",
    version="0.0.1",
    description="The DNS tunneling detection tool.",
    packages=find_packages(),
    license="MIT",
    entry_points={
        "console_scripts": [
            "dnstun = dnstun.app.main:main"],
    },
)
