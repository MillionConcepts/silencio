from setuptools import setup, find_packages

# this is provided as a convenience for automated install
# hooks. we do not recommend using this file to install
# marslab or its dependencies. please use conda along with
# the provided environment.yml file.

setup(
    name="silencio",
    version="0.1.0",
    url="https://github.com/millionconcepts/silencio.git",
    author="Million Concepts",
    author_email="mstclair@millionconcepts.com",
    description="Occasional networking API interfaces.",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "dustgoggles", "pip", "pydrive2", "python-dateutil"
    ],
)
