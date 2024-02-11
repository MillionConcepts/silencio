from setuptools import setup, find_packages

# this is provided as a convenience for automated install
# hooks. we do not recommend using this file to install
# silencio or its dependencies. please use conda along with
# the provided environment.yml file.

setup(
    name="silencio",
    version="0.5.0",
    url="https://github.com/millionconcepts/silencio.git",
    author="Million Concepts",
    author_email="mstclair@millionconcepts.com",
    description="Google Drive interfaces.",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "dustgoggles", "pip", "python-dateutil"
    ],
)
