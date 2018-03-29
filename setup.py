import os

from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip
from setuptools import setup


def requirements() -> list:
    pipfile = Project(chdir=False).parsed_pipfile
    return convert_deps_to_pip(pipfile['packages'], r=False)


def readme() -> str:
    with open('README.md') as f:
        return f.read()


def version() -> str:
    tag_version = os.getenv('CIRCLE_TAG')
    return tag_version if tag_version else "LOCAL"


setup(
    name="blurr",
    version=version(),
    description=
    "Data aggregation pipeline for running real-time predictive models",
    long_description=readme(),
    author="productml.com",
    author_email="info@productml.com",
    url="https://github.com/productml/blurr",
    packages=['blurr'],
    install_requires=requirements(),
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 1 - Planning",  # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        # license commented out while deciding the final license
        # "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3 :: Only",
    ],
    entry_points={'console_scripts': ['blurr = __main__:main']})
