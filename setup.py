import os

from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip
from setuptools import setup, find_packages
'''
This setup.py script is expected to perform development and final releases using 
CircleCI (https://circleci.com/gh/productml/blurr).

 - Release Builds are created from tags, using CIRCLE_TAG environment variable
 - Development builds create a different artifact, 'blurr-dev', that can be installed
   using 'pip install blurr-dev'
'''


def is_ci_build():
    return True if os.getenv('CIRCLECI') else False


def is_release():
    if not is_ci_build():
        return False
    return True if os.getenv('CIRCLE_TAG') else False


def requirements():
    pipfile = Project(chdir=False).parsed_pipfile
    return convert_deps_to_pip(pipfile['packages'], r=False)


def readme():
    with open('README.md', 'r', encoding='utf-8') as f:
        return f.read()


def name():
    return "blurr" if is_release() else "blurr-dev"


def version():
    if not is_ci_build():
        return "LOCAL"
    elif is_release():
        return os.getenv('CIRCLE_TAG')
    else:
        return "0." + os.getenv('CIRCLE_BUILD_NUM')


# emit a VERSION file the CLI can use to check current version
version_file = open("blurr/VERSION", "w")
version_file.write(version())
version_file.close()

setup(
    name=name(),
    version=version(),
    description="Data aggregation pipeline for running real-time predictive models",
    long_description=readme(),
    long_description_content_type='text/markdown',
    author="productml.com",
    author_email="info@productml.com",
    url="https://github.com/productml/blurr",
    packages=find_packages(),
    data_files=[
        "blurr/core/syntax/dtc_window_schema.yml", "blurr/core/syntax/dtc_streaming_schema.yml",
        "blurr/VERSION"
    ],
    include_package_data=True,
    install_requires=requirements(),
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 1 - Planning",  # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3 :: Only",
    ],
    entry_points={'console_scripts': ['blurr = blurr.__main__:main']})
