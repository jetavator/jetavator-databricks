# -*- coding: utf-8 -*-

import io
import os

from setuptools import setup, find_packages

# Package metadata
# ----------------

SHORT_NAME = 'azure_storage'
NAME = 'jetavator_azure_storage'
DESCRIPTION = (
    'Azure storage support for the Jetavator engine'
)
URL = 'https://github.com/jetavator/jetavator'
EMAIL = 'joetaylorconsulting@gmail.com'
AUTHOR = 'Joe Taylor'
REQUIRES_PYTHON = '>=3.7.0'
VERSION = None

# What packages are required for this module to be executed?
REQUIRED = [
    'jetavator>=0.1.5',
    'lazy-property>=0.0.1,<1',
    'azure-storage-blob>=12.1.0,<13',
    'azure-storage-queue>=12.1.0,<13',
]

# What packages are optional?
EXTRAS = {
    # 'some-feature': ['requirement'],
}

# Package setup
# -------------

# Import the README and use it as the long description

here = os.path.abspath(os.path.dirname(__file__))

try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Import the LICENSE

with open(os.path.join(here, 'LICENSE')) as f:
    license_text = f.read()

# Load the package's __version__.py module as a dictionary

about = {}
if not VERSION:
    with open(os.path.join(here, NAME, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION

setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    license=license_text,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7'
    ],
    entry_points={'jetavator.plugins': f'{SHORT_NAME} = {NAME}'}
)
