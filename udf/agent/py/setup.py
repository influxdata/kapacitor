from setuptools import setup
from os import path

import sys
setup_dir = path.dirname(__file__)
sys.path.insert(0, setup_dir)

from kapacitor.udf.agent import VERSION

setup(name='kapacitor_udf_agent',
    version=VERSION,
    packages=[
        'kapacitor',
        'kapacitor.udf',
        'kapacitor.udf.agent',
    ],
)
