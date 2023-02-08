# -*- coding: utf-8 -*-
# 23/02/07
# create by: snower

import sys
import os
from setuptools import find_packages, setup

version = "0.0.1"

if os.path.exists("README.md"):
    if sys.version_info[0] >= 3:
        try:
            with open("README.md", encoding="utf-8") as fp:
                long_description = fp.read()
        except Exception as e:
            print("Waring: " + str(e))
            long_description = 'https://github.com/snower/syncany-sql'
    else:
        try:
            with open("README.md") as fp:
                long_description = fp.read()
        except Exception as e:
            print("Waring: " + str(e))
            long_description = 'https://github.com/snower/syncany-sql'
else:
    long_description = 'https://github.com/snower/syncany-sql'

setup(
    name='syncanysql',
    version=version,
    url='https://github.com/snower/syncany-sql',
    author='snower',
    author_email='sujian199@gmail.com',
    license='MIT',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        "pyyaml>=5.1.2",
        "sqlglot>=10.6.2"
        "syncany>=0.1.10",
        'Pygments>=2.14.0',
        'Pygments>=2.14.0',
        'prompt-toolkit>=3.0.36'
    ],
    package_data={
        '': ['README.md'],
    },
    entry_points={
        'console_scripts': [
            'syncany-sql = syncanysql.main:main',
        ],
    },
    description='简单易用的数据同步转换导出框架',
    long_description=long_description,
    long_description_content_type='text/markdown'
)
