# coding: utf-8

from setuptools import setup, find_packages

setup(
    name='epro_common',
    version='0.1',
    packages=find_packages(),
    install_requires = [
        'pika',
        'pymongo',
    ],
)
