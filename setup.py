import subprocess from setuptools import setup, find_packages, Extension

setup(
    name='sqs_fdw',
    version='0.0.1',
    author='Daniel Caldeweyher',
    author_email='dcalde@gmail.com',
    license='MIT',
    packages=['sqs_fdw'],
    install_requires=["boto3"],
)