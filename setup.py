from setuptools import setup, find_packages

setup(
    name='pypitest',
    packages=find_packages(exclude=["*.tests", "*.tests.*"]),
    include_package_data=True,
    version=0.1,
    description='The shared resources project',
    url='https://github.com/vasu4132/pypitest.git',
    author='vasu',
    author_email='vasuchowdary413@gmail.com',
)
