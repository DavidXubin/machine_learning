import sys

#from distutils.core import setup
#from setuptools import find_packages
from setuptools import setup, find_packages

if any(arg.startswith('bdist') for arg in sys.argv):
    import setuptools

version_ns = {}
version_ns['__version__'] = 0.9

setup(
    name='ml_algo_libs',
    version=version_ns['__version__'],
    description='Some tools or libraries for ML project',
    author='xubin xu',
    author_email='xxb1114@hotmail.com',
    license='3 Clause BSD',
    packages=find_packages(exclude=['*.pyc']),
    zip_safe = False,
    platforms = "Independant",
    include_package_data=True,
    install_requires=[
                      'redis',
                      'numpy'
                     ],
)

