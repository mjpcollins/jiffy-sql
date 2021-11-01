import unittest
from setuptools import find_packages, setup


def test_suite_for_jiffy():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    return test_suite


if __name__ == '__main__':
    setup(
        name='jiffy-sql',
        packages=find_packages(include=['jiffysql', 'jiffysql.*']),
        version='0.0.5',
        description='Works out SQL file dependencies and runs them in a jiffy',
        author='Matt Collins',
        install_requires=['google-cloud-bigquery'],
        url='https://github.com/mjpcollins/jiffy-sql',
        test_suite='setup.test_suite_for_jiffy'
    )
