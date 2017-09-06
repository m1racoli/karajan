from setuptools import setup

setup(
    name='karajan',
    version='0.3.12',
    url='https://github.com/wooga/bit.karajan',
    license='',
    packages=['karajan', 'karajan.bin'],
    author='Wooga Business Intelligence Team',
    author_email='bit-admin@wooga.com',
    description='A conductor of aggregations in Apache Airflow',
    scripts=['karajan/bin/karajan'],
    install_requires=[
        "pyyaml",
        "apache-airflow[jdbc]~=1.8",
    ],
    extras_require={
        'dev': [
            'behave',
            'nose',
            'nose-timer',
            'rednose',
            'coverage',
            'parameterized',
            'mock',
        ],
    },
)
