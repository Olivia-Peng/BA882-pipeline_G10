from setuptools import setup

setup(
    name='sarima-training',
    version='1.0',
    py_modules=['main'],
    install_requires=[
        'pandas==1.3.5',
        'google-cloud-bigquery==3.11.4',
        'statsmodels==0.13.5',
        'functions-framework==3.1.0',
        'db-dtypes==1.3.0',
        'protobuf==3.19.6',
        'pandas-gbq==0.17.9',
    ],
)
