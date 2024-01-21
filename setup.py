from setuptools import setup, find_packages

setup(
    name='duckdb-query-runner_component',
    version='1.0.0',
    packages=find_packages(),
    url='',
    license='',
    author='',
    author_email='',
    description='A Kubeflow/Argo component for Running DuckDB queries',
    install_requires=[
        "kfp~=1.8.12"
    ]
)