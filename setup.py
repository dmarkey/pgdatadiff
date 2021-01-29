from setuptools import setup

setup(
    name='pgdatadiff',
    packages=['pgdatadiff'],
    url='https://github.com/dmarkey/pgdatadiff',
    python_requires='>3.6.0',
    license='MIT',
    author='dmarkey',
    author_email='david@dmarkey.com',
    description='A small tool to diff the *data* in 2 postgresql databases',
    entry_points={
        'console_scripts': ['pgdatadiff=pgdatadiff.main:main'],
    },
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    install_requires=[
        'SQLAlchemy<=1.3.11',
        'halo<=0.0.28',
        'psycopg2-binary<=2.8.4',
        'fabulous<=0.3.0',
        'docopt<=0.6.2'
    ],
)
