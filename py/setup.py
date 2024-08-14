from setuptools import setup, find_packages

setup(
    name='lsm_tree_project',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'bintrees',
        'pytest',
    ],
    entry_points={
        'console_scripts': [
            'run_tests=pytest:main',
        ],
    },
    test_suite='test',
)
