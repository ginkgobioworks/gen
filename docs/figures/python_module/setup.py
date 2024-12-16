from setuptools import setup, find_packages

setup(
    name='GenFig',
    version='0.0.1',
    url='',
    author='Bob Van Hove',
    author_email='author@gmail.com',
    description='Generate figures for documentation',
    packages=find_packages(),    
    install_requires=['matplotlib', 'networkx', 'pygraphviz'],
    entry_points={
        'console_scripts': [
            'genfig=genfig.graph:main',
        ],
    },
)