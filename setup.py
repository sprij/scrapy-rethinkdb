from setuptools import setup, find_packages
from pip.req import parse_requirements

setup(
    name='scrapy-rethinkdb',
    version='0.0.2',
    author='sprij',
    author_email='s.rijo@yahoo.com',
    description='Scrapy pipeline for rethinkdb.',
    keywords="scrapy pipeline rethinkdb",
    url='https://github.com/sprij/scrapy-rethinkdb',
    packages=find_packages(),
    include_package_data=True,
    long_description=__doc__,
    install_requires=[
        str(install_req.req)
        for install_req in parse_requirements('requirements.txt')
    ],
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Topic :: Utilities",
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2',
    ]
)
