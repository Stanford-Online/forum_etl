import multiprocessing
from setuptools import setup, find_packages
setup(
    name = "forum_etl",
    version = "0.1",
    packages = find_packages(),

    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = [],
    install_requires = ['pymysql_utils>=0.32',
			'configparser>=3.3.0',
			'json_to_relation>=0.3'
			],
    tests_require    = ['sentinels>=0.0.6', 'nose>=1.0'],

    # Unit tests; they are initiated via 'python setup.py test'
    test_suite       = 'nose.collector', 

    package_data = {
        # If any package contains *.txt or *.rst files, include them:
     #   '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
     #   'hello': ['*.msg'],
    },

    # metadata for upload to PyPI
    author = "Jagadish Venkatraman",
    author_email = "paepcke@cs.stanford.edu",
    description = "Retrieves OpenEdX Forum data from .bson file, and creates anonymized relational table.",
    license = "BSD",
    keywords = "OpenEdx, Forum",
    url = "https://github.com/Stanford-Online/forum-etl",   # project home page, if any
)
