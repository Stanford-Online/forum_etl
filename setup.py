import multiprocessing
from setuptools import setup, find_packages
setup(
    name = "forum_etl",
    version = "0.1",
    packages = find_packages(),

    # Dependencies on other packages:
    setup_requires   = ['nose>=1.1.2'],
    install_requires = [
			'pymongo>=2.6.2', 
			'pymysql_utils>=0.24',
			'MySQL-python>=1.2.5', 
			'configparser>=3.3.0r2', 
			'argparse>=1.2.1', 
			'unidecode>=0.04.14', 
			],
    #tests_require    = ['mongomock>=1.0.1', 'sentinels>=0.0.6', 'nose>=1.0'],

    # Unit tests; they are initiated via 'python setup.py test'
    #test_suite       = 'json_to_relation/test',
    test_suite       = 'nose.collector', 

    package_data = {
        # If any package contains *.txt or *.rst files, include them:
     #   '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
     #   'hello': ['*.msg'],
    },

    # metadata for upload to PyPI
    author = "Jagadish Venkatraman",
    #author_email = "me@example.com",
    description = "Anonymizes forum dumps.",
    license = "BSD",
    keywords = "forum",
    url = "https://github.com/Stanford-Online/forum_etl.git",   # project home page, if any
)
