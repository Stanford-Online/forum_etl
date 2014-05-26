'''
Created on May 25, 2014

@author: paepcke
'''
import unittest
from unittest.case import skipIf

from piazza_etl.piazza_to_relation import PiazzaImporter


DO_ALL = True

class Test(unittest.TestCase):

    def setUp(self):
        PiazzaImporter.CONVERT_FUNCTIONS_DB = 'unittest'
    
    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testPiazzaToAnonMapping(self):
        piazzaImporter = None
        # Try specifying a json content file (not a zip file),
        # without then specifying a file with account ID mapping:
        try:
            piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                            '',               # MySQL pwd
                                            'unittest',       # MySQL db
                                            'piazza_content', # MySQL table
                                            'data/test_PiazzaContent.json', # Test file from Piazza
                                             mappingFile=None)
            raise(AssertionError)
        except ValueError:
            # properly raises exception
            pass
                        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')
        
        
        print(piazzaImporter.piazza2Anon)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPiazzaToAnonMappinig']
    unittest.main()