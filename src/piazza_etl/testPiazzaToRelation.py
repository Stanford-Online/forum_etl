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
        # Try specifying a json content file (not a zip file),
        # without then specifying a file with account ID mapping:
        try:
            PiazzaImporter('unittest',       # MySQL user 
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
        
        self.assertEqual(
                        {'47bf69315b7391dace7ccbc344690969': u'8491933cf7fd48668da31fdcddc1e55a3fdb120b', 
                         'aff1b14edf5054292a31e584b4749f42': u'8caf8996ed242c081908e29e134f93f075343e4f', 
                         'fad3f083830511c86cb2ac72d61b7c08': u'ac79b0b077dd8c44d9ea6dfac1f08e6cd0ba29ea'},
                         piazzaImporter.piazza2Anon)
        #print(piazzaImporter.piazza2Anon)


    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testContentLoadingToMemory(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')

        print(piazzaImporter.getPosterAnon(0))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPiazzaToAnonMappinig']
    unittest.main()