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
        
        self.assertEqual(piazzaImporter.piazza2Anon['h8ndx888SKN'], u'ac79b0b077dd8c44d9ea6dfac1f08e6cd0ba29ea')
        self.assertEqual(piazzaImporter.piazza2Anon['hc19qkoyc9C'], u'8491933cf7fd48668da31fdcddc1e55a3fdb120b')
        self.assertEqual(piazzaImporter.piazza2Anon['hr7xjaytsC8'], u'8caf8996ed242c081908e29e134f93f075343e4f')
        #print(piazzaImporter.piazza2Anon)


    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testContentLoadingToMemory(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')


        # Getting an anon_screen_name from first JSON post object in JSON array of posts:
        anon_screen_name_1st = piazzaImporter.getPosterUidAnon(0)
        self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', anon_screen_name_1st)
        
        # Getting the subject of the first post:
        subject = piazzaImporter.getSubject(0)
        self.assertEqual('wireshark shows same packet sent twice', subject)
        
        # Get first post's content (i.e. body):
        content = piazzaImporter.getContent(0)
        self.assertEqual('<p>I tried to use', content[0:17])
        
        # Get first post's tags:
        tags = piazzaImporter.getTags(0)
        self.assertEqual([u'lectures',u'student'], tags)
        
        # First post's Piazze UID:
        piazzaUID = piazzaImporter.getPiazzaId(0)
        self.assertEqual('hr7xjaytsC8', piazzaUID)
        
        # First post's status:
        status = piazzaImporter.getStatus(0)
        self.assertEqual('active', status)
        
        # First post's 'no answer followup':
        noAnswerFollowup = piazzaImporter.getNoAnswerFollowup(0)
        self.assertEqual(0, noAnswerFollowup)
        
        # First post's creation date:
        cDate = piazzaImporter.getCreationDate(0)
        self.assertEqual('2014-01-26T10:08:18Z', cDate)
        
        # First post's type:
        theType = piazzaImporter.getPostType(0)
        self.assertEqual('question', theType)
        
        # First post's type:
        tagGoodArr = piazzaImporter.getTagGoodAnons(0)
        self.assertEqual(['8491933cf7fd48668da31fdcddc1e55a3fdb120b'], tagGoodArr)
        
        # First post's type:
        tagEndorseArr = piazzaImporter.getTagEndorseAnons(0)
        self.assertEqual(['ac79b0b077dd8c44d9ea6dfac1f08e6cd0ba29ea'], tagEndorseArr)
        
        # Second post's number of up-votes received:
        numVotes = piazzaImporter.getNumUpVotes(1) # second JSON obj!
        self.assertEqual(2, numVotes)
        
        # Second post's number of answers received:
        numAnswers = piazzaImporter.getNumAnswers(1) # second JSON obj!
        self.assertEqual(0, numAnswers)
        
        # First post's number of answers received:
        anonLevel = piazzaImporter.getIsAnonPost(0)
        self.assertEqual('no', anonLevel)
        
        # First post's bucket name:
        bucketName = piazzaImporter.getBucketName(0)
        self.assertEqual(None, bucketName)

        
        # Second post's bucket name:
        bucketName = piazzaImporter.getBucketName(1) # second JSON obj!
        self.assertEqual('Week 1/19 - 1/25', bucketName)

        # Second post's 'updated' field:
        updated = piazzaImporter.getUpdated(1) # second JSON obj!
        self.assertEqual('2014-01-22T06:55:56Z', updated)

        # First post's 'folders' (names) array:
        folders = piazzaImporter.getFolders(0)
        self.assertEqual('lectures', folders)

        # Test idPiazza2Anon():
        self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', piazzaImporter.idPiazza2Anon('hr7xjaytsC8'))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPiazzaToAnonMappinig']
    unittest.main()