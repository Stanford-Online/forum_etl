'''
Created on May 25, 2014

@author: paepcke
'''
import unittest
from unittest.case import skipIf

from piazza_etl.piazza_to_relation import PiazzaImporter, PiazzaPost


DO_ALL = False

class Test(unittest.TestCase):

    def setUp(self):
        PiazzaImporter.CONVERT_FUNCTIONS_DB = 'unittest'

    ####@skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testPiazzaPostSingletonMechanism(self):

        # Two instantiations with same data 
        # should only create one instance:
        aPost = PiazzaPost('oneUser', jsonDict={'foo' : 10, 'bar' : 20})
        print(aPost)
        bPost = PiazzaPost('oneUser', jsonDict={'foo' : 10, 'bar' : 20})
        print(bPost)
        self.assertEqual(aPost, bPost)
        
        # Making any change in the JSON object should
        # create a new instance:
        cPost = PiazzaPost('oneUser', jsonDict={'foo' : 20, 'bar' : 20})
        print(cPost)
        self.assertNotEqual(aPost, cPost)

        # Search object by OID: 
        dPost = PiazzaPost('oneUser', oid=bPost['oid'])
        print(cPost)
        self.assertEqual(bPost, dPost)
        
        # Use minimal number of args:
        ePost = PiazzaPost(aPost['oid'])
        self.assertEqual(ePost, bPost)
    
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


        firstObj = piazzaImporter[0]
        self.assertEqual('wu_ug_FC_rbo0Lhca1YOUA==', firstObj['oid'])
        secondObj = piazzaImporter[1]
        self.assertEqual('z0df3VaEGTBxAWQrfuv3hw==', secondObj['oid'])
        self.assertEqual('D7ObrIJu2xZS-vCNSH9RqQ==', firstObj['children'][0]['oid'])

        anon_screen_name = firstObj['anon_screen_name']
        self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', anon_screen_name)
        
        
        #   --- Getting Subject --- 
        
        # Getting subject from a PiazzaPost instance:
        subject = firstObj['subject']
        self.assertEqual('wireshark shows same packet sent twice', subject)
        
        # Getting subject from a PiazzaPost OID:
        subject = piazzaImporter.getSubject(firstObj)
        self.assertEqual('wireshark shows same packet sent twice', subject)
        
        # Getting subject from a PiazzaPost JSON object:
        subject = piazzaImporter.getSubject(firstObj.nameValueDict)
        self.assertEqual('wireshark shows same packet sent twice', subject)
        
        # Getting the subject of the first post:
        subject = piazzaImporter[0]['subject']
        self.assertEqual('wireshark shows same packet sent twice', subject)
        
        #   --- Getting Content --- 

        # Getting content from a PiazzaPost instance:
        content = piazzaImporter.getContent(secondObj)
        self.assertEqual('<p>Just wanted to pop in and say hello just as class starts.', content)
        
        # Getting content from a PiazzaPost OID:
        content = piazzaImporter.getContent(secondObj['oid'])
        self.assertEqual('<p>Just wanted to pop in and say hello just as class starts.', content)
        
        # Get first post's content (i.e. body):
        content = piazzaImporter[0]['content']
        self.assertEqual('<p>I tried to use', content[0:17])
        
        # Get first post's content (i.e. body):
        content = piazzaImporter[0]['body']    # For compatibility with OpenEdX forum
        self.assertEqual('<p>I tried to use', content[0:17])

        
        # Get first post's tags:
        tags = piazzaImporter[0]['tags']
        self.assertEqual([u'lectures',u'student'], tags)
        
        # First post's Piazze UID:
        piazzaUID = piazzaImporter[0]['id']
        self.assertEqual('hr7xjaytsC8', piazzaUID)
        
        # First post's status:
        status = piazzaImporter[0]['status']
        self.assertEqual('active', status)
        
        # First post's 'no answer followup':
        noAnswerFollowup = piazzaImporter[0]['no_answer_followup']
        self.assertEqual(0, noAnswerFollowup)
        
        # First post's creation date:
        cDate = piazzaImporter[0]['created']
        self.assertEqual('2014-01-26T10:08:18Z', cDate)
        
        # First post's creation date using get()
        cDate = piazzaImporter[0].get('created', '0000-00-00T00:00:00')
        self.assertEqual('2014-01-26T10:08:18Z', cDate)
        
        # Keys of a PiazzaPost object:
        postObjKeys = piazzaImporter[0].keys()
        postObjKeys.sort()
        trueKeys = ['anon_screen_name', u'change_log', u'children', u'config', u'created', u'folders', u'history', u'id', u'no_answer', u'no_answer_followup', u'nr', 'oid', u'status', u'tag_endorse_arr', u'tag_good', u'tag_good_arr', u'tags', u'type', u'unique_views']
        self.assertEqual(trueKeys, postObjKeys)

        
        # First post's type:
        theType = piazzaImporter[0]['type']
        self.assertEqual('question', theType)
        
        # First post's good-tags-array of anon_screen_name:
        tagGoodArr = piazzaImporter[0]['tag_good_arr']
        self.assertEqual(['8491933cf7fd48668da31fdcddc1e55a3fdb120b'], tagGoodArr)
        # Synonym for tag_good_arr: good_tag:
        tagGoodArr = piazzaImporter[0]['good_tags']
        self.assertEqual(['8491933cf7fd48668da31fdcddc1e55a3fdb120b'], tagGoodArr)
        
        # First endorse-tags array:
        tagEndorseArr = piazzaImporter[0]['tag_endorse_arr']
        self.assertEqual(['ac79b0b077dd8c44d9ea6dfac1f08e6cd0ba29ea'], tagEndorseArr)
        # Synonym for tag_endorse_arr: endorse_tags:
        tagEndorseArr = piazzaImporter[0]['endorse_tags']
        self.assertEqual(['ac79b0b077dd8c44d9ea6dfac1f08e6cd0ba29ea'], tagEndorseArr)
        
        
        # Second post's number of up-votes received:
        numVotes = piazzaImporter[1]['no_upvotes'] # second JSON obj!
        self.assertEqual(2, numVotes)
        
        # Second post's number of answers received:
        numAnswers = piazzaImporter[1]['no_answer'] # second JSON obj!
        self.assertEqual(0, numAnswers)
        
        # Second post's number of followup answers received:
        numAnswers = piazzaImporter[1]['no_answer_followup'] # second JSON obj!
        self.assertEqual(0, numAnswers)
        
        # Second post's 'is anonymous post':
        anonLevel = piazzaImporter[1]['anon']
        self.assertEqual('no', anonLevel)
        
        # First post's bucket name:
        try:
            piazzaImporter[0]['bucket_name']
            self.fail('Obj 0 has no bucket name; should be KeyError')
        except KeyError:
            pass

        # Second post's bucket name:
        bucketName = piazzaImporter[1]['bucket_name'] # second JSON obj!
        self.assertEqual('Week 1/19 - 1/25', bucketName)

        # Second post's 'updated' field:
        updated = piazzaImporter[1]['updated'] # second JSON obj!
        self.assertEqual('2014-01-22T06:55:56Z', updated)

        # First post's 'folders' (names) array:
        folders = piazzaImporter[0]['folders']
        self.assertEqual(['lectures'], folders)

        # Test idPiazza2Anon():
        self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', piazzaImporter.idPiazza2Anon('hr7xjaytsC8'))

    #****@skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testGetPosterId(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')

        # Getting an anon_screen_name from one JSON object:
        oneJsonDict = piazzaImporter.jData[0]
        anon_screen_name_1st = piazzaImporter.getPosterUidAnon(oneJsonDict)
        #*****self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', anon_screen_name_1st)

        # Now with PiazzaPost obj:
        oneObj = piazzaImporter[0]
        anon_screen_name_1st = piazzaImporter.getPosterUidAnon(oneObj)
        self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', anon_screen_name_1st)


    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testChildOps(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')

        # Get children of first JSON obj:
        children = piazzaImporter[0]['children']
        self.assertEqual(1, len(children))
        
        firstChild = children[0]
        childCDate = firstChild['created']
        self.assertEqual('2014-01-26T18:13:50Z', childCDate)
       
    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testPiazzaImporterAsOIDDict(self):
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')

        secondObj = piazzaImporter[1]
        self.assertEqual(secondObj, piazzaImporter[secondObj['oid']])
       
    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testPiazzaImporterIterator(self):
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')
        firstObj = piazzaImporter[0]
        secondObj = piazzaImporter[1]
        for i,obj in enumerate(piazzaImporter):
            if i == 0:
                self.assertEqual(firstObj, obj)
            elif i == 1:
                self.assertEqual(secondObj, obj)

        
    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testChildRecursion(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                         mappingFile='data/test_AccountMappingInput.csv')

        # List all creation dates:
        createDates = []
        for piazzaObj in piazzaImporter:
            thisObjCreateDates = self.childGetObjDates(piazzaObj)
            if thisObjCreateDates is None:
                continue
            createDates.extend(thisObjCreateDates)
        print(createDates)
        
    def childGetObjDates(self, piazzaPostObj):
        createDates = [piazzaPostObj['created']]
        children = piazzaPostObj['children']
        for child in children:
            childCreateDates = self.childGetObjDates(child)
            if childCreateDates is None:
                continue
            createDates.extend(childCreateDates)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPiazzaToAnonMappinig']
    unittest.main()