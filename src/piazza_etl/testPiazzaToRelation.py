'''
Created on May 25, 2014

@author: paepcke

TODO:
- Test for user in Piazza, but not in student_anonymoususerid. Try:
     swift@cs.rochester.edu,hqwn8jq5UdZ,stanford.edu__37c7c49c477d4689526ffbf924f3f528                                                                                                                kyleisliving@gmail.com,hqv1vcncSST,stanford.edu__70511e8ac611c4e730405e5998aef007
     kyleisliving@gmail.com,hqv1vcncSST,stanford.edu__70511e8ac611c4e730405e5998aef007
'''
import unittest
from unittest.case import skipIf

from piazza_etl.piazza_to_relation import PiazzaImporter, PiazzaPost


DO_ALL = False

class Test(unittest.TestCase):

    def setUp(self):
        PiazzaImporter.CONVERT_FUNCTIONS_DB = 'unittest'

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testPiazzaPostSingletonMechanism(self):

        # Initialize data structures, even though 
        # for this text the PiazzaImporter is not
        # explicitly used:
        PiazzaImporter('unittest',       # MySQL user 
                        '',               # MySQL pwd
                        'unittest',       # MySQL db
                        'piazza_content', # MySQL table
                        'data/test_PiazzaContent.json', # Test file from Piazza
                        'data/test_PiazzaUsers.json',
                        mappingFile='data/test_AccountMappingInput.csv')


        # Two instantiations with same data 
        # should only create one instance:
        aPost = PiazzaPost({'id' : 'hr7xjaytsC8', 'foo' : 10, 'bar' : 20})
        #print(aPost)
        bPost = PiazzaPost({'id' : 'hr7xjaytsC8', 'foo' : 10, 'bar' : 20})
        #print(bPost)
        self.assertEqual(aPost, bPost)
        
        # Making any change in the JSON object should
        # create a new instance:
        cPost = PiazzaPost({'id' : 'hr7xjaytsC8', 'foo' : 20, 'bar' : 20})
        #print(cPost)
        self.assertNotEqual(aPost, cPost)

        # Search object by OID: 
        dPost = PiazzaPost(bPost['oid'])
        #print(cPost)
        self.assertEqual(bPost, dPost)
        

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testBadPiazzaJsonPostsInput(self):
        try:
            PiazzaPost({'foo' : 10, 'bar' : 20})
        except ValueError:
            pass
        else:
            self.fail('Bad JSON input should have raised a ValueError exception.')

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testUnmapablePiazzaId(self):
        postObj = PiazzaPost({'id' : 'badPiazzaId', 'foo' : 10, 'bar' : 20})
#        anon = PiazzaImporter.makeUnknowAnonScreenName(postObj['id'])
#        self.assertEqual('unknown_piazza_mapping_badPiazzaId', anon)

    #*****@skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testUsersImport(self):
        # Get an importer with minimal initialization;
        # accomplished by the 'unittesting=True':
        importer = PiazzaImporter(None,
                                  None,
                                  None,
                                  None,
                                  'data/test_PiazzaContent.json',
                                  usersFileName='data/test_PiazzaUsers.json',
                                  unittesting=True)

        importer.importJsonUsersFromPiazzaZip('data/test_PiazzaUsers.json')
        hc19qkoyc9C_UserObj = PiazzaImporter.usersByPiazzaId['hc19qkoyc9C']
        
        truth = [(u'asks', 0), 
                 (u'views', 7), 
                 ('ext_id', u'47bf69315b7391dace7ccbc344690969'), 
                 ('piazza_id', u'hc19qkoyc9C'), 
                 (u'posts', 0), 
                 (u'days', 7), 
                 (u'answers', 0)
                 ]
        
        self.assertItemsEqual(truth, hc19qkoyc9C_UserObj.items())

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testContentsImport(self):
        # Get an importer with minimal initialization;
        # accomplished by the 'unittesting=True':
        importer = PiazzaImporter(None,
                                  None,
                                  None,
                                  None,
                                  'data/test_PiazzaContent.json',
                                  usersFileName='data/test_PiazzaUsers.json',
                                  unittesting=True)

        importer.importJsonContentFromPiazzaZip('data/test_PiazzaContent.json')
        

                         

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testPiazzaToAnonMapping(self):
        #***** MUTILATED
        importer = PiazzaImporter(None,
                                  None,
                                  None,
                                  None,
                                  'data/test_PiazzaContent.json',
                                  usersFileName='data/test_PiazzaUsers.json',
                                  unittesting=True)
        
        #importer.createPiazzaId2Anon('data/test_PiazzaUsers.json')
        
        # Try specifying a json content file (not a zip file),
        # without then specifying a file with account ID mapping:
        try:
            PiazzaImporter('unittest',       # MySQL user 
                           '',               # MySQL pwd
                           'unittest',       # MySQL db
                           'piazza_content', # MySQL table
                           'data/test_PiazzaContent.json', # Test file from Piazza
                           'data/test_PiazzaUsers.json',
                           mappingFile=None)
            
            # If no error, then an earlier test created
            # the singleton PiazzaImporter instance, and
            # we are just getting that instance back. 
            # So: force creation of a new PiazzaImporter instance;
            # we don't normally do this!:

            PiazzaImporter.singletonPiazzaImporter = None
            
            # and create a fresh PiazzaImporter. This
            # definitely needs to throw a ValueError:

            PiazzaImporter('unittest',       # MySQL user 
                           '',               # MySQL pwd
                           'unittest',       # MySQL db
                           'piazza_content', # MySQL table
                           'data/test_PiazzaContent.json', # Test file from Piazza
                           'data/test_PiazzaUsers.json',
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
                                        'data/test_PiazzaUsers.json',
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
                                        'data/test_PiazzaUsers.json',
                                        mappingFile='data/test_AccountMappingInput.csv')


        firstObj = piazzaImporter[0]
        self.assertEqual('XJ8LO55EUbrCAcMyzSbP3w==', firstObj['oid'])
        secondObj = piazzaImporter[1]
        self.assertEqual('9OOUV8g_SqbPDuRldhHUhw==', secondObj['oid'])
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
        trueKeys =  ['anon_screen_name', u'change_log', u'children', u'config', u'created', u'folders', u'history', u'id', u'no_answer', u'no_answer_followup', u'nr', 'oid', u'status', u'tag_endorse_arr', u'tag_good', u'tag_good_arr', u'tags', u'type', u'unique_views']

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

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testGetPosterId(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                        'data/test_PiazzaUsers.json',
                                        mappingFile='data/test_AccountMappingInput.csv')

        oneObj = piazzaImporter[0]
        anon_screen_name_1st = oneObj['anon_screen_name']
        self.assertEqual('8caf8996ed242c081908e29e134f93f075343e4f', anon_screen_name_1st)


    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testChildOps(self):
        
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                        'data/test_PiazzaUsers.json',
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
                                        'data/test_PiazzaUsers.json',
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
                                        'data/test_PiazzaUsers.json',
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
                                        'data/test_PiazzaUsers.json',
                                        mappingFile='data/test_AccountMappingInput.csv')

        # List all creation dates:
        createDates = []
        for piazzaObj in piazzaImporter:
            createDates.extend(self.childGetObjDates(piazzaObj))
        #print(createDates)
        groundTruth = [u'2014-01-26T10:08:18Z', 
                       u'2014-01-26T18:13:50Z', 
                       u'2014-01-22T01:25:00Z', 
                       u'2014-01-22T06:49:14Z', 
                       u'2014-01-28T02:30:03Z', 
                       u'2014-01-28T02:30:03Z']
        self.assertEqual(groundTruth, createDates)
        
    def childGetObjDates(self, piazzaPostObj):
        createDates = [piazzaPostObj['created']]
        children = piazzaPostObj['children']
        for child in children:
            createDates.extend(self.childGetObjDates(child))
        return createDates

    @skipIf (not DO_ALL, 'comment me if do_all == False, and want to run this test')
    def testGetAllFieldsX(self):
        piazzaImporter = PiazzaImporter('unittest',       # MySQL user 
                                        '',               # MySQL pwd
                                        'unittest',       # MySQL db
                                        'piazza_content', # MySQL table
                                        'data/test_PiazzaContent.json', # Test file from Piazza
                                        'data/test_PiazzaUsers.json',
                                        mappingFile='data/test_AccountMappingInput.csv')
        types = []
        for piazzaObj in piazzaImporter:
            types.extend(self.getAllFieldsFromX(piazzaObj, 'type'))
        #print(types)
        gold = [u'question', u'i_answer', u'note', u'followup', u'feedback', u'feedback']
        self.assertEqual(gold, types)

    def getAllFieldsFromX(self, piazzaPostObj, fieldName):
        '''
        Given a PiazzaPost instance, and a field name,
        return an array of the instance and, recursively,
        all its children as an array.
        
        :param piazzaPostObj:
        :type piazzaPostObj:
        :param fieldName:
        :type fieldName:
        '''
        fieldValues = [piazzaPostObj[fieldName]]
        children = piazzaPostObj['children']
        for child in children:
            fieldValues.extend(self.getAllFieldsFromX(child, fieldName))
        return fieldValues


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPiazzaToAnonMappinig']
    unittest.main()