'''
Created on Apr 21, 2014

@author: paepcke
'''
from collections import OrderedDict
import datetime
import json
import os
import unittest

from json_to_relation.mongodb import MongoDB

from extractor import EdxForumScrubber
from pymysql_utils.pymysql_utils import MySQLDB

# To run just one selected test method,
# set the following to False, and comment
# the desired method's 'skip-if' decoration:
RUN_ALL_TESTS = True

class TestForumEtl(unittest.TestCase):

    # Forum rows have the following columns:
    #  type, anonymous, anonymous_to_peers, at_position_list, user_int_id, body, course_display_name, created_at, votes, count, down_count, up_count, up, down, comment_thread_id, parent_id, parent_ids, sk   

    # Correct result for relationization of tinyForum.json
    # (in <projDir>/src/forum_etl/data). This result is anonymized and not relatable,
    # i.e. poster name UIDs use integers, while other tables use hashes:
    tinyForumGoldAnonymized = \
    [
    # poster Otto van Homberg: body is clean to start with:
    ('anon_screen_name_redacted','CommentThread', 'False', 'False', '[]', 5L, 'Harmless body', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 20), "{u'count': 10, u'point': -6, u'down_count': 8, u'up': [u'2', u'10'], u'down': [u'1', u'3', u'4', u'5', u'6', u'7', u'8', u'9'], u'up_count': 2}", 10L, 8L, 2L, "['2', '10']", "['1', '3', '4', '5', '6', '7', '8', '9']", None, None, None, None),
    # poster Andreas Fritz: body has someone's email:
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 7L, ' Body with <emailRedac> email.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 10, u'point': -4, u'down_count': 7, u'up': [u'6', u'8', u'10'], u'down': [u'1', u'2', u'3', u'4', u'5', u'7', u'9'], u'up_count': 3}", 10L, 7L, 3L, "['6', '8', '10']", "['1', '2', '3', '4', '5', '7', '9']", '519461545924670200000001', None, '[]', '519461555924670200000006'),
    # poster Otto van Homberg: body has 'Otto':
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 5L, 'Body with poster name <nameRedac_anon_screen_name_redacted> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),
    # poster Andreas Fritz: body has a phone number:
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 10L, 'Body with <phoneRedac> a phone number.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461545924670200000005', "[u'519461545924670200000005']", '519461545924670200000005-519461555924670200000008'),
    # poster Otto van Homberg: body has his screen name (otto_king):
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 5L, 'Body with poster screen name <nameRedac_anon_screen_name_redacted> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),    
    # poster Otto van Homberg: body has his full name (Otto van Homberg):
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 5L, 'Body with poster screen name <nameRedac_anon_screen_name_redacted> <nameRedac_anon_screen_name_redacted> <nameRedac_anon_screen_name_redacted> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007')    
    ]
    
    # Gold result for anonymization that allows relating to other tables (i.e. hashes are constant)
    tinyForumGoldRelatable = \
    [
    # poster Otto van Homberg: body is clean to start with:
    ('abc','CommentThread', 'False', 'False', '[]', 5L, 'Harmless body', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 20), "{u'count': 10, u'point': -6, u'down_count': 8, u'up': [u'2', u'10'], u'down': [u'1', u'3', u'4', u'5', u'6', u'7', u'8', u'9'], u'up_count': 2}", 10L, 8L, 2L, "['2', '10']", "['1', '3', '4', '5', '6', '7', '8', '9']", None, None, None, None),
    # poster Andreas Fritz: body has someone's email:
    ('def','Comment', 'False', 'False', '[]', 7L, ' Body with <emailRedac> email.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 10, u'point': -4, u'down_count': 7, u'up': [u'6', u'8', u'10'], u'down': [u'1', u'2', u'3', u'4', u'5', u'7', u'9'], u'up_count': 3}", 10L, 7L, 3L, "['6', '8', '10']", "['1', '2', '3', '4', '5', '7', '9']", '519461545924670200000001', None, '[]', '519461555924670200000006'),
    # poster Otto van Homberg: body has 'Otto':
    ('abc','Comment', 'False', 'False', '[]', 5L, 'Body with poster name <nameRedac_abc> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),
    # poster Andreas Fritz: body has a phone number:
    ('ghi','Comment', 'False', 'False', '[]', 10L, 'Body with <phoneRedac> a phone number.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461545924670200000005', "[u'519461545924670200000005']", '519461545924670200000005-519461555924670200000008'),
    # poster Otto van Homberg: body has his screen name (otto_king):
    ('abc','Comment', 'False', 'False', '[]', 5L, 'Body with poster screen name <nameRedac_abc> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),    
    # poster Otto van Homberg: body has his full name (Otto van Homberg):
    ('abc','Comment', 'False', 'False', '[]', 5L, 'Body with poster screen name <nameRedac_abc> <nameRedac_abc> <nameRedac_abc> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007')    
    ]
    
    # Gold result for non-anonymized forum:
    tinyForumGoldClear = \
    [
    # poster Otto van Homberg: body is clean to start with:
    ('otto_king','CommentThread', 'False', 'False', '[]', 5L, 'Harmless body', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 20), "{u'count': 10, u'point': -6, u'down_count': 8, u'up': [u'2', u'10'], u'down': [u'1', u'3', u'4', u'5', u'6', u'7', u'8', u'9'], u'up_count': 2}", 10L, 8L, 2L, "['2', '10']", "['1', '3', '4', '5', '6', '7', '8', '9']", None, None, None, None),
    # poster Andreas Fritz: body has someone's email:
    ('fritzL','Comment', 'False', 'False', '[]', 7L, ' Body with joe@comcast.com email.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 10, u'point': -4, u'down_count': 7, u'up': [u'6', u'8', u'10'], u'down': [u'1', u'2', u'3', u'4', u'5', u'7', u'9'], u'up_count': 3}", 10L, 7L, 3L, "['6', '8', '10']", "['1', '2', '3', '4', '5', '7', '9']", '519461545924670200000001', None, '[]', '519461555924670200000006'),
    # poster Otto van Homberg: body has 'Otto':
    ('otto_king','Comment', 'False', 'False', '[]', 5L, 'Body with poster name Otto embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),
    # poster Andreas Fritz: body has a phone number:
    ('bebeW','Comment', 'False', 'False', '[]', 10L, 'Body with 650-333-4567 a phone number.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461545924670200000005', "[u'519461545924670200000005']", '519461545924670200000005-519461555924670200000008'),
    # poster Otto van Homberg: body has his screen name (otto_king):
    ('otto_king','Comment', 'False', 'False', '[]', 5L, 'Body with poster screen name otto_king embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),    
    # poster Otto van Homberg: body has his full name (Otto van Homberg):
    ('otto_king','Comment', 'False', 'False', '[]', 5L, 'Body with poster screen name Otto van Homberg embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007')    
    ]    

    def setUp(self):
        
        self.mongoDb = MongoDB(dbName="unittest", collection="tinyForum")
        # Fill the little MongoDB with test JSON lines
        self.resetMongoTestDb()
        
        self.mysqldb = MySQLDB(mySQLUser='unittest', db='unittest')
        # Start with an empty result MySQL table for each test:
        self.mysqldb.dropTable('contents')
        # Fill the fake UserGrade table with records of course participants:
        self.resetMySQLUserListDb()
        
        # Instantiate a Forum scrubber without the 
        # name of a bson file that contains forum
        # records. That 'None' for the bson file will
        # make the class understand that it's being
        # instantiated for a unit test. 
        self.forumScrubberAnonymized = EdxForumScrubber(None, mysqlDbObj=self.mysqldb, forumTableName='contents', allUsersTableName='unittest.UserGrade')
        self.forumScrubberRelatable  = EdxForumScrubber(None, mysqlDbObj=self.mysqldb, forumTableName='contents', allUsersTableName='unittest.UserGrade', allowAnonScreenName=True)
        self.forumScrubberClear      = EdxForumScrubber(None, mysqlDbObj=self.mysqldb, forumTableName='contents', allUsersTableName='unittest.UserGrade', anonymize=False)

    def tearDown(self):
        self.mysqldb.close()

    @unittest.skipIf(not RUN_ALL_TESTS, 
                     'Uncomment this decoration if RUN_ALL_TESTS is False, and you want to run just this test.')    
    def testAnonymized(self):
        self.forumScrubberAnonymized.populateUserCache()
        self.forumScrubberAnonymized.forumMongoToRelational(self.mongoDb, self.mysqldb, 'contents')  
        for rowNum, forumPost in enumerate(self.mysqldb.query('SELECT * FROM unittest.contents')):
            # print(str(rowNum) + ':' + str(forumPost))
            self.assertEqual(TestForumEtl.tinyForumGoldAnonymized[rowNum], forumPost)
            
    @unittest.skipIf(not RUN_ALL_TESTS, 
                     'Uncomment this decoration if RUN_ALL_TESTS is False, and you want to run just this test.')    
    def testNonAnonymizedRelatable(self):
        self.forumScrubberRelatable.populateUserCache()
        self.forumScrubberRelatable.forumMongoToRelational(self.mongoDb, self.mysqldb, 'contents')  
        for rowNum, forumPost in enumerate(self.mysqldb.query('SELECT * FROM unittest.contents')):
            # print(str(rowNum) + ':' + str(forumPost))
            self.assertEqual(TestForumEtl.tinyForumGoldRelatable[rowNum], forumPost)

    @unittest.skipIf(not RUN_ALL_TESTS, 
                     'Uncomment this decoration if RUN_ALL_TESTS is False, and you want to run just this test.')    
    def testNonAnonymized(self):
        self.forumScrubberClear.populateUserCache()
        self.forumScrubberClear.forumMongoToRelational(self.mongoDb, self.mysqldb, 'contents')  
        for rowNum, forumPost in enumerate(self.mysqldb.query('SELECT * FROM unittest.contents')):
            # print(str(rowNum) + ':' + str(forumPost))
            self.assertEqual(TestForumEtl.tinyForumGoldClear[rowNum], forumPost)


    
    def resetMongoTestDb(self):
        self.mongoDb.clearCollection()
        # Use small, known forum collection:
        currDir = os.path.dirname(__file__)     
        with open(os.path.join(currDir, 'data/tinyForum.json'), 'r') as jsonFd:
            for line in jsonFd:
                forumPost = json.loads(line)
                self.mongoDb.insert(forumPost)

    def resetMySQLUserListDb(self):
        '''
        Prepare a MySQL table that mimicks EdxPrivate.UserGrade.
        '''
        
        userGradeColSpecs = OrderedDict(
                                        {
                                         'name' : 'varchar(255)',
                                         'screen_name' : 'varchar(255)',
                                         'grade' : 'int',
                                         'course_id' : 'varchar(255)',
                                         'distinction' : 'tinyint',
                                         'status' : 'varchar(50)',
                                         'user_int_id' : 'int(11)',
                                         'anon_screen_name' : 'varchar(40)'
                                         })
        self.mysqldb.dropTable('UserGrade')
        self.mysqldb.createTable('UserGrade', userGradeColSpecs)
        self.mysqldb.bulkInsert('UserGrade', 
                                ('name','screen_name','grade','course_id','distinction','status','user_int_id','anon_screen_name'),
                                [
                                 ('Otto van Homberg','otto_king',5,'oldCourse',0,'notpassing',5,'abc'),
                                 ('Andreas Fritz','fritzL',2,'newCourse',0,'notpassing',7,'def'),
                                 ('Bebe Winter', 'bebeW',10,'History of Baking',1,'passing',10,'ghi')
                                 ])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testForumEtl']
    unittest.main()