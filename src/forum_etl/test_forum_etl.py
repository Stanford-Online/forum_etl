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


#from pymongo import MongoClient
class TestForumEtl(unittest.TestCase):
    
    # Forum rows have the following columns:
    #  type, anonymous, anonymous_to_peers, at_position_list, user_int_id, body, course_display_name, created_at, votes, count, down_count, up_count, up, down, comment_thread_id, parent_id, parent_ids, sk   

    # Correct result for relationization of tinyForum.json
    # (in <projDir>/src/forum_etl/data)
    tinyForumGoldResult = \
    [
    ('anon_screen_name_redacted','CommentThread', 'False', 'False', '[]', 3L, 'Harmless body', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 20), "{u'count': 10, u'point': -6, u'down_count': 8, u'up': [u'2', u'10'], u'down': [u'1', u'3', u'4', u'5', u'6', u'7', u'8', u'9'], u'up_count': 2}", 10L, 8L, 2L, "['2', '10']", "['1', '3', '4', '5', '6', '7', '8', '9']", None, None, None, None),
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 7L, ' Body with <emailRedac> email.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 10, u'point': -4, u'down_count': 7, u'up': [u'6', u'8', u'10'], u'down': [u'1', u'2', u'3', u'4', u'5', u'7', u'9'], u'up_count': 3}", 10L, 7L, 3L, "['6', '8', '10']", "['1', '2', '3', '4', '5', '7', '9']", '519461545924670200000001', None, '[]', '519461555924670200000006'),
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 1L, 'Body with poster name <redacName> embedded.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461555924670200000006', "[u'519461555924670200000006']", '519461555924670200000006-519461555924670200000007'),
    ('anon_screen_name_redacted','Comment', 'False', 'False', '[]', 10L, 'Body with <phoneRedac> a phone number.', 'MITx/6.002x/2012_Fall', datetime.datetime(2013, 5, 16, 4, 32, 21), "{u'count': 0, u'point': 0, u'down_count': 0, u'up': [], u'down': [], u'up_count': 0}", 0L, 0L, 0L, '[]', '[]', '519461545924670200000001', '519461545924670200000005', "[u'519461545924670200000005']", '519461545924670200000005-519461555924670200000008')
    ]

    def setUp(self):
        
        self.mongoDb = MongoDB(dbName="unittest", collection="tinyForum")
        self.resetMongoTestDb()
        
        self.mysqldb = MySQLDB(user='unittest', db='unittest')
        self.resetMySQLUserListDb()
        
        # Instantiate a Forum scrubber without the 
        # name of a bson file that contains forum
        # records: 
        self.forumScrubber = EdxForumScrubber(None, mysqlDbObj=self.mysqldb, forumTableName='contents', allUsersTableName='unittest.UserGrade')

    def tearDown(self):
        self.mysqldb.close()
    
    def testForumEtl(self):
        self.forumScrubber.populateUserCache()
        self.forumScrubber.forumMongoToRelational(self.mongoDb, self.mysqldb, 'contents')  
        for rowNum, forumPost in enumerate(self.mysqldb.query('SELECT * FROM unittest.contents')):
            # print(str(rowNum) + ':' + str(forumPost))
            self.assertEqual(TestForumEtl.tinyForumGoldResult[rowNum], forumPost)
              

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
                                [('Andreas Fritz','fritzL',2,'newCourse',0,'notpassing',3,'abcde'),
                                 ('Otto van Homberg','otto_king',5,'oldCourse',0,'notpassing',5,'defg')])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testForumEtl']
    unittest.main()