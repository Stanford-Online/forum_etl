'''
Created on Apr 21, 2014

@author: paepcke
'''
from collections import OrderedDict
import json
import unittest

from json_to_relation.mongodb import MongoDB

from extractor import EdxForumScrubber
from pymysql_utils.pymysql_utils import MySQLDB


#from pymongo import MongoClient
class TestForumEtl(unittest.TestCase):


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
        self.forumScrubber.forumMongoToRelational(self.mongoDb, self.mysqldb, 'contents')        

    def resetMongoTestDb(self):
        self.mongoDb.clearCollection()
        # Use small, known forum collection:     
        with open('data/tinyForum.json', 'r') as jsonFd:
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
                                         'screen_name' : 'int',
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
                                [('Fritz','fritzL',2,'newCourse',0,'notpassing',3,'abcde'),
                                 ('Otto','otto_king',5,'oldCourse',0,'notpassing',5,'defg')])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testForumEtl']
    unittest.main()