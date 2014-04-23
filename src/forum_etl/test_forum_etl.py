'''
Created on Apr 21, 2014

@author: paepcke
'''
import json
from pymongo import MongoClient
import unittest

from pymysql_utils.pymysql_utils import MySQLDB
from extractor import EdxForumScrubber


class TestForumEtl(unittest.TestCase):


    def setUp(self):
        
        mongoClient = MongoClient()
        db = mongoClient['unittest']
        self.forumPostsColl = db['tinyForum']
        self.resetMongoTestDb()

        self.mysqldb = MySQLDB(user='unittest', db='unittest')
        
        # Instantiate a Forum scrubber without the 
        # name of a bson file that contains forum
        # records: 
        self.forumScrubber = EdxForumScrubber(None, mysqlDbObj=self.mysqldb, forumTableName='contents')
    
    def testForumEtl(self):
        self.forumScrubber.forumMongoToRelational(self.forumPostsColl, self.mysqldb, 'contents')        

    def resetMongoTestDb(self):
        self.forumPostsColl.remove()
        # Use small, known forum collection:     
        with open('data/tinyForum.json', 'r') as jsonFd:
            line = jsonFd.readline()
            forumPost = json.loads(line)
            self.forumPostsColl.insert(forumPost)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testForumEtl']
    unittest.main()