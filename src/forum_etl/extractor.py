import MySQLdb
from UserDict import DictMixin
import argparse
from collections import OrderedDict
from datetime import datetime
import getpass
import logging
import os
from pymongo import MongoClient
import re
import subprocess
import sys
import warnings

from json_to_relation.mongodb import MongoDB

from pymysql_utils.pymysql_utils import MySQLDB


class EdxForumScrubber(object):
    '''
    
    Given a .bson file of OpenEdX Forum posts, load the file
    into a MongoDB. Then pull a post at a time, anonymize, and
    insert a selection of fields into a MySQL db. The MongoDb
    entries look like this::
    
    {   
    	"_id" : ObjectId("51b75a48f359c40a00000028"),
    	"_type" : "Comment",
    	"abuse_flaggers" : [ ],
    	"anonymous" : false,
    	"anonymous_to_peers" : false,
    	"at_position_list" : [ ],
    	"author_id" : "26344",
    	"author_username" : "Minelly48",
    	"body" : "I am Gwen.I am a nursing professor who took statistics many years ago and want to refresh my knowledge.",
    	"comment_thread_id" : ObjectId("51b754e5f359c40a0000001d"),
    	"course_id" : "Medicine/HRP258/Statistics_in_Medicine",
    	"created_at" : ISODate("2013-06-11T17:11:36.831Z"),
    	"endorsed" : false,
    	"historical_abuse_flaggers" : [ ],
    	"parent_ids" : [ ],
    	"updated_at" : ISODate("2013-06-11T17:11:36.831Z"),
    	"visible" : true,
    	"votes" : {
    		"count" : 2,
    		"down" : [ ],
    		"down_count" : 0,
    		"point" : 2,
    		"up" : [
    			"40325",
    			"20323"
    		],
    		"up_count" : 2
    	},
    	"sk" : "51b75a48f359c40a00000028"
    }    
    
    Depending on parameter allowAnonScreenName in the __init__() method,
    forum entries in the relational database will be associated with the
    same hash that is used to anonymize other parts of the OpenEdX data.
    
    '''
    
    LOG_DIR = '/home/dataman/Data/EdX/NonTransformLogs'

    # Pattern for email id - strings of alphabets/numbers/dots/hyphens followed
    # by an @ or at followed by combinations of dot/. followed by the edu/com
    # also, allow for spaces
    
    emailPattern='(.*)\s+([a-zA-Z0-9\(\.\-]+)[@]([a-zA-Z0-9\.]+)(.)(edu|com)\\s*(.*)'
    #emailPattern='(.*)\\s+([a-zA-Z0-9\\.]+)\\s*(\\(f.*b.*)?(@)\\s*([a-zA-Z0-9\\.\\s;]+)\\s*(\\.)\\s*(edu|com)\\s+(.*)'
    compiledEmailPattern = re.compile(emailPattern);

    # Pattern for replacing embedded double quotes in post bodies,
    # unless they are already escaped w/ a backslash. The
    # {0,1} means a match if zero or one repetition. It's
    # needed so that double quotes at the very start of a 
    # string are matched: no preceding character at all: 
    #doublQuoteReplPattern = re.compile(r'[^\\]{0,1}"')
    doublQuoteReplPattern = re.compile(r'[\\]{0,}"')
    
    def __init__(self, 
                 bsonFileName, 
                 mysqlDbObj=None, 
                 forumTableName='contents', 
                 allUsersTableName='EdxPrivate.UserGrade',
                 anonymize=True,
                 allowAnonScreenName=False):
        '''
        Given a .bson file containing OpenEdX Forum entries, anonymize the entries (if desired),
        and place them into a MySQL table.  
        
        :param bsonFileName: full path the .bson table. Set to None if instantiating
            for unit testing.
        :type bsonFileName: String
        :param mysqlDbObj: a pymysql_utils.MySQLDB object where anonymized entries are
            to be placed. If None, a new such object is created into MySQL db 'EdxForum'
        :type mysqlDbObj: MySQLDB
        :param forumTableName: name of table into which anonymized Forum entries are to be placed
        :type forumTableName: String
        :param allUsersTable: fully qualified name of table listing all in-the-clear mySQLUser names
            of users who post to the Forum. Used to redact their names from their own posts.
        :type allUsersTable: String
        :param anonymize: If true, Forum post entries in the MySQL table will be anonymized
        :type anonymize: bool
        :param allow_anon_screen_name: if True, then occurrences of poster's name in
            post bodies are replaced by <redacName_<anon_screen_name>>, where anon_screen_name
            is the hash used in other tables of the OpenEdX data.
        :type allow_anon_screen_name: Bool 
        '''
        
        self.bsonFileName = bsonFileName
        self.forumTableName = forumTableName
        self.forumDbName = 'EdxForum'
        self.allUsersTableName = allUsersTableName
        self.anonymize = anonymize
        self.allowAnonScreenName = allowAnonScreenName
        
        # If not unittest, but regular run, then mysqlDbObj is None
        if mysqlDbObj is None:
            self.mysql_passwd = self.getMySQLPasswd()
            self.mysql_dbhost ='localhost'
            self.mysql_user = getpass.getuser() # mySQLUser that started this process
            self.mydb = MySQLDB(mySQLUser=self.mysql_user, passwd=self.mysql_passwd, db=self.forumDbName)
        else:
            self.mydb = mysqlDbObj

        self.counter=0
        
        self.userCache = {}
        self.userSet   = set()

        warnings.filterwarnings('ignore', category=MySQLdb.Warning)        
        self.setupLogging()
        self.prepDatabase()

        #******mysqldb.commit();    
        #******logging.info('commit completed!')

    def runConversion(self):
        '''
        Do the actual work. We don't call this method from __init__()
        so that unittests can create an EdxForumScrubber instance without
        doing the actual work. Instead, unittests call individual methods. 
        '''
        self.populateUserCache();

        self.mongo_database_name = 'TmpForum'
        self.collection_name = 'contents'

        # Load bson file into Mongodb:
        self.loadForumIntoMongoDb(self.bsonFileName)
        self.mongodb = MongoDB(dbName=self.mongo_database_name, collection=self.collection_name)
        
        # Anonymize each forum record, and transfer to MySQL db:
        self.forumMongoToRelational(self.mongodb, self.mydb,'contents' )
        
        self.mydb.close()
        self.mongodb.close()
        self.logInfo('Entered %d records into %s' % (self.counter, self.forumDbName + self.forumTableName))

    def loadForumIntoMongoDb(self, bsonFilename):

        mongoclient = MongoClient();
        db = mongoclient[self.mongo_database_name];

        # Get collection object:
        collection = db[self.collection_name];

        # Clear out any old forum entries:
        self.logInfo('Preparing to delete the collection ')
        collection.remove()
        self.logInfo('Deleting mongo collection completed. Will now attempt a mongo restore')
        
        self.logInfo('Spawning subprocess to execute mongo restore')
        with open(self.logFilePath,'w') as outfile:
            ret = subprocess.call(
                   ['mongorestore',
                    '--drop',
                    '--db', self.mongo_database_name, 
                    '--collection', self.collection_name,
                    bsonFilename], 
                stdout=outfile, stderr=outfile)

            self.logDebug('Return value from mongorestore is %s' % (ret))

            objCount = subprocess.check_output(
                       ['mongo',
                        '--quiet',
                        '--eval',
                        'printjson(db.contents.count())',
                        self.mongo_database_name, 
                        ], 
                        stderr=outfile)
            self.numMongoItems = objCount
            
            self.logInfo('Available Forum posts %s' % objCount)

    def forumMongoToRelational(self, mongodb, mysqlDbObj, mysqlTable):
        '''
        Given a pymongo collection object in which Forum posts are stored,
        and a MySQL db object and table name, anonymize each mongo record,
        and insert it into the MySQL table.
        
        :param collection: collection object obtained via a mangoclient object
        :type collection: Collection
        :param mysqlDbObj: wrapper to MySQL db. See pymysql_utils.py
        :type mysqlDbObj: MYSQLDB
        :param mysqlTable: name of table where posts are to be deposited.
            Example: 'contents'.
        :type mysqlTable: String
        '''

        #command = 'mongorestore %s -db %s -mongoForumRec %s'%(self.bson_filename,self.mongo_database_name,self.collection_name)
        #print command
    
        self.logInfo('Will start inserting from mongo collection to MySQL')

        for mongoForumRec in mongodb.query({}):
            mongoRecordObj = MongoRecord(mongoForumRec)

            try:
                # Check whether 'up' can be converted to a list
                list(mongoRecordObj['up'])
            except Exception as e:
                self.logInfo('Error in conversion' + `e`)
                mongoRecordObj['up'] ='-1'
            
            self.insert_content_record(mysqlDbObj, mysqlTable, mongoRecordObj);
        
    def prepDatabase(self):
        '''
        Declare variables and execute statements preparing the database to 
        configure options - e.g.: setting char set to utf, connection type to utf
        truncating the already existing table.
        '''
        try:
            self.logDebug("Setting and assigning char set for mysqld. will truncate old values")
            self.mydb.execute('SET NAMES utf8;');
            self.mydb.execute('SET CHARACTER SET utf8;');
            self.mydb.execute('SET character_set_connection=utf8;');
            
            # Compose fully qualified table name from the db name to 
            # which self.mydb is connected, and the forum table name
            # that was established in __init__():
            fullTblName = self.mydb.dbName() + '.' + self.forumTableName
            # Clear old forum data out of the table:
            try:
                self.mydb.dropTable(fullTblName)
                # Create MySQL table for the posts. If we are to
                # anonymize, the poster name column will be 'screen_name',
                # else it will be 'anon_screen_name':
                self.createForumTable(self.anonymize)
                self.logDebug("setting and assigning char set complete. Truncation succeeded")                
            except ValueError as e:
                self.logDebug("Failed either to set character codes, or to create forum table %s: %s" % (fullTblName, `e`))
        
        except MySQLdb.Error,e:
            self.logInfo("MySql Error exiting %d: %s" % (e.args[0],e.args[1]))
            # print e
            sys.exit(1)
    
    def getMySQLPasswd(self):
        homeDir=os.path.expanduser('~'+getpass.getuser())
        f_name = homeDir + '/.ssh/mysql'
        try:
            with open(f_name, 'r') as f:
                password = f.readline().strip()
        except IOError:
            return ''
        return password

    def populateUserCache (self) : 
        '''
        Populate the User Cache and preload information on mySQLUser id int, screen name
        and the actual name
        '''
        try:
            self.logInfo("Beginning to populate mySQLUser cache");
            # Cache all in-the-clear mySQLUser names of participants who
            # might post posts. We get those from the EdxPrivate.UserGrade table
            # Result tuple positions:                  0        1        2            3
            for userRow in self.mydb.query('select user_int_id,name,screen_name,anon_screen_name from %s' % self.allUsersTableName):
                userCacheEntry=[]
                userCacheEntry.append(userRow[1]) # full name
                userCacheEntry.append(userRow[2]) # screen_name
                userCacheEntry.append(userRow[3]) # anon_screen_name
    
                # Get poster's full name as firstName/lastName array:
                posterName=userRow[1].split()
            
                if len(posterName)>0:
                    # Collect the first name:
                    self.userSet.add(posterName[0])
    
                """for word in posterName:
                    if(len(word)>2 and '\\'    not in repr(word) ):
                        self.userSet.add(word)
                        if '-' in word:
                            l2=word.split('-')
                            for data in l2:
                                self.userSet.add(data)"""
                """if(len(userRow[1])>0):
                    self.userSet|=set([userRow[1]])
                    self.userSet|=set(userRow[2])"""
                # Add a cache entry mapping user_int_id
                # to the triplet full name/screen_name/anon_screen_name
                self.userCache[int(userRow[0])] = userCacheEntry;    
            self.logInfo("loaded objects in usercache %d"%(len(self.userCache)))
            # Save the mySQLUser cache in Python pickled format:
            #pickle.dump( self.userSet, open( "mySQLUser.p", "wb" ) )
    
            #print self.userSet
        except MySQLdb.Error,e:
            self.logInfo("MySql Error while mySQLUser cache exiting %d: %s" % (e.args[0],e.args[1]))
            sys.exit(1)
    
    def prune_numbers(self, body):
        '''
        Prunes phone numbers from a given string and returns the string with
        phone numbers replaced by <phoneRedac>

        :param body: forum post
        :type body: String
        :returns: body with all phone number-like substrings replaced by <phoneRedac>
        :rtype: String
        '''
        #re from stackoverflow. seems to do an awesome job at capturing all phone nos :)
        s='((?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?)'
        match=re.findall(s,body)
        for phoneMatchHit in match:
            body=body.replace(phoneMatchHit[0],"<phoneRedac>")
        return body    
    
    def prune_zipcode(self, body):
        '''
        Prunes the zipcdoe from a given string and returns the string without zipcode

        :param body: forum post
        :type body: String
        '''
        s='\d{5}(?:[-\s]\d{4})?'
        match=re.findall(s,body)
        for zipcodeMatchHit in match:
            body=body.replace(zipcodeMatchHit[0],"<zipRedac>")
        return body

    def trimnames(self, body):
        '''
        UNTESTED:
        Removes all person names known in the forum from the given
        post. We currently return the body unchanged, because we
        found that too many names match regular English words.  
        :param body: forum post
        :type body: String
        '''
        return body
        #Trims all firstnames and last names from the body of the post.
        
        #print 'processing body %s' %(body)
        #print 'en %s' %(len(self.userSet))
        s3=set(body.split())
        s4=s3&self.userSet
        #print 's4 is %s' %(s4)
        
        for s in s4:
            if len(s)>1 and s[0].isupper():
                body = re.sub(r"\b%s\b" % s , "NAME_REMOVED", body)
    
                #body=body.replace(s,"NAME_REMOVED")
        
     
        return body



    def anonymizeRecord(self, mongoRecordObj):
        
        body = mongoRecordObj['body']
        body = self.prune_numbers(body)
        body = self.prune_zipcode(body)

        if EdxForumScrubber.compiledEmailPattern.match(body) is not None:
            #print 'BODY before EMAIL STRIPING %posterNamePart \n'%(body);
            match = re.findall(EdxForumScrubber.emailPattern, body)
            new_body = " "
            for emailMatchHit in match:
                new_body += emailMatchHit[0] + " <emailRedac> " + emailMatchHit[-1] #print 'NEW BODY AFTER EMAIL STRIPING %posterNamePart \n'%(new_body);
            
            body = new_body

        # Redact poster'posterNamePart fullName from the post;
        # get tuple (fullUserName, screenName, anon_screen_name) from
        # the fullName cache (which is keyed off user_int_id):
        fullName, screen_name, anon_screen_name = self.userCache.get(int(mongoRecordObj['user_int_id']), ('', '', ''))
            # If not allowed to use hash of other db parts,
            # then drop anon_screen_name:
        if not self.allowAnonScreenName:
            anon_screen_name = 'anon_screen_name_redacted'
        try:
        # Check whether any part of the poster's
        # name is in the body, and redact if needed:
            bodyLowerCase = body.lower()
            for posterNamePart in fullName.split():
                if len(posterNamePart) >= 3:
                    posterNameLowered = posterNamePart.lower().encode('UTF-8', 'replace')
                    if posterNameLowered in bodyLowerCase: # Look for this loop iteration's part of the name
                        # the poster's name. The '\b' ensures that
                        # partial matches don't happen: e.g. name
                        # "Theo" shouldn't match "Theology"
                        pat = re.compile(r'\b%s\b' % posterNamePart, re.IGNORECASE)
                        body = pat.sub("<nameRedac_" + anon_screen_name + ">", body)
                    
        except Exception as e:
            self.logInfo("Error while redacting poster name in forum post body: %s: %s" % (body, `e`))

        if len(screen_name) > 0:
            screenNamePattern = re.compile(screen_name, re.IGNORECASE)
            body = screenNamePattern.sub("<nameRedac_" + anon_screen_name + ">", body)

        # Trim the name of anyone in the class from the
        # post. This method currently does nothing, b/c
        # some of the names people give are very common
        # English words:
        body = self.trimnames(body)
        
        # Update the record instance with the modified body:
        mongoRecordObj['body'] = body
        mongoRecordObj['anon_screen_name'] = anon_screen_name
        
        return mongoRecordObj

    def insert_content_record(self, mysqlDbObj, mysqlTableName, mongoRecordObj):
        '''
        Given all fields of one forum post record, anonymize the post, if self.anonymize is True,
        and insert the result into EdxForum.contents.
        
        :param mysqlDbObj: MySQLDB instance into which to place transformed forum posts (see pymysql_utils)
        :type mysqlDbObj: MySQLDB
        :param mysqlTableName: Name of table into which record is to be inserted. Ex: 'contents'
        :type mysqlTalbeName: String
        :param mongoRecordObj: a Python object that contains the Forum record fields we export. 
            These instances behave like dicts.
        :type _type: MongoRecord
        '''

        # Ensure body is UTF-8 only (again!).
        # I don't know why the encoding we do 
        # in makeDict() isn't enough, but it's not.
        # Who the hell knows with these encodings:
        mongoRecordObj['body'] = mongoRecordObj['body'].encode('utf-8').strip();
    
        if self.anonymize:    
            mongoRecordObj = self.anonymizeRecord(mongoRecordObj)
     
        try:
            fullTblName = mysqlDbObj.dbName() + '.' + mysqlTableName
            self.mydb.insert(fullTblName, mongoRecordObj)            
            
        except MySQLdb.Error as e:
            self.logErr("MySql error while inserting record %d: author name %s created_at %s: %s" % \
                         (self.counter, mongoRecordObj.getUserNameClear(), mongoRecordObj['created_at'], `e`))
            self.logErr("   Corresponding column values: %s" % str(mongoRecordObj.items()))
            return

        self.counter += 1;
        if(self.counter%100 == 0):
            #self.logInfo('inserted record %d'%( self.counter))
            # print '_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes
            #print '%d value \n'%(self.counter);
            pass

    def createForumTable(self, anonymize):
        '''
        Create an empty EdxForum.contents table. Requires
        CREATE privileges;
        
        :param anonymize: if true, column header for forum poster
            will be 'anon_screen_name', else it will be 'screen_name'
        :type anonymize: Boolean
        '''
        
        if anonymize:
            posterColHeader = 'anon_screen_name'
        else:
            posterColHeader = 'screen_name'
            
        createCmd = "CREATE TABLE `contents` ( \
                  `%s` varchar(40) NOT NULL DEFAULT 'anon_screen_name_redacted', \
                  `type` varchar(20) NOT NULL, \
                  `anonymous` varchar(10) NOT NULL, \
                  `anonymous_to_peers` varchar(10) NOT NULL, \
                  `at_position_list` varchar(200) NOT NULL, \
                  `user_int_id` int(11) NOT NULL, \
                  `body` varchar(2500) NOT NULL, \
                  `course_display_name` varchar(100) NOT NULL, \
                  `created_at` datetime NOT NULL, \
                  `votes` varchar(200) NOT NULL, \
                  `count` int(11) NOT NULL, \
                  `down_count` int(11) NOT NULL, \
                  `up_count` int(11) NOT NULL, \
                  `up` varchar(200) DEFAULT NULL, \
                  `down` varchar(200) DEFAULT NULL, \
                  `comment_thread_id` varchar(255) DEFAULT NULL, \
                  `parent_id` varchar(255) DEFAULT NULL, \
                  `parent_ids` varchar(255) DEFAULT NULL, \
                  `sk` varchar(255) DEFAULT NULL, \
                  `confusion` varchar(20) NOT NULL DEFAULT 'none', \
                  `happiness` varchar(20) NOT NULL DEFAULT 'none' \
                ) ENGINE=MYISAM DEFAULT CHARSET=latin1" % posterColHeader
                
        self.mydb.execute(createCmd)

#     def prepLogging(self):
#         logFileName = 'forum_%s.log'%(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
#         self.logFilePath = os.path.join(EdxForumScrubber.LOG_DIR, logFileName)
#         logging.basicConfig(filename=self.logFilePath,level=logging.DEBUG)


    def setupLogging(self):
        '''
        Set up the standard Python logger. 
        '''

        loggingLevel = logging.INFO
        logFileName = 'forum_%s.log'%(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
        self.logFilePath = os.path.join(EdxForumScrubber.LOG_DIR, logFileName)

        self.logger = logging.getLogger(os.path.basename(__file__))

        # Create file handler if requested:
        if self.logFilePath is not None:
            handler = logging.FileHandler(self.logFilePath)
            print('Logging to %s' % self.logFilePath)            
        else:
            # Create console handler:
            handler = logging.StreamHandler()
        handler.setLevel(loggingLevel)

        # Create formatter
        formatter = logging.Formatter("%(name)s: %(asctime)s;%(levelname)s: %(message)s")       
        handler.setFormatter(formatter)
        
        # Add the handler to the logger
        self.logger.addHandler(handler)
        self.logger.setLevel(loggingLevel)

    def logDebug(self, msg):
        self.logger.debug(msg)

    def logWarn(self, msg):
        self.logger.warn(msg)

    def logInfo(self, msg):
        self.logger.info(msg)

    def logErr(self, msg):
        self.logger.error(msg)

class MongoRecord(DictMixin):
    
    def __init__(self, rawMongoStruct):
        self.nameValueDict = self.makeDict(rawMongoStruct)
        # Get the screen name in the clear:
        self.user_name_clear = rawMongoStruct.get('author_username')

    def getUserNameClear(self):
        return self.user_name_clear

    def makeDict(self, mongoRecordStruct):

        # Create a dict of the raw Mongo name/value pairs, converting
        # types where needed. Recall: dict.get(key,[default]) returns
        # None if no default is provided. Need an ordered dict of these
        # column names, so that they match up with column values elsewhere.
        # The anon_user_name col value is initialized from the true poster
        # screen name. Anonymization of this column happens later:
        mongoRecordDict = OrderedDict(
                         {
                           'anon_screen_name' : str(mongoRecordStruct.get('author_username', '')),
                           'type' : str(mongoRecordStruct.get('_type')),
                		   'anonymous' : str(mongoRecordStruct.get('anonymous')),
                		   'anonymous_to_peers' : str(mongoRecordStruct.get('anonymous_to_peers')),
                		   'at_position_list' : str(mongoRecordStruct.get('at_position_list')),
                		   'user_int_id' : mongoRecordStruct.get('author_id'), # numeric id
                           'body' : re.sub(EdxForumScrubber.doublQuoteReplPattern, '\\"', mongoRecordStruct.get('body')),
                		   'course_display_name' : str(mongoRecordStruct.get('course_id')),
                		   'created_at' : str(mongoRecordStruct.get('created_at')),
                		   'votes' : str(mongoRecordStruct.get('votes')),
                           }) 
        try:
            # If body is not already UTF-8, encode it:
            mongoRecordDict['body'] = unicode(mongoRecordDict['body'], 'UTF-8', 'replace')
        except TypeError:
            # Body was already in Unicode, so all is well:
            pass
        
        votesObject= mongoRecordStruct.get('votes')
        if votesObject is not None:
            mongoRecordDict['count'] = votesObject.get('count')
            mongoRecordDict['down_count'] = votesObject.get('down_count')
            mongoRecordDict['up_count'] = votesObject.get('up_count')
            mongoRecordDict['up'] = str(votesObject.get('up'))
            if mongoRecordDict['up'] is not None:
                mongoRecordDict['up'] = mongoRecordDict['up'].replace("u","")
            mongoRecordDict['down'] = str(votesObject.get('down'))
            if mongoRecordDict['down'] is not None:
                mongoRecordDict['down'] = mongoRecordDict['down'].replace("u","")
        
        mongoRecordDict['sk'] = str(mongoRecordStruct.get('sk'))
        mongoRecordDict['comment_thread_id'] = str(mongoRecordStruct.get('comment_thread_id'))
        mongoRecordDict['parent_id'] = str(mongoRecordStruct.get('parent_id'))
        mongoRecordDict['parent_ids'] = str(mongoRecordStruct.get('parent_ids'))
        
        return mongoRecordDict

    def __getitem__(self, key):
        return self.nameValueDict[key]
    
    def __setitem__(self, key, value):
        self.nameValueDict[key] = value
    
    def __delitem__(self, key):
        del self.nameValueDict[key]
    
    def keys(self):
        return self.nameValueDict.keys()
        
#        ObjectId("519461545924670200000005")
#    ],
"""collectionObject=collection.find_one();

def generateInsertQuery(collectionobject)
    insertStatement='insert into EdxForum.contents(%s) values(%s)';
    cols=', '.join(collectionobject);
    vals=', '.join('?'*len(collectionobject));
    query=insertStatement%(cols,vals)
    db=MySQLdb.connect(host="localhost",mySQLUser='root',passwd='root',db='test')
    cur=db.cursor();
    cur.execute();
"""

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-a', '--anonymize', 
                        help='anonyimize the relational table of posts. If this flag absent, -r is ignored. Default: False', 
                        action='store_true',
                        default=False
                        );
    parser.add_argument('-r', '--relatable', 
                        help='This option creates an anonymized relational table for Forum posts,\n' +
                             'using the same UID hash as in other tables --> can relate posts with performance. Default: different UID hash.',
                        action='store_true',
                        default=False
                        );
    parser.add_argument('bson_filename',
                        help='Full path to MongoDB dump of Forum in .bson format.',
                        ) 
    
    args = parser.parse_args();

#     print('Anonymize: %s. Relatable: %s. File: %s' % (args.anonymize, args.relatable, args.bson_filename))
#     sys.exit(0)

    extractor = EdxForumScrubber(args.bson_filename, allowAnonScreenName=args.relatable)
    extractor.runConversion()
