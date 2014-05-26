'''
Created on May 23, 2014

@author: paepcke


Example JSON element within a Piazza dump. All
elements are held within one array:
{

    "change_log": [
        {
            "type": "create",
            "anon": "no",
            "when": "2012-11-24T13:22:32Z",
            "uid": "Piazza Team"
        }
    ],
    "history": [
        {
            "subject": "Tips & Tricks for a successful class",
            "content": "<script type=\"text/javascript\">PA.load(\"/dashboard/tips_tricks\", null, function(data){ $('#' + 'questionText').html(data);});</script>",
            "created": "2012-11-24T13:22:32Z",
            "anon": "no",
            "uid": "Piazza Team"
        }
    ],
    "tags": [
        "private",
        "student"
    ],
    "id": "h9wrqvpjrke1zy",
    "status": "private",
    "no_answer_followup": 0,
    "created": "2012-11-24T13:22:32Z",
    "type": "note",
    "children": [ ],
    "tag_good_arr": [ ],
    "tag_good": [ ],
    "nr": 1,
    "config": {
        "is_default": 1
    }

},

Example entry in CSV file that maps Piazza internal IDs to Learning Tool Interoperability (LTI) user IDs::
	Email.UID,LTI Ids
	afterallforpeace@gmail.com,hr7xjaytsC8,stanford.edu__aff1b14edf5054292a31e584b4749f42
	alvaro.tolosa@hotmail.com,hc19qkoyc9C,"stanford.edu__88115, stanford.edu__47bf69315b7391dace7ccbc344690969"
	omn143@live.com,h8ndx888SKN,"stanford.edu__6808, stanford.edu__fad3f083830511c86cb2ac72d61b7c08"


Endorsement:
   anon_screen_name, endorsement_type, endorsement_post

'''

import argparse
import getpass
import json
import logging
import os
import sys
import zipfile

from pymysql_utils.pymysql_utils import MySQLDB


class PiazzaImporter(object):
    '''
    classdocs
    '''
    STANDARD_CONTENT_FILE_NAME = 'class_content.json'
    STANDARD_MAPPING_FILE_NAME = 'account_mapping.csv'
    
    MYSQL_PIAZZA_DB = 'Edx_Piazza'
    
    # Db in MySQL that holds function
    # idExt2Anon()
    CONVERT_FUNCTIONS_DB = 'EdxPrivate'

    def __init__(self, mysqlUser, mysqlPwd, dbname, tablename, jsonFileName, mappingFile=None, loggingLevel=logging.INFO, logFile=None):
        '''
        Create an instance that will hold a dict between
        Piazza IDs and anon_screen_name ids:
        
        :param mysqlUser: MySQL UID to use for looking up mapping between external (LTI) IDs and anon_screen_names
        :type mysqlUser: String
        :param mysqlPwd: MySQL PWD to use for user mysqlUser
        :type mysqlPwd: String
        :param dbname: name of MySQL database to which the forum content tables will be written.
        :type dbname: String
        :param tablename: name of table within dbname that will hold the forum contents
        :type tablename: String
        :param jsonFileName: name of JSON formatted file that holds the forum contents. If jsonFileName
            is a zip file, then that zip archive must contain a file named STANDARD_CONTENT_FILE_NAME (a class variable)
        :type jsonFileName: String
        :param mappingFile: name of CSV file that contains a mapping between Piazza internal IDs, and LTI IDs.
            If None, then jsonFileName must be a zip archive that contains the mapping file
            with the name STANDARD_MAPPING_FILE_NAME.
        :type mappingFile: {String | None}
        :param loggingLevel: detail of logging to do; default is INFO
        :type loggingLevel: logging
        :param logFile: file to send log into. If None: log to console
        :type String 
        '''
        self.mysqlUser = mysqlUser
        self.mysqlPwd = mysqlPwd
        self.dbname = dbname
        self.tablename = tablename
        self.jsonFileName = jsonFileName
        self.mappingFile = mappingFile
        
        self.logger = None
        self.setupLogging(loggingLevel, logFile)
        
        # Import JSON from Piazza content file, 
        # plus a mapping dict PiazzaIDs-->anon_screen_name IDs:
        if zipfile.is_zipfile(jsonFileName):
            # Grab and import content JSON file from zip archive:
            self.importJsonContentFromPiazzaZip(jsonFileName)
            if mappingFile is None:
                # If no CSV file mapping Piazza ID to LTI (Ext) 
                # id was provided, then assume that the mapping
                # file is within the zip file from Piazza (in which
                # the content file is as well): 
                self.createPiazzaId2Anon(jsonFileName)
            else:
                # Caller specified a CSV file with mappings
                # from Piazza to LTI ids outside of the zip file: 
                self.createPiazzaId2Anon(mappingFile)
        else:
            # Caller did not provide a zip file from Piazza, but
            # a separate JSON file with the forum content:
            with open(jsonFileName, 'r') as jsonFd:
                self.jData = json.load(jsonFd)
                # Since we have no zip file, caller *must*
                # specify a file that contains the Piazza ID to LTI 
                # mapping:
                if mappingFile is None:
                    raise ValueError('If providing a JSON file for the Piazza content (rather than a zip file), then a UID mapping file must be provided.')
                self.createPiazzaId2Anon(mappingFile)            

    def importJsonContentFromPiazzaZip(self, zipFileName):
        '''
        Given a 
        
        :param zipFileName:
        :type zipFileName:
        '''
        zipObj = zipfile.ZipFile(zipFileName)
        fileNameList = zipObj.namelist()
        if not PiazzaImporter.STANDARD_CONTENT_FILE_NAME in fileNameList:
            raise ValueError('Zip file %s does not contain a file %s.' % (zipFileName, PiazzaImporter.STANDARD_CONTENT_FILE_NAME))
        
        with zipObj.open(PiazzaImporter.STANDARD_CONTENT_FILE_NAME) as jsonFd:
            jsonArr = jsonFd.readlines()
            jsonStr = ''.join([line.strip() for line in jsonArr])
            self.jData = json.loads(jsonStr)
            #print('Read')
    
    def createPiazzaId2Anon(self, mappingOrZipFile):
        self.piazza2Anon = {}
        
        # Pull all mapping lines out of the csv
        # into a memory resident array of strings:
        if zipfile.is_zipfile(mappingOrZipFile):
            zipObj = zipfile.ZipFile(mappingOrZipFile)
            with zipObj.open(PiazzaImporter.STANDARD_MAPPING_FILE_NAME) as mappingFd:
                mappingRows = mappingFd.readlines()
        else:
            with open(mappingOrZipFile, 'r') as mappingFd:
                mappingRows = mappingFd.readlines()
        
        # Need to use a MySQL function to map from the
        # LTI (Ext) ID to anon_screen_name:
        try:
            db = None
            db = MySQLDB(user=self.mysqlUser, passwd=self.mysqlPwd,  db=PiazzaImporter.CONVERT_FUNCTIONS_DB)
            # Skipping past header line, convert one Pizza UID after another:
            for mappingRow in mappingRows[1:]:
                # Rows are like: 'myemail@gmail.com,hr7xjaytsC8,stanford.edu__aff1b14edf5054292a31e584b4749f42'
                csvCols = mappingRow.split(',')
                (email, piazzaUID, stanfordLtiUid) = csvCols[0:3]  # @UnusedVariable
                ltiUid = stanfordLtiUid.split('__')[-1].strip()
                for anon in db.query("SELECT idExt2Anon('%s');" % ltiUid.strip()):
                    try:
                        self.piazza2Anon[ltiUid] = anon[0]
                    except IndexError:
                        self.logWarn("No anon_screen_name for external (LTI) id '%s'" % ltiUid)
        finally:
            if db is not None:
                db.close()
            
    
    def getPosterAnon(self, arrIndex):
        self.piazza2Anon(self.jData[arrIndex]['uid'])
          
    def getSubject(self, arrIndex):
        return self.jData[arrIndex]['history'][0]['subject']

    def getContent(self, arrIndex):
        return self.jData[arrIndex]['history'][0]['content']

    def getTags(self, arrIndex):
        return self.jData[arrIndex]['tags']
    
    def getPiazzaId(self, arrIndex):
        return self.jData[arrIndex]['id']
    
    def getStatus(self, arrIndex):
        return self.jData[arrIndex]['status']
    
    def getNoAnswerFollowup(self, arrIndex):
        return self.jData[arrIndex]['no_answer_followup']
    
    def getCreationDate(self, arrIndex):
        return self.jData[arrIndex]['created']
    
    def getPostType(self, arrIndex):
        return self.jData[arrIndex]['type']
    
    def getTagGoodAnons(self, arrIndex):
        anons = []
        for piazzaId in self.jData[arrIndex]['tag_good_arr']:
            anons.append(self.piazza2Anon(piazzaId))
        return ','.join(anons)
    
    def getTagEndorseAnons(self, arrIndex):
        anons = []
        for piazzaId in self.jData[arrIndex]['tag_endorse_arr']:
            anons.append(self.piazza2Anon(piazzaId))
        return ','.join(anons)
    
    def getNumUpVotes(self, arrIndex):
        return self.jData[arrIndex]['no_upvotes']
    
    def getNumAnswers(self, arrIndex):
        return self.jData[arrIndex]['no_answers']
    
    def getIsAnonPost(self, arrIndex):
        return self.jData[arrIndex]['anon']
    
    def getBucketName(self, arrIndex):
        return self.jData[arrIndex]['bucket_name']
    
    def getUpdated(self, arrIndex):
        return self.jData[arrIndex]['updated']
    
    def getFolders(self, arrIndex):
        return ','.join(self.jData[arrIndex]['folders'])
    
    def idPiazza2Anon(self, piazzaId):
        pass
            

    def setupLogging(self, loggingLevel, logFile):
        '''
        Set up the standard Python logger. 
        TODO: have the logger add the script name as Sef's original
        @param loggingLevel:
        @type loggingLevel:
        @param logFile:
        @type logFile:
        '''
        # Set up logging:
        #self.logger = logging.getLogger('pullTackLogs')
        self.logger = logging.getLogger(os.path.basename(__file__))

        # Create file handler if requested:
        if logFile is not None:
            handler = logging.FileHandler(logFile)
            print('Logging of control flow will go to %s' % logFile)            
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
            
    
    
    
    
    

if __name__ == '__main__':
    
    # -------------- Manage Input Parameters ---------------
    
    #usage = 'Usage: pizza_to_relation.py mysql_db_name {<courseFile>.zip | <class_content>.json}\n'

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', '--mySQLUser',
                        action='store',
                        help='User ID that is to log into MySQL. Default: the mySQLUser who is invoking this script.')
    parser.add_argument('-p', '--mySQLPwd',
                        action='store_true',
                        help='Request to be asked for mySQLPwd for operating MySQL;\n' +\
                             '    default: content of scriptInvokingUser$Home/.ssh/mysql if --mySQLUser is unspecified,\n' +\
                             '    or, if specified mySQLUser is root, then the content of scriptInvokingUser$Home/.ssh/mysql_root.'
                        )
    parser.add_argument('-w', '--password',
                        action='store',
                        help='User explicitly provided password to log into MySQL.\n' +\
                             '    default: content of scriptInvokingUser$Home/.ssh/mysql if --mySQLUser is unspecified,\n' +\
                             '    or, if specified mySQLUser is root, then the content of scriptInvokingUser$Home/.ssh/mysql_root.'
                        )

    parser.add_argument('-m', '--mappingFile',
                        action='store',
                        help='Path to file that maps Piazza IDs to LTI IDs. If not present, then the zip file\n' +\
                             '    must contain a file called account_mapping.csv'
                        )
    
    parser.add_argument('dbname',
                        action='store',
                        help='Name of MySQL database into which forum data is to be placed.' 
                        ) 
    
    parser.add_argument('tablename',
                        action='store',
                        help='Name of MySQL database into which forum data is to be placed.' 
                        ) 
    
    parser.add_argument('jsonFileName',
                        action='store',
                        help='Either a zipped Piazza dump, which contains a file named class_content.json, or ' +\
                        'a .json file that contains the class content in JSON format.'
                        ) 
    
    args = parser.parse_args();
    if args.mySQLUser is None:
        mySQLUser = getpass.getuser()
    else:
        mySQLUser = args.mySQLUser

    if args.password and args.pwd:
        raise ValueError('Use either -p, or -w, but not both.')
        
    if args.mySQLPwd:
        mySQLPwd = getpass.getpass("Enter %s's MySQL password on localhost: " % mySQLUser)
    elif args.password:
        mySQLPwd = args.password
    else:
        # Try to find mySQLPwd in specified mySQLUser's $HOME/.ssh/mysql
        currUserHomeDir = os.getenv('HOME')
        if currUserHomeDir is None:
            mySQLPwd = None
        else:
            # Don't really want the *current* mySQLUser's homedir,
            # but the one specified in the -u cli arg:
            userHomeDir = os.path.join(os.path.dirname(currUserHomeDir), mySQLUser)
            try:
                if mySQLUser == 'root':
                    with open(os.path.join(currUserHomeDir, '.ssh/mysql_root')) as fd:
                        mySQLPwd = fd.readline().strip()
                else:
                    with open(os.path.join(userHomeDir, '.ssh/mysql')) as fd:
                        mySQLPwd = fd.readline().strip()
            except IOError:
                # No .ssh subdir of mySQLUser's home, or no mysql inside .ssh:
                mySQLPwd = ''

    # -------------- Run the Loading ---------------

    piazzaImporter = PiazzaImporter(args.dbname, 
                                    mySQLUser,
                                    mySQLPwd,
                                    args.tablename, 
                                    args.jsonFileName, 
                                    mappingFile=args.mappingFile if args.mappingFile else None)
    piazzaImporter.doImport()