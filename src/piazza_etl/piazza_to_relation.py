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

from UserDict import DictMixin
import argparse
import base64
import csv
import getpass
import hashlib
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

    singletonPiazzaImporter = None
    
    STANDARD_CONTENT_FILE_NAME = 'class_content.json'
    STANDARD_MAPPING_FILE_NAME = 'account_mapping.csv'
    
    # How many rows to skip at start of mapping file
    # (skip past header):
    MAPPING_FILE_ROW_SKIPS     = 1
    
    MYSQL_PIAZZA_DB = 'Edx_Piazza'
    
    # Db in MySQL that holds function
    # idExt2Anon()
    CONVERT_FUNCTIONS_DB = 'EdxPrivate'
  
    # Cache of materialized Piazza Post objects.
    # Keys will be computed from the objs themselves,
    # using md5 with urlsafe_64encode:
    postObjs = {}


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

        if PiazzaImporter.singletonPiazzaImporter is not None:
            return PiazzaImporter.singletonPiazzaImporter
        
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

    def importJsonContentFromPiazzaZip(self, zipContentFileName):
        '''
        Given a JSON file with all of one class' Piazza forum data,
        import the JSON, creating a messy in-memory data structure
        that is made sense of by 'get-'methods like getSubject().
        The file may be a stand-alone JSON file, or it can be 
        inside a zip file, of which zipContentFileName is the name. In that
        case the JSON within the zip file must be named class_contents.json.
        
        :param zipContentFileName: name of file, or zip file with JSON encoded Piazz forum content
        :type zipFileName: String
        '''

        contentFd = None
        try:
            if zipfile.is_zipfile(zipContentFileName):
                zipObj = zipfile.ZipFile(zipContentFileName)
                contentFd = zipObj.open(PiazzaImporter.STANDARD_CONTENT_FILE_NAME)
                fileNameList = zipObj.namelist()
                if not PiazzaImporter.STANDARD_CONTENT_FILE_NAME in fileNameList:
                    raise ValueError('Zip file %s does not contain a file %s.' % (zipContentFileName, PiazzaImporter.STANDARD_CONTENT_FILE_NAME))
            else:
                contentFd = open(zipContentFileName, 'r')

            jsonArr = contentFd.readlines()
            jsonStr = ''.join([line.strip() for line in jsonArr])
            self.jData = json.loads(jsonStr)
            #print('Read')
        finally:
            if contentFd is not None:
                contentFd.close()
    
    def createPiazzaId2Anon(self, mappingOrZipFile):
        '''
        Create a dict mapping Piazza IDs to anon_screen_name
         
        :param mappingOrZipFile: CSV file containing a mapping triplet email, PiazzaId, strWithLTI,
            as illustrated in class header comment. Or: a zip file that contains
            a file named account_mapping.csv with the same mapping.
        :type mappingOrZipFile: String
        '''
        
        self.piazza2Anon = {}
        mappingFd = None
        
        # Pull all mapping lines out of the csv
        # into a memory resident array of strings:
        if zipfile.is_zipfile(mappingOrZipFile):
            zipObj = zipfile.ZipFile(mappingOrZipFile)
            mappingFd = zipObj.open(PiazzaImporter.STANDARD_MAPPING_FILE_NAME)
        else:
            mappingFd = open(mappingOrZipFile, 'r')

        csvReader = csv.reader(mappingFd)
        # Skip past the header row(s):
        linesToSkip = PiazzaImporter.MAPPING_FILE_ROW_SKIPS
        while linesToSkip > 0:
            next(csvReader)
            linesToSkip -= 1
                
        # Need to use a MySQL function to map from the
        # LTI (Ext) ID to anon_screen_name:
        try:
            db = None
            db = MySQLDB(user=self.mysqlUser, passwd=self.mysqlPwd,  db=PiazzaImporter.CONVERT_FUNCTIONS_DB)
            # Skipping past header line, convert one Pizza UID after another:
            for mappingRow in csvReader:
                
                # Rows are like: ['myemail@gmail.com,hr7xjaytsC8,stanford.edu__aff1b14edf5054292a31e584b4749f42'],
                # but also like ['myemail@gmail.com,hr7xjaytsC8,"stanford.edu__88115, stanford.edu__47bf69315b7391dace7ccbc344690969"]

                (email, piazzaUID, stanfordLtiUid) = mappingRow[0:3]  # @UnusedVariable
                # Grab the last of the __-separated pieces:
                ltiUid = stanfordLtiUid.split('__')[-1].strip()
                for anon in db.query("SELECT idExt2Anon('%s');" % ltiUid.strip()):
                    try:
                        self.piazza2Anon[piazzaUID] = anon[0]
                    except IndexError:
                        self.logWarn("No anon_screen_name for Piazza UID '%s' (a.k.a. external (LTI) id '%s'" % (piazzaUID, ltiUid))
        finally:
            if db is not None:
                db.close()
            if mappingFd is not None:
                mappingFd.close()
    
    
    # ----------------------------------------  Getters ------------------------------------------
    
    def getChildArr(self, jsonObj):
        '''
        Return an array that contains the children internalized
        JSON structures of the given JSON object structure.
        
        :param jsonObj:
        :type jsonObj:
        '''
        return jsonObj.get('children', None)
                
    
    def getPosterUidAnon(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return self.piazza2Anon.get(jsonObjArrOrObj[arrIndex].get('id',None), None)
        except (TypeError, IndexError) :
            return None
          
    def getSubject(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('history', None)[0].get('subject', None)
        except (TypeError, IndexError, KeyError):
            return None

    def getContent(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex]['history'][0].get('content', None)
        except (TypeError, IndexError, KeyError):
            return None

    def getTags(self, jsonObjArrOrObj, arrIndex=0):
        '''
        Return post's tags as a Python array of strings.
        
        :param arrIndex:
        :type arrIndex:
        '''
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('tags', None)
        except IndexError:
            return None
    
    def getPiazzaId(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('id', None)
        except IndexError:
            return None
          
    def getStatus(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('status', None)
        except IndexError:
            return None
    
    def getNoAnswerFollowup(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('no_answer_followup', None)
        except IndexError:
            return None       
    
    def getCreationDate(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('created', None)
        except IndexError:
            return None
    
    def getPostType(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('type', None)
        except IndexError:
            return None
    
    def getTagGoodAnons(self, jsonObjArrOrObj, arrIndex=0):
        '''
        Returns a Python array of anon_screen_name
        who tagged post as good (tag_good_arr')
        
        :param arrIndex:
        :type arrIndex:
        '''
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        anons = []
        try:
            for piazzaId in jsonObjArrOrObj[arrIndex].get('tag_good_arr', None):
                anons.append(self.idPiazza2Anon(piazzaId))
        except (IndexError, TypeError):
            pass
        return anons
    
    def getTagEndorseAnons(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        anons = []
        try:
            for piazzaId in jsonObjArrOrObj[arrIndex].get('tag_endorse_arr', None):
                anons.append(self.idPiazza2Anon(piazzaId))
        except (IndexError, TypeError):
            pass
        return anons
    
    def getNumUpVotes(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('no_upvotes', None)
        except IndexError:
            return None
            
    def getNumAnswers(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        try:
            return jsonObjArrOrObj[arrIndex].get('no_answer', None)
        except IndexError:
            return None        
    
    def getIsAnonPost(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('anon', 'no')
        except IndexError:
            return None
            
    def getBucketName(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('bucket_name', None)
        except IndexError:
            return None
    
    def getUpdated(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return jsonObjArrOrObj[arrIndex].get('updated', None)
        except IndexError:
            return None
    
    def getFolders(self, jsonObjArrOrObj, arrIndex=0):
        if jsonObjArrOrObj is None:
            jsonObjArrOrObj = self.jData
        if not type(jsonObjArrOrObj) == list:
            jsonObjArrOrObj = [jsonObjArrOrObj]
        try:
            return ','.join(jsonObjArrOrObj[arrIndex].get('folders', None))
        except IndexError:
            return None
    

    # ----------------------------------------  Make  PiazzaImport Act Like a Read-Only List of JSON dicts ------------------------------------------
    
    def __getitem__(self, offsetOrObjId):
        
        if type(offsetOrObjId) == int:
            # Behave like a list.
            # offsetOrObjId is an offset into the original JSON list:
            objDict = list.__getitem__(self.jData, offsetOrObjId)
            return self.findOrCreatePostObj(objDict)
        
        elif type(offsetOrObjId) == dict:
            # Behave like a PiazzaPost instance factory:
            return self.findOrCreatePostObj(offsetOrObjId)
        
        else: # offsetOrObjId is the obj ID of an already materialized obj
            # Behave like a dict:
            return PiazzaImporter.postObjs[offsetOrObjId]

    def findOrCreatePostObj(self, jsonDict):
        objId = PiazzaImporter.makeHashFromJsonDict(jsonDict)
        try:
            return PiazzaImporter.getObjFromOid(objId)
        except KeyError:
            obj = PiazzaPost.createInstance(jsonDict, objId)
            PiazzaImporter.postObjs[objId] = obj
            return obj
            
    @classmethod
    def getObjFromOid(cls, oid):
        '''
        Return a previously materialized post obj, given
        an oid. KeyError if obj does not exist.
        
        :param cls:
        :type cls:
        :param oid:
        :type oid:
        '''
        return cls.postObjs[oid]
    
    def __iter__(self):
        return self.jData.__iter__()
    
    # ----------------------------------------  Utilities ------------------------------------------

    @classmethod
    def makeHashFromJsonDict(cls, jsonDict):
        return base64.urlsafe_b64encode(hashlib.md5(str(jsonDict)).digest())
    
    def idPiazza2Anon(self, piazzaId):
        try:
            return self.piazza2Anon[piazzaId]
        except KeyError:
            return None
            

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
            
    
class PiazzaPost(DictMixin):    
    '''
    Wraps one dict that represents a Piazza Post
    '''

    @classmethod
    def createInstance(cls, jsonDict, oid):
        resObj = PiazzaPost(jsonDict)
        resObj['oid'] = oid
        return resObj
    
    def __init__(self, jsonDict):
        self.nameValueDict = jsonDict
        # Get the screen name in the clear:

    def __getitem__(self, key):
        jsonValue = self.nameValueDict[key]
        if type(jsonValue) == list:
            jsonValueArr = []
            for jsonValueEl in jsonValue:
                jsonValueArr.append(PiazzaImporter.singletonPiazzaImporter[jsonValueEl]) 
            return jsonValueArr
        else:
            return jsonValue
    
    def __setitem__(self, key, value):
        self.nameValueDict[key] = value
    
    def __delitem__(self, key):
        del self.nameValueDict[key]
    
    def keys(self):
        return self.nameValueDict.keys()
    
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