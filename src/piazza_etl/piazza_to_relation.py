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

Some leniency in column names:
    # Some columns mean the same in the OpenEdX
    # forum and the Piazza forum. Allow use of
    # both names here. Key: alternate name,
    # value: Piazza-native name: 
    SCHEMA_NAME_EQUIVALENCES = {'body' : 'content',
                                'creation_date' : 'create_date',
                                'created_at' : 'create_date', 
                                'good_tags' : 'tag_good_arr',
                                'endorse_tags' : 'tag_endorse_arr'
                                }
    
    

    SCHEMA_NAME_EQUIVALENCES = {'body' : 'content',
                                'creation_date' : 'create_date',
                                'created_at' : 'create_date', 
                                'good_tags' : 'tag_good_arr',
                                'endorse_tags' : 'tag_endorse_arr'
                                }



Endorsement:
   anon_screen_name, endorsement_type, endorsement_post

'''

#from UserDict import DictMixin
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


class PiazzaImporterMetaclass(type):
    
    def __init__(self, className, bases, namespace):

        super(PiazzaImporterMetaclass, self).__init__(className, bases, namespace)
        if not hasattr(self, 'piazzaImporterInstance'):
            self.singletonPiazzaImporter = None
    
    def __call__(self, *args, **kwdargs):

        if self.singletonPiazzaImporter is not None:
            return self.singletonPiazzaImporter
                # Call the PiazzaPost class' init method:

        # Call PiazzaImporter's __init__() method:
        newPiazzaImporterObj = super(PiazzaImporterMetaclass, self).__call__(*args, **kwdargs)
        self.singletonPiazzaImporter = newPiazzaImporterObj
        return newPiazzaImporterObj


class PiazzaImporter(object):
    '''
    classdocs
    '''
    __metaclass__ = PiazzaImporterMetaclass
    
    STANDARD_CONTENT_FILE_NAME = 'class_content.json'
    STANDARD_MAPPING_FILE_NAME = 'account_mapping.csv'
    
    # How many rows to skip at start of mapping file
    # (skip past header):
    MAPPING_FILE_ROW_SKIPS     = 1
    
    MYSQL_PIAZZA_DB = 'Edx_Piazza'
    
    # Db in MySQL that holds function
    # idExt2Anon()
    CONVERT_FUNCTIONS_DB = 'Edx'
    
    # Dict to hold map between Piazza 'id' field, and anon:
    piazza2Anon = {}

    logger = None
  
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
        return None


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
                if len(ltiUid) == 0:
                    PiazzaImporter.logWarn('Piazza user id %s has an empty LTI mapping in mapping file.' % ltiUid)
                    continue
                for anon in db.query("SELECT %s.idExt2Anon('%s');" % (PiazzaImporter.CONVERT_FUNCTIONS_DB,ltiUid.strip())):
                    try:
                        PiazzaImporter.piazza2Anon[piazzaUID] = anon[0]
                    except IndexError:
                        PiazzaImporter.logWarn("No anon_screen_name for Piazza UID '%s' (a.k.a. external (LTI) id '%s'" % (piazzaUID, ltiUid))
        finally:
            if db is not None:
                db.close()
            if mappingFd is not None:
                mappingFd.close()
    
    @classmethod
    def resolveLTIToAnon(cls, piazzaId):
        '''
        Given a Piazza ID, return a corresponding
        anon_screen_name. Piazza provides a mapping
        file from their IDs to LTI ids, which method
        createPiazzaId2Anon() will have materialized
        into a dict mapping Piazza ID to anon_screen_name
        of users in other tables in the database.
        
        When a piazzaId is not found in the dict, b/c
        the Piazza company has not provided the mapping,
        an anon_screen_name is created that is clearly recognizable
        as an unmappable Piazza ID, but which is unique,
        and the same for multiple calls to this method
        with the same piazzaId.
        
        Note: for this method to work, first call createPiazzaId2Anon(). 
        
        :param cls:
        :type cls:
        :param piazzaId:
        :type piazzaId:
        '''
        try:
            return cls.piazza2Anon[piazzaId]
        except KeyError:
            madeUpAnon = cls.makeUnknowAnonScreenName(piazzaId)
            PiazzaImporter.logWarn("Piazza id with missing LTI mapping; assigning %s" % madeUpAnon)
            return madeUpAnon 
    
    @classmethod
    def makeUnknowAnonScreenName(cls, piazzaId):
        '''
        Given a Piazza user id, return a recognizable,
        but unique anon_user_id. Given the same
        piazzaId, the return value will always be
        the same. 
        
        Used when Piazza company's account name mapping
        does not contain a mapping for one of their 
        own IDs 
        
        :param cls:
        :type cls:
        :param piazzaId:
        :type piazzaId:
        '''
        return 'unknown_piazza_mapping_' + piazzaId        
    
    # ----------------------------------------  Getters ------------------------------------------
    
    def getChildArr(self, jsonObj):
        '''
        Return an array that contains the children internalized
        JSON structures of the given JSON object structure.
        
        :param jsonObj:
        :type jsonObj:
        '''
        return jsonObj.get('children', None)
                
    
    def getPosterUidAnon(self, oidOrDict):
        '''
        Return a anon_screen_name type UID, given
        either a PiazzaPost OID, or a dict that
        represents a raw JSON object. 
        
        If parameter is an OID, simply return
        that object's anon_screen_name attribute.
        Else look for field name 'id', get its 
        (Piazza UID type) value, and look up the
        equivalent anon_screen_name. If that mapping
        is unknown, return None. 
        
        :param oidOrDict: a PiazzaPost object id, or a JSON object dict
        :type oidOrDict: {PiazzaPost | dict}
        '''
        
        if type(oidOrDict) == basestring:
            try:
                oid = oidOrDict
                return PiazzaImporter[oid]['anon_screen_name']
            except KeyError:
                raise ValueError('Value %s is not a PiazzaPost instance identifier.' % oidOrDict)

        # Parameter is a dict, i.e. a JSON object.
        # Get its (Piazza) 'id' attribute, and look
        # up the equivalent anon_screen_name: 
        try:
            theDict = oidOrDict
            return self.piazza2Anon.get(theDict.get('id',None))
        except KeyError:
            return None
          
    def getSubject(self, oidOrDictOrPiazzaPostObj):

        if isinstance(oidOrDictOrPiazzaPostObj, basestring):
            oid = oidOrDictOrPiazzaPostObj
            try:
                piazzaObj = PiazzaImporter.singletonPiazzaImporter[oid]
            except KeyError:
                raise KeyError("No PiazzaPost object with OID %s is known." % piazzaObj)
            
        elif type(oidOrDictOrPiazzaPostObj) == dict or isinstance(oidOrDictOrPiazzaPostObj, PiazzaPost): 
            piazzaObj = oidOrDictOrPiazzaPostObj
            
        try:
            historyArr = piazzaObj['history']
        except (KeyError):
            raise KeyError("Dict parameter (%s) contains no 'history' array." % str(piazzaObj))
        try:
            return historyArr[0]['subject']
        except KeyError:
            raise ValueError("History array's first dict element contains no 'subject' entry (%s)." % str(historyArr))
        except IndexError:
            raise ValueError("Dict parameter 'history' in an empty array (%s)." % str(piazzaObj))
            

    def getContent(self, oidOrDictOrPiazzaPostObj):

        if isinstance(oidOrDictOrPiazzaPostObj, basestring):
            oid = oidOrDictOrPiazzaPostObj
            try:
                piazzaObj = PiazzaImporter.singletonPiazzaImporter[oid]
            except KeyError:
                raise KeyError("No PiazzaPost object with OID %s is known." % piazzaObj)
            
        elif type(oidOrDictOrPiazzaPostObj) == dict or isinstance(oidOrDictOrPiazzaPostObj, PiazzaPost): 
            piazzaObj = oidOrDictOrPiazzaPostObj
            
        try:
            historyArr = piazzaObj['history']
        except (KeyError):
            raise KeyError("Dict parameter (%s) contains no 'history' array." % str(piazzaObj))
        try:
            return historyArr[0]['content']
        except KeyError:
            raise ValueError("History array's first dict element contains no 'subject' entry (%s)." % str(historyArr))
        except IndexError:
            raise ValueError("Dict parameter 'history' in an empty array (%s)." % str(piazzaObj))
            

    #*********** Continue testing here


#     def getTags(self, jsonObjArrOrObj, arrIndex=0):
#         '''
#         Return post's tags as a Python array of strings.
#         
#         :param arrIndex:
#         :type arrIndex:
#         '''
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('tags', None)
#         except IndexError:
#             return None
#     
#     def getPiazzaId(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('id', None)
#         except IndexError:
#             return None
#           
#     def getStatus(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('status', None)
#         except IndexError:
#             return None
#     
#     def getNoAnswerFollowup(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('no_answer_followup', None)
#         except IndexError:
#             return None       
#     
#     def getCreationDate(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('created', None)
#         except IndexError:
#             return None
#     
#     def getPostType(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('type', None)
#         except IndexError:
#             return None
    
    def getTagGoodAnons(self, oidOrDictOrPiazzaPostObj):
        if isinstance(oidOrDictOrPiazzaPostObj, basestring):
            oid = oidOrDictOrPiazzaPostObj
            try:
                piazzaObj = PiazzaImporter.singletonPiazzaImporter[oid]
            except KeyError:
                raise KeyError("No PiazzaPost object with OID %s is known." % piazzaObj)
            
        elif type(oidOrDictOrPiazzaPostObj) == dict or isinstance(oidOrDictOrPiazzaPostObj, PiazzaPost): 
            piazzaObj = oidOrDictOrPiazzaPostObj

        anons = []

        for piazzaId in piazzaObj.get('tag_good_arr', None):
            if piazzaId is None:
                continue
            anons.append(self.idPiazza2Anon(piazzaId))

        return anons
            
    
    def getTagEndorseAnons(self, oidOrDictOrPiazzaPostObj):
        if isinstance(oidOrDictOrPiazzaPostObj, basestring):
            oid = oidOrDictOrPiazzaPostObj
            try:
                piazzaObj = PiazzaImporter.singletonPiazzaImporter[oid]
            except KeyError:
                raise KeyError("No PiazzaPost object with OID %s is known." % piazzaObj)
            
        elif type(oidOrDictOrPiazzaPostObj) == dict or isinstance(oidOrDictOrPiazzaPostObj, PiazzaPost): 
            piazzaObj = oidOrDictOrPiazzaPostObj

        anons = []

        for piazzaId in piazzaObj.get('tag_endorse_arr', None):
            if piazzaId is None:
                continue
            anons.append(self.idPiazza2Anon(piazzaId))

        return anons
            
    
#     def getNumUpVotes(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('no_upvotes', None)
#         except IndexError:
#             return None
#             
#     def getNumAnswers(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         try:
#             return jsonObjArrOrObj[arrIndex].get('no_answer', None)
#         except IndexError:
#             return None        
#     
#     def getIsAnonPost(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('anon', 'no')
#         except IndexError:
#             return None
#             
#     def getBucketName(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('bucket_name', None)
#         except IndexError:
#             return None
#     
#     def getUpdated(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return jsonObjArrOrObj[arrIndex].get('updated', None)
#         except IndexError:
#             return None
#     
#     def getFolders(self, jsonObjArrOrObj, arrIndex=0):
#         if jsonObjArrOrObj is None:
#             jsonObjArrOrObj = self.jData
#         if not type(jsonObjArrOrObj) == list:
#             jsonObjArrOrObj = [jsonObjArrOrObj]
#         try:
#             return ','.join(jsonObjArrOrObj[arrIndex].get('folders', None))
#         except IndexError:
#             return None
    

    # ----------------------------------------  Make  PiazzaImport Act Like a Read-Only List of Dicts ------------------------------------------
    
    # Want ability for instance of PiazzaImporter to act as a list
    # of PiazzaPost instances. Those instances correspond to the top
    # level JSON array of JSON object structures. 
    #
    # Also want ability to retrieve a PiazzaPost obj from an
    # instance of PiazzaImporter by providing the OID of one of
    # the PiazzaPost objects. We provide that facility via a
    # dict behavior.
    #
    # Finally: want ability to act as iterator over a list
    # of objects
    
    # Dict functionality
    def __getitem__(self, offsetOrObjId):
        '''
        Given a list index or a JSON object dictionary,
        or a PiazzaPost oid, return a PiazzaPost instance
        if possible. List index identifies one of the 
        top level JSON dicts in the JSON array. 
        
        :param offsetOrObjId: list index, JSON dict, or oid that describe desired PiazzaPost instance
        :type offsetOrObjId: {int | dict | String}
        :return desired PiazzaPost instance, if found
        :rtype PiazzaPos
        :raise KeyError
        '''
        
        if type(offsetOrObjId) == int:
            # Behave like a list.
            # offsetOrObjId is an offset into the original JSON list:
            objDict = list.__getitem__(self.jData, offsetOrObjId)
            return self.findOrCreatePostObj(objDict)
        
        elif type(offsetOrObjId) == dict:
            # Behave like a PiazzaPost instance factory:
            return self.findOrCreatePostObj(offsetOrObjId)
        
        else: # offsetOrObjId is the obj ID of an already materialized obj
            # Behave like a dict: return the obj with that ID,
            # or raise KeyError:
            return PiazzaPost.getPiazzaPostObj(offsetOrObjId)
            
    # The iterator functionality:
    class PiazzaImporterIterator(object):
        def __init__(self, piazzaImporterObj):
            self.piazzaImporterObj = piazzaImporterObj
            self.currObjIndex = -1
            
        def next(self):
            self.currObjIndex += 1
            try:
                return self.piazzaImporterObj[self.currObjIndex]
            except IndexError:
                raise StopIteration
            
    def  __iter__(self):
        return PiazzaImporter.PiazzaImporterIterator(self)

    def findOrCreatePostObj(self, jsonDict):
        
        # Find existing instance for this JSON obj (dict),
        # or have a new one made:

        piazzaPostObj = PiazzaPost(jsonDict)

        return piazzaPostObj
        
      
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
        PiazzaImporter.logger = logging.getLogger(os.path.basename(__file__))

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
        PiazzaImporter.logger.addHandler(handler)
        PiazzaImporter.logger.setLevel(loggingLevel)
         
    @classmethod
    def logDebug(cls, msg):
        PiazzaImporter.logger.debug(msg)

    @classmethod
    def logWarn(cls, msg):
        PiazzaImporter.logger.warn(msg)

    @classmethod
    def logInfo(cls, msg):
        PiazzaImporter.logger.info(msg)

    @classmethod
    def logErr(cls, msg):
        PiazzaImporter.logger.error(msg)
            
class PiazzaPostMetaclass(type):
    '''
    Metaclass that governs creation of PiazzaPost instances.
    Imposes a singleton pattern, with existing objects held
    in a class level dict called piazzaPostInstances. Keys
    are OIDs, which are computed from the JSON dict that is
    passed into the __call__() method. 
    '''
    
    def __init__(self, className, bases, namespace):
        super(PiazzaPostMetaclass, self).__init__(className, bases, dict)
        if not hasattr(self, 'piazzePostInstances'):
            self.piazzaPostInstances = {}

    def __call__(self, objIdOrJsonDict):
        '''
        Invoked whenever a PiazzaPost instance is created.
        Checks whether object with given OID or JSON object
        already exists; if so, returns it. Else creates instance,
        initializes its jsonDict instance variable, and
        adds oid and anon_screen_name to the JSON dict.
        
        :param anon_screen_name_uid: new object's anonymized user name
        :type anon_screen_name_uid: String
        :param jsonDict: JSON object (not needed if given an oid****
        :type jsonDict: dict
        :param oid: 
        :type oid:
        '''
        # For readability: figure out which
        # type of parm was passed in, and assign
        # to appropriate var:
        if type(objIdOrJsonDict) == dict:
            jsonDict = objIdOrJsonDict
            oid = None
        elif isinstance(objIdOrJsonDict, basestring):
            oid = objIdOrJsonDict
            jsonDict = None
        else:
            raise ValueError("Must pass either an OID or a JSON dictionary; oid was None, jsonDict was %s" % str(objIdOrJsonDict))
        
        # If caller provided an oid, try to find it.
        # NameError if doesn't exist:
        if oid is not None:
            try:
                return self.piazzaPostInstances[oid]
            except KeyError:
                raise NameError("Object with oid '%s' does not exist." % oid)
        
        # No OID provided. Compute OID from the JSON dict;
        oid = PiazzaImporter.makeHashFromJsonDict(jsonDict)

        # Try to find this OID among the already
        # created instances: caller may make multiple
        # PiazzaPost instantiations with the same JSON
        # object; we'll just find the respective object
        # and return it:
        try:
            return self.piazzaPostInstances[oid]
        except KeyError:
            pass

        # Really don't have this instance yet:
        
        # Call the PiazzaPost class' init method:
        resObj = super(PiazzaPostMetaclass, self).__call__(jsonDict)

        # The JSON dict will become an instance level
        # variable called nameValueDict. Add OID and
        # anon_screen_name to that dict:

        resObj['oid'] = oid
        
        # Remember this object by oid at the
        # class level (i.e. in a class var):
        self.piazzaPostInstances[oid] = resObj

        return resObj

    def __str__(self):
        return self.__name__
    
    
class PiazzaPost(object):    
    '''
    Wraps one dict that represents a Piazza Post
    cashes objects.
    '''
    
    # Make PiazzaPost instantiation work as
    # defined in PiazzaPostMetaclass. I.e.
    # return object if already exists. Only
    # otherwise create a new one (Singleton pattern).
    # Also: compute and initialize oid, store it in
    # instance level new object's JSON dict.
    # It is named nameValueDict, and is an instance
    # level member.
    # Init anon_screen_name in the JSON dict.
    __metaclass__ = PiazzaPostMetaclass

    
    def __init__(self, jsonDict):
        '''
        Note: because PiazzaPostMetaclass is this class'
        metaclass, instantiation of PiazzaPost will 
        call the metaclass' __class__() method, which
        will in turn call this __init__() method. 
        So to add any additional args, that __call__()
        method must also take those args and do the 
        call to this __init__() method with the args. 
        '''
        self.nameValueDict = jsonDict
        # Add anon_screen_name to this instance's attribute:
        piazzaId = jsonDict.get('id', None)
        if piazzaId is None:
            raise ValueError("The JSON dict that is to be a PiazzaPost object does not have the required 'id' attribute (%s)" % str(jsonDict))
        # Find the anon_screen_name that corresponds
        # to the Piazza id in the JSON dict:
        anonId = PiazzaImporter.resolveLTIToAnon(piazzaId)
        self.anon_screen_name = anonId

    @classmethod
    def getPiazzaPostObj(cls, oid):
        '''
        Return instance with given oid if such an
        instance exists. Else raise KeyError.
        
        :param oid: object identifier to check
        :type oid: String
        :return the previously existing PiazzaPost object
        :rtype PiazzaPost
        :raise KeyError if instance with given oid does not exist
        '''
        return PiazzaPost.piazzaPostInstances[oid]

    def __repr__(self):
        return '<PiazzaPost oid=%s>' % self.oid

    def __getitem__(self, key):

        # Oid and anon_screen_name are stored
        # in an instance variable (not in the 
        # JSON dict we keep in each instance:
        if key == 'oid':
            return self.oid
        if key == 'anon_screen_name':
            return self.anon_screen_name

        # Some values are buried in the lower levels
        # of the JSON dict; pick those out, and call
        # the proper retrieval methods:         
        if key == 'subject':
            return PiazzaImporter.singletonPiazzaImporter.getSubject(self)
        
        # Allow 'body' instead of content for compatibility
        # with OpenEdX forum:
        if key == 'content' or key == 'body':
            return PiazzaImporter.singletonPiazzaImporter.getContent(self)
        
        if key == 'tag_good_arr' or key == 'good_tags':
            return PiazzaImporter.singletonPiazzaImporter.getTagGoodAnons(self)

        if key == 'tag_endorse_arr' or key == 'endorse_tags':
            return PiazzaImporter.singletonPiazzaImporter.getTagEndorseAnons(self)

        # Allow create_date and creation_date instead of 'created':
        if key == 'create_date' or key == 'creation_date':
            key = 'created' 
        
        # Allow 'piazza_id' in place of 'id':
        if key == 'piazza_id':
            key = 'id'
        
        jsonValue = self.nameValueDict[key]
        if key == 'children':
            jsonValueArr = []
            for jsonValueEl in jsonValue:
                jsonValueArr.append(PiazzaImporter.singletonPiazzaImporter[jsonValueEl]) 
            return jsonValueArr
        else:
            return jsonValue
    
    def __setitem__(self, key, value):
        
        # Anon_screen_name and oid
        # are kept in instance variables.
        # All others are kept in nameValueDict 
        if key == 'anon_screen_name':
            self.anon_screen_name = value
            return
        elif key == 'oid':
            self.oid = value
            return
        self.nameValueDict[key] = value
    
    def __delitem__(self, key):
        if key == 'anon_screen_name' or key == 'oid':
            raise ValueError('Cannot delete anon_screen_name or oid from PiazzaPost instances')
        del self.nameValueDict[key]
    
    def keys(self):
        theKeys = self.nameValueDict.keys()
        theKeys.extend(['anon_screen_name', 'oid'])
        return theKeys
    
    def get(self, key, default=None):
        return self.nameValueDict.get(key, default)
    
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