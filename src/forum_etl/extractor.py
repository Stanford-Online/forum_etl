from pymongo import MongoClient
import subprocess
from datetime import datetime
from pymysql_utils.pymysql_utils import MySQLDB

import pickle
import MySQLdb
import re
import warnings
import logging
import sys
import getpass
import os
def getmysqlpasswd():
    homeDir=os.path.expanduser('~'+getpass.getuser())
    f_name=homeDir+'/.ssh/mysql'
    with open(f_name, 'r') as f:
        password = f.readline().strip()
    print 'password got from file is %s'%(password) 
    return password




mysql_dbhost='localhost'
mysql_user=getpass.getuser()

mysql_db='EdxForum'
mydb=MySQLDB('127.0.0.1',3306,'jagadish','5PinkPenguines','EdxPrivate')

mongo_database_name='TmpForum';
collection_name='ForumContents';

bson_filename='/hme/jagadish/edx/forums_dump_20131018/app15682028/EdxForum.contents.bson'
if(len(sys.argv)!=2):
    print 'Usage: Provide one argument <filename>'
    sys.exit(0)
bson_filename=sys.argv[1]
print bson_filename




filename_='forum_%s.log'%(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
#sys.stderr=open(filename_,'w')
#sys.stdout=open(filename_,'w')
logging.basicConfig(filename=filename_,level=logging.DEBUG)




#--------------------------------------------------------------------
#declare variables and execute statements preparing the database to 
#configure options - eg: setting char set to utf, connection type to utf
#truncating the already existing table.
#---------------------------------------------------------------------

counter=0;


mysql_passwd=getmysqlpasswd()

logging.debug('got passwd from file %s'%(mysql_passwd))

try:
    #mysqldb=MySQLdb.connect(host=mysql_dbhost,user=mysql_user,passwd=mysql_passwd,db=mysql_db)
   
    mysqldb=MySQLdb.connect(host=mysql_dbhost,user=mysql_user,passwd=mysql_passwd)
    logging.debug("Connection to MYSql db successful %s"%(mysqldb))
    cur=mysqldb.cursor();
    mysqldb.set_character_set('utf8')
    logging.debug("Setting and assigning char set for mysqld. will truncate old values")
    mydb.execute('SET NAMES utf8;');
    mydb.execute('SET CHARACTER SET utf8;');
    mydb.execute('SET character_set_connection=utf8;');
    mydb.execute('truncate table EdxForum.contents');
    logging.debug("setting and assigning char set complete. Truncation succeeded")

except MySQLdb.Error,e:
    logging.info("MySql Error exiting %d: %s" % (e.args[0],e.args[1]))
    print e
    sys.exit(1)

    #pattern='(.*)\\s+([a-zA-Z0-9\\.]+)\\s*(\\(f.*b.*)?(@)\\s*([a-zA-Z0-9\\.\\s;]+)\\s*(\\.)\\s*(edu|com)\\s+(.*)'
warnings.filterwarnings('ignore', category=MySQLdb.Warning)
#----------------------------------------------------------------------
#pattern for email id - strings of alphabets/numbers/dots/hyphens followed
#by an @ or at followed by combinations of dot/. followed by the edu/com
#also, allow for spaces
#----------------------------------------------------------------------
pattern='(.*)\s+([a-zA-Z0-9\(\.\-]+)[@]([a-zA-Z0-9\.]+)(.)(edu|com)\\s*(.*)'




compiledRe=re.compile(pattern);
userCache={};
userSet=set()


'''def getData():
    try:
        cur.execute("select body from EdxForum.contents");
        for c in cur.fetchall():
            print c[0]
    except MySQLdb.Error,e:
        logging.info("MySql Error while user cache exiting %d: %s" % (e.args[0],e.args[1]))
        sys.exit(1)'''

#Populate the User Cache and preload information on user id int, screen name
#and the actual name

def populateUserCache () : 
    global userSet
    try:
        logging.info("Beginning to populate user cache");
        cur.execute("select user_int_id,name,screen_name,anon_screen_name from EdxPrivate.UserGrade");
        #for c in cur.fetchall() :
        for c in mydb.query('select user_int_id,name,screen_name,anon_screen_name from EdxPrivate.UserGrade'):
            obj=[];
            obj.append(c[1]);
            obj.append(c[2]);
            obj.append(c[3]);

            l1=c[1].split()
        
            if len(l1)>0:
                userSet.add(l1[0])

            """for word in l1:
                if(len(word)>2 and '\\'    not in repr(word) ):
                    userSet.add(word)
                    if '-' in word:
                        l2=word.split('-')
                        for data in l2:
                            userSet.add(data)"""
            """if(len(c[1])>0):
                userSet|=set([c[1]])
                userSet|=set(c[2])"""
                

     
            userCache[int(c[0])]=obj;    
        logging.info("loaded objects in usercache %d"%(len(userCache)))
        pickle.dump( userSet, open( "user.p", "wb" ) )

        print userSet
    except MySQLdb.Error,e:
        logging.info("MySql Error while user cache exiting %d: %s" % (e.args[0],e.args[1]))
        sys.exit(1)

    
populateUserCache();

#prunes phone numbers from a given string and returns the string without phonenumber
def prune_numbers(body):
#re from stackoverflow. seems to do an awesome job at capturing all phone nos :)
    s='((?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?)'
    match=re.findall(s,body)
    for m in match:
        body=body.replace(m[0],"<phoneRedac>")
    return body    

#prunes the zipcdoe from a given string and returns the string without zipcode
def prune_zipcode(body):
    s='\d{5}(?:[-\s]\d{4})?'
    match=re.findall(s,body)
    for m in match:
        body=body.replace(m[0],"zipRedac")
    return body






def trimnames(body):
    return body
    #Trims all firstnames and last names from the body of the post.
    
    #print 'processing body %s' %(body)
    #print 'en %s' %(len(userSet))
    s3=set(body.split())
    s4=s3&userSet
    #print 's4 is %s' %(s4)
    
    for s in s4:
        if len(s)>1 and s[0].isupper():
            body = re.sub(r"\b%s\b" % s , "NAME_REMOVED", body)

            #body=body.replace(s,"NAME_REMOVED")
    
 
    return body


def insert_content_record(_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes,count,down_count,up_count,up,down):
    global counter
    #print len(userCache);
    #line='\t'.join(data);
    #f.write(line+'\n');    
    counter=counter+1;
    
    body=body.encode('utf-8').strip();


    body=prune_numbers(body) 
    body=prune_zipcode(body)

    if compiledRe.match(body) != None :
        #print 'BODY before EMAIL STRIPING %s \n'%(body);
        match=re.findall(pattern,body);
        new_body=" ";
        for m in match:
            new_body+=(m[0]+" <emailRedac> " + m[-1]);
        #print 'NEW BODY AFTER EMAIL STRIPING %s \n'%(new_body);
        body=new_body;
        
    user_info=userCache.get(int(author_id),['xxxx','xxxx']);
    name=user_info[0];
    screen_name=user_info[1];

    if(len(user_info)==3):
        anon_s=user_info[2]
        anon_s=" "
    else:
        anon_s=" "

    #flag=0;
    #body=body.encode('utf-8').strip();
    try:
     for s in name.split() :
        if len(s) >= 3:
            #flag=1;
            if s.lower() in body.lower():
                #print 'NEW BODY found before NAME STRIPING %s \n'%(body);
                pat=re.compile(s,re.IGNORECASE);
                anon_s=''
                body=pat.sub("<nameRedac_"+anon_s+">",body);
                #body.replace(s,"<NAME REMOVED>");
    except:
     print 'blah %s --'%(body)
     #print 'blah %s -- %s'%(body,s)

    pat1=re.compile(screen_name,re.IGNORECASE);
    body=pat1.sub("<nameRedac_"+anon_s+">",body);    
 
    body=trimnames(body)
 
#    if ('REMOVED' in body or 'CLIPPED' in body) :
#         print 'NEW COMBINED BODY AFTER NAME STRIPING %s \n'%(body);
    try:
#        cur.execute("insert into EdxForum.contents(type,anonymous,anonymous_to_peers,at_position_list,user_int_id,body,course_display_name,created_at,votes,count,down_count,up_count,up,down) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes,count,down_count,up_count,up,down));
#        print "BOOHOO %s %s %s %s %s %s %s blah %s"%(_type,anonymous,anonymous_to_peers,at_position_list,author_id,course_id,created_at,str(body))
#        print "insert into EdxForum.contents(type,anonymous,anonymous_to_peers,at_position_list,user_int_id,body,course_display_name,created_at,votes,count,down_count,up_count,up,down) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%(_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes,count,down_count,up_count,up,down)
#        print "insert into EdxForum.contents(type,anonymous,anonymous_to_peers,at_position_list,user_int_id,body,course_display_name,created_at,votes,count,down_count,up_count,up,down) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%tup
        #body='d'
       # print 'inserting body %s'%(body)
        #mydb.executeParameterized('insert into EdxForum.contents(anonymous,body) values (%s,%s)',(anonymous,body))

        mydb.executeParameterized("insert into EdxForum.contents(type,anonymous,anonymous_to_peers,at_position_list,user_int_id,body,course_display_name,created_at,votes,count,down_count,up_count,up,down) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes,count,down_count,up_count,up,down));
        
    except MySQLdb.Error,e:
        logging.info("MySql Error exiting while inserting record %d: %s auhtorid %s created_at %s " % (e.args[0],e.args[1],author_id,created_at))
        logging.info(" values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%(_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes,count,down_count,up_count,up,down))
        sys.exit(1)

    if(counter%100==0):
        logging.info('inserted record %d'%( counter))
    # print '_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes
    #print '%d value \n'%(counter);

mongoclient=MongoClient();


db=mongoclient[mongo_database_name];

#fix collection name
collection=db[collection_name];

logging.info('Preparing to delete the collection ')
collection.remove()
logging.info('Deleting mongo collection completed.Will now attempt a mongo restore')

command='mongorestore %s -db %s -c %s'%(bson_filename,mongo_database_name,collection_name)
print command
logging.info('spawning subprocess to execute mongo restore')

with open(filename_,'w') as outfile:
    ret=subprocess.call(["mongorestore",bson_filename,"-db",mongo_database_name,"-c",collection_name],stdout=outfile,stderr=outfile)
print 'ret val is %s'%(ret)


logging.info('will start inserting from mongo collection to mysql')

for c in collection.find():
    _type=str(c['_type']);
    anonymous=str(c['anonymous']);
    anonymous_to_peers=str(c['anonymous_to_peers']);
    at_position_list=str(c['at_position_list']);
    author_id=c['author_id'];
    body=c['body'];
    course_id=str(c['course_id']);
    created_at=c['created_at'];
    votes=str(c['votes']); 
    votesObject=c['votes']
    count=votesObject['count']
    down_count=votesObject['down_count']
    up_count=votesObject['up_count']
    up=str(votesObject['up']).replace("u","")
    down=str(votesObject['down']).replace("u","")

    try:
        up_list=list(up)
    except Exception as e:
        logging.info('Error in conversion' + `e`)
        up='-1'
    
    insert_content_record(_type,anonymous,anonymous_to_peers,at_position_list,author_id,body,course_id,created_at,votes,count,down_count,up_count,up,down);
    
 
mysqldb.commit();    
logging.info('commit completed!')

#    print c



"""collectionObject=collection.find_one();



def generateInsertQuery(collectionobject)
    insertStatement='insert into EdxForum.contents(%s) values(%s)';
    cols=', '.join(collectionobject);
    vals=', '.join('?'*len(collectionobject));
    query=insertStatement%(cols,vals)
    db=MySQLdb.connect(host="localhost",user='root',passwd='root',db='test')
    cur=db.cursor();
    cur.execute();
"""
