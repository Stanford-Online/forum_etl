'''
Created on May 23, 2014

@author: paepcke
'''

import json

class PiazzImporter(object):
    '''
    classdocs
    '''


    def __init__(self, jsonFileName):
        with open(jsonFileName, 'r') as jsonFd:
            self.jData = json.load(jsonFd)
    
        