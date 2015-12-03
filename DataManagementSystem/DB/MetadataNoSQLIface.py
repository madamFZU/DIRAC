'''
Created on Sep 8, 2015


@author: madam
'''

from DIRAC import S_OK, S_ERROR, gLogger
from __builtin__ import str

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch import helpers
from elasticsearch.exceptions import ConnectionTimeout,TransportError

from pprint import pprint
from copy import copy,deepcopy
from datetime import datetime

# IPs of the cluster servers
clusterIPs = ["147.231.25.99"]

# name of the used index
indexName = 'fclive'

# the following two parameters should be fetched from the configuration service when in production
# number of replicas
NUMOFREP = 0
#number of shards
NUMOFSHARD = 1

# translation dict between DIRAC MetaQuery comparison operators and ES compatible ones
OP2ES = { '>' : 'gt', '>=' : 'gte', '<' : 'lt', '<=' : 'lte'}

# date format string
DATEFORMAT = '%Y/%m/%dT%H:%M:%S'

# types = ['int', 'float', 'timestamp', 'string']

# structure of the query to be submited 
emptyFindDict = { "fields" : ["_id"] ,  "query" : { "filtered" : { "filter" : { "bool" : { "should" : [ ] } } } } }



class ESHandler:
  def __init__(self):
    """
    Handler constructor. Makes shure that the wanted index exists
    """
    print "ES module initializing"
    self.es = Elasticsearch(clusterIPs)
    
    # check if indexes are in place
    self.ic = IndicesClient(self.es)
    if not self.ic.exists(index="fclive"):
      res = self.ic.create(index=indexName, body={'settings': {"number_of_shards" : NUMOFSHARD, "number_of_replicas" : NUMOFREP}})
      if not 'acknowledged' in res or not res.get('acknowledged'): gLogger.error('Cannot connect to index')
      else: gLogger.info('Elasticsearch: index created')
    gLogger.info('Elasticsearch ready')

  def addField(self, table, pname, ptype): # done
    """
    Add a metaname
    :param string file/dir
    :param string metaname
    :param string type of metaname 
    :return S_OK empty or S_ERROR with the exception in 'Message'
    """
    reqDict = {}
    reqDict['use'] = self.__getUse(table)
    reqDict['metType'] = ptype
    try:
      self.es.index(index=indexName, doc_type='metas',id=pname, body=reqDict)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def rmField(self,table, pname): # done 
    """
    Remove a metaname and delete all its occurences in files/dirs
    :param string file/dir
    :param string metaname
    :return S_OK empty or S_ERROR with the exception in 'Message'
    """
    use = self.__getUse(table)

    # get all ids with field set
    gen = helpers.scan(self.es, 
                       index=indexName, 
                       doc_type=use,
                       query = {"query": {"filtered": {"filter": {"exists": {"field": pname}}}}} 
                       )
    
    # delete the field and reindex the document
    for res in gen:
      try:
        docId = res['_id']
        doc = res['_source']
        doc.pop(pname, None)
        self.es.index(index=indexName, doc_type=use, body=doc, id=docId)
      except Exception, e:
        gLogger.error('Error deleting doc: %s ' % str(doc), e)
    
    # delete the metaname from 'metas' type
    self.es.delete(index=indexName, doc_type='metas', id=pname)
      
    return S_OK()
  
  def getMetadataFields(self, table): # done
    """
    Get all available metanames for file/dir
    :param string file/dir
    :return S_OK with dictionary of metadata in 'Value' or S_ERROR
    """
    
    if table == 'all':
      req = {"query": {"match_all" : {} } }
    else:
      use = self.__getUse(table)
      req = {"query": {"match" : {"use" : use } } }
    
    metaDict = {}
    try:
        dic = self.es.search(index=indexName, doc_type='metas',body=req)
        for hit in [body for body in dic['hits']['hits']]:
          did = hit['_id']
          doc = hit['_source']
          metaDict[did] = doc['metType']
    except Exception, e:
      return S_ERROR(str(e))
    return S_OK( metaDict )
  
  def setMeta(self, table, metaName, metaValue, typ, idNum): # done
    """
    Set metadata to file/dir
    :param string file/dir
    :param string name of the metadata
    :param typ value of metadata
    :param string type of metadata
    :param int file/dir id
    :return S_OK empty or S_ERROR with the exception in 'Message'
    """
    use = self.__getUse(table)
    
    if typ.lower() == 'int':
      if not self.__isInt(metaValue):
        return S_ERROR('Value is not int type')
      else:
        realVal = int(metaValue)
    
    elif typ.lower() == 'float':
      if not self.__isFloat(metaValue):
        return S_ERROR('Value is not float type')
      else:
        realVal = float(metaValue)
        
    elif typ.lower() == 'timestamp':
      if not self.__isTimestamp(metaValue):
        return S_ERROR('Value is not timestamp type')
      else:
        realVal =  datetime.strptime(metaValue, DATEFORMAT)
      
    else: # value is string
      realVal = metaValue
      
    
    req = {"doc": {metaName : realVal}, "doc_as_upsert" : True}
    # pprint(req)
    try:
      self.es.update( index = indexName, doc_type = use, id = idNum, body = req)
    except Exception, e:
      return S_ERROR('Unable to update %s %d with meta %s: %s'%(table,idNum, metaName, str(e)))
    
    return S_OK()

  
  def rmMeta(self,table, metaName, idNum): # done
    """
    Remove a metadata from a dir/file
    :param string file/dir
    :param string name of the metadata
    :param int file/dir id
    :return S_OK empty or S_ERROR with the exception in 'Message'
    """
    use = self.__getUse(table)
    
    try:
      res = self.es.get(index=indexName, doc_type=use, id=idNum)
    except Exception,e:
      gLogger.error('Cannot fetch %s with id %d:' % (table,idNum), e)
      return S_ERROR(str(e))
    
    # remove the metaname from document
    doc = res['_source']
    v = doc.pop(metaName, None)
    if not v:
      return S_ERROR('Requested metadata is not defined')
      
    
    # re-insert updated document or delete if document is now empty
    
    try:
      if doc:
        self.es.index(index=indexName, doc_type=use, id=idNum, body=doc)
      else:
        self.es.delete(index=indexName, doc_type=use, id=idNum)
    except Exception,e:
      gLogger.error('Failed to re-index or delete %s with id %d:' % (table,idNum), e)
      return S_ERROR(str(e))
    
    return S_OK()

  
  def getAllMeta(self, table, IDs): # done
    """
    Retrieve metadata from a list of ids
    :param string file or dir
    :param list of file/dir ids
    :return S_OK with dictionary {meta:value} with 'id' as one of the metas in 'Value'
    """
    
    req = {"ids" : IDs}
    use = self.__getUse(table)
    #print table, IDs
    try:
      res = self.es.mget(index = indexName, doc_type = use, body = req)
    except Exception,e:
      return S_ERROR(str(e))
    
    #pprint(res)
    # extract documents from result, leaving out the not found ones
    resList = []
    #try:
    for doc in res['docs']:
      if doc['found']:
        outDoc = {}
        outDoc = doc['_source']
        outDoc['id'] = doc['_id']
        resList.append(outDoc)
      # else do a list of not found
          
    #except Exception,e:
    #  return S_ERROR('Unable to extract result from ES: %s' % str(e))
    #pprint(resList)
    return S_OK(resList)
    
  def getDirMeta(self, dirId, metaList): # done
    return S_ERROR('Using deprecated method getDirMeta')
  
  def find(self, queryList, typeDict): 
    """
    Find all the files satisfying the inputed metaquery
    :param list serialized metaquery
    :param dict dictionary of types
    """
    
    disList = []
    # convert internal representation of MQ to ES
    for conj in queryList: # iterate over conjunctions
      conjList = []
      negList = []
      for name,val in conj.items(): # iterate over conj elements
        elDict = {}
        if isinstance(val, dict):
          for op,limit in val.items(): # iterate over multiple limitations
            
            if isinstance(limit, list):
              if typeDict[name] == 'timestamp':
                elDict["terms"] = { name : [datetime.strptime(it, DATEFORMAT) for it in limit]}
              else:
                elDict["terms"] = { name : limit}
              if op == '=':
                conjList.append(elDict)
              else: # not in list
                negList.append(elDict)
                
            else: # simple limit
              if typeDict[name] == 'timestamp':
                elDict["range"] = { name: { OP2ES[op] : datetime.strptime(limit, DATEFORMAT) } }
              else:
                elDict["range"] = { name: { OP2ES[op] : limit } }
              conjList.append(elDict)
              
        else: # simple value
          if typeDict[name] == 'timestamp':
            elDict["term"] = { name : datetime.strptime(val, DATEFORMAT)}
          else:
            elDict["term"] = { name : val}
          conjList.append(elDict)
        
      innerDict = { "must" : conjList }
      if negList: innerDict["must_not"] = negList
      disList.append( { "bool" : innerDict } ) 
    
    # insert disjunction list in correct context
    findDict = deepcopy(emptyFindDict)
    findDict["query"]["filtered"]["filter"]["bool"]["should"] = disList
    
    # pprint(findDict)
    
    try:
      # submit query
      resGen = helpers.scan(self.es,index=indexName, doc_type='files', query= findDict)
      # get results
      resList = [res['_id'] for res in resGen]
    except ConnectionTimeout, e:
      return S_ERROR('Connection timeout on Elasctisearch: ' + str(e))
    except TransportError,e:
      return S_ERROR('Data retrieving error on Elasctisearch: ' + str(e))
    except Exception,e:
      return S_ERROR('Error in find operation: ' + str(e))

    return S_OK(resList)
  
# =============================== PRIVATE METHODS =================================================
  
  def __getUse(self,table):
    """
    Translate input to data_type
    """
    if table == 'dir': return 'dirs'
    else: return'files'
    
  def __isInt(self, intstr):
    """
    Check if string can be converted to int type
    :param string integer in string type that needs to be checked
    """
    try:
      int(intstr)
      return True
    except ValueError:
      return False
    
  def __isFloat(self, floatstr):
    """
    Check if string can be converted to float type
    :param string float in string type that needs to be checked
    """
    try:
      float(floatstr)
      return True
    except ValueError:
      return False
    
  def __isTimestamp(self, dateStr):
    """
    Check if string can be converted to datetime type
    :param string timestamp in string type that needs to be checked
    """
    try:
      datetime.strptime(dateStr, DATEFORMAT)
      return True
    except ValueError:
      return False
  
  