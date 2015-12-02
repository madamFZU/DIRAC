'''
Created on Sep 8, 2015


@author: madam
'''

from DIRAC import S_OK, S_ERROR, gLogger
from pprint import pprint
from __builtin__ import str
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch import helpers

clusterIPs = ["147.231.25.99"]

indexName = 'fclive'

translationTypes = {"org.apache.cassandra.db.marshal.UTF8Type"      : "STRING", 
                    "org.apache.cassandra.db.marshal.Int32Type"     : "INT", 
                    "org.apache.cassandra.db.marshal.FloatType"     : "FLOAT",
                    "org.apache.cassandra.db.marshal.TimestampType" : "DATE"}

types = ['int', 'float', 'timestamp', 'string']


class ESHandler:
  def __init__(self):  
    print "ES module initializing"
    self.es = Elasticsearch(clusterIPs)
    
    # check if indexes are in place
    ic = IndicesClient(self.es)
    if not ic.exists(index="fclive"):
      res = ic.create(index=indexName, body={'settings': {"number_of_shards" : 1, "number_of_replicas" : 0}})
      if not 'acknowledged' in res or not res.get('acknowledged'): gLogger.error('Cannot connect to index')
    gLogger.info('Elasticsearch connected')

  def addField(self, table, pname, ptype): # done
    reqDict = {}
    reqDict['use'] = self.__getUse(table)
    reqDict['metType'] = ptype
    try:
      self.es.index(index=indexName, doc_type='metas',id=pname, body=reqDict)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def rmField(self,table, pname): # done 
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
    use = self.__getUse(table)
    
    req = {"doc": {metaName : metaValue}, "doc_as_upsert" : True}
    # pprint(req)
    try:
      self.es.update( index = indexName, doc_type = use, id = idNum, body = req)
    except Exception, e:
      return S_ERROR('Unable to update %s %d with meta %s: %s'%(table,idNum, metaName, str(e)))
    
    return S_OK()

  
  def rmMeta(self,table, metaName, idNum): # done
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
    
    req = {"ids" : IDs}
    use = self.__getUse(table)
    print table, IDs
    try:
      res = self.es.mget(index = indexName, doc_type = use, body = req)
    except Exception,e:
      return S_ERROR(str(e))
    
    pprint(res)
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
    pprint(resList)
    return S_OK(resList)
    
  def getDirMeta(self, dirId, metaList): # done
    return S_ERROR('Using deprecated method getDirMeta')
  
  def find(self, table, queryDict, typeDict): # TODO: 
    idFieldName = self.__getIdField(table)
    
    req = "select %s from %s_%s where metaname = '%s' and value %s %s"
    setList = []
    for metaName, metaDict in queryDict.items():
      if isinstance(metaDict, dict):
        op,val = metaDict.items()[0]
      else:
        op, val = '=', metaDict
      typ = typeDict[metaName]  
      if typ == 'varchar': 
        typ = 'string'
        val = "'" + val + "'"
        
      #print req % (idFieldName, table, typ, metaName, op, val)
      rows = self.cassandra.execute(req % (idFieldName, table, typ, metaName, op, val))
      if not rows:
        continue
      setList.append(set(rows[0][0]))
      
    if not setList:
      return S_OK([])
    return S_OK(set.intersection(*setList))
  
# =============================== PRIVATE METHODS =================================================
  
  def __getUse(self,table):
    if table == 'dir': return 'dirs'
    else: return'files'
    
  def __getIdField(self, table):
    if "file" in table:
      return "fileid"
    else:
      return "dirid"
    
  def __isInt(self, intstr):
    try:
      int(intstr)
      return True
    except ValueError:
      return False
  
  