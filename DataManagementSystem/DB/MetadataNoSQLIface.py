'''
Created on Sep 8, 2015

@author: madam
'''

from DIRAC import S_OK, S_ERROR
from cassandra.cluster import Cluster
from pprint import pprint

translationTypes = {"org.apache.cassandra.db.marshal.UTF8Type"      : "STRING", 
                    "org.apache.cassandra.db.marshal.Int32Type"     : "INT", 
                    "org.apache.cassandra.db.marshal.FloatType"     : "FLOAT",
                    "org.apache.cassandra.db.marshal.TimestampType" : "DATE"}

class CassandraHandler:
  def __init__(self):
    cluster = Cluster(['147.231.25.99'])
    self.cassandra =  cluster.connect('fc')

  def addField(self, table, pname, ptype):
    req_alter = "ALTER TABLE %s ADD %s %s" %(table, pname, ptype)
    req_index = "CREATE INDEX ind_%s_%s ON %s (%s)" % (table, pname, table, pname)
    try:
      self.cassandra.execute(req_alter)
      self.cassandra.execute(req_index)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def rmField(self,table, pname):
    req_table = "ALTER TABLE %s DROP %s" % (table, pname)
    req_index = "DROP INDEX ind_%s_%s" %(table, pname)
    try:
      self.cassandra.execute(req_index)
      self.cassandra.execute(req_table)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def getMetadataFields(self, table):
    req = "SELECT column_name,validator FROM system.schema_columns WHERE keyspace_name='fc' AND columnfamily_name='%s' allow filtering;" % table
    try: 
      rows = self.cassandra.execute( req )
    except Exception, e:
      return S_ERROR(e)

    metaDict = {}
    for row in rows:
      metaDict[str(row[0])] = translationTypes[str(row[1])]

    return S_OK( metaDict )
  
  def setMeta(self, table, metaName, metaValue, type, id):
    if table == "files":
      idFieldName = "fileid"
    else:
      idFieldName = "dirid"
    
    # getting rid of quotes
    if metaValue[0] == "'" or metaValue[0] == '"':
      metaValue = metaValue[1:-1]
    if type in ['STRING', 'VARCHAR(128)', 'varchar']:
      req = "UPDATE %s SET %s = '%s' WHERE %s = %s" % (table, metaName, metaValue, idFieldName, id)
    else:
      req = "UPDATE %s SET %s = %s WHERE %s = %s" % (table, metaName, metaValue, idFieldName, id)
    try:
      self.cassandra.execute(req)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def rmMeta(self,table, metaName, id):
    if table == "files":
      idFieldName = "fileid"
    else:
      idFieldName = "dirid"
    
    req = "DELETE %s FROM %s WHERE %s = %s" % (metaName, table, idFieldName, id)
    try:
      self.cassandra.execute(req)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def getMeta(self, table, dirIDsString, metaList):
    if table == "files":
      idFieldName = "fileid"
    else:
      idFieldName = "dirid"
      
    req = "select %s from %s where %s in (%s)" % (",".join(metaList), table, idFieldName, dirIDsString)
    try: 
      rows = self.cassandra.execute(req)
    except Exception, e:
      return S_ERROR(e)
    return S_OK([dict._asdict() for dict in rows ])
  
  
  
  
  