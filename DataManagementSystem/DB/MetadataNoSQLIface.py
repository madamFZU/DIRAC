'''
Created on Sep 8, 2015

@author: madam
'''

from DIRAC import S_OK, S_ERROR
from cassandra.cluster import Cluster
from pprint import pprint
import cassandra

KEYSPACE = 'fcii'

translationTypes = {"org.apache.cassandra.db.marshal.UTF8Type"      : "STRING", 
                    "org.apache.cassandra.db.marshal.Int32Type"     : "INT", 
                    "org.apache.cassandra.db.marshal.FloatType"     : "FLOAT",
                    "org.apache.cassandra.db.marshal.TimestampType" : "DATE"}

types = ['int', 'float', 'timestamp', 'string']

class CassandraHandler:
  def __init__(self):
    print "NoSQL module initialized"
    cluster = Cluster(['147.231.25.99'])
    try:
      self.cassandra =  cluster.connect(KEYSPACE)
    except Exception:
      self.cassandra =  cluster.connect()
      print "connecting to keyspace %s failed, creating keyspace" % KEYSPACE
      self.cassandra.execute("CREATE KEYSPACE "+ KEYSPACE +" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")
      self.cassandra.execute("use " + KEYSPACE + ";")
      self.__createKeyspace()

  def addField(self, table, pname, ptype):
    req = "INSERT INTO metaTypes(metaname, type, usage) values('%s','%s', '%s')"
    try:
      self.cassandra.execute(req % (pname, ptype, table))
    except Exception, e:
      return S_ERROR(e)
    
    if table == "dir":
      req_alter = "ALTER TABLE dirmeta ADD %s %s" %(pname, ptype)
      try:
        self.cassandra.execute(req_alter)
      except Exception, e:
        return S_ERROR(e)
    return S_OK()
  
  def rmField(self,table, pname):
    req = "DELETE FROM metaTypes WHERE metaname='%s'"
    try:
      self.cassandra.execute(req % pname)
    except Exception, e:
      return S_ERROR("1" + str(e))
    
    req = "DELETE FROM %s_%s WHERE metaname='%s'"
    for typ in types:
      try:
        self.cassandra.execute(req % ( table, typ, pname ))
      except Exception, e:
        return S_ERROR("2" + str(e))
    
    if table == "dir":
      req_alter = "ALTER TABLE dirmeta DROP %s" % pname
      try:
        self.cassandra.execute(req_alter)
      except Exception, e:
        return S_ERROR("3" + str(e))
      
    return S_OK()
  
  def getMetadataFields(self, table):
    req = "SELECT metaname,type FROM metaTypes WHERE usage='%s'" % table
    metaDict = {}
    try:
        rows = self.cassandra.execute( req )
        for row in rows:
          metaDict[str(row[0])] = str(row[1])
    except Exception, e:
      return S_ERROR(e)

    return S_OK( metaDict )
  
  def setMeta(self, table, metaName, metaValue, typ, idNum):
    idFieldName = self.__getIdField(table)
    
    self.rmMeta(table, metaName, idNum)
    # getting rid of quotes
    if metaValue[0] == "'" or metaValue[0] == '"':
      metaValue = metaValue[1:-1]
    reqDir = ""
    
    if typ in ['STRING', 'VARCHAR(128)', 'varchar', 'string', 'timestamp']:
      typ = 'string'
      req = "UPDATE %s_%s SET %s = %s + {%s} WHERE metaname = '%s' and value = '%s'" % (table, typ, idFieldName, idFieldName, idNum, metaName, metaValue)
      if table == "dir": reqDir = "UPDATE dirmeta SET %s = '%s' WHERE dirid = %s" % ( metaName, metaValue, idNum)
    else:
      req = "UPDATE %s_%s SET %s = %s + {%s} WHERE metaname = '%s' and value = %s" % (table, typ, idFieldName, idFieldName, idNum, metaName, metaValue)
      if table == "dir": reqDir = "UPDATE dirmeta SET %s = %s WHERE dirid = %s" % ( metaName, metaValue, idNum)
    try:
      self.cassandra.execute(req)
      if reqDir:
        self.cassandra.execute(reqDir)
    except Exception, e:
      return S_ERROR(e)
    return S_OK()
  
  def rmMeta(self,table, metaName, idNum):
    idFieldName = self.__getIdField(table)
    
    res = self.getAllMeta(table, str(idNum))
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Meta not set for specified " + table)
    if metaName not in res['Value'][0]:
      return S_ERROR("Meta %s not found for specified %s" % (metaName, table))
    val = res['Value'][0][metaName]
    typ = res['TypeDict'][metaName]
    
    if typ in ['STRING', 'VARCHAR(128)', 'varchar', 'string', 'timestamp']:
      req = " UPDATE %s_%s SET %s = %s - {%s} where metaname='%s' and value = '%s';"
    else:
      req = " UPDATE %s_%s SET %s = %s - {%s} where metaname='%s' and value = %s;"
    reqDir = ""
    if table == "dir":
      reqDir = "DELETE %s FROM dirmeta WHERE dirid = %s" % (metaName, idNum)
    try:
      # print req % (table, typ, idFieldName, idFieldName, idNum, metaName, val)
      self.cassandra.execute(req % (table, typ, idFieldName, idFieldName, idNum, metaName, val))
      if reqDir:
        # print reqDir
        self.cassandra.execute(reqDir)
    except Exception, e:
      return S_ERROR(e)
    
    
    return S_OK()
  
  def getAllMeta(self, table, IDsString):
    idFieldName = self.__getIdField(table)
    
    req = "select metaname, value from %s_%s where %s contains %s"
    outList = []
    rowDict = {}
    typeDict = {}
    for idNum in IDsString.split(","):
      for typ in types: 
        try:
          rows = self.cassandra.execute(req % (table, typ, idFieldName, idNum))
        except Exception, e:
          return S_ERROR(e)
        
        # TODO: ucesat: rows je 0-1, neni treba mit for
        if rows:
          rowDict['id'] = idNum
        for row in rows:
          row = row._asdict()
          rowDict[row['metaname']] = row['value']
          typeDict[row['metaname']] = typ
        
      if rowDict:
        outList.append(rowDict)
        rowDict = {}
        
    outDict = S_OK(outList)
    outDict['TypeDict'] = typeDict
    return outDict

  def getDirMeta(self, dirId, metaList):
    req = "select %s from dirMeta where dirid=%s" % (",".join(metaList), dirId)
    try: 
      rows = self.cassandra.execute(req)
    except Exception, e:
      return S_ERROR(e)
    if not rows:
      return S_OK([])
    
    # only one record per dirid
    row = rows[0]
    outDict = {}
    for i in range(0,len(metaList)): outDict[metaList[i]] = row[i]

    return S_OK([outDict])
  
  def __createKeyspace(self):
    req_table = 'CREATE TABLE %s_%s( metaname text, value %s, %sid set<int>, PRIMARY KEY(metaname, value));'
    req_index = 'CREATE INDEX %s_%s_id ON %s_%s(%sid);'
    for table in ['file', 'dir']:
      for typ in types:
        if typ == 'string': ctyp = 'text'
        else: ctyp = typ
        self.cassandra.execute(req_table % (table, typ, ctyp, table))
        self.cassandra.execute(req_index % (table, typ, table, typ, table))
    self.cassandra.execute('CREATE TABLE metaTypes(metaname text primary key, type text, usage text)')
    self.cassandra.execute('CREATE INDEX ON metaTypes(usage)')
    self.cassandra.execute('CREATE TABLE dirMeta(dirid int primary key)')
  
  def find(self, table, queryDict, typeDict):
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
        
      # print req % (idFieldName, table, typ, metaName, op, val)
      rows = self.cassandra.execute(req % (idFieldName, table, typ, metaName, op, val))
      if not rows:
        continue
      setList.append(set(rows[0][0]))
      
    if not setList:
      return S_OK([])
    return S_OK(set.intersection(*setList))
    
  def __getIdField(self, table):
    if "file" in table:
      return "fileid"
    else:
      return "dirid"
  
  