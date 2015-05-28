########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog plug-in class to manage dynamic datasets defined by a metadata query
"""

__RCSID__ = "$Id$"

try:
  import hashlib
  md5 = hashlib
except ImportError:
  import md5
from types import StringTypes, ListType, DictType
from pprint import pprint
import os
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List import stringListToString

class DatasetManager:

  _tables = {}
  _tables["FC_MetaDatasets"] = { "Fields": {
                                             "DatasetID": "INT AUTO_INCREMENT",
                                             "DatasetName": "VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL",
                                             "MetaQuery": "VARCHAR(512)",
                                             "DirID": "INT NOT NULL DEFAULT 0",
                                             "TotalSize": "BIGINT UNSIGNED NOT NULL",
                                             "NumberOfFiles": "INT NOT NULL", 
                                             "UID": "SMALLINT UNSIGNED NOT NULL",
                                             "GID": "TINYINT UNSIGNED NOT NULL",
                                             "Status": "SMALLINT UNSIGNED NOT NULL",
                                             "CreationDate": "DATETIME",
                                             "ModificationDate": "DATETIME",
                                             "DatasetHash": "CHAR(36) NOT NULL",
                                             "Mode": "SMALLINT UNSIGNED NOT NULL DEFAULT 509"
                                            },
                                  "UniqueIndexes": { "DatasetName_DirID": ["DatasetName","DirID"] },
                                  "PrimaryKey": "DatasetID"
                                }
  _tables["FC_MetaDatasetFiles"] = { "Fields": {
                                                "DatasetID": "INT NOT NULL",
                                                "FileID": "INT NOT NULL",      
                                               },
                                     "UniqueIndexes": {"DatasetID_FileID":["DatasetID","FileID"]}
                                   }
  _tables["FC_DatasetAnnotations"] = { "Fields": {
                                                  "DatasetID": "INT NOT NULL",
                                                  "Annotation": "VARCHAR(512)"
                                                 },
                                       "PrimaryKey": "DatasetID",
                                     }

  # list for full query
  _parameterList = ['DatasetID', 'MetaQuery', 'DirID', 'TotalSize', 'NumberOfFiles',
                     'UID', 'GID', 'Status', 'CreationDate', 'ModificationDate',
                     'DatasetHash', 'Mode']
  # list of query fields returning int
  _ints = ['DatasetID', 'DirID', 'TotalSize', 'NumberOfFiles', 'UID', 'GID', 'Status']

  def __init__( self, database = None ):
    self.db = None
    if database:
      self.setDatabase( database )

  def setDatabase( self, database ):
    self.db = database
    result = self.db._createTables( self._tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._tables.keys() ) )
    elif result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )  
    return result

  def __getConnection( self, connection=False ):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    return connection

  def addDataset( self, datasetName, metaQuery, frozen, credDict ):  # ok

    # get credentials
    result = self.db.ugManager.getUserAndGroupID( credDict )
    if not result['OK']:
      return result
    uid, gid = result['Value']

    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result
    empty = result['Value'] == None
    if empty and frozen:
      return S_ERROR( "Cannot add an empty frozen dataset" )

    totalSize = result['Value']['TotalSize'] if not empty else 0
    datasetHash = result['Value']['DatasetHash'] if not empty else ""
    numberOfFiles = result['Value']['NumberOfFiles'] if not empty else 0
    fileIDs = result['Value']['LFNIDList'] if not empty else []

    status = 'Frozen' if frozen else 'Dynamic'
    result = self.db.fileManager._getStatusInt( status )
    if not result['OK']:
      return result
    intStatus = result['Value']

    dsDir = os.path.dirname( datasetName )
    dsName = os.path.basename( datasetName )
    if not dsDir:
      # this should never occur, since the path is controlled on the client
      return S_ERROR( 'Dataset name should be specified with full path' )

    # getting the dirID, or creating dir if no such is present in the dirTree
    result = self.db.dtree.existsDir( dsDir )
    if not result['OK']:
      return result
    if result['Value']['Exists']:
      dirID = result['Value']['DirID']
    else:  
      result = self.db.dtree.makeDirectories( dsDir )
      gLogger.info( "Creating directories for dataset %s" % datasetName )
      if not result['OK']:
        return result
      dirID = result['Value']

    # Add the new dataset entry now
    inDict = {
               'DatasetName': dsName,
               'MetaQuery': str(metaQuery),
               'DirID': dirID,
               'TotalSize': totalSize,
               'NumberOfFiles': numberOfFiles,
               'UID': uid,
               'GID': gid,
               'CreationDate': 'UTC_TIMESTAMP()',
               'ModificationDate': 'UTC_TIMESTAMP()',
               'DatasetHash': datasetHash,
               'Status': intStatus
             }
    result = self.db.insertFields( 'FC_MetaDatasets', inDict = inDict )
    if not result['OK']:
      if "Duplicate" in result['Message']:
        return S_ERROR( 'Dataset %s already exists' % datasetName )
      else:
        return result
    datasetID = result['lastRowId']

    # if frozen, add values to the DatasetFiles table
    if frozen:
      result = self.db._update( "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID )
      if not result['OK']:
        return result

      vals = ','.join( ['(%s,%d)' % ( datasetID, fileID ) for fileID in fileIDs] )
      req = 'INSERT INTO FC_MetaDatasetFiles (DatasetID,FileID) VALUES %s ' % vals
      # gLogger.debug( req )
      result = self.db._update( req )
      if not result['OK']:
        return result
    return S_OK( datasetID )
  
  def addDatasetAnnotation( self, annotDict, credDict ):  # ok

    dsIDs = []
    numDs = len( annotDict.keys() )
    only = numDs == 1
    

    if only:
      result = self.__getDatasetID( annotDict.keys()[0] )
    else:
      result = self.__getDatasetIDMult( annotDict.keys() )

    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( "Could not any find dataset(s)" )
    
    values = ""
    if only:
      dsIDs.append( str( result['Value']['ID'] ) )
      values = "(%s,'%s')" % ( str( result['Value']['ID'] ), annotDict.values()[0].replace("'","") )
    else:
      dsIDtoNames = { v['ID']: k.replace( "//", "/" ) for k, v in result['Value'].iteritems() }
      dsIDs = [str( ID ) for ID in dsIDtoNames.keys()]

      values = ','.join( ["(%s,'%s')" % ( dsID, annotDict[dsIDtoNames[int( dsID )]].replace("'","") ) for dsID in dsIDs ] )

    gLogger.info( "Annotating datasets with ID: ", str( dsIDs ) )
    req = "REPLACE FC_DatasetAnnotations (DatasetID,Annotation) VALUE %s" % values
    print req
    result = self.db._update( req )
    
    if not result['OK']:
      return result
    elif result['Value'] < numDs:
      return S_ERROR( "%d of %d datasets was not annotated" % ( numDs - result['Value'], numDs ) )
    else:
      return S_OK()



#    datasetID = result['Value']['ID']
#
#    connection = self.__getConnection()
#    successful = {}
#    result = self._findDatasets( datasets.keys(), connection )
#    if not result['OK']:
#      return result
#    failed = result['Value']['Failed']
#    datasetDict = result['Value']['Successful']
#    if not datasetDict:
#      result['OK'] = False
#      result['Message'] = "No such datasets found: " + str( datasets.keys() )
#      return  result
#    for dataset, annotation in datasets.items():
#      if dataset in datasetDict:
#        req = "REPLACE FC_DatasetAnnotations (Annotation,DatasetID) VALUE ('%s',%d)" % (annotation,datasetDict[dataset]['DatasetID'])
#        result = self.db._update( req, connection)
#        if not result['OK']:
#          failed[dataset] = "Failed to add annotation"
#        else:
#          successful[dataset] = True
#
#    return S_OK( {'Successful':successful, 'Failed':failed} )

  def rmDatasetAnnotation( self, datasetName, credDict ):  # ok
    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result

    gLogger.info( "Removing annotation from dataset:", datasetName )
    req = "DELETE FROM FC_DatasetAnnotations WHERE DatasetID=%d" % result['Value']['ID']
    result = self.db._update( req )
    return result

  def removeDataset( self, datasetName, credDict ):
    """ Remove existing dataset
    """

    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result

    datasetID = result['Value']['ID']

    result = self.db.fileManager._getIntStatus( result['Value']['Status'] )
    if not result['OK']:
      return result
    status = result['Value']

    fileIDs = []
    if status in ['Frozen', 'Static']:
      inside = "SELECT FileID FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
      req = "SELECT FileID FROM FC_MetaDatasetFiles WHERE FileID IN (%s) GROUP BY FileID HAVING Count(*)=1" % inside
      result = self.db._query( req )
      if not result['OK']:
        gLogger.warn( 'Selecting files from a frozen dataset failed' )
        return result
      fileIDs = [int( fid[0] ) for fid in result['Value']]

    req = ""
    for table in ["FC_MetaDatasets", "FC_DatasetAnnotations", "FC_MetaDatasetFiles"]:
      req += "DELETE FROM %s WHERE DatasetID=%s; " % ( table, datasetID )
    gLogger.info( "Deleting dataset " + datasetName )
    result = self.db._update( req )
    res = self.db.fileManager._getFileLFNs( fileIDs )
    # pprint( res )
    if not res['OK']:
      return res
    result['LFNs'] = [ lfn for lfn in res['Value']['Successful'].values()]
    result['Failed'] = res['Value']['Failed']
    return result

  def checkDataset( self, dsNames, credDict ):  # ok
    """ Check that the dataset parameters correspond to the actual state
    """
    dsIDs = []
    only = len( dsNames ) == 1

    if only:
      result = self.__getDatasetID(dsNames[0])
    else:
      result = self.__getDatasetIDMult( dsNames )

    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( "Could not any find dataset(s)" )

    if only:
      dsIDs.append( str( result['Value']['ID'] ) )
    else:
      dsIDtoNames = {v['ID']: k for k, v in result['Value'].iteritems()  }
      dsIDs = [str( ID ) for ID in dsIDtoNames.keys()]
      
    props = ['DatasetID', 'MetaQuery', 'DatasetHash', 'TotalSize', 'NumberOfFiles']
    req = "SELECT %s FROM FC_MetaDatasets" % ','.join( props )
    req += " WHERE DatasetID IN (%s)" % ','.join( dsIDs )
    result = self.db._query( req )
    if not result['OK']:
      return result
    

    # getting the result value in a dict:{'name':{dict}}
    dsProps = {}
    if only:
      dsProps[dsNames[0]] = dict( zip( props, result['Value'][0] ) )
    else:
      for prop in result['Value']:
        dsProps[dsIDtoNames[prop[0]]] = dict( zip( props, prop ) )

    bad = False
    metrics = ['TotalSize', 'DatasetHash', 'NumberOfFiles']
    for ds in dsProps.keys():
      res = self.__getMetaQueryParameters( eval( dsProps[ds]['MetaQuery'] ), credDict )
      # pprint( res )
      if not res['OK']:
        gLogger.warn( "Failed to check dataset %s" % ds )
        continue
      elif not res['Value']:
        res = dict( zip( metrics, [0 for m in metrics] ) )
        # pprint( res )
      else:
        res = res['Value']
      dsProps[ds]['errCode'] = []

      dsProps[ds]['new'] = res
      for m in metrics:
        if dsProps[ds][m] != res[m]:
          bad = True
          dsProps[ds]['errCode'].append( m )

    if not bad:
      return S_OK()
    else:
      return S_OK( dsProps )

  def updateDataset( self, dsNames, credDict, changeDict = None ):  # ok
    """ Update the dataset parameters
    """

    result = self.checkDataset( dsNames, credDict )
    if not result['OK'] or not result['Value']:
      return result
    dsDicts = result['Value']
    reqs = []
    for ds in dsDicts.keys():
      if not dsDicts[ds]['errCode']:
        continue
      prefix = "UPDATE FC_MetaDatasets SET "
      postfix = "ModificationDate=UTC_TIMESTAMP() WHERE DatasetID=%d;" % dsDicts[ds]['DatasetID']
      req = ""
      for err in dsDicts[ds]['errCode']:
        req += " %s='%s', " % ( err, str( dsDicts[ds]['new'][err] ) )
      reqs.append( prefix + req + postfix )

    return self.db._transaction( reqs )




    # old implementation

#    if changeDict is None:
#      result = self.checkDataset( datasetName, credDict )
#      if not result['OK']:
#        return result
#      if not result['Value']:
#        # The dataset is not changed
#        return S_OK()
#      else:
#        changeDict = result['Value']
#
#    req = "UPDATE FC_MetaDatasets SET "
#    for field in changeDict:
#      req += "%s='%s', " % ( field, str( changeDict[field] ) )
#    req += "ModificationDate=UTC_TIMESTAMP() "
#    req += "WHERE DatasetName='%s'" % datasetName
#    result = self.db._update( req )
#    return result

  def showDatasets( self, datasetName, long_, every, credDict ):  # ok
    """ Get information about existing datasets
    """
    # called from dataset show
    
    # will be used for long_ false
    parameterString = 'DatasetName, DirID'

    if long_:
      parameterString = ','.join( self._parameterList )
      if every:
        parameterString += ",DatasetName"

    postfix = ''
    if not every:
      dsName = os.path.basename( datasetName )
      if '*' in dsName:
        dName = dsName.replace( '*', '%' )
        postfix = " WHERE DatasetName LIKE '%s'" % dName
      else:
        result = self.__getDatasetID( datasetName )
        if not result['OK']:
          return result
        dsID = result['Value']['ID']
        postfix = ' WHERE DatasetID=%d' % dsID

    req = "SELECT %s FROM FC_MetaDatasets" % parameterString
    req += postfix
    result = self.db._query( req )
    if not result['OK']:
      return result

    if long_:
      resultDict = {}
      if every:
        dirs = {}
        for row in result['Value']:
          dName = row[0]
          resultDict[dName] = self.__getDatasetDictAuto( row )

          # getting path for dirID
          dirID = resultDict[dName]['DirID']
          resultDict[dName]['Path'] = self.__getDirName( dirs, dirID )
          resultDict[dName]['DatasetName'] = row[-1]

      else:  # only one
        resultDict[os.path.basename( datasetName )] = self.__getDatasetDictAuto( result['Value'][0] )
        resultDict[os.path.basename( datasetName )]['DatasetName'] = datasetName
      return S_OK( resultDict )
    else:  # not long
      dirs = {}
      resultField = []
      for row in result['Value']:
        path = self.__getDirName( dirs, row[1] )
        resultField.append( path + '/' + row[0] )
      return S_OK( resultField )

  def __getDirName( self, dirDict, dirID ):  # ok
    if dirID not in dirDict.keys():
      result = self.db.dtree.getDirectoryPath( dirID )
      if not result['OK']:
        dirDict[dirID] = "ERROR: could not find dirPath"
      else:
        dirDict[dirID] = result['Value']
    return dirDict[dirID]
# # old implementation
#    parameterList = ['DatasetID','MetaQuery','DirID','TotalSize','NumberOfFiles',
#                     'UID','GID','Status','CreationDate','ModificationDate',
#                     'DatasetHash','Mode','DatasetName']
#    parameterString = ','.join( self._parameterList.append( 'DatasetName' ) )
#
#    req = "SELECT %s FROM FC_MetaDatasets" % parameterString
#    if type( datasetName ) in StringTypes:
#      dsName = os.path.basename(datasetName)
#      if '*' in dsName:
#        dName = dsName.replace( '*', '%' )
#        req += " WHERE DatasetName LIKE '%s'" % dName
#      elif dsName:
#        req += " WHERE DatasetName='%s'" % dsName
#    elif type( datasetName ) == ListType:
#      dsNames = [ os.path.basename(d) for d in datasetName ]
#      datasetString = stringListToString( dsNames )
#      req += " WHERE DatasetName in (%s)" % datasetString
#
#    result = self.db._query( req )
#    if not result['OK']:
#      return result
#
#    resultDict = {}
#    for row in result['Value']:
#      dName = row[12]
#      resultDict[dName] = self.__getDatasetDict( row )
#
#    return S_OK( resultDict )

  def getDatasetsInDirectory( self, dirID, verbose = False, connection = False ):  # ok
    """ Get datasets in the given directory
    """
    parameterString = ','.join( self._parameterList )
    parameterString += ",DatasetName"

    req = "SELECT %s FROM FC_MetaDatasets WHERE DirID=%s" % ( parameterString, str( dirID) )
    result = self.db._query( req )
    if not result['OK']:
      return result

    datasets = {}
    userDict = {}
    groupDict = {}
    for row in result['Value']:
      dsDict = self.__getDatasetDictAuto( row, num = True )

      # get user name
      if 'UID' in dsDict.keys():
        uid = dsDict['UID']
        if uid in userDict:
            owner = userDict[uid]
        else:
          owner = 'unknown'
          result = self.db.ugManager.getUserName( uid )
          if result['OK']:
            owner = result['Value']
          userDict[uid] = owner
        dsDict['Owner'] = owner

      # get group name
      if 'GID' in dsDict.keys():
        gid = dsDict['GID']
        if gid in groupDict:
            group = groupDict[gid]
        else:
          group = 'unknown'
          result = self.db.ugManager.getGroupName( gid )
          if result['OK']:
            group = result['Value']
          groupDict[gid] = group
        dsDict['OwnerGroup'] = group

      dsName = row[-1]
      datasets[dsName] = {}
      datasets[dsName]['dsDict'] = dsDict

    return S_OK( datasets )

# # old implementation, can be deleted
#    if verbose and datasets:
#      result = self.getDatasetAnnotation( datasets.keys() )
#      if result['OK']:
#        for dataset in result['Value']['Successful']:
#          datasets[dataset]['Annotation'] = result['Value']['Successful'][dataset]
#        for dataset in result['Value']['Failed']:
#          datasets[dataset]['Annotation'] = result['Value']['Failed'][dataset]
#
#    return S_OK( datasets )

  def getDatasetParameters( self, datasetName, credDict ):  # ok
    """ Get the currently stored dataset parameters
    """

    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result
    dsID = result['Value']['ID']

    parameterString = 'd.' + ',d.'.join( self._parameterList )

    fromT = "FC_MetaDatasets as d LEFT OUTER JOIN FC_DatasetAnnotations as a ON d.DatasetID=a.DatasetID"
    req = "SELECT %s,a.Annotation FROM %s WHERE d.DatasetID=%d" % ( parameterString, fromT, dsID )
    result = self.db._query( req )
    if not result['OK']:
      return result

    row = result['Value'][0]

    resultDict = self.__getDatasetDictAuto( row )
    if row[-1]:
      resultDict['Annotation'] = row[-1]

    return S_OK( resultDict )

  def getDatasetStatuses( self, datasetNames, credDict ):  # ok
    """ Get status of the given dataset
    """
    if len( datasetNames ) == 1:
      out = {}
      out[ os.path.basename( datasetNames[0] ) ] = self.getDatasetParameters( datasetNames[0], credDict )['Value']
      if not out[ os.path.basename( datasetNames[0] ) ]:
        return S_ERROR( "Dataset not found" )
      # else:
      return S_OK( out )

    result = self.__getDatasetIDMult( datasetNames )
    if not result['OK']:
      return result

    dsIDtoNames = {v['ID']: k for k, v in result['Value'].iteritems()  }
    dsIDs = [str( row[1]['ID'] )  for row in result['Value'].items()]
    if not dsIDs:
      return S_ERROR( "None of the datasets were found" )

    parameterString = 'd.' + ',d.'.join( self._parameterList ) + ',a.Annotation'

    fromT = "FC_MetaDatasets as d LEFT OUTER JOIN FC_DatasetAnnotations as a ON d.DatasetID=a.DatasetID"
    req = "SELECT %s FROM %s WHERE d.DatasetID in (%s)" % ( parameterString, fromT, ','.join( dsIDs ) )
    result = self.db._query( req )
    if not result['OK']:
      return result

    out = {}
    for row in result['Value']:
      name = dsIDtoNames[row[0]]
      out[name] = {}
      out[name] = self.__getDatasetDictAuto( row )
      if row[-1]:
        out[name]['Annotation'] = row[-1]

    return S_OK( out )

  def getDatasetFiles( self, datasetName, credDict ):  # ok
    """ Get dataset files
    """
    # get ID and (int)status
    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    dsID = result['Value']['ID']
    dsMeta = result['Value']['MetaQ']

    # get status in string
    result = self.db.fileManager._getIntStatus( status )
    if not result['OK']:
      return result
    status = result['Value']

    # get DS files
    if status in ["Frozen","Static"]:
      return self.__getFrozenDatasetFiles( dsID )
    else:
      return self.__getDynamicDatasetFiles( dsID, credDict, dsMeta )

  def getDatasetFilesWithChecksums( self, datasetName, credDict ):
    """ Similar to getDatasetFiles, but works only for frozen datasets and returns
        checksums. Used for dataset replicate command
    """
    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    dsID = result['Value']['ID']

    # get status in string
    result = self.db.fileManager._getIntStatus( status )
    if not result['OK']:
      return result
    status = result['Value']

    if status in ["Frozen", "Static"]:
      req = "SELECT D.FileID, FI.Checksum, FI.CheckSumType FROM FC_MetaDatasetFiles AS D "
      req += "INNER JOIN FC_FileInfo AS FI ON D.FileID=FI.FileID "
      # req += "INNER JOIN FC_Files AS F ON D.FileID=F.FileID "
      req += "WHERE D.DatasetID=%d" % dsID
      result = self.db._query( req )
      if not result['OK']:
        return result

      # parsing results
      query = {}
      for fil in result['Value']:
        query[fil[0]] = {}
        query[fil[0]]['Checksum'] = fil[1]
        query[fil[0]]['ChecksumType'] = fil[2]

      # getting LFNs from fileIDs
      result = self.db.fileManager._getFileLFNs( [f[0] for f in result['Value']] )
      if not result['OK']:
        return result

      # parsing both query results
      out = {}
      for k, v in result['Value']['Successful'].items():
        out[v] = query[k]

      out = S_OK( out )
      if result['Value']['Failed']:
        out['Failed'] = result['Value']['Failed']
      # TODO: FUNISH
      return out


    else:  # dynamic dataset
      return S_ERROR( "Replication of dynamic datasets is yet to be implemented" )

  def freezeDataset( self, datasetName, credDict ):  # ok
    """ Freeze the contents of the dataset
    """
    # get ID and (int)status
    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    dsID = result['Value']['ID']
    dsMeta = result['Value']['MetaQ']

    # get status in string
    result = self.db.fileManager._getIntStatus( status )
    if not result['OK']:
      return result
    status = result['Value']

    if status in ["Frozen", "Static"]:
      return S_OK()

    # delete any remaining files (should not be necessary)
    req = "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % dsID
    result = self.db._update( req )
    if not result['OK']:
      return result

    result = self.__getDynamicDatasetFiles( dsID, credDict, dsMeta )
    if not result['OK']:
      return result
    if not result['FileIDList']:
      return S_ERROR( "Freezing a dataset with no files" )

    # getting the string of new values, that will be inserted
    valueString = ','.join( ['(%d,%d)' % ( dsID, fileID ) for fileID in result['FileIDList']] )

    req = "INSERT INTO FC_MetaDatasetFiles (DatasetID,FileID) VALUES %s" % valueString
    result = self.db._update( req )
    if not result['OK']:
      return result

    result = self.__setDatasetStatus( dsID, 'Frozen' )
    return result

  def releaseDataset( self, datasetName, credDict ):  # ok
    """ return the dataset to a dynamic state
    """
    # get ID and (int)status
    result = self.__getDatasetID( datasetName )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    dsID = result['Value']['ID']

    # get status in string
    result = self.db.fileManager._getIntStatus( status )
    if not result['OK']:
      return result
    status = result['Value']

    if status == "Dynamic":
      return S_OK()

    req = "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % dsID
    result = self.db._update( req )

    result = self.__setDatasetStatus( dsID, 'Dynamic' )
    return result

  def downloadDataset( self, dsName, credDict ):

    result = self.getDatasetFiles( dsName, credDict )
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR( "Unable to retrieve dataset files" )

    result = self.db.fileManager.getFileSize( result['Value'] )
    if not result['OK']:
      return result
    else:
      return S_OK( result['Value']['Successful'] )

    # sizeDict = result['Value']['Successful'] if result['Value']['Successful'] else {}

  def getDatasetLocation( self, dsName, credDict ):

    result = self.getDatasetFiles( dsName, credDict )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( "Dataset is empty" )
    lfnList = result['Value']
    fileSizes = result['FileSizes']
    totalSize = result['TotalSize']

    result = self.db.fileManager.getReplicas( lfnList, False, credDict )
    if not result['OK']:
      return result
    if not result['Value']['Successful']:
      return S_ERROR( "Could not retrieve any replicas" )

    replicas = result['Value']['Successful']
    return S_OK( {'replicas': replicas,
                  'fileSizes' : fileSizes,
                  'totalSize' :  totalSize,
                  # 'SEs' : result['Value']['SEPrefixes'].keys()
                  } )

  def checkOverlapingDatasets( self, datasetNames, credDict ):

      result = self.__getDatasetIDMult( datasetNames )
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR( "Problem resolving datasetID, please check dataset status" )
      if len( result['Value'] ) != 2:
        return S_ERROR( "Problem resolving datasetID, please check dataset status" )

      dsDicts = result['Value']
      # pprint( dsDicts )

      # check if both statuses are the same
      if dsDicts[datasetNames[0]]['Status'] == dsDicts[datasetNames[1]]['Status']:
        result = self.db.fileManager._getIntStatus( dsDicts[datasetNames[0]]['Status'] )
        if not result['OK']:
          return result
        status = result['Value']
        if status in ['Frozen', 'Static']:
          dsIDs = ", ".join( [str( i ) for i in [dsDicts[datasetNames[0]]['ID'], dsDicts[datasetNames[1]]['ID']]] )
          req = "select FileID from FC_MetaDatasetFiles where DatasetID in (%s) " % dsIDs
          req += "group by FileID having count(*)>1"
          result = self.db._query( req )
          if not result['OK']:
            return result
          if not result['Value']:
            # datasets don't overlap
            return S_OK()

          fileIDs = [fID[0] for fID in result['Value']]

        else:  # two dynamic datasets
          combinedMeta = self.__compareMetaqueries(dsDicts[datasetNames[0]]['MetaQ'],dsDicts[datasetNames[1]]['MetaQ'])
          # check, if there can be any same files in both datasets
          if not combinedMeta:
            return S_OK()

          result = self.__getMetaQueryParameters( combinedMeta, credDict )
          if not result['OK']:
            return result

          return S_OK( result['Value']['LFNList'] )

      else:  # statuses are different
        combinedMeta = [ 1 ]  # self.__compareMetaqueries( dsDicts[datasetNames[0]]['MetaQ'], dsDicts[datasetNames[1]]['MetaQ'] )
        gLogger.error( "REMOVE DEVELOPMENT ASSIGNEMENT!" )
        # check, if there can be any same files in both datasets
        if not combinedMeta:
          return S_OK()
        # now we know there can be some files in the intersection, but are they in the frozen dataset?
        # determine which one is the frozen
        result = self.db.fileManager._getIntStatus( dsDicts[datasetNames[0]]['Status'] )
        if not result['OK']:
          return result
        status = result['Value']
        frozenIndex = 0 if status in ['Frozen', 'Static'] else 1
        result = self.__getFrozenDatasetFiles( dsDicts[datasetNames[frozenIndex]]['ID'] )
        if not result['OK']:
          # maybe add some error message here?
          return result
        filesFrozen = result['Value']
        # modulate the ++frozenIndex with two to get the other index (0 -> 1, 1 -> 0)
        dynamicID = dsDicts[datasetNames[( frozenIndex + 1 ) % 2]]['ID']
        dynamicMeta = dsDicts[datasetNames[( frozenIndex + 1 ) % 2]]['MetaQ']
        result = self.__getDynamicDatasetFiles( dynamicID, credDict, dynamicMeta )
        if not result['OK']:
          return result
        filesDynamic = result['Value']

        # get intersection of sets and return
        out = list( set( filesDynamic ).intersection( filesFrozen ) )
        return S_OK( out )

      result = self.db.fileManager._getFileLFNs( fileIDs )
      if not result['OK']:
        return result
      lfnDict = result['Value']['Successful']
      lfns = [ lfnDict[i] for i in lfnDict.keys() ]
      return S_OK( lfns )

  def __compareMetaqueries( self, meta1, meta2 ):
    """ Gets two metaqueries and combines them so that files, that may belong to both datasets
        have to be in the result of the combined metaquery
    """
    meta1 = eval(meta1)
    meta2 = eval(meta2)
    pprint( meta1 )
    pprint( meta2 )
    return []

  def __setDatasetStatus( self, dsID, status ):
    """ Set the given dataset status
    """
    result = self.db.fileManager._getStatusInt( status )
    if not result['OK']:
      return result
    intStatus = result['Value']

    req = "UPDATE FC_MetaDatasets SET Status=%d, ModificationDate=UTC_TIMESTAMP() " % intStatus
    req += "WHERE DatasetID=%d" % dsID
    result = self.db._update( req )
    return result


  def __getDynamicDatasetFiles( self, datasetID, credDict, MetaQ ):  # ok
    """ Get dataset lfns from a dynamic meta query
    """
    # making dict from string
    metaQuery = eval( MetaQ )
    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK()


    lfnList = result['Value']['LFNList']
    finalResult = S_OK(lfnList)
    finalResult['FileIDList'] = result['Value']['LFNIDList']
    finalResult['FileSizes'] = result['Value']['FileSizes']

    # maybe change this
    finalResult['TotalSize'] = result['Value']['TotalSize'] if 'TotalSize' in result['Value'] else 0
    return finalResult

  def __getFrozenDatasetFiles( self, datasetID ):  # ok
    """
      Get datasets file lfns from a frozen dataset
      :param int datasetID id of the dataset in question
      :return dictionary in Value is a lfnList, in FileIDDict dictionary of lfns indexed by fileIds
    """

    req = "SELECT FileID FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._query( req )
    if not result['OK']:
      return result

    fileIDList = [ row[0] for row in result['Value'] ]
    if not fileIDList:
      return S_ERROR( "frozen dataset has no files" )
    result = self.db.fileManager._getFileLFNs( fileIDList )
    if not result['OK']:
      return result

    lfnDict = result['Value']['Successful']
    lfnList = [ lfnDict[i] for i in lfnDict.keys() ]
    finalResult = S_OK( lfnList )
    finalResult['FileIDDict'] = lfnDict

    result = self.db.fileManager.getFileSize( lfnList )
    finalResult['FileSizes'] = result['Value']['Successful'] if 'Successful' in result['Value'] else {}
    finalResult['TotalSize'] = result['TotalSize'] if 'TotalSize' in result else 0

    return finalResult

  def __getDatasetDict( self, row ):  # depricated: __getDatasetDictAuto

    resultDict = {}
    resultDict['DatasetID'] = int( row[0] )
    resultDict['MetaQuery'] = eval( row[1] )
    resultDict['DirID'] = int( row[2] )
    resultDict['TotalSize'] = int( row[3] )
    resultDict['NumberOfFiles'] = int( row[4] )
    uid = int( row[5] )
    gid = int( row[6] )
    result = self.db.ugManager.getUserName( uid )
    if result['OK']:
      resultDict['Owner'] = result['Value']
    else:
      resultDict['Owner'] = 'Unknown'
    result = self.db.ugManager.getGroupName( gid )
    if result['OK']:
      resultDict['OwnerGroup'] = result['Value']
    else:
      resultDict['OwnerGroup'] = 'Unknown'
    intStatus = int( row[7] )
    result = self.db.fileManager._getIntStatus( intStatus )
    if result['OK']:
      resultDict['Status'] = result['Value']
    else:
      resultDict['Status'] = 'Unknown'
    resultDict['CreationDate'] = row[8]
    resultDict['ModificationDate'] = row[9]
    resultDict['DatasetHash'] = row[10]
    resultDict['Mode'] = row[11]

    return resultDict


  def __getMetaQueryParameters( self, metaQuery, credDict ):  # OK-ish
    """ Get parameters ( hash, total size, number of files ) for the given metaquery
    """
    findMetaQuery = dict( metaQuery )

    path = '/'
    if "Path" in findMetaQuery:
      path = findMetaQuery['Path']
      findMetaQuery.pop( 'Path' )

    result = self.db.fmeta.findFilesByMetadata( findMetaQuery, path, credDict, extra=True )
    if not result['OK']:
      return S_ERROR( 'Failed to apply the metaQuery' )
    if not result['Value']:
      return S_OK()

    if type( result['Value'] ) == ListType:
      lfnList = result['Value']
    elif type( result['Value'] ) == DictType:
      # Process into the lfn list
      lfnList = []
      for dir_,fList in result['Value'].items():
        for f in fList:
          lfnList.append(dir_+'/'+f)

    lfnIDDict = result.get( 'LFNIDDict', {} )
    lfnIDList = result.get( 'LFNIDList', [] )
    if not lfnIDList:
      lfnIDList = lfnIDDict.keys()
    lfnList.sort()

    myMd5 = md5.md5()
    myMd5.update( str( lfnList ) )
    datasetHash = myMd5.hexdigest().upper()

    numberOfFiles = len( lfnList )

    result = self.db.fileManager.getFileSize( lfnList )

    totalSize = 0
    if result['OK']:
      totalSize = result['TotalSize']

    sizeDict = result['Value']['Successful'] if result['Value']['Successful'] else {}

    result = S_OK( { 'DatasetHash': datasetHash,
                     'NumberOfFiles': numberOfFiles,
                     'TotalSize': totalSize,
                     'LFNList': lfnList,
                     'LFNIDList': lfnIDList,
                     'FileSizes': sizeDict } )
    return result

######################################################## MADAMs new code ##################################################################################


  def __getDatasetID( self, datasetFullName ):
    """
        Get ID for datasets full path + name
        :param str full path to dataset
        :return dict with ID, status and MetaQ
    """

    if ( datasetFullName[0] != '/' ):
      return S_ERROR( '__getDatasetID got not a full name!!' )

    gLogger.info( "getting ID for dataset %s" % datasetFullName )

    dsDir = os.path.dirname( datasetFullName )
    dsName = os.path.basename( datasetFullName )

    result = self.db.dtree.findDir( dsDir )
    # check if the query executed
    if not result['OK']:
      return result

    # check if there is a valid answer
    if not result['Value']:
      result['OK'] = False
      result['Message'] = "No such directory found: " + dsDir
      return result

    dirID = int( result['Value'] )
    req = "SELECT DatasetID,Status,MetaQuery FROM FC_MetaDatasets WHERE DirID=%d AND DatasetName='%s'" % ( dirID, dsName )
    result = self.db._query( req )

    # check if the query executed
    if not result['OK']:
      return result

    # check if there is a valid answer
    if not result['Value']:
      result['OK'] = False
      result['Message'] = "No dataset found on: " + dsDir + "/" + dsName
      return result

    # parsing query output
    resultOut = {}
    resultOut['Value'] = {}
    resultOut['Value']['ID'] = int( result['Value'][0][0] )
    resultOut['Value']['Status'] = int( result['Value'][0][1] )
    resultOut['Value']['MetaQ'] = result['Value'][0][2]
    resultOut['OK'] = True

    return resultOut

  def __getDatasetIDMult( self, dsNameList ):

    dirs = [os.path.dirname( path ) for path in dsNameList]
    result = self.db.dtree.findDirs( dirs )
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR( "Unable to find no dirs" )

    # making a list of tuples (dirID, dsName)
    dirAndIDs = result['Value']
    dirAndNames = [( int( dirAndIDs[os.path.dirname( dsName )] ),
                     os.path.basename( dsName ) ) for dsName in dsNameList]

    dirIDs = ','.join( [str( ID[0] ) for ID in dirAndNames] )
    names = "','".join( [ID[1] for ID in dirAndNames] )
    dirAndNames = ','.join( ['(%s,"%s")' % ( item[0], item[1] ) for item in dirAndNames] )

    inside = "SELECT DatasetID,Status,MetaQuery,DirID,DatasetName FROM FC_MetaDatasets WHERE DirID IN (%s) AND DatasetName IN ('%s')" % ( dirIDs, names )
    req = "SELECT * FROM (%s) AS Inside WHERE (DirID,DatasetName) IN (%s)" % ( inside, dirAndNames )
    result = self.db._query( req )
    if not result['OK']:
      return result

    gLogger.info( "Resolved ID for %d of %d datasets" % ( len( result['Value'] ), len( dsNameList ) ) )

    out = {}
    IDtoDir = {v: k for k, v in dirAndIDs.iteritems()}
    for row in result['Value']:
      # getting back the full name
      name = IDtoDir[row[3]] + "/" + row[4]
      name = name.replace( "//", "/" )
      out[name] = {}
      out[name]['ID'] = int( row[0] )
      out[name]['Status'] = int( row[1] )
      out[name]['MetaQ'] = row[2]

    return S_OK(out)



  def __getDatasetDictAuto( self, row, num = False ):
    """
      Get dictionary with dataset properties based on the query which selects the
      parameters from self._parameterList

      :param list one line of the query result
      :param boolean True if you want to return only numerical GID, UID, status
      :param boolean True if the query was also for annotations
    """
    resultDict = {}
    paramList = self._parameterList

    # this could probably be done with some python magic, but this is probably easier to read
    for attr in paramList:
      resultDict[attr] = row[paramList.index( attr )]
      if attr in self._ints:
        resultDict[attr] = int( resultDict[attr] )

    if not num:
      # get owner name
      result = self.db.ugManager.getUserName( resultDict['UID'] )
      if result['OK']:
        resultDict['Owner'] = result['Value']
      else:
        resultDict['Owner'] = 'Unknown'

      # get group name
      result = self.db.ugManager.getGroupName( resultDict['GID'] )
      if result['OK']:
        resultDict['OwnerGroup'] = result['Value']
      else:
        resultDict['OwnerGroup'] = 'Unknown'

      result = self.db.fileManager._getIntStatus( resultDict['Status'] )
      if result['OK']:
        resultDict['Status'] = result['Value']
      else:
        resultDict['Status'] = 'Unknown'

    return resultDict
