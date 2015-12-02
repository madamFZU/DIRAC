########################################################################
# $HeadURL$
########################################################################
""" DIRAC FileCatalog mix-in class to manage directory metadata
"""
#from DataManagementSystem.DB.test.TestFileCatalogDB import credDict
__RCSID__ = "$Id$"

import os, types
from pprint import pprint
from copy import deepcopy
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import queryTime
from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery
from DIRAC.DataManagementSystem.DB.MetadataNoSQLIface import ESHandler

class DirectoryMetadata:

  def __init__( self, database = None ):

    gLogger.info('Initializing dmeta')
    self.db = database
    gLogger.info('MySQL initialized')
    self.nosql = ESHandler()
    gLogger.info('ES initialized')

  def setDatabase( self, database ):
    self.db = database

##############################################################################
#
#  Manage Metadata fields
#
##############################################################################  
  def addMetadataField( self, pname, ptype, credDict ):
    """ Add a new metadata parameter to the Metadata Database.
        pname - parameter name, ptype - parameter type in the MySQL notation
    """
    
    result = self.db.fmeta.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    if pname in result['Value'].keys():
      return S_ERROR( 'The metadata %s is already defined for Files' % pname )

    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    if pname in result['Value'].keys():
      if ptype.lower() == result['Value'][pname].lower():
        return S_OK( 'Already exists' )
      else:
        return S_ERROR( 'Attempt to add an existing metadata with different type: %s/%s' %
                        ( ptype, result['Value'][pname] ) )

    return self.nosql.addField("dir", pname, ptype)

  def deleteMetadataField( self, pname, credDict ):
    """ Remove metadata field
    """
    return self.nosql.rmField("dir", pname)

  def getMetadataFields( self, credDict ):
    """ Get all the defined metadata fields
    """
    return self.nosql.getMetadataFields("dir")

  def addMetadataSet( self, metaSetName, metaSetDict, credDict ):
    """ Add a new metadata set with the contents from metaSetDict
    """
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaTypeDict = result['Value']
    # Check the sanity of the metadata set contents
    for key in metaSetDict:
      if not key in metaTypeDict:
        return S_ERROR( 'Unknown key %s' % key )

    result = self.db._insert( 'FC_MetaSetNames', ['MetaSetName'], [metaSetName] )
    if not result['OK']:
      return result

    metaSetID = result['lastRowId']

    req = "INSERT INTO FC_MetaSets (MetaSetID,MetaKey,MetaValue) VALUES %s"
    vList = []
    for key, value in metaSetDict.items():
      vList.append( "(%d,'%s','%s')" % ( metaSetID, key, str( value ) ) )
    vString = ','.join( vList )
    result = self.db._update( req % vString )
    return result

  def getMetadataSet( self, metaSetName, expandFlag, credDict ):
    """ Get fully expanded contents of the metadata set 
    """
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaTypeDict = result['Value']

    req = "SELECT S.MetaKey,S.MetaValue FROM FC_MetaSets as S, FC_MetaSetNames as N "
    req += "WHERE N.MetaSetName='%s' AND N.MetaSetID=S.MetaSetID" % metaSetName
    result = self.db._query( req )
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK( {} )

    resultDict = {}
    for key, value in result['Value']:
      if not key in metaTypeDict:
        return S_ERROR( 'Unknown key %s' % key )
      if expandFlag:
        if metaTypeDict[key] == "MetaSet":
          result = self.getMetadataSet( value, expandFlag, credDict )
          if not result['OK']:
            return result
          resultDict.update( result['Value'] )
        else:
          resultDict[key] = value
      else:
        resultDict[key] = value
    return S_OK( resultDict )

#############################################################################################  
#
# Set and get directory metadata
#
#############################################################################################  

  def setMetadata( self, dpath, metadict, credDict ):
    """ Set the value of a given metadata field for the the given directory path
    """
    result = self.db.dtree.findDir( dpath )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Path not found: %s' % dpath )
    dirID = result['Value']

    dirmeta = self.getDirectoryMetadata( dpath, credDict, owndata = False )
    pprint(dirmeta)
    if not dirmeta['OK']:
      return dirmeta
    metadataTypeDict = dirmeta['MetadataType']

    for metaName, metaValue in metadict.items():
   
      #if not metaName in metadataTypeDict:
      #  return S_ERROR("MetaField not found")
      # Check that the metadata is not defined for the parent directories
      if metaName in dirmeta['Value']:
        return S_ERROR( 'Metadata conflict detected for %s for directory %s' % ( metaName, dpath ) )
      # Change the DB record
      #print "type: " +  metadataTypeDict[metaName]
      result =  self.nosql.setMeta("dir", metaName, metaValue, metadataTypeDict[metaName], dirID)
      if not result['OK']:
        return result
      
      # set the metadata for all the files in the sub-tree
      res = self.__findAllFilesInSubtree(dirID, credDict)
      if not res['OK']:
        return res
      # set metadata for each file
      for fileId in res['Value']:
        res = self.nosql.setMeta("file", metaName, metaValue, metadataTypeDict[metaName], fileId)
        if not res['OK']:
          gLogger.error('Failed to set metaName %s for fileID %s' % (metaName,str(fileId)), res['Message'])
        else:
          gLogger.info('Metadata set: metaName %s for fileID %s' % (metaName,str(fileId)))
      
    return S_OK()

  def removeMetadata( self, dpath, metadata, credDict ):
    """ Remove the specified metadata for the given directory
    """
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    result = self.db.dtree.findDir( dpath )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Path not found: %s' % dpath )
    dirID = result['Value']

    failed = []
    for meta in metadata:
      result = self.nosql.rmMeta("dir", meta, dirID)
      if not result['OK']:
        failed.append(meta)
    
    if failed:
      return S_ERROR("Failed to remove metadata: " + ",".join(failed))

    # remove metadata from all files in subtree
    
    res = self.__findAllFilesInSubtree(dirID, credDict)
    if not res['OK']:
      return res
    
    for fileId in res['Value']:
        for meta in metadata:
          res = self.nosql.rmMeta("file", meta, fileId)
          if not res['OK']:
            gLogger.error('Failed to remove metaName %s for fileID %s' % (meta,str(fileId)), res['Message'])
          else:
            gLogger.info('Metadata remove: metaName %s for fileID %s' % (meta,str(fileId)))
    
    return S_OK()
    
  def getDirectoryMetadata( self, path, credDict, inherited = True, owndata = True ):
    """ Get metadata for the given directory aggregating metadata for the directory itself
        and for all the parent directories if inherited flag is True. Get also the non-indexed
        metadata parameters.
    """
    result = self.db.dtree.existsDir( path )
    if not result['OK']:
      return result
    elif not result['Value']['Exists']:
      return S_ERROR("Directory %s doesn't exist" % path)
    
    result = self.db.dtree.getPathIDs( path )
    if not result['OK']:
      return result
    pathIDs = result['Value']

    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    metaDict = {}
    metaOwnerDict = {}
    metaTypeDict = {}
    dirID = pathIDs[-1]
    if not inherited:
      pathIDs = pathIDs[-1:]
    if not owndata:
      pathIDs = pathIDs[:-1]
    #pathString = ','.join( [ str( x ) for x in pathIDs ] )

    metaList = metaFields.keys()
    result = self.nosql.getAllMeta("dir", pathIDs)
    if not result['OK']:
      return result
    rows = result['Value']
    
    for row in rows:
      if int(row['id']) == dirID:
        ownerProp = 'OwnMetadata'
      else:
        ownerProp = 'ParentMetadata'
      
      row.pop('id')
      for key in row.keys():
        if row[key] == None:
          continue
        metaDict[key] = row[key]
        metaOwnerDict[key] = ownerProp
        
    result = S_OK( dict(metaDict) )
    result['MetadataOwner'] = metaOwnerDict
    result['MetadataType'] = metaFields
    return result

  def __transformMetaParameterToData( self, metaname ):
    """ Relocate the meta parameters of all the directories to the corresponding
        indexed metadata table
    """
    return S_ERROR('Method "__transformMetaParameterToData" is deprecated')
      
  def __findAllFilesInSubtree(self,dirID,credDict):
      """
      Return all fileIds of files in subtree under directory identified by dirID
      Returned values in a list in S_OK()['Value']
      """
      # getting list of all files in directory subtree
      res = self.db.dtree.getSubdirectoriesByID(dirID)
      if not res['OK']:
        return S_ERROR('Cannot navigate directory tree')
      dirList = res['Value'].keys()
      dirList.append(dirID)  
      res = self.db.dtree.getFileLFNsInDirectoryByDirectory( dirList, credDict )
      if not res['OK']:
        return S_ERROR('Cannot retrieve all files in sub-tree')
      return S_OK(res['LFNIDList'])

############################################################################################
#
# Find directories corresponding to the metadata 
#
############################################################################################  

  def __createMetaSelection( self, meta, value, table = '' ):

    if type( value ) == types.DictType:
      selectList = []
      for operation, operand in value.items():
        if operation in ['>', '<', '>=', '<=']:
          if type( operand ) == types.ListType:
            return S_ERROR( 'Illegal query: list of values for comparison operation' )
          if type( operand ) in [types.IntType, types.LongType]:
            selectList.append( "%sValue%s%d" % ( table, operation, operand ) )
          elif type( operand ) == types.FloatType:
            selectList.append( "%sValue%s%f" % ( table, operation, operand ) )
          else:
            selectList.append( "%sValue%s'%s'" % ( table, operation, operand ) )
        elif operation == 'in' or operation == "=":
          if type( operand ) == types.ListType:
            vString = ','.join( [ "'" + str( x ) + "'" for x in operand] )
            selectList.append( "%sValue IN (%s)" % ( table, vString ) )
          else:
            selectList.append( "%sValue='%s'" % ( table, operand ) )
        elif operation == 'nin' or operation == "!=":
          if type( operand ) == types.ListType:
            vString = ','.join( [ "'" + str( x ) + "'" for x in operand] )
            selectList.append( "%sValue NOT IN (%s)" % ( table, vString ) )
          else:
            selectList.append( "%sValue!='%s'" % ( table, operand ) )
        selectString = ' AND '.join( selectList )
    elif type( value ) == types.ListType:
      vString = ','.join( [ "'" + str( x ) + "'" for x in value] )
      selectString = "%sValue in (%s)" % ( table, vString )
    else:
      if value == "Any":
        selectString = ''
      else:
        selectString = "%sValue='%s' " % ( table, value )

    return S_OK( selectString )

  def __findSubdirByMeta( self, meta, value, pathSelection = '', subdirFlag = True ):
    """ Find directories for the given meta datum. If the the meta datum type is a list,
        combine values in OR. In case the meta datum is 'Any', finds all the subdirectories
        for which the meta datum is defined at all.
    """

    result = self.__createMetaSelection( meta, value, "M." )
    if not result['OK']:
      return result
    selectString = result['Value']

    req = " SELECT M.DirID FROM FC_Meta_%s AS M" % meta
    if pathSelection:
      req += " JOIN ( %s ) AS P WHERE M.DirID=P.DirID" % pathSelection
    if selectString:
      if pathSelection:
        req += " AND %s" % selectString
      else:
        req += " WHERE %s" % selectString

    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK( [] )

    dirList = []
    for row in result['Value']:
      dirID = row[0]
      dirList.append( dirID )
      #if subdirFlag:
      #  result = self.db.dtree.getSubdirectoriesByID( dirID )
      #  if not result['OK']:
      #    return result
      #  dirList += result['Value']
    if subdirFlag:
      result = self.db.dtree.getAllSubdirectoriesByID( dirList )
      if not result['OK']:
        return result
      dirList += result['Value']

    return S_OK( dirList )

  def __findSubdirMissingMeta( self, meta, pathSelection ):
    """ Find directories not having the given meta datum defined
    """
    result = self.__findSubdirByMeta( meta, 'Any', pathSelection )
    if not result['OK']:
      return result
    dirList = result['Value']
    table = self.db.dtree.getTreeTable()
    dirString = ','.join( [ str( x ) for x in dirList ] )
    if dirList:
      req = 'SELECT DirID FROM %s WHERE DirID NOT IN ( %s )' % ( table, dirString )
    else:
      req = 'SELECT DirID FROM %s' % table
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK( [] )

    dirList = [ x[0] for x in result['Value'] ]
    return S_OK( dirList )

  def __checkDirsForMetadata( self, meta, value, pathString ):
    """ Check if any of the given directories conform to the given metadata
    """
    S_ERROR('Method "__checkDirsForMetadata" is deprecated')
    
  def __dirCollidesWithQuery(self, queryDict, dirMeta, metaList):
    missing = [key for key in metaList if key not in dirMeta.keys()]
    tmpQuery = deepcopy(queryDict)
    for key in missing: tmpQuery[key] = 'Missing'
    print "tmpQuery ",str(dirMeta)
    mq = MetaQuery(tmpQuery)
    return mq.applyQuery(dirMeta)
    

  def __findAllSubdirsByMeta(self, queryDictIn, dirID):
    #print "call with %s on %s" %(str(queryDictIn), str(dirID))
    queryDict = deepcopy(queryDictIn)
    if queryDict:
      result = self.nosql.getDirMeta(dirID, queryDict.keys())
      if not result['OK']:
        return S_ERROR('Unable to connect to NoSQL:' + result['Message'])
      
      # check if the current dir has defined any relevant metadata
      dirMeta = {}
      if result['Value']:
        dirMeta = { key : val for key,val in result['Value'][0].items() if val != None }
         
      if dirMeta:
        # making sure MetaQuery doesn't take missing metadata as a problem
        metaList = result['Value'][0].keys()
        res = self.__dirCollidesWithQuery(queryDict, dirMeta, metaList)
        #pprint(res)
        if not res['OK']:
          return res
        if res['Value'] == False:
          return S_OK([])
        # Directory does satisfy the metaquery
        # Filter out the part of Query satisfied by the path dir
        for key in dirMeta.keys(): 
          queryDict.pop(key)
          
      result = self.db.dtree.getChildren(int(dirID))
      if result['OK']:
        if queryDict:
          outList = []
        else:
          outList = [str(dirID)]
        for curID in result['Value']:
          res = self.__findAllSubdirsByMeta(queryDict, curID )
          if res['OK']:
            if res['Value']:
              outList.extend(res['Value'])
          else:
            gLogger.error(res['Message'])
    
    else: # no queryDict left
      result = self.db.dtree.getSubdirectoriesByID(int(dirID))
      if result['OK'] and result['Value']:
        outList = [str(res) for res in result['Value'].keys()]
        outList.append(str(dirID))
      else:
        outList = [str(dirID)]
    return S_OK(outList)
    
  @queryTime
  def findDirIDsByMetadata( self, queryDictIn, path, credDict ):
    """ Find Directories satisfying the given metadata and being subdirectories of 
        the given path
        :return Empty S_OK when no dir satisfies the query, else returns ALL the dirs satisfying the query
    """
    queryDict = deepcopy(queryDictIn)
    mq = MetaQuery(queryDict)
    
    result = self.getDirectoryMetadata(path, credDict)
    if not result['OK']:
      return S_ERROR('Problem with connectiong to the database')
    allDirMeta = result['Value']
    typeDict = result['MetadataType']
    remainingMeta = [meta for meta in queryDict.keys() if meta not in typeDict.keys()]
    
    if allDirMeta:
      res = mq.applyQuery(allDirMeta)
      if not res['OK']:
        return S_ERROR('Failed to apply query to path:' + res['Message'] )
      if res['Value'] == True: # if the first dir satisfies the MQ, return all subdirs
        result = self.db.dtree.findDir( path )
        if not result['OK']:
          return S_ERROR('Unable to get dir ID for dir ' + path)
        dirID = result['Value']
        result = self.db.dtree.getSubdirectoriesByID(int(dirID))
        if result['Value']:
          outList = [str(res) for res in result['Value'].keys()]
          outList.append(str(dirID))
        else:
          outList = [str(dirID)] 
        print "Returning all subdirs"
        return S_OK(outList)
      else: # check if the directory meta doesn't collide with the MQ
        dirMeta = { key : val for key,val in allDirMeta.items() if val != None }
        res = self.__dirCollidesWithQuery(queryDict, dirMeta, typeDict.keys())
        if not res['OK']:
          return res
        if res['Value'] == False:
          print "COLISION!!"
          return S_OK([])
    
    print "Continuing"
    # Filtering the query for only directory metadata
    toPop = []
    for key in queryDict.keys():
      if key not in typeDict.keys():
        toPop.append(key)
    for key in toPop: queryDict.pop(key)
    
    result = self.db.dtree.findDir( path )
    if not result['OK']:
      return S_ERROR('Unable to get dir ID for dir ' + path)
    result = self.__findAllSubdirsByMeta(queryDict, str(result['Value']))
    result['RemainingMeta'] = remainingMeta
    return result
    
  @queryTime
  def findDirectoriesByMetadata( self, queryDict, path, credDict ):
    """ Find Directory names satisfying the given metadata and being subdirectories of 
        the given path
    """

    result = self.findDirIDsByMetadata( queryDict, path, credDict )
    if not result['OK']:
      return result

    dirIDList = result['Value']

    dirNameDict = {}
    if dirIDList:
      result = self.db.dtree.getDirectoryPaths( dirIDList )
      if not result['OK']:
        return result
      dirNameDict = result['Value']
    elif result['Selection'] == 'None':
      dirNameDict = { 0:"None" }
    elif result['Selection'] == 'All':
      dirNameDict = { 0:"All" }

    return S_OK( dirNameDict )

  def findFilesByMetadata( self, metaDict, path, credDict ):
    """ Find Files satisfying the given metadata
    """

    result = self.findDirectoriesByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result

    dirDict = result['Value']
    dirList = dirDict.keys()
    fileList = []
    result = self.db.dtree.getFilesInDirectory( dirList, credDict )
    if not result['OK']:
      return result
    for _fileID, dirID, fname in result['Value']:
      fileList.append( dirDict[dirID] + '/' + os.path.basename( fname ) )

    return S_OK( fileList )

  def findFileIDsByMetadata( self, metaDict, path, credDict, startItem = 0, maxItems = 25 ):
    """ Find Files satisfying the given metadata
    """
    result = self.findDirIDsByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result

    dirList = result['Value']
    return self.db.dtree.getFileIDsInDirectory( dirList, credDict, startItem, maxItems )

################################################################################################
#
# Find metadata compatible with other metadata in order to organize dynamically updated
# metadata selectors 
#
################################################################################################  
  def __findCompatibleDirectories( self, meta, value, fromDirs ):
    """ Find directories compatible with the given meta datum.
        Optionally limit the list of compatible directories to only those in the
        fromDirs list 
    """

    # The directories compatible with the given meta datum are:
    # - directory for which the datum is defined
    # - all the subdirectories of the above directory
    # - all the directories in the parent hierarchy of the above directory

    # Find directories defining the meta datum and their subdirectories
    result = self.__findSubdirByMeta( meta, value, subdirFlag = False )
    if not result['OK']:
      return result
    selectedDirs = result['Value']
    if not selectedDirs:
      return S_OK( [] )

    result = self.db.dtree.getAllSubdirectoriesByID( selectedDirs )
    if not result['OK']:
      return result
    subDirs = result['Value']

    # Find parent directories of the directories defining the meta datum
    parentDirs = []
    for psub in selectedDirs:
      result = self.db.dtree.getPathIDsByID( psub )
      if not result['OK']:
        return result
      parentDirs += result['Value']

    # Constrain the output to only those that are present in the input list  
    resDirs = parentDirs + subDirs + selectedDirs
    if fromDirs:
      resDirs = list( set( resDirs ) & set( fromDirs ) )

    return S_OK( resDirs )

  def __findDistinctMetadata( self, metaList, dList ):
    """ Find distinct metadata values defined for the list of the input directories.
        Limit the search for only metadata in the input list
    """

    if dList:
      dString = ','.join( [ str( x ) for x in dList ] )
    else:
      dString = None
    metaDict = {}
    for meta in metaList:
      req = "SELECT DISTINCT(Value) FROM FC_Meta_%s" % meta
      if dString:
        req += " WHERE DirID in (%s)" % dString
      result = self.db._query( req )
      if not result['OK']:
        return result
      if result['Value']:
        metaDict[meta] = []
        for row in result['Value']:
          metaDict[meta].append( row[0] )

    return S_OK( metaDict )

  def __expandMetaDictionary( self, metaDict, credDict ):
    """ Expand the dictionary with metadata query 
    """
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaTypeDict = result['Value']
    resultDict = {}
    extraDict = {}
    for key, value in metaDict.items():
      if not key in metaTypeDict:
        #return S_ERROR( 'Unknown metadata field %s' % key )
        extraDict[key] = value
        continue
      keyType = metaTypeDict[key]
      if keyType != "MetaSet":
        resultDict[key] = value
      else:
        result = self.getMetadataSet( value, True, credDict )
        if not result['OK']:
          return result
        mDict = result['Value']
        for mk, mv in mDict.items():
          if mk in resultDict:
            return S_ERROR( 'Contradictory query for key %s' % mk )
          else:
            resultDict[mk] = mv

    result = S_OK( resultDict )
    result['ExtraMetadata'] = extraDict
    return result

  def getCompatibleMetadata( self, queryDict, path, credDict ):
    """ Get distinct metadata values compatible with the given already defined metadata
    """

    pathDirID = 0
    if path != '/':
      result = self.db.dtree.findDir( path )
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR( 'Path not found: %s' % path )
      pathDirID = int( result['Value'] )
    pathDirs = []
    if pathDirID:
      result = self.db.dtree.getSubdirectoriesByID( pathDirID, includeParent = True )
      if not result['OK']:
        return result
      if result['Value']:
        pathDirs = result['Value'].keys()
      result = self.db.dtree.getPathIDsByID( pathDirID )
      if not result['OK']:
        return result
      if result['Value']:
        pathDirs += result['Value']

    # Get the list of metadata fields to inspect
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']
    comFields = metaFields.keys()

    # Commented out to return compatible data also for selection metadata
    #for m in metaDict:
    #  if m in comFields:
    #    del comFields[comFields.index( m )]

    result = self.__expandMetaDictionary( queryDict, credDict )
    if not result['OK']:
      return result
    metaDict = result['Value']

    fromList = pathDirs
    anyMeta = True
    if metaDict:
      anyMeta = False
      for meta, value in metaDict.items():
        result = self.__findCompatibleDirectories( meta, value, fromList )
        if not result['OK']:
          return result
        cdirList = result['Value']
        if cdirList:
          fromList = cdirList
        else:
          fromList = []
          break

    if anyMeta or fromList:
      result = self.__findDistinctMetadata( comFields, fromList )
    else:
      result = S_OK( {} )
    return result

  def removeMetadataForDirectory( self, dirList, credDict ):
    """ Remove all the metadata for the given directory list
    """
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']
    
    okay = False
    
    if not isinstance(dirList, list): dirList = [dirList]
    for dirId in dirList:
        for meta in metaFields:
            res = self.nosql.rmMeta("dir", meta, dirId)
            if not res['OK']: gLogger.error('Unable to remove metadata %s from dir with id %d' % (meta, dirId))
            else: okay = True
            
    if not okay: return S_ERROR('Unable to remova any metadata')
    else: return S_OK()