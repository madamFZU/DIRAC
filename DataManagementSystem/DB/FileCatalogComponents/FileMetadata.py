########################################################################
# $HeadURL$
########################################################################
""" DIRAC FileCatalog plugin class to manage file metadata. This contains only
    non-indexed metadata for the moment.
"""

__RCSID__ = "$Id$"
from pprint import pprint
from types import IntType, ListType, LongType, DictType, StringTypes, FloatType
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import queryTime
from DIRAC.Core.Utilities.List import intListToString
from DIRAC.DataManagementSystem.Client.MetaQuery import FILE_STANDARD_METAKEYS, \
                                                        FILES_TABLE_METAKEYS, \
                                                        FILEINFO_TABLE_METAKEYS
from DIRAC.DataManagementSystem.DB.MetadataNoSQLIface import ESHandler

class FileMetadata:

  def __init__( self, database = None ):

    self.db = database
    self.nosql = ESHandler()

  def setDatabase( self, database ):
    self.db = database

##############################################################################
#
#  Manage Metadata fields
#
##############################################################################
  def addMetadataField( self, pname, ptype, credDict ):
    """ Add a new metadata parameter to the Metadata Database.
        :param string parameter name 
        :param string  parameter type 
    """

    if pname in FILE_STANDARD_METAKEYS:
      return S_ERROR( 'Illegal use of reserved metafield name' )

    result = self.db.dmeta.getMetadataFields( credDict )
    if not result['OK']:
      return result
    if pname in result['Value'].keys():
      return S_ERROR( 'The metadata %s is already defined for Directories' % pname )

    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    if pname in result['Value'].keys():
      if ptype.lower() == result['Value'][pname].lower():
        return S_OK( 'Already exists' )
      else:
        return S_ERROR( 'Attempt to add an existing metadata with different type: %s/%s' %
                        ( ptype, result['Value'][pname] ) )

    return self.nosql.addField("file", pname, ptype)
    
  def deleteMetadataField( self, pname, credDict ):
    """ Remove metadata field
    """
    return self.nosql.rmField("file", pname)

  def getFileMetadataFields( self, credDict ):
    """ Get all the defined metadata fields
    """
    return self.nosql.getMetadataFields("file")
    
###########################################################
#
# Set and get metadata for files
#
###########################################################

  def setMetadata( self, path, metadict, credDict ):
    """ Set the value of a given metadata field for the the given directory path
    """
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    result = self.__getFileID(path)
    if not res['OK']:
        return res
    fileID = res['Value']

    for metaName, metaValue in metadict.items():
      #if not metaName in metaFields:
      #  return S_ERROR("MetaField not found")
      result = self.nosql.setMeta("file", metaName, metaValue,  metaFields[metaName], fileID)
      if not result['OK']:
        return S_ERROR(result['Message'])
    return S_OK()

  def removeMetadata( self, path, metadata, credDict ):
    """ Remove the specified metadata for the given file
    """
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result

    result = self.__getFileID(path)
    if not res['OK']:
        return res
    fileID = res['Value']

    for meta in metadata:
      result = self.nosql.rmMeta("file", meta, fileID)
      if not result['OK']:
        return result
    return S_OK()

  def __getFileID( self, path ):
    """
    Get file ID from path
    :param string path
    :return S_OK with fileID in 'Value'
    """

    result = self.db.fileManager._findFiles( [path] )
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR( 'File not found' )
    return S_OK( fileID )

  def setFileMetaParameter( self, path, metaName, metaValue, credDict ):
    return S_ERROR('Method "setFileMetaParameter" is deprecated')

  def _getFileUserMetadataByID( self, fileIDList, credDict, connection = False ):
    """ Get file user metadata for the list of file IDs
    """
    return S_ERROR('Method "_getFileUserMetadataByID" is deprecated')

  def getFileUserMetadata( self, path, credDict ):
    """ 
    Get metadata for the given file
    :param string path of given file
    :param credDict
    :return S_OK with metaDict in 'Value' + metaTypeDict in 'MetadataType'
    """
    # First file metadata
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    metaTypeDict = result['Value']

    result = self.__getFileID( path )
    if not result['OK']:
      return result
    fileID = result['Value']

    result = self.nosql.getAllMeta("file", [fileID])
    #pprint(result)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK()
    metaDict = result['Value'][0]
    metaDict.pop('id')
    
    result = S_OK( dict( metaDict ) )
    result['MetadataType'] = metaTypeDict
    return result


  def getFileMetaParameters( self, path, credDict ):
    """ Get meta parameters for the given file
    """
    return S_ERROR('Using deprecated method')


#########################################################################
#
#  Finding files by metadata
#
#########################################################################

  def __findFilesByMetadata( self, metaDict, dirList, credDict ):
    """ Find a list of file IDs meeting the metaDict requirements and belonging
        to directories in dirList
    """
    # 1.- classify Metadata keys
    storageElements = None
    standardMetaDict = {}
    userMetaDict = {}
    leftJoinTables = []
    for meta, value in metaDict.items():
      if meta == "SE":
        if isinstance( value, DictType ):
          storageElements = value.get( 'in', [] )
        else:
          storageElements = [ value ]
      elif meta in FILE_STANDARD_METAKEYS:
        standardMetaDict[meta] = value
      else:
        userMetaDict[meta] = value

    tablesAndConditions = []
    leftJoinTables = []
    # 2.- standard search
    if standardMetaDict:
      result = self.__buildStandardMetaQuery( standardMetaDict )
      if not result['OK']:
        return result
      tablesAndConditions.extend( result['Value'] )
    # 3.- user search
    if userMetaDict:
      result = self.__buildUserMetaQuery( userMetaDict )
      if not result['OK']:
        return result
      tablesAndConditions.extend( result['Value'] )
      leftJoinTables = result['LeftJoinTables']
    # 4.- SE constraint
    if storageElements:
      result = self.__buildSEQuery( storageElements )
      if not result['OK']:
        return result
      tablesAndConditions.extend( result['Value'] )

    query = 'SELECT F.FileID FROM FC_Files F '
    conditions = []
    tables = []

    if dirList:
      dirString = intListToString( dirList )
      conditions.append( "F.DirID in (%s)" % dirString )

    counter = 0
    for table, condition in tablesAndConditions:
      if table == 'FC_FileInfo':
        query += 'INNER JOIN FC_FileInfo FI USING( FileID ) '
        condition = condition.replace( '%%', '%' )
      elif table == 'FC_Files':
        condition = condition.replace( '%%', '%' )
      else:
        counter += 1
        if table in leftJoinTables:
          tables.append( 'LEFT JOIN %s M%d USING( FileID )' % ( table, counter ) )
        else:
          tables.append( 'INNER JOIN %s M%d USING( FileID )' % ( table, counter ) )
        table = 'M%d' % counter
        condition = condition % table
      conditions.append( condition )

    query += ' '.join( tables )
    if conditions:
      query += ' WHERE %s' % ' AND '.join( conditions )

    result = self.db._query( query )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK( [] )

#     fileList = [ row[0] for row in result['Value' ] ]
    fileList = []
    for row in result['Value']:
      fileID = row[0]
      fileList.append( fileID )
    
    return S_OK( fileList )
  
  def __findFilesForQueryDict(self,path, metaDict, credDict):
    # TODO: maybe when no dir meta is suplied, don't list all the dirs, only make a flag
    
    # if only path is specified
    if not metaDict:
      result = self.db.dtree.findDir( path )
      if not result['OK']:
        return result
      dirID = result['Value']
      return self.db.dtree.getFileLFNsInDirectoryByDirectory( dirID, credDict )
      
    result = self.db.dmeta.findDirIDsByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result
    if not result['Value']:
      # print "No value -> no directory satisfies"
      return S_OK([])
    dirList = result['Value']
    notDirMeta = result['RemainingMeta']
    #print "dirlist ", dirList
    
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    fileMetaKeys = result['Value'].keys()
    # check if all non-dir meta is file meta
    undefinedMeta = [meta for meta in notDirMeta if meta not in fileMetaKeys]
    if undefinedMeta:
      return S_ERROR('Undefined metafields: ' + ",".join(undefinedMeta))
    
    typeDict = result['Value']
    fileMetaDict = dict( item for item in metaDict.items() if item[0] in fileMetaKeys )
    
    # if no unsatisfied metadata are found, return all files in the sub-tree
    if not fileMetaDict:
      # print "Getting all files!"
      return self.db.dtree.getFileLFNsInDirectoryByDirectory( dirList, credDict )
    
    result = self.nosql.find("file", fileMetaDict, typeDict)
    if not result['OK']:
      return result
    fileIdSet = result['Value']
    #pprint(fileIdSet)
    if not fileIdSet: out = S_OK( [] )
    else: out = S_OK(fileIdSet)
    out['dirList'] = dirList
    return out
    
  @queryTime
  def findFilesByMetadata( self, metaList, path, credDict, extra = False ):
    """ Find Files satisfying the given metadata
    """
    if not path:
      path = '/'
      
    sets = []
    done = []
    dirList = []
    idList = []
    
    for metaDict in metaList:
      if 'Path' in metaDict:
        path = metaDict['Path']
      result = self.__findFilesForQueryDict(path, metaDict, credDict)
      #pprint(result)
      if not result['OK']:
        return result
      if 'LFNIDList' in result:
        done.extend(result['LFNIDList'])
      else:
        sets.extend(result['Value'])
        if 'dirList' in result:
          dirList.extend(result['dirList'])
    
    fileIdSet = set.union(set(sets))
    dirSet = set(dirList)
    if fileIdSet and dirSet:
      req = "SELECT FileID FROM FC_Files WHERE FileID in (%s) AND DirID in (%s)" % (",".join([str(fid) for fid in fileIdSet]), ",".join([str(did) for did in dirSet]))
      result = self.db._query( req )
      if not result['OK']:
        return result
      if not result['Value']:
        return S_OK( [] )
      #pprint([fid[0] for fid in result['Value']])
      idList = [str(fid[0]) for fid in result['Value']]
      
    idList.extend(done)
    if not idList:
      return S_OK([])
    result = self.db.fileManager._getFileLFNs( idList )
    if not result['OK']:
      return result
    if 'Successful' not in result['Value']:
      return S_OK([])
    out =  S_OK([name for name in result['Value']['Successful'].values()])
    out['LFNIDDict'] = result['Value']['Successful']
    return out
  
  
    #---------- OLD -------------------
    # 1.- Get Directories matching the metadata query
    result = self.db.dmeta.findDirIDsByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result
    dirList = result['Value']
    dirFlag = result['Selection']

    # 2.- Get known file metadata fields
#     fileMetaDict = {}
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    fileMetaKeys = result['Value'].keys() + FILE_STANDARD_METAKEYS.keys()
    fileMetaDict = dict( item for item in metaDict.items() if item[0] in fileMetaKeys )

    fileList = []
    lfnIdDict = {}
    lfnList = []

    if dirFlag != 'None':
      # None means that no Directory satisfies the given query, thus the search is empty
      if dirFlag == 'All':
        # All means that there is no Directory level metadata in query, full name space is considered
        dirList = []

      if fileMetaDict:
        # 3.- Do search in File Metadata
        result = self.__findFilesByMetadata( fileMetaDict, dirList, credDict )
        if not result['OK']:
          return result
        fileList = result['Value']
      elif dirList:
        # 4.- if not File Metadata, return the list of files in given directories
        return self.db.dtree.getFileLFNsInDirectoryByDirectory( dirList, credDict )
      else:
        # if there is no File Metadata and no Dir Metadata, return an empty list
        lfnList = []

    if fileList:
      # 5.- get the LFN
      result = self.db.fileManager._getFileLFNs( fileList )
      if not result['OK']:
        return result
      lfnList = result['Value']['Successful'].values()
      if extra:
        lfnIdDict = result['Value']['Successful']

    result = S_OK( lfnList )
    if extra:
      result['LFNIDDict'] = lfnIdDict

    return result