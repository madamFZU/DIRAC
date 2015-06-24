#!/usr/bin/env python
""" File Catalog Client Command Line Interface. """
#from yum.plugins import ArgsPluginConduit

__RCSID__ = "$Id$"

import cmd
import commands
import os.path
import time
import sys
from types  import DictType, ListType

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Utilities.PrettyPrint import int_with_commas, printTable
from DIRAC.DataManagementSystem.Client.DirectoryListing import DirectoryListing
from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
from DIRAC.DataManagementSystem.Client.CmdDirCompletion.AbstractFileSystem import DFCFileSystem, UnixLikeFileSystem
from DIRAC.DataManagementSystem.Client.CmdDirCompletion.DirectoryCompletion import DirectoryCompletion
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

from pprint import pprint

class FileCatalogClientCLI(cmd.Cmd):
  """ usage: FileCatalogClientCLI.py xmlrpc-url.

    The URL should use HTTP protocol, and specify a port.  e.g.::

        http://localhost:7777

    This provides a command line interface to the FileCatalog Exported API::

        ls(path) - lists the directory path

    The command line interface to these functions can be listed by typing "help"
    at the prompt.

    Other modules which want access to the FileCatalog API should simply make
    their own internal connection to the XMLRPC server using code like::

        server = xmlrpclib.Server(xmlrpc_url)
        server.exported_function(args)
  """

  intro = """
File Catalog Client $Revision: 1.17 $Date: 
            """

  def __init__(self, client):
    cmd.Cmd.__init__(self)
    self.fc = client
    self.cwd = '/'
    self.prompt = 'FC:'+self.cwd+'> '
    self.previous_cwd = '/'

    self.dfc_fs = DFCFileSystem(self.fc)
    self.lfn_dc = DirectoryCompletion(self.dfc_fs)

    self.ul_fs = UnixLikeFileSystem()
    self.ul_dc = DirectoryCompletion(self.ul_fs)

  def getPath(self,apath):

    if apath.find('/') == 0:
      path = apath
    else:
      path = self.cwd+'/'+apath
      path = path.replace('//','/')

    return os.path.normpath(path)
  
  def do_register(self,args):
    """ Register a record to the File Catalog
    
        usage:
          register file <lfn> <pfn> <size> <SE> [<guid>]  - register new file record in the catalog
          register replica <lfn> <pfn> <SE>   - register new replica in the catalog
    """
    
    argss = args.split()
    if (len(argss)==0):
      print self.do_register.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'file':
      if (len(argss) < 4):
        print self.do_register.__doc__
        return
      return self.registerFile(argss)
    elif option == 'pfn' or option == "replica":
      # TODO
      # Is the __doc__ not complete ?
      if (len(argss) != 3):
        print self.do_register.__doc__
        return
      return self.registerReplica(argss)
    else:
      print "Unknown option:",option

  # An Auto Completion For ``register``
  _available_register_cmd = ['file', 'replica']
  def complete_register(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) >= 2 and (args[1] in self._available_register_cmd):
      # if 'register file' or 'register replica' exists,
      # try to do LFN auto completion.
      cur_path = ""
      if (len(args) == 3):
        cur_path = args[2]
      result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)
      return result

    result = [i for i in self._available_register_cmd if i.startswith(text)]
    return result
  
  def do_add(self,args):
    """ Upload a new file to a SE and register in the File Catalog
    
        usage:
        
          add <lfn> <pfn> <SE> [<guid>] 
    """
    
    # ToDo - adding directories
    
    argss = args.split()
    
    if len(argss) < 3:
      print "Error: insufficient number of arguments"
      return
    
    lfn = argss[0]
    lfn = self.getPath(lfn)
    pfn = argss[1]
    se = argss[2]
    guid = None
    if len(argss)>3:
      guid = argss[3]
        
    dirac = Dirac()
    result = dirac.addFile(lfn,pfn,se,guid,printOutput=False)
    if not result['OK']:
      print 'Error: %s' %(result['Message'])
    else:
      print "File %s successfully uploaded to the %s SE" % (lfn,se)  

  def complete_add(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_get(self,args):
    """ Download file from grid and store in a local directory
    
        usage:
        
          get <lfn> [<local_directory>] 
    """
    
    argss = args.split()
    if (len(argss)==0):
      print self.do_get.__doc__
      return
    lfn = argss[0]
    lfn = self.getPath(lfn)
    dir_ = ''
    if len(argss)>1:
      dir_ = argss[1]
        
    dirac = Dirac()
    localCWD = ''
    if dir_:
      localCWD = os.getcwd()
      os.chdir(dir_)
    result = dirac.getFile(lfn)
    if localCWD:
      os.chdir(localCWD)
      
    if not result['OK']:
      print 'Error: %s' %(result['Message'])
    else:
      print "File %s successfully downloaded" % lfn      

  def complete_get(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

  def do_unregister(self,args):
    """ Unregister records in the File Catalog
    
        usage:
          unregister replica  <lfn> <se>
          unregister file <lfn>
          unregister dir <path>
    """        
    argss = args.split()
    if (len(argss)==0):
      print self.do_unregister.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'replica':
      if (len(argss) != 2):
        print self.do_unregister.__doc__
        return
      return self.removeReplica(argss)
    elif option == 'file': 
      if (len(argss) != 1):
        print self.do_unregister.__doc__
        return
      return self.removeFile(argss)
    elif option == "dir" or option == "directory":
      if (len(argss) != 1):
        print self.do_unregister.__doc__
        return
      return self.removeDirectory(argss)    
    else:
      print "Error: illegal option %s" % option

  # An Auto Completion For ``register``
  _available_unregister_cmd = ['replica', 'file', 'dir', 'directory']
  def complete_unregister(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) >= 2 and (args[1] in self._available_unregister_cmd):
      # if 'unregister file' or 'unregister replica' and so on exists,
      # try to do LFN auto completion.
      cur_path = ""
      if (len(args) == 3):
        cur_path = args[2]
      result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)
      return result

    result = [i for i in self._available_unregister_cmd if i.startswith(text)]
    return result
      
  def do_rmreplica(self,args):
    """ Remove LFN replica from the storage and from the File Catalog
    
        usage:
          rmreplica <lfn> <se>
    """        
    argss = args.split()
    if (len(argss) != 2):
      print self.do_rmreplica.__doc__
      return
    lfn = argss[0]
    lfn = self.getPath(lfn)
    print "lfn:",lfn
    se = argss[1]
    try:
      result =  self.fc.setReplicaStatus( {lfn:{'SE':se,'Status':'Trash'}} )
      if result['OK']:
        print "Replica at",se,"moved to Trash Bin"
      else:
        print "Failed to remove replica at",se
        print result['Message']
    except Exception, x:
      print "Error: rmreplica failed with exception: ", x

  def complete_rmreplica(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

    
  def do_rm(self,args):
    """ Remove file from the storage and from the File Catalog
    
        usage:
          rm <lfn>
          
        NB: this method is not fully implemented !    
    """  
    # Not yet really implemented
    argss = args.split()
    if len(argss) != 1:
      print self.do_rm.__doc__
      return
    self.removeFile(argss)

  def complete_rm(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
    
  def do_rmdir(self,args):
    """ Remove directory from the storage and from the File Catalog
    
        usage:
          rmdir <path>
          
        NB: this method is not fully implemented !  
    """  
    # Not yet really implemented yet
    argss = args.split()
    if len(argss) != 1:
      print self.do_rmdir.__doc__
      return
    self.removeDirectory(argss)  

  def complete_rmdir(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
          
  def removeReplica(self,args):
    """ Remove replica from the catalog
    """          
    
    path = args[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    rmse = args[1]
    try:
      result =  self.fc.removeReplica( {lfn:{'SE':rmse}} )
      if result['OK']:
        if 'Failed' in result['Value']:
          if lfn in result['Value']['Failed']:
            print "ERROR: %s" % ( result['Value']['Failed'][lfn])
          elif  lfn in result['Value']['Successful']:
            print "File %s at %s removed from the catalog" %( lfn, rmse )
          else:
            "ERROR: Unexpected returned value %s" % result['Value']
        else:
          print "File %s at %s removed from the catalog" %( lfn, rmse )
      else:
        print "Failed to remove replica at",rmse
        print result['Message']
    except Exception, x:
      print "Error: rmpfn failed with exception: ", x
      
  def removeFile(self,args):
    """ Remove file from the catalog
    """  
    
    path = args[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    try:
      result =  self.fc.removeFile(lfn)
      if result['OK']:
        if 'Failed' in result['Value']:
          if lfn in result['Value']['Failed']:
            print "ERROR: %s" % ( result['Value']['Failed'][lfn] )
          elif lfn in result['Value']['Successful']:
            print "File",lfn,"removed from the catalog"
          else:
            print "ERROR: Unexpected result %s" % result['Value']
        else:
          print "File",lfn,"removed from the catalog"
      else:
        print "Failed to remove file from the catalog"  
        print result['Message']
    except Exception, x:
      print "Error: rm failed with exception: ", x       
      
  def removeDirectory(self,args):
    """ Remove file from the catalog
    """  
    
    path = args[0]
    lfn = self.getPath(path)
    print "lfn:",lfn
    try:
      result =  self.fc.removeDirectory(lfn)
      if result['OK']:
        if result['Value']['Successful']:
          print "Directory",lfn,"removed from the catalog"
        elif result['Value']['Failed']:
          print "ERROR:", result['Value']['Failed'][lfn]  
      else:
        print "Failed to remove directory from the catalog"  
        print result['Message']
    except Exception, x:
      print "Error: rm failed with exception: ", x            
      
  def do_replicate(self,args):
    """ Replicate a given file to a given SE
        
        usage:
          replicate <LFN> <SE> [<SourceSE>]
    """
    argss = args.split()
    if len(argss) < 2:
      print "Error: unsufficient number of arguments"
      return
    lfn = argss[0]
    lfn = self.getPath(lfn)
    se = argss[1]
    sourceSE = ''
    if len(argss)>2:
      sourceSE=argss[2]
    try:
      dirac = Dirac()
      result = dirac.replicateFile(lfn,se,sourceSE,printOutput=True)      
      if not result['OK']:
        print 'Error: %s' %(result['Message'])
      elif not result['Value']:
        print "Replica is already present at the target SE"
      else:  
        print "File %s successfully replicated to the %s SE" % (lfn,se)  
    except Exception, x:
      print "Error: replicate failed with exception: ", x      
      
  def complete_replicate(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

  def do_replicas(self,args):
    """ Get replicas for the given file specified by its LFN

        usage: replicas <lfn>
    """
    argss = args.split()
    if (len(argss) == 0):
      print self.do_replicas.__doc__
      return
    apath = argss[0]
    path = self.getPath(apath)
    print "lfn:",path
    try:
      result =  self.fc.getReplicas(path)    
      if result['OK']:
        if result['Value']['Successful']:
          for se,entry in result['Value']['Successful'][path].items():
            print se.ljust(15),entry
      else:
        print "Replicas: ",result['Message']
    except Exception, x:
      print "replicas failed: ", x

  def complete_replicas(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
        
  def registerFile(self,args):
    """ Add a file to the catatlog 

        usage: add <lfn> <pfn> <size> <SE> [<guid>]
    """      
       
    path = args[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = args[1]
    infoDict['Size'] = int(args[2])
    infoDict['SE'] = args[3]
    if len(args) == 5:
      guid = args[4]
    else:
      _status,guid = commands.getstatusoutput('uuidgen')
    infoDict['GUID'] = guid
    infoDict['Checksum'] = ''    
      
    fileDict = {}
    fileDict[lfn] = infoDict  
      
    try:
      result = self.fc.addFile(fileDict)         
      if not result['OK']:
        print "Failed to add file to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(lfn):
          print 'Failed to add file:',result['Value']['Failed'][lfn]  
      elif result['Value']['Successful']:
        if result['Value']['Successful'].has_key(lfn):
          print "File successfully added to the catalog"    
    except Exception, x:
      print "add file failed: ", str(x)    
    
  def registerReplica(self,args):
    """ Add a file to the catatlog 

        usage: addpfn <lfn> <pfn> <SE> 
    """      
    path = args[0]
    infoDict = {}
    lfn = self.getPath(path)
    infoDict['PFN'] = args[1]
    if infoDict['PFN'] == "''" or infoDict['PFN'] == '""':
      infoDict['PFN'] = ''
    infoDict['SE'] = args[2]
      
    repDict = {}
    repDict[lfn] = infoDict    
      
    try:
      result = self.fc.addReplica(repDict)                    
      if not result['OK']:
        print "Failed to add replica to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        print 'Failed to add replica:',result['Value']['Failed'][lfn]   
      else:
        print "Replica added successfully:", result['Value']['Successful'][lfn]    
    except Exception, x:
      print "add pfn failed: ", str(x)    
      
  def do_ancestorset(self,args):
    """ Set ancestors for the given file
    
        usage: ancestorset <lfn> <ancestor_lfn> [<ancestor_lfn>...]
    """            
    
    argss = args.split()    
    if (len(argss) == 0):
      print self.do_ancestorset.__doc__
      return 
    lfn = argss[0]
    if lfn[0] != '/':
      lfn = self.cwd + '/' + lfn
    ancestors = argss[1:]
    tmpList = []
    for a in ancestors:
      if a[0] != '/':
        a = self.cwd + '/' + a
      tmpList.append(a)
    ancestors = tmpList       
    
    try:
      result = self.fc.addFileAncestors({lfn:{'Ancestors':ancestors}})
      if not result['OK']:
        print "Failed to add file ancestors to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        print "Failed to add file ancestors to the catalog: ",
        print result['Value']['Failed'][lfn]
      else:
        print "Added %d ancestors to file %s" % (len(ancestors),lfn)
    except Exception, x:
      print "Exception while adding ancestors: ", str(x)                
                         
  def complete_ancestorset(self, text, line, begidx, endidx):

    args = line.split()

    if ( len(args) == 1 ):
      cur_path = ""
    elif ( len(args) > 1 ):
      # If the line ends with ' '
      # this means a new parameter begin.
      if line.endswith(' '):
        cur_path = ""
      else:
        cur_path = args[-1]

    result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_ancestor(self,args):
    """ Get ancestors of the given file
    
        usage: ancestor <lfn> [depth]
    """            
    
    argss = args.split()
    if (len(argss) == 0):
      print self.do_ancestor.__doc__
      return
    lfn = argss[0]
    if lfn[0] != '/':
      lfn = self.cwd + '/' + lfn
    depth = [1]
    if len(argss) > 1:
      depth = int(argss[1])
      depth = range(1,depth+1)
        
    try:      
      result = self.fc.getFileAncestors([lfn],depth)
      if not result['OK']:
        print "ERROR: Failed to get ancestors: ",
        print result['Message']       
      elif result['Value']['Failed']:
        print "Failed to get ancestors: ",
        print result['Value']['Failed'][lfn]
      else:
        depthDict = {}  
        depSet = set()    
        for lfn,ancestorDict in  result['Value']['Successful'].items():
          for ancestor,dep in ancestorDict.items():     
            depthDict.setdefault(dep,[])
            depthDict[dep].append(ancestor)
            depSet.add(dep)
        depList = list(depSet)
        depList.sort()
        print lfn   
        for dep in depList:
          for lfn in depthDict[dep]:      
            print dep,' '*dep*5, lfn
    except Exception, x:
      print "Exception while getting ancestors: ", str(x)    

  def complete_ancestor(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
                                                                     
  def do_descendent(self,args):
    """ Get descendents of the given file
    
        usage: descendent <lfn> [depth]
    """            
    
    argss = args.split()
    if (len(argss) == 0):
      print self.do_descendent.__doc__
      return
    lfn = argss[0]
    if lfn[0] != '/':
      lfn = self.cwd + '/' + lfn
    depth = [1]
    if len(argss) > 1:
      depth = int(argss[1])
      depth = range(1,depth+1)
        
    try:
      result = self.fc.getFileDescendents([lfn],depth)
      if not result['OK']:
        print "ERROR: Failed to get descendents: ",
        print result['Message']       
      elif result['Value']['Failed']:
        print "Failed to get descendents: ",
        print result['Value']['Failed'][lfn]
      else:
        depthDict = {}  
        depSet = set()    
        for lfn,descDict in  result['Value']['Successful'].items():
          for desc,dep in descDict.items():     
            depthDict.setdefault(dep,[])
            depthDict[dep].append(desc)
            depSet.add(dep)
        depList = list(depSet)
        depList.sort()
        print lfn   
        for dep in depList:
          for lfn in depthDict[dep]:      
            print dep,' '*dep*5, lfn
    except Exception, x:
      print "Exception while getting descendents: ", str(x)              

  def complete_descendent(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
#######################################################################################
# User and group methods      
      
  def do_user(self,args):
    """ User related commands
    
        usage:
          user add <username>  - register new user in the catalog
          user delete <username>  - delete user from the catalog
          user show - show all users registered in the catalog
    """    
    argss = args.split()
    if (len(argss)==0):
      print self.do_user.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'add':
      if (len(argss)!=1):
        print self.do_user.__doc__
        return
      return self.registerUser(argss) 
    elif option == 'delete':
      if (len(argss)!=1):
        print self.do_user.__doc__
        return
      return self.deleteUser(argss) 
    elif option == "show":
      result = self.fc.getUsers()
      if not result['OK']:
        print ("Error: %s" % result['Message'])            
      else:  
        if not result['Value']:
          print "No entries found"
        else:  
          for user,id_ in result['Value'].items():
            print user.rjust(20),':',id_
    else:
      print "Unknown option:",option

  # completion for ``user``
  _available_user_cmd = ['add', 'delete', 'show']
  def complete_user(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) == 2 and (args[1] in self._available_user_cmd):
      # if the sub command exists,
      # Don't need any auto completion
      return result

    result = [i for i in self._available_user_cmd if i.startswith(text)]
    return result
    
  def do_group(self,args):
    """ Group related commands
    
        usage:
          group add <groupname>  - register new group in the catalog
          group delete <groupname>  - delete group from the catalog
          group show - how all groups registered in the catalog
    """    
    argss = args.split()
    if (len(argss)==0):
      print self.do_group.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'add':
      if (len(argss)!=1):
        print self.do_group.__doc__
        return
      return self.registerGroup(argss) 
    elif option == 'delete':
      if (len(argss)!=1):
        print self.do_group.__doc__
        return
      return self.deleteGroup(argss) 
    elif option == "show":
      result = self.fc.getGroups()
      if not result['OK']:
        print ("Error: %s" % result['Message'])            
      else:  
        if not result['Value']:
          print "No entries found"
        else:  
          for user,id_ in result['Value'].items():
            print user.rjust(20),':',id_
    else:
      print "Unknown option:",option  
  
  # completion for ``group``
  _available_group_cmd = ['add', 'delete', 'show']
  def complete_group(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) == 2 and (args[1] in self._available_group_cmd):
      # if the sub command exists,
      # Don't need any auto completion
      return result

    result = [i for i in self._available_group_cmd if i.startswith(text)]
    return result
  def registerUser(self,argss):
    """ Add new user to the File Catalog
    
        usage: adduser <user_name>
    """
 
    username = argss[0] 
    
    result =  self.fc.addUser(username)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "User ID:",result['Value']  
      
  def deleteUser(self,args):
    """ Delete user from the File Catalog
    
        usage: deleteuser <user_name>
    """
 
    username = args[0] 
    
    result =  self.fc.deleteUser(username)
    if not result['OK']:
      print ("Error: %s" % result['Message'])    
      
  def registerGroup(self,argss):
    """ Add new group to the File Catalog
    
        usage: addgroup <group_name>
    """
 
    gname = argss[0] 
    
    result =  self.fc.addGroup(gname)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "Group ID:",result['Value']    
      
  def deleteGroup(self,args):
    """ Delete group from the File Catalog
    
        usage: deletegroup <group_name>
    """
 
    gname = args[0] 
    
    result =  self.fc.deleteGroup(gname)
    if not result['OK']:
      print ("Error: %s" % result['Message'])         
         
  def do_mkdir(self,args):
    """ Make directory
    
        usage: mkdir <path>
    """
    
    argss = args.split()
    if (len(argss)==0):
      print self.do_group.__doc__
      return
    path = argss[0] 
    if path.find('/') == 0:
      newdir = path
    else:
      newdir = self.cwd + '/' + path
      
    newdir = newdir.replace(r'//','/')
    
    result =  self.fc.createDirectory(newdir)    
    if result['OK']:
      if result['Value']['Successful']:
        if result['Value']['Successful'].has_key(newdir):
          print "Successfully created directory:", newdir
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(newdir):  
          print 'Failed to create directory:',result['Value']['Failed'][newdir]
    else:
      print 'Failed to create directory:',result['Message']

  def complete_mkdir(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

  def do_cd(self,args):
    """ Change directory to <path>
    
        usage: cd <path>
               cd -
    """
 
    argss = args.split()
    if len(argss) == 0:
      path = '/'
    else:  
      path = argss[0] 
      
    if path == '-':
      path = self.previous_cwd
      
    newcwd = self.getPath(path)
    if len(newcwd)>1 and not newcwd.find('..') == 0 :
      newcwd=newcwd.rstrip("/")
    
    result =  self.fc.isDirectory(newcwd)        
    if result['OK']:
      if result['Value']['Successful']:
        if result['Value']['Successful'][newcwd]:
        #if result['Type'] == "Directory":
          self.previous_cwd = self.cwd
          self.cwd = newcwd
          self.prompt = 'FC:'+self.cwd+'>'
        else:
          print newcwd,'does not exist or is not a directory'
      else:
        print newcwd,'is not found'
    else:
      print 'Server failed to find the directory',newcwd

  def complete_cd(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_id(self,args):
    """ Get user identity
    """
    result = getProxyInfo()
    if not result['OK']:
      print "Error: %s" % result['Message']
      return
    user = result['Value']['username']
    group = result['Value']['group']
    result = self.fc.getUsers()
    if not result['OK']:
      print "Error: %s" % result['Message']
      return
    userDict = result['Value']
    result = self.fc.getGroups()
    if not result['OK']:
      print "Error: %s" % result['Message']
      return
    groupDict = result['Value']    
    idUser = userDict.get(user,0)
    idGroup = groupDict.get(group,0)
    print "user=%d(%s) group=%d(%s)" % (idUser,user,idGroup,group)
      
  def do_lcd(self,args):
    """ Change local directory
    
        usage:
          lcd <local_directory>
    """    
    argss = args.split()
    if (len(argss) != 1):
      print self.do_lcd.__doc__
      return
    localDir = argss[0]
    try:
      os.chdir(localDir)
      newDir = os.getcwd()
      print "Local directory: %s" % newDir
    except:
      print "%s seems not a directory" % localDir

  def complete_lcd(self, text, line, begidx, endidx):
    # TODO
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.ul_dc.parse_text_line(text, cur_path, self.cwd)

    return result
          
  def do_pwd(self,args):
    """ Print out the current directory
    
        usage: pwd
    """
    print self.cwd      

  def do_ls(self,args):
    """ Lists directory entries at <path> 

        usage: ls [-ltrn] <path>
    """
    
    argss = args.split()
    # Get switches
    _long = False
    reverse = False
    timeorder = False
    numericid = False
    path = self.cwd
    if len(argss) > 0:
      if argss[0][0] == '-':
        if 'l' in argss[0]:
          _long = True
        if 'r' in  argss[0]:
          reverse = True
        if 't' in argss[0]:
          timeorder = True
        if 'n' in argss[0]:
          numericid = True  
        del argss[0]  
          
      # Get path    
      if argss:        
        path = argss[0]       
        if path[0] != '/':
          path = self.cwd+'/'+path      
    path = path.replace(r'//','/')

    # remove last character if it is "/"    
    if path[-1] == '/' and path != '/':
      path = path[:-1]
    
    # Check if the target path is a file
    result =  self.fc.isFile(path)          
    if not result['OK']:
      print "Error: can not verify path"
      return
    elif path in result['Value']['Successful'] and result['Value']['Successful'][path]:
      result = self.fc.getFileMetadata(path)      
      dList = DirectoryListing()
      fileDict = result['Value']['Successful'][path]
      dList.addFile(os.path.basename(path),fileDict,{},numericid)
      dList.printListing(reverse,timeorder)
      return         
    
    # Get directory contents now
    try:
      result =  self.fc.listDirectory(path,_long)                   
      dList = DirectoryListing()
      if result['OK']:
        if result['Value']['Successful']:
          for entry in result['Value']['Successful'][path]['Files']:
            fname = entry.split('/')[-1]
            # print entry, fname
            # fname = entry.replace(self.cwd,'').replace('/','')
            if _long:
              fileDict = result['Value']['Successful'][path]['Files'][entry]['MetaData']
              repDict = result['Value']['Successful'][path]['Files'][entry].get( "Replicas", {} )
              if fileDict:
                dList.addFile(fname,fileDict,repDict,numericid)
            else:  
              dList.addSimpleFile(fname)
          for entry in result['Value']['Successful'][path]['SubDirs']:
            dname = entry.split('/')[-1]
            # print entry, dname
            # dname = entry.replace(self.cwd,'').replace('/','')  
            if _long:
              dirDict = result['Value']['Successful'][path]['SubDirs'][entry]
              if dirDict:
                dList.addDirectory(dname,dirDict,numericid)
            else:    
              dList.addSimpleFile(dname)
          
          for entry in result['Value']['Successful'][path]['Links']:
            pass
          
          if 'Datasets' in result['Value']['Successful'][path]:
            for entry in result['Value']['Successful'][path]['Datasets']:
              dname = os.path.basename( entry )    
              if _long:
                dsDict = result['Value']['Successful'][path]['Datasets'][entry]['Metadata']  
                if dsDict:
                  dList.addDataset(dname,dsDict,numericid)
              else:    
                dList.addSimpleFile(dname)
              
          if _long:
            dList.printListing(reverse,timeorder)      
          else:
            dList.printOrdered()
      else:
        print "Error:",result['Message']
    except Exception, x:
      print "Error:", str(x)

  def complete_ls(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_cnt = 0

    if (len(args) > 1):
      if ( args[1][0] == "-"):
        index_cnt = 1

    # the first argument -- LFN.
    if (1+index_cnt<=len(args)<=2+index_cnt):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_cnt) or (len(args)==2+index_cnt and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_cnt):
          cur_path = args[1+index_cnt]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_chown(self,args):
    """ Change owner of the given path

        usage: chown [-R] <owner> <path> 
    """         
    
    argss = args.split()
    recursive = False
    if (len(argss) == 0):
      print self.do_chown.__doc__
      return
    if argss[0] == '-R':
      recursive = True
      del argss[0]
    if (len(argss) != 2):
      print self.do_chown.__doc__
      return
    owner = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    pathDict[lfn] = owner
    
    try:
      result = self.fc.changePathOwner( pathDict, recursive )        
      if not result['OK']:
        print "Error:",result['Message']
        return
      if lfn in result['Value']['Failed']:
        print "Error:",result['Value']['Failed'][lfn]
        return  
    except Exception, x:
      print "Exception:", str(x)         

  def complete_chown(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0+1

    if '-R' in args:
      index_counter = 1+1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_chgrp(self,args):
    """ Change group of the given path

        usage: chgrp [-R] <group> <path> 
    """         
    
    argss = args.split()
    recursive = False
    if (len(argss) == 0):
      print self.do_chgrp.__doc__
      return
    if argss[0] == '-R':
      recursive = True
      del argss[0]
    if (len(argss) != 2):
      print self.do_chgrp.__doc__
      return
    group = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    pathDict[lfn] = group
    
    try:
      result = self.fc.changePathGroup( pathDict, recursive )         
      if not result['OK']:
        print "Error:",result['Message']
        return
      if lfn in result['Value']['Failed']:
        print "Error:",result['Value']['Failed'][lfn]
        return  
    except Exception, x:
      print "Exception:", str(x)    

  def complete_chgrp(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0+1

    if '-R' in args:
      index_counter = 1+1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_chmod(self,args):
    """ Change permissions of the given path
        usage: chmod [-R] <mode> <path> 
    """         
    
    argss = args.split()
    recursive = False
    if (len(argss) < 2):
      print self.do_chmod.__doc__
      return
    if argss[0] == '-R':
      recursive = True
      del argss[0]
    mode = argss[0]
    path = argss[1]
    lfn = self.getPath(path)
    pathDict = {}
    # treat mode as octal 
    pathDict[lfn] = eval('0'+mode)
    
    try:
      result = self.fc.changePathMode( pathDict, recursive )             
      if not result['OK']:
        print "Error:",result['Message']
        return
      if lfn in result['Value']['Failed']:
        print "Error:",result['Value']['Failed'][lfn]
        return  
    except Exception, x:
      print "Exception:", str(x)       
      
  def complete_chmod(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0+1

    if '-R' in args:
      index_counter = 1+1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_size(self,args):
    """ Get file or directory size. If -l switch is specified, get also the total
        size per Storage Element 

        usage: size [-l] [-f] <lfn>|<dir_path>
        
        Switches:
           -l  long output including per SE report
           -f  use raw file information and not the storage tables  
    """      
    
    argss = args.split()
    _long = False
    fromFiles = False
    if len(argss) > 0:
      if argss[0] == '-l':
        _long = True
        del argss[0]
    if len(argss) > 0:
      if argss[0] == '-f':
        fromFiles = True
        del argss[0]    
        
    if len(argss) == 1:
      path = argss[0]
      if path == '.':
        path = self.cwd    
    else:
      path = self.cwd
    path = self.getPath(path)
    
    try:
      result = self.fc.isFile(path)
      if not result['OK']:
        print "Error:",result['Message']
      if result['Value']['Successful']:
        if result['Value']['Successful'][path]:  
          print "lfn:",path
          result =  self.fc.getFileSize(path)
          if result['OK']:
            if result['Value']['Successful']:
              print "Size:",result['Value']['Successful'][path]
            else:
              print "File size failed:", result['Value']['Failed'][path]  
          else:
            print "File size failed:",result['Message']
        else:
          print "directory:",path
          result =  self.fc.getDirectorySize( path, _long, fromFiles )          
          if result['OK']:
            if result['Value']['Successful']:
              print "Logical Size:",int_with_commas(result['Value']['Successful'][path]['LogicalSize']), \
                    "Files:",result['Value']['Successful'][path]['LogicalFiles'], \
                    "Directories:",result['Value']['Successful'][path]['LogicalDirectories']
              if _long:
                fields = ['StorageElement','Size','Replicas']
                values = []
                if "PhysicalSize" in result['Value']['Successful'][path]:
                  print 
                  totalSize = result['Value']['Successful'][path]['PhysicalSize']['TotalSize']
                  totalFiles = result['Value']['Successful'][path]['PhysicalSize']['TotalFiles'] 
                  for se,sdata in result['Value']['Successful'][path]['PhysicalSize'].items():
                    if not se.startswith("Total"):
                      size = sdata['Size']
                      nfiles = sdata['Files']
                      #print se.rjust(20),':',int_with_commas(size).ljust(25),"Files:",nfiles
                      values.append( (se, int_with_commas(size), str(nfiles)) )
                  #print '='*60
                  #print 'Total'.rjust(20),':',int_with_commas(totalSize).ljust(25),"Files:",totalFiles
                  values.append( ('Total', int_with_commas(totalSize), str(totalFiles)) )
                  printTable(fields,values)  
              if "QueryTime" in result['Value']:
                print "Query time %.2f sec" % result['Value']['QueryTime']
            else:
              print "Directory size failed:", result['Value']['Failed'][path]
          else:
            print "Directory size failed:",result['Message']  
      else:
        print "Failed to determine path type"        
    except Exception, x:
      print "Size failed: ", x

  def complete_size(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    index_counter = 0

    if '-l' in args:
      index_counter = 1

    # the first argument -- LFN.
    if ((1+index_counter) <=len(args)<= (2+index_counter)):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1+index_counter) or (len(args)==2+index_counter and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2+index_counter):
          cur_path = args[1+index_counter]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def do_guid(self,args):
    """ Get the file GUID 

        usage: guid <lfn> 
    """      
    
    argss = args.split()
    if (len(argss) == 0):
      print self.do_guid.__doc__
      return
    path = argss[0]
    try:
      result =  self.fc.getFileMetadata(path)
      if result['OK']:
        if result['Value']['Successful']:
          print "GUID:",result['Value']['Successful'][path]['GUID']
        else:
          print "ERROR: getting guid failed"  
      else:
        print "ERROR:",result['Message']
    except Exception, x:
      print "guid failed: ", x   

  def complete_guid(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result

##################################################################################
#  Metadata methods
      
  def do_meta(self,args):
    """ Metadata related operations
    
        Usage:
          meta index [-d|-f|-r] <metaname> [<metatype>]  - add new metadata index. Possible types are:
                                                           'int', 'float', 'string', 'date';
                                                         -d  directory metadata
                                                         -f  file metadata
                                                         -r  remove the specified metadata index
          meta set <path> <metaname> <metavalue> - set metadata value for directory or file
          meta remove <path> <metaname>  - remove metadata value for directory or file
          meta get [-e] [<path>] - get metadata for the given directory or file
          meta tags <path> <metaname> where <meta_selection> - get values (tags) of the given metaname compatible with 
                                                        the metadata selection
          meta show - show all defined metadata indice
    
    """    
    argss = args.split()
    if (len(argss)==0):
      print self.do_meta.__doc__
      return
    option = argss[0]
    del argss[0]
    if option == 'set':
      if (len(argss) != 3):
        print self.do_meta.__doc__
        return
      return self.setMeta(argss)
    elif option == 'get':
      return self.getMeta(argss)  
    elif option[:3] == 'tag':
      # TODO
      if (len(argss) == 0):
        print self.do_meta.__doc__
        return
      return self.metaTag(argss)    
    elif option == 'index':
      if (len(argss) < 1):
        print self.do_meta.__doc__
        return
      return self.registerMeta(argss)
    elif option == 'metaset':
      # TODO
      if (len(argss) == 0):
        print self.do_meta.__doc__
        return
      return self.registerMetaset(argss)
    elif option == 'show':
      return self.showMeta()
    elif option == 'remove' or option == "rm":
      if (len(argss) != 2):
        print self.do_meta.__doc__
        return
      return self.removeMeta(argss) 
    else:
      print "Unknown option:",option  

  # auto completion for ``meta``
  # TODO: what's the doc for metaset?
  _available_meta_cmd = ["set", "get", "tag", "tags", 
                         "index", "metaset","show",
                         "rm", "remove"]
  _meta_cmd_need_lfn = ["set", "get",
                        "rm", "remove"]
  def complete_meta(self, text, line, begidx, endidx):
    result = []
    args = line.split()
    if len(args) >= 2 and (args[1] in self._available_meta_cmd):
      # if the sub command is not in self._meta_cmd_need_lfn
      # Don't need any auto completion
      if args[1] in self._meta_cmd_need_lfn:
        # TODO
        if len(args) == 2:
          cur_path = ""
        elif len(args) > 2:
          # If the line ends with ' '
          # this means a new parameter begin.
          if line.endswith(' '):
            cur_path = ""
          else:
            cur_path = args[-1]
          
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)
        pass
      return result

    result = [i for i in self._available_meta_cmd if i.startswith(text)]
    return result
            
  def removeMeta(self,argss):
    """ Remove the specified metadata for a directory or file
    """    
    apath = argss[0]
    path = self.getPath(apath)
    if len(argss) < 2:
      print "Error: no metadata is specified for removal"
      return
    
    metadata = argss[1:]
    result = self.fc.removeMetadata(path,metadata)
    if not result['OK']:
      print "Error:", result['Message']
      if "FailedMetadata" in result:
        for meta,error in result['FailedMetadata']:
          print meta,';',error
     
  def setMeta(self,argss):
    """ Set metadata value for a directory
    """      
    if len(argss) != 3:
      print "Error: command requires 3 arguments, %d given" % len(argss)
      return
    path = argss[0]
    if path == '.':
      path = self.cwd
    elif path[0] != '/':
      path = self.cwd+'/'+path  
    meta = argss[1]
    value = argss[2]
    print path,meta,value
    metadict = {}
    metadict[meta]=value
    result = self.fc.setMetadata(path,metadict)
    if not result['OK']:
      print ("Error: %s" % result['Message'])     
      
  def getMeta(self,argss):
    """ Get metadata for the given directory
    """            
    expandFlag = False
    dirFlag = True
    if len(argss) == 0:
      path ='.'
    else:  
      if argss[0] == "-e":
        expandFlag = True
        del argss[0]
      if len(argss) == 0:
        path ='.'  
      else:  
        path = argss[0]
        dirFlag = False
    if path == '.':
      path = self.cwd
    elif path[0] != '/':
      path = self.getPath(path)

    path = path.rstrip( '/' )
      
    if not dirFlag:
      # Have to decide if it is a file or not
      result = self.fc.isFile(path)
      if not result['OK']:
        print "ERROR: Failed to contact the catalog"      
      if not result['Value']['Successful']:
        print "ERROR: Path not found"
      dirFlag = not result['Value']['Successful'][path]        
        
    if dirFlag:    
      result = self.fc.getDirectoryUserMetadata(path)
      if not result['OK']:
        print ("Error: %s" % result['Message']) 
        return
      if result['Value']:
        metaDict = result['MetadataOwner']
        metaTypeDict = result['MetadataType']
        for meta, value in result['Value'].items():
          setFlag = metaDict[meta] != 'OwnParameter' and metaTypeDict[meta] == "MetaSet"
          prefix = ''
          if setFlag:
            prefix = "+"
          if metaDict[meta] == 'ParentMetadata':
            prefix += "*"
            print (prefix+meta).rjust(20),':',value
          elif metaDict[meta] == 'OwnMetadata':
            prefix += "!"
            print (prefix+meta).rjust(20),':',value   
          else:
            print meta.rjust(20),':',value 
          if setFlag and expandFlag:
            result = self.fc.getMetadataSet(value,expandFlag)
            if not result['OK']:
              print ("Error: %s" % result['Message']) 
              return
            for m,v in result['Value'].items():
              print " "*10,m.rjust(20),':',v      
      else:
        print "No metadata defined for directory"   
    else:
      result = self.fc.getFileUserMetadata(path)      
      if not result['OK']:
        print ("Error: %s" % result['Message']) 
        return
      if result['Value']:      
        for meta,value in result['Value'].items():
          print meta.rjust(20),':', value
      else:
        print "No metadata found"        
      
  def metaTag(self,argss):
    """ Get values of a given metadata tag compatible with the given selection
    """    
    path =  argss[0]
    del argss[0]
    tag = argss[0]
    del argss[0]
    path = self.getPath(path)
    
    # Evaluate the selection dictionary
    metaDict = {}
    if argss:
      if argss[0].lower() == 'where':
        result = self.fc.getMetadataFields()        
        if not result['OK']:
          print ("Error: %s" % result['Message']) 
          return
        if not result['Value']:
          print "Error: no metadata fields defined"
          return
        typeDictfm = result['Value']['FileMetaFields']
        typeDict = result['Value']['DirectoryMetaFields']

        
        del argss[0]
        for arg in argss:
          try:
            name,value = arg.split('=')
            if not name in typeDict:
              if not name in typeDictfm:
                print "Error: metadata field %s not defined" % name
              else:
                print 'No support for meta data at File level yet: %s' % name
              return
            mtype = typeDict[name]
            mvalue = value
            if mtype[0:3].lower() == 'int':
              mvalue = int(value)
            if mtype[0:5].lower() == 'float':
              mvalue = float(value)
            metaDict[name] = mvalue
          except Exception,x:
            print "Error:",str(x)
            return  
      else:
        print "Error: WHERE keyword is not found after the metadata tag name"
        return
      
    result = self.fc.getCompatibleMetadata( metaDict, path )  
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return
    tagDict = result['Value']
    if tag in tagDict:
      if tagDict[tag]:
        print "Possible values for %s:" % tag
        for v in tagDict[tag]:
          print v
      else:
        print "No compatible values found for %s" % tag       

  def showMeta(self):
    """ Show defined metadata indices
    """
    result = self.fc.getMetadataFields()  
    if not result['OK']:
      print ("Error: %s" % result['Message'])            
    else:
      if not result['Value']:
        print "No entries found"
      else:  
        for meta,_type in result['Value'].items():
          print meta.rjust(20),':',_type

  def registerMeta(self,argss):
    """ Add metadata field. 
    """

    if len(argss) < 2:
      print "Unsufficient number of arguments"
      return

    fdType = '-d'
    removeFlag = False
    if argss[0].lower() in ['-d','-f']:
      fdType = argss[0]
      del argss[0]
    if argss[0].lower() == '-r':
      removeFlag = True
      del argss[0]

    if len(argss) < 2 and not removeFlag:
      print "Unsufficient number of arguments"
      return

    mname = argss[0]
    if removeFlag:
      result = self.fc.deleteMetadataField(mname)
      if not result['OK']:
        print "Error:", result['Message']
      return

    mtype = argss[1]

    if mtype.lower()[:3] == 'int':
      rtype = 'INT'
    elif mtype.lower()[:7] == 'varchar':
      rtype = mtype
    elif mtype.lower() == 'string':
      rtype = 'VARCHAR(128)'
    elif mtype.lower() == 'float':
      rtype = 'FLOAT'
    elif mtype.lower() == 'date':
      rtype = 'DATETIME'
    elif mtype.lower() == 'metaset':
      rtype = 'MetaSet'
    else:
      print "Error: illegal metadata type %s" % mtype
      return

    result =  self.fc.addMetadataField(mname,rtype,fdType)
    if not result['OK']:
      print ("Error: %s" % result['Message'])
    else:
      print "Added metadata field %s of type %s" % (mname,mtype)   
 
  def registerMetaset(self,argss):
    """ Add metadata set
    """
    
    setDict = {}
    setName = argss[0]
    del argss[0]
    for arg in argss:
      key,value = arg.split('=')
      setDict[key] = value
      
    result =  self.fc.addMetadataSet(setName,setDict)
    if not result['OK']:
      print ("Error: %s" % result['Message'])  
    else:
      print "Added metadata set %s" % setName  
    
  def do_find(self,args):
    """ Find all files satisfying the given metadata information 
    
        usage: find [-q] [-D] <path> <meta_name>=<meta_value> [<meta_name>=<meta_value>]
    """   

    argss = args.split()
    if (len(argss) < 1):
      print self.do_find.__doc__
      return
    
    verbose = True
    if argss[0] == "-q":
      verbose  = False
      del argss[0]

    dirsOnly = False
    if argss[0] == "-D":
      dirsOnly = True
      del argss[0]

    path = argss[0]
    path = self.getPath(path)
    del argss[0]
 
    if argss:
      if argss[0][0] == '{':
        metaDict = eval(argss[0])
      else:  
        result = self.__createQuery(' '.join(argss))
        if not result['OK']:
          print "Illegal metaQuery:", ' '.join(argss), result['Message']
          return
        metaDict = result['Value']
    else:
      metaDict = {}    
    if verbose: print "Query:",metaDict

    result = self.fc.findFilesByMetadata(metaDict,path)
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return 

    if result['Value']:

      if dirsOnly:
        listToPrint = set( "/".join(fullpath.split("/")[:-1]) for fullpath in result['Value'] )
      else:
        listToPrint = result['Value']

      for dir_ in listToPrint:
        print dir_

    else:
      if verbose:
        print "No matching data found"      

    if verbose and "QueryTime" in result:
      print "QueryTime %.2f sec" % result['QueryTime']  

  def complete_find(self, text, line, begidx, endidx):
    result = []
    args = line.split()

    # skip "-q" optional switch
    if len(args) >= 2 and args[1] == "-q":
      if len(args) > 2 or line.endswith(" "):
        del args[1]

    # the first argument -- LFN.
    if (1<=len(args)<=2):
      # If last char is ' ',
      # this can be a new parameter.
      if (len(args) == 1) or (len(args)==2 and (not line.endswith(' '))):
        cur_path = ""
        if (len(args) == 2):
          cur_path = args[1]
        result = self.lfn_dc.parse_text_line(text, cur_path, self.cwd)

    return result
      
  def __createQuery(self,args):
    """ Create the metadata query out of the command line arguments
    """    
    result = self.fc.getMetadataFields()

    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return None
    if not result['Value']:
      print "Error: no metadata fields defined"
      return None
    typeDict = result['Value']['FileMetaFields']
    typeDict.update(result['Value']['DirectoryMetaFields'])
    
    # Special meta tags
    typeDict.update( FILE_STANDARD_METAKEYS )

    mq = MetaQuery( typeDict = typeDict )
    if not args:
      return S_ERROR("No MetaQuery passed to __createQuery()")
    elif args[0] in ['"', "'"]:
      if args[-1] not in ['"', "'"]:
        return S_ERROR("Missing %s" % (args[0]))
      args = args[1:-1]
    return mq.setMetaQuery( mq.parseQueryString(args) )

  def do_dataset( self, args ):
    """ A set of dataset manipulation commands
    
        Usage:
          
          dataset add [-f] <dataset_name> <meta_query>     - add a new dataset definition
          dataset anotate [-r] <dataset_name> <anotation>  - add annotation to a dataset
          dataset show [-l] [<dataset_name>]               - show existing datasets
          dataset status <dataset_name> [<dataset_name>]*  - display the dataset status
          dataset files <dataset_name>                     - show dataset files     
          dataset rm <dataset_name>                        - remove dataset
          dataset check <dataset_name> [<dataset_name>]*   - check if the dataset parameters are still valid
          dataset update <dataset_name> [<dataset_name>]*  - update the dataset parameters
          dataset freeze <dataset_name>                    - fix the current contents of the dataset     
          dataset release <dataset_name>                   - release the dynamic dataset
          dataset overlap <dataset_name1> <dataset_name2>  - check if two datasets have the same files
          dataset download <dataset_name>  [-d <target_dir>] [<percentage]
                                                           - download dataset
          dataset locate <dataset_name>                    - show dataset distribution over SEs
          dataset replicate <dataset_name> <SE>            - init a bulk replication of a frozen dataset files
    """
    argss = args.split()
    if (len(argss)==0):
      print self.do_meta.__doc__
      return
    command = argss[0]
    del argss[0]
    if command == "add":
      self.dataset_add( argss )
    elif command == "annotate":
      self.dataset_annotate( argss )    
    elif command == "show":
      self.dataset_show( argss )  
    elif command == "files":
      self.dataset_files( argss )
    elif command == "rm":
      self.dataset_rm( argss )   
    elif command == "check":
      self.dataset_check( argss )
    elif command == "update":
      self.dataset_update( argss )     
    elif command == "freeze":
      self.dataset_freeze( argss )
    elif command == "release":
      self.dataset_release( argss )      
    elif command == "status":
      self.dataset_status( argss )
    elif command == "download":
      self.dataset_download( argss )
    elif command == "locate":
      self.dataset_locate( argss )
    elif command == "overlap":
      self.dataset_overlap( argss )
    elif command == "replicate":
      self.dataset_replicate( argss )

  def dataset_add( self, argss ):
    """ Add a new dataset
    """
    usage_add = "dataset add [-f] <dataset_name> <meta_query>"
    if (len(argss) < 1):
      print usage_add
      return
    
    start = 0
    frozen = False
    if argss[0] == '-f':
      frozen = True
      start = 1
    datasetName = self.__dsCkeckArgs( argss[start:], usage_add )
    if not datasetName:
      return

    # parsing query
    metaSelections = ' '.join( argss[start + 1:] )
    metaDict = self.__createQuery(metaSelections)
    
    # parsing metaQuery testing !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if metaDict['OK']:
      uq = {'Meta1':1, 'Meta2':2.0, 'Meta3':'3'}
      mq = MetaQuery(metaDict['Value'], {'Meta1':'integer', 'Meta2':'float'})
      print MetaQuery(metaDict['Value']).prettyPrintMetaQuery()
      print mq.applyQuery(uq)
    else:
      print metaDict['Message']
    return
    
    if not metaDict:
      print usage_add
      print "ERROR: No or invalid meta query specified:"
      print "The meta query parts should be formated: <MetaField><Operator><Value>"
      print "Spaces should be only between parts"
      return
    
    result = self.fc.addDataset( datasetName, metaDict, frozen )
    if not result['OK']:
      print "ERROR: failed to add dataset:", result['Message']
    else:
      print "Successfully added dataset", datasetName

  def dataset_annotate( self, argss ):
    """ Add dataset annotation
    """
    rem = False
    usage = "Usage: dataset anotate [-r] <dataset_name> <anotation>"
    if (len(argss) < 1):
      print usage
      return
    
    if ( argss[0] == '-r' ):
      rem = True
      datasetName = self.__dsCkeckArgs( argss[1:], usage )
    else:
      datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return

    if rem:
      result = self.fc.rmDatasetAnnotation( datasetName )
      if result['OK']:
        print "Successfully removed annotation from", datasetName
      else:
        print "ERROR: failed to remove annotation:", result['Message']
      return

    annotation = ' '.join( argss[1:] )
    if not annotation:
      print usage
      return
    
    result = self.fc.addDatasetAnnotation( {datasetName: annotation} )
    if result['OK']:
      print "Successfully added annotation to", datasetName
    else:
      print "ERROR: failed to add annotation:", result['Message']

  def dataset_status( self, argss ):
    """ Display the dataset status
    """
    usage = "Usage: dataset status <dataset_name> [<dataset_name>]* "
    datasetNames = self.__dsCkeckArgsForArray( argss, usage )
    if not datasetNames:
      return  # exiting after printing usage in function

    result = self.fc.getDatasetStatuses( datasetNames )
    if not result['OK']:
      print "ERROR: failed to get status of dataset:", result['Message']
    else:
      self.__printDsPropertiesTable( result['Value'] )
#      fields = ["Key", "Value"]
#      for name, dicti in result['Value'].items():
#        print '\n' + name + ":"
#        print '=' * ( len( name ) + 1 )
#        records = {}
#        records = [[k, str( v )] for k, v in dicti.items()]
#        printTable( fields, records )

#      parDict = result['Value']
#      for par,value in parDict.items():
#        print par.rjust(20),':',value

  def dataset_rm( self, argss ):
    """ Remove the given dataset
    """
    usage = "Usage: dataset rm <dataset_name>"
    datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return

    result = self.fc.removeDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to remove dataset:", result['Message']
    else:
      print "Successfully removed dataset", datasetName  
      if result['Failed']:
        print "Some fileIDs couldn't be resolved:"
        # pprint( result['Failed'] )
      if result['LFNs']:
        print "The deleted dataset was frozen."
        print "Folowing files shall be deleted: "
        print '\n'.join( result['LFNs'] )
        ans = raw_input( "Do you agree to deleting these files? [y/n]" )
        if ans not in ['y', 'Y', 'yes', 'YES', 'Yes']:
          print "Files will not be deleted."  # Warning?
          return

        print "Initializing delete request"
        # making delete request
        req = Request()
        req.RequestName = "deletion of dataset %s files" % datasetName
        top = 0
        op = Operation()
        op.Type = 'RemoveFile'
        for lfn in result['LFNs']:

          if top > 90:
            req.addOperation( op )
            op = Operation()
            op.Type = 'RemoveFile'
            top = 0
            
          opFile = File()
          opFile.LFN = lfn
          op.addFile( opFile )
          top += 1

        pprint( req )
        # result = gRequestValid.validate( req )
        # if not result['OK']:
        #  print "ERROR:" , result['Message']

        # print result  # debug

        # TODO: uncomment inserting request to DB
        rc = ReqClient()
        # execute request: rc.putRequest(req)





  def dataset_check( self, argss ):
    """ check if the dataset parameters are still valid
    """
    usage = "Usage: dataset check <dataset_name> [<dataset_name>]* "
    datasetNames = self.__dsCkeckArgsForArray( argss, usage )
    if not datasetNames:
      return  # exiting after printing usage in function

    result = self.fc.checkDataset( datasetNames )
    if not result['OK']:
      print "ERROR: failed to check datasets:", result['Message']
    elif not result['Value']:
      print "OK: The dataset(s) are up to date"    
    else:
      dsProps = result['Value']
      labels = ['Key', 'Old Value', 'New Value']
      for ds in dsProps.keys():
        none = True
        records = []
        for err in dsProps[ds]['errCode']:
          if none:
            self.__printDsName( ds )
            none = False
          records.append( [err, str( dsProps[ds][err] ), str( dsProps[ds]['new'][err] )] )
        if not none:
          printTable( labels, records )
    return  # old implementation is next


#    datasetName = argss[0]
#    result = self.fc.checkDataset( datasetName )
#    if not result['OK']:
#      print "ERROR: failed to check dataset:", result['Message']
#    else:
#      changeDict = result['Value']
#      if not changeDict:
#        print "Dataset is not changed"
#      else:
#        print "Dataset changed:"
#        for par in changeDict:
#          print "   ",par,': ',changeDict[par][0],'->',changeDict[par][1]
          
  def dataset_update( self, argss ):
    """ Update the given dataset parameters
    """
    usage = "Usage: dataset update <dataset_name> [<dataset_name>]* "
    
    datasetNames = self.__dsCkeckArgsForArray( argss, usage )
    if not datasetNames:
      return  # exiting after printing usage in function

    result = self.fc.updateDataset(datasetNames)
    
    if not result['OK']:
      print result
    elif not result['Value']:
      print "All datasets were already up to date"
    else:
      print "Dataset(s) updated"
    return #old implementation is next

#    datasetName = argss[0]
#    result = self.fc.updateDataset( datasetName )
#    if not result['OK']:
#      print "ERROR: failed to update dataset:", result['Message']
#    else:
#      print "Successfully updated dataset", datasetName

  def dataset_freeze( self, argss ):
    """ Freeze the given dataset
    """
    usage = "Usage: dataset freeze <dataset_name>"
    datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return

    result = self.fc.freezeDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to freeze dataset:", result['Message']
    else:
      print "Successfully frozen dataset", datasetName            

  def dataset_release( self, argss ):
    """ Release the given dataset
    """
    usage = "Usage: dataset release <dataset_name>"
    datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return

    result = self.fc.releaseDataset( datasetName )
    if not result['OK']:
      print "ERROR: failed to release dataset:", result['Message']
    else:
      print "Successfully released dataset", datasetName       

  def dataset_files( self, argss ):
    """ Get the given dataset files
    """
    usage = "Usage: dataset files <dataset_name>"
    datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return
    result = self.fc.getDatasetFiles( datasetName )
    if not result['OK']:
      print "ERROR: failed to get files for dataset:", result['Message']
    elif not result['Value'] or result['Value'] == None:
      print "Dataset is empty"
    else:
      lfnList = result['Value']
      for lfn in lfnList:
        print lfn

  def dataset_show( self, argss ):
    """ Show existing requested datasets
    """
    long_ = False
    every = False
    largs = len( argss )
    datasetName = ''
    
    if largs == 0:
      every = True
    elif largs == 1:
      if argss[0] == '-l':
        every = True
        long_ = True
      else:
        datasetName = argss[0]
    elif largs == 2:
      long_ = True
      datasetName = argss[1]
    # too much params
    else: 
      print "Usage: dataset show [-l] [<dataset_name>]"
      return
    
    if datasetName:
      datasetName = self.getPath( datasetName )
    
    result = self.fc.showDatasets( datasetName, long_, every )
    if not result['OK']:
      print "ERROR: failed to get datasets"
      return

    datasetDict = result['Value']
    if not long_:
      for dName in datasetDict:
        print dName.replace( "//", "/" )
    else:
      self.__printDsPropertiesTable( datasetDict, True )
#      fields = ['Key','Value']
#      wanted = ['Status', 'MetaQuery', 'NumberOfFiles', 'Path']
#      datasets = datasetDict.keys()
#      dsAnnotations = {}
#      resultAnno = self.fc.getDatasetAnnotation( datasets )
#      if resultAnno['OK']:
#        dsAnnotations = resultAnno['Value']['Successful']
#      for dName in datasets:
#        records = []
#        print '\n'+dName+":"
#        print '='*(len(dName)+1)
#        for key,value in datasetDict[dName].items():
#          if key in wanted:
#            records.insert( 0, [key, str( value )] )
#          else:
#            records.append( [key, str( value )] )
# #        if dName in dsAnnotations:
# #          records.append( [ 'Annotation',dsAnnotations[dName] ] )
#        printTable( fields, records )

  def dataset_locate(self, argss):
    usage = "Usage: dataset locate <dataset_name>"
    datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return
    result = self.fc.getDatasetLocation( datasetName )
    if not result['OK']:
      print result['Message']
      return

    # pprint( result )

    dsSize = result['Value']['totalSize']
    repDict = result['Value']['replicas']
    sizeDict = result['Value']['fileSizes']
    location = {}

    # transform the replica dictionary in a se dictionary
    # creating SE dictionary
#    for se in result['Value']['SEs']:
#      location[se] = {}
#      location[se]['files'] = []
#      location[se]['size'] = 0

    # populationg SE dictionary
    for rep in repDict.keys():
      for se in repDict[rep].keys():
        if se not in location.keys():
          location[se] = {}
          location[se]['files'] = []
          location[se]['size'] = 0

        location[se]['files'].append( rep )
        location[se]['size'] += sizeDict[rep]

    # creating a pretty printable table
    values = []

    for se in location.keys():
      perc = int( float( location[se]['size'] ) / ( float( dsSize ) / 100 ) )
      line = [se, str( len( location[se]['files'] ) ), str( location[se]['size'] ) + "(" + str( perc ) + "%)"]
      values.append( line )

    fields = ['SE', 'Files', 'Size']
    printTable( fields, values )

  
  def dataset_overlap(self, argss):
    usage = "Usage: dataset overlap <dataset_name1> <dataset_name2>"
    datasetNames = self.__dsCkeckArgsForArray( argss, usage )
    if len( datasetNames ) != 2:
      print usage
      return

    result = self.fc.checkOverlapingDatasets( datasetNames )
    if not result['OK']:
      print result['Message']
    elif not result['Value']:
      print "Datasets don't overlap"
    else:
      lfns = result['Value']
      print '\n'.join( lfns )


  def dataset_replicate( self, argss ):
    usage = "Usage: dataset replicate <dataset_name> <SE>"
    if len( argss ) > 2:
      print usage
      return
    datasetName = self.__dsCkeckArgs( argss[:-1], usage )
    if not datasetName:
      return
    se = argss[-1]

    # get dataset files
    result = self.fc.getDatasetFilesWithChecksums( datasetName )
    if not result['OK']:
      print "ERROR: failed to replicate dataset:", result['Message']
    elif not result['Value'] or result['Value'] == None:
      print "Dataset is empty"
    else:  # we have a frozen non-empty dataset

      lfnDict = result['Value']

      print "initializing bulk replication"
      req = Request()
      req.RequestName = "replication of dataset %s files" % datasetName
      top = 0
      op = Operation()
      op.Type = 'ReplicateAndRegister'
      op.TargetSE = [ se ]
      for lfn, dicti in lfnDict.items():
        if top > 90:
          req.addOperation( op )
          op = Operation()
          op.Type = 'ReplicateAndRegister'
          op.TargetSE = [ se ]
          top = 0

        opFile = File()
        opFile.LFN = lfn
        opFile.Checksum = dicti['Checksum']
        opFile.ChecksumType = dicti['ChecksumType']
        op.addFile( opFile )
        top += 1

      print "Debug output:"
      for opFile in op:
        print opFile.LFN, opFile.Status, opFile.Checksum, opFile.ChecksumType
      # result = gRequestValid.validate( req )
      # if not result['OK']:
      #  print "ERROR:" , result['Message']

      # TODO: uncomment inserting request to DB
      rc = ReqClient()
      # execute request:
      # rc.putRequest(req)

    return

  def dataset_download(self, argss):
    usage = "Usage: dataset download <dataset_name>  [-d <target_dir>] [<percentage>]"
    datasetName = self.__dsCkeckArgs( argss, usage )
    if not datasetName:
      return
    
    dsBaseName = os.path.basename( datasetName )

    perc = 100
    lcwd = os.getcwd()
    # if dir given, change the working dir
    if len( argss ) > 1 and '-d' == argss[1]:
      target_dir = argss[2]
      try:
        os.chdir(target_dir)
      except:
        print "Directory %s doesn't exist, creating" % target_dir
        os.makedirs(target_dir)
        os.chdir(target_dir)
    # parse percentage
    elif len( argss ) == 2:
      perc = int( argss[1] )
    elif len( argss ) == 4:
      perc = int( argss[3] )

    # make dataset directory
    if os.path.exists( dsBaseName ):
      ans = raw_input( "The directory %s/%s exists, do you want to replace it [y/n]" % ( os.getcwd(), dsBaseName ) )
      if ans not in ['y', 'Y', 'yes', 'YES', 'Yes']:
        print "user abort"
        return
      from shutil import rmtree
      rmtree( os.getcwd() + '/' + dsBaseName )

    os.makedirs( dsBaseName )
    os.chdir( dsBaseName )

    # get dataset files with sizes
    result = self.fc.getDatasetFiles( datasetName )
    if not result['OK']:
      print "Unable to retrieve dataset files: %s" % result['Message']
    if not result['Value']:
      print "Dataset is empty"
    # pprint( result )
    
    # determine what files to download
    lfns = []
    if perc < 100:
      target = perc * result['TotalSize'] / 100
      # what a nice N-P problem we got here :)

      # sorting the lfn list by file size
      files = sorted( [( value, key ) for ( key, value ) in result['FileSizes'].items()] , reverse = True )

      testLast = result['TotalSize']
      curSize = 0
      lfns = []
      # iterating through the sorted list
      for size, lfn in files:
        # just a test
        if size > testLast:
          print "Error in sorting files by size"
        # if the file can be added, add it
        if curSize + size < target:
          lfns.append( lfn )
        testLast = size
      print "perc not 100"
    else:
      lfns = result['Value']
    
    # download the files
    failed = {}
    successful = []
    dirac = Dirac()
    for lfn in lfns:
      result = dirac.getFile( lfn )
      if not result['OK']:
        mes = eval( result['Message'] )
        failed[lfn] = mes['Failed'][lfn]
      else:
        successful.append( lfn )

    if successful:
      print "Successfuly downloaded files: %s" % ', '.join( successful )
    if failed:
      print "Faild to download files:"
      printTable( ['lfn', 'error'], [[k, v] for k, v in failed.items()] )
     
    if lcwd:
      os.chdir( lcwd )
    return

  def __printDsPropertiesTable( self, datasetDict, byID = False ):
    """
      Prints a pretty table of dictionary[dName][propDict]
    """
    fields = ['Key', 'Value']
    wanted = ['Status', 'MetaQuery', 'NumberOfFiles', 'Path']
    datasets = datasetDict.keys()
    for dName in datasets:
      records = []
      if byID:
        name = datasetDict[dName]['DatasetName']
      else:
        name = dName
      self.__printDsName( name )
      for key, value in datasetDict[dName].items():
        if key in wanted:
          records.insert( 0, [key, str( value )] )
        else:
          records.append( [key, str( value )] )
      printTable( fields, records )

  def __printDsName( self, dsName ):
    print '\n' + dsName.replace( "//", "/" ) + ":"
    print '=' * ( len( dsName ) + 1 )

  def __dsCkeckArgs( self, argss, usage ):
    """
      Ckecks args with one argument commands. If OK,
    """
    if len( argss ) < 1:
      print usage
      return ''

    # return datasetName
    return  self.getPath( argss[0] )


  def __dsCkeckArgsForArray( self, argss, usage ):
    if len( argss ) < 1:
      print usage
      return ''

    return [self.getPath( dsName ) for dsName in argss]

  def do_stats( self, args ):
    """ Get the catalog statistics
    
        Usage:
          stats
    """
    
    try:
      result = self.fc.getCatalogCounters()
    except AttributeError, x:
      print "Error: no statistics available for this type of catalog:", str(x)
      return
      
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return 
    fields = ['Counter','Number']
    records = []
    for key,value in result['Value'].items():
      records.append( ( key, str(value) ) )
      #print key.rjust(15),':',result['Value'][key]
    printTable( fields, records )    
      
  def do_rebuild( self, args ):
    """ Rebuild auxiliary tables
    
        Usage:
           rebuild <option>
    """
    
    argss = args.split()
    _option = argss[0]
    start = time.time()
    result = self.fc.rebuildDirectoryUsage( timeout = 300 )
    if not result['OK']:
      print "Error:", result['Message']
      return 
      
    total = time.time() - start
    print "Directory storage info rebuilt in %.2f sec", total    
    
  def do_repair( self, args ):
    """ Repair catalog inconsistencies
    
        Usage:
           repair catalog
    """
    
    argss = args.split()
    _option = argss[0]
    start = time.time()
    result = self.fc.repairCatalog()
    if not result['OK']:
      print "Error:", result['Message']
      return 
      
    total = time.time() - start
    print "Catalog repaired in %.2f sec", total      
      
  def do_exit(self, args):
    """ Exit the shell.

    usage: exit
    """
    sys.exit(0)
    
  def do_quit(self,args):
    """ Exit the shell
    
    usage: quit
    """
    sys.exit(0)

  def emptyline(self): 
    pass      
      
if __name__ == "__main__":
  
  if len(sys.argv) > 2:
    print FileCatalogClientCLI.__doc__
    sys.exit(2)

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  catalogs = None
  if len(sys.argv) == 2:
    catalogs = [ sys.argv[1] ]
  fc = FileCatalog( catalogs = catalogs )
  cli = FileCatalogClientCLI( fc )
  if catalogs:
    print "Starting %s file catalog client", catalogs[0]
  cli.cmdloop()
      
