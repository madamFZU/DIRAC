########################################################################
# File: MetaQuery.py
# Author: A.T.
# Date: 24.02.2015
# Updated: VI.2015 by M.Adam
# $HeadID$
########################################################################
#from _ctypes import Array
from __builtin__ import list, True
from pprint import pprint

""" Utilities for managing metadata based queries
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
import DIRAC.Core.Utilities.Time as Time

import json
import copy
from string import maketrans

FILE_STANDARD_METAKEYS = { 'SE': 'VARCHAR',
                           'CreationDate': 'DATETIME',
                           'ModificationDate': 'DATETIME',
                           'LastAccessDate': 'DATETIME',
                           'User': 'VARCHAR',
                           'Group': 'VARCHAR',
                           'Path': 'VARCHAR',
                           'Name': 'VARCHAR',
                           'FileName': 'VARCHAR',
                           'CheckSum': 'VARCHAR',
                           'GUID': 'VARCHAR',
                           'UID': 'INTEGER',
                           'GID': 'INTEGER',
                           'Size': 'INTEGER',
                           'Status': 'VARCHAR' }

FILES_TABLE_METAKEYS = { 'Name': 'FileName',
                         'FileName': 'FileName',
                         'Size': 'Size',
                         'User': 'UID',
                         'Group': 'GID',
                         'UID': 'UID',
                         'GID': 'GID',
                         'Status': 'Status' }

FILEINFO_TABLE_METAKEYS = { 'GUID': 'GUID',
                            'CheckSum': 'CheckSum',
                            'CreationDate': 'CreationDate',
                            'ModificationDate': 'ModificationDate',
                            'LastAccessDate': 'LastAccessDate' }

LOGICAL_OPERATORS = ['AND', 'OR', 'NOT']

OPOSITES =          {'=' : '!=',
                     '!=': '=',
                     '>' : '<=',
                     '<=': '>',
                     '<' : '>=',
                     '>=': '<'}

#TODO: probably change this
DEFAULT_TYPE = "String"

TRANSLATETAB = maketrans("[]", "()")

class MetaQuery( object ):

  # helper construct to translate operator to function
  def do_gt(self,left, right):
    return left > right
  def do_lt(self,left, right):
    return left < right
  def do_ge(self,left, right):
    return left >= right
  def do_le(self,left, right):
    return left <= right
  def do_eq(self,left, right):
    return left == right
  def do_neq(self,left,right):
    return left != right

  def __init__( self, queryList = None, typeDict = None ):

    self.__metaQueryList = []
    if isinstance(queryList, list):
      self.__metaQueryList = queryList
    if isinstance(queryList, dict):
      self.__metaQueryList = [queryList]
      
    self.__metaTypeDict = {}
    if typeDict is not None:
      self.__metaTypeDict = typeDict
      
    self.compareFunct = {'eq' : self.do_eq, 
                  '!=': self.do_neq,
                  '>' : self.do_gt,
                  '>=': self.do_ge,
                  '<' : self.do_lt,
                  '<=': self.do_le}
  
  def loadQueryList(self, queryList):
    """ Load a new query list  without loosing the metaTypeDict
    """
    self.__metaQueryList = []
    if isinstance(queryList, list):
      self.__metaQueryList = queryList
    if isinstance(queryList, dict):
      self.__metaQueryList = [queryList]
      
  def parseQueryString(self, queryString):
    """
        Correctly parse the meta query string from user input
        
        :param queryString (string): user input to be parsed to become queryList
        :returns array of parsed literals for the setMetaQuery function
    """
    operators = ['>','<','!','=',',',')','(']
    allowed = ['/','.']
    tokens = []
    nextStr = ""
    
    def __push(nextStr):
      if not nextStr:
        return ""
      tokens.append(nextStr)
      return ""
    
    op = False
    quote = ""
    
    for i in range(0, len(queryString)):
      ch = queryString[i]
    
      # first check for string parsing (parse values in quotes)
      if ch == quote:
        nextStr = __push(nextStr)
        quote = ""
      elif quote:
        nextStr += (ch)
      elif ch in ["'", '"']:
        quote = ch
        if op:
          nextStr = __push(nextStr)
          op = False          
      
      # check for two char operators, all of which have the second char '='
      elif op and ch == '=':
        op = False
        nextStr += ch
        nextStr = __push(nextStr)
    
      # the operator is just single char
      elif op:
        op = False
        nextStr = __push(nextStr)
        if not ch.isspace():
          nextStr += (ch)
    
      # char can be part of name or value, just push it
      elif ch.isalpha() or ch.isdigit() or ch in allowed:
        nextStr += (ch)
    
      # char can be part of an operator
      elif ch in operators:
        __push(nextStr)
        nextStr = ch
        op = True
    
      elif ch.isspace():
        nextStr = __push(nextStr)
    
    __push(nextStr)
    return tokens

  def setMetaQuery( self, queryList, metaTypeDict = None ):
    """ 
        Create the metadata query out of the command line arguments
        
        :param  queryList (list): list of correctly parsed meta query string
        :param  metaTypeDict (dict): dictionary of the metadata to types (
    """
    
    if metaTypeDict is not None:
      self.__metaTypeDict = metaTypeDict
      
    # making the old notation compatible
    if 'AND' not in queryList and 'OR' not in queryList:
      opInds = []
      ind = 0
      for atom in queryList:
        if atom in OPOSITES and ind > 2:
          opInds.append(ind-1)
        ind += 1
      opInds.reverse()
      for ind in opInds:
        queryList.insert(ind, 'AND')
      
    # iterate through queryList input parameter and normalize the query into DNF
    result = self.__convertToDNF(queryList)
    if result['OK']:
      self.__metaQueryList = result['Value']
      self.__metaQueryList = self.__optimize(self.__metaQueryList)
      return S_OK(self.__metaQueryList)
    else: 
      return S_ERROR(result['Message'])
      

  def getMetaQuery( self ):

    return self.__metaQueryList

  def getMetaQueryAsJson( self ):

    return json.dumps( self.__metaQueryList )
  
  def prettyPrintMetaQuery(self):
    
    orFirst = True
    out = ""
    sortedMQList = sorted( self.__metaQueryList, key=lambda k: k[sorted(k.keys())[0]] )
    for conj in sortedMQList:
      if not orFirst:
        out += "OR "
      else:
        orFirst = False
        
      andFirst = True
      for meta, mDict in conj.items():
        if not andFirst:
          out += "AND "
        else:
          andFirst = False
        if isinstance(mDict, dict):
          out += "%s %s %s " % (meta, str(mDict.keys()[0]), str(mDict[mDict.keys()[0]]))
        else:
          if self.__metaTypeDict and meta in self.__metaTypeDict and self.__metaTypeDict[meta] in ['STRING', 'varchar', 'VARCHAR(128)']:
            out += "%s = '%s'" % (meta,mDict)
          else:
            out += "%s = %s " % (meta, str(mDict))
        if '[' in out: 
          # translate '=' to 'in'
          out = out.translate(TRANSLATETAB)
    return out

  def applyQuery( self, userMetaDict ):
    """  Return a list of tuples with tables and conditions to locate files for a given user Metadata
    """
    # TODO: asd
    def getOperands( value ):
      if isinstance( value, list ):
        return [ ('=', value) ]
      elif isinstance( value, dict ):
        resultList = []
        for operation, operand in value.items():
          resultList.append( ( operation, operand ) )
        return resultList
      else:
        return [ ("eq", value) ]

    def getTypedValue( value, mtype ):
      if mtype[0:3].lower() == 'int':
        return int( value )
      elif mtype[0:5].lower() == 'float':
        return float( value )
      elif mtype[0:4].lower() == 'date':
        return Time.fromString( value )
      else:
        return value
    
    for conj in self.__metaQueryList:
      for meta, value in conj.items():
        #print "meta %s value %s" % (meta, str(value))
        conjRes = True
        # Check if user dict contains all the requested meta data
        userValue = userMetaDict.get( meta, None )
        #print "userValue ", userValue
        if userValue is None:
          if str( value ).lower() == 'missing':
            continue
          else:
            conjRes = False
            break
        elif str( value ).lower() == 'any':
          continue
  
        mtype = self.__metaTypeDict.get(meta, 'None')
        if mtype == 'None':
          pass
          #return S_ERROR('Cannot check type of meta')
        
        try:
          userValue = getTypedValue( userValue, mtype )
        except ValueError:
          return S_ERROR( 'Illegal type for metadata %s: %s in user data' % ( meta, str( userValue ) ) )
  
        # Get parsed values 
        for operation, operand in getOperands( value ):
          #print "operation %s and operand %s" % (operation, operand)
          try:
            if isinstance( operand, list ):
              typedValue = [ getTypedValue( x, mtype ) for x in operand ]
            else:
              typedValue = getTypedValue( operand, mtype )
          except ValueError:
            return S_ERROR( 'Illegal type for metadata %s: %s in filter' % ( meta, str( operand ) ) )
  
          # Apply query operation
          if isinstance( typedValue, list ):
            if operation == '!=':
              if userValue in typedValue:
                conjRes = False
                break
            elif operation == '=':
              if userValue not in typedValue:
                conjRes = False
                break
          
          else: # value is not a list
            if not self.compareFunct[operation](userValue,typedValue):
              #print "applied + false"
              conjRes = False
              break
            #print "applied + true"
      if conjRes:
        return S_OK( True )

    return S_OK( False )
  
  def combineWithMetaQuery(self, combQueryList):
    """ Combine with another MetaQuery in a conjunction
    """
    try:
      out = self.__addToConj(self.__metaQueryList, combQueryList)
      out = self.__optimize(out)
    except RuntimeError as e:
      return S_ERROR('Error combining: %s' % str(e))
    
    return S_OK(out)    

  #============================================Private Methods===============================================
  
  def __optimize(self, queryList):
    mq = queryList
    toDel = []
    
    # check for subsets and supersets in the metaQueryList
    for i in range(0,len(mq)-1):
      for j in range(i+1,len(mq)):
        # check if mq[i] is superset of mq[j]
        d = self.__overlaps(mq[i], mq[j])
        if d == 1:
          toDel.append(i)
        elif d == 2: 
          toDel.append(j)
          
    
    # create a list of indexes of elements to delete from the metaQueryList
    toDel = list(set(toDel))
    toDel.sort(reverse=True) 
    
    # delete
    for i in toDel:
      del mq[i]
      
    return mq
  
  def __overlaps(self, lDict, rDict):
    """
    Pick a weaker term to delete from disjunction
    :return 1 if lDict is weaker than rDict, 2 if rDict is weaker than lDict, 0 otherwise
    """
    commonKeys = set(lDict.keys()).intersection(set(rDict.keys()))
    # check if the two terms consider at least one common meta 
    if not commonKeys:
      return 0
    lscore = 0
    rscore = 0
    
    # for each meta
    for key in lDict.keys():
      lVal = lDict[key]
      rVal = rDict[key]
      
      # check if both conditions are '=' 
      if not isinstance(lVal, dict) and not isinstance(rVal, dict):
        # if they are not the same, both sides are relevant
        if lVal != rVal:
          return 0
        else: # values are the same
          continue
      
      done = False
      tried = False
      while not done:
        
        if isinstance(lVal.values()[0], list):
          tried = True
          lVal ,rVal  = rVal ,lVal
          lDict,rDict = rDict,lDict
          continue
        
        # one of the conditions is '=' for a single value
        if not isinstance(lVal, dict):
          if dict.keys()[0] == '!=' and isinstance(dict.values()[0],list):
            
          rscore += 1
          if not self.__valueIsInInterval(lDict, rDict):
            lscore += 1
        # similar situation 
        if not isinstance(rVal, dict):
          tried = True
          lVal ,rVal  = rVal ,lVal
          lDict,rDict = rDict,lDict
          continue
            
        # now both are dicts
        
        #TODO: finish 
        
        if not tried:
          lVal ,rVal  = rVal ,lVal
          lDict,rDict = rDict,lDict
        elif not done:
          raise RuntimeError("Problem when optimizing. Conditions: " + str(lDict) + " and " + str(rDict) )
        
    # both conditions have =/= 0 score -> both are relevant
    if lscore > 0 and rscore > 0: return 0
    # only left has non-zero score -> right is weaker 
    elif lscore > 0: return 2
    # last situation
    else: return 1
  
  def __valueIsInInterval(self,val,cond):
    # getting operand
    op = cond.keys()[0]
    
    # testing a simple exclusion
    if op == '!=' and not isinstance(cond['!='], list):
      if val == cond['!=']: return False
      else: return True 
    
    # checking value to a list of value
    if op == '=':
      if val in cond[op]: return True
      else: return False
      
    # checking if the value is in a exclusive list
    if op == '!=':
      if val in cond[op]: return False
      else: return True
    return False
  
  def __convertToDNF(self,inputList,negGlobal=False):
    out = []
    termTmp = []
    newTerm = []
    last = []
    conjBuff = [] # buffer for storing intermediate conjunction 
    neg = negGlobal
  
    i = 0
    while i < len(inputList):
      atom = inputList[i]
      # cut out the correct bracket and send it down in recursion
      if atom == '(':
        iLocal = i +1
        depth = 1
        while depth > 0:
          if iLocal == len(inputList):
            return S_ERROR("Wrong bracketing")
          if inputList[iLocal] == '(':
            depth += 1
          elif inputList[iLocal] == ')':
            depth -= 1
          iLocal += 1
          
        #print "recursion IN"
        result = self.__convertToDNF(inputList[(i+1):iLocal-1],neg)
        if result['OK']:
          #print "recursion OK"
          last = result['Value']
        else:
          #print "recursion FAILED"
          return result
        i = iLocal - 1
          
      elif atom in LOGICAL_OPERATORS:
        # if there is a term in the buffer, parse it
        if termTmp:
          newTerm = self.__parseTerm(termTmp)
          if newTerm == None:
            return S_ERROR('Wrong term syntax: ' + ' '.join(termTmp))
          termTmp = []
        
        # the operators mean opposite things, when a bracket with a not in front is parsed
        if negGlobal:
          if atom == 'AND':
            atom = 'OR'
          elif atom == 'OR':
            atom = 'AND'
        
        
        if atom == 'AND':
          try:
            if last:
              conjBuff = self.__addToConj(conjBuff, last)
              last = []
            elif newTerm:
              conjBuff = self.__addToConj(conjBuff, [newTerm])
              newTerm = []          
            else:
              return S_ERROR('logical AND in invalid position')
          except RuntimeError as e:
            return S_ERROR( 'Error occured: %s' % str(e) )
        
        else:
          if atom == 'NOT':
            neg = not neg # toggle the neg flag
          
          elif atom == 'OR':
            # if the or comes after a plain term and there was not a conjunction      
            if not conjBuff and newTerm:
              out.append(newTerm)
              newTerm = []
            
            # there was a conjunction
            if conjBuff:
              try:
                if last:
                  out.extend(self.__addToConj(conjBuff, last))
                  last = []
                           
                elif newTerm:
                  out.extend(self.__addToConj(conjBuff, [newTerm]))
                  newTerm = []
                  
                conjBuff = []
              except RuntimeError as e:
                return S_ERROR( 'Error occured: %s' % str(e) )
              
      else:
        if neg and atom in OPOSITES.keys():
          termTmp.append(OPOSITES[atom])
        else:
          termTmp.append(atom)
          
      # incrementing the counter
      i += 1
    
    # adding the last part
    if termTmp:
      #print "termTmp ", termTmp, " newTerm ", newTerm, " last ", last, " conjBuff", conjBuff
      newTerm = self.__parseTerm(termTmp)
      if newTerm == None:
        return S_ERROR('Wrong term syntax: ' + ' '.join(termTmp))
    else:
      newTerm = []
    
    if not conjBuff and last:
      out.extend(last)
      if newTerm:
        out.append(newTerm)
    elif not conjBuff:
      if newTerm:
        out.append(newTerm)
      elif last:
        out.extend(last)
      else:
        return S_ERROR('no term in the end')
    else:
      try:
        if last:
          out.extend(self.__addToConj(conjBuff, last))
        elif newTerm:
          out.extend(self.__addToConj(conjBuff, [newTerm]))
        else:
          return S_ERROR('no term in the end')
      except RuntimeError as e:
        return S_ERROR( 'Error occured: %s' % str(e) )
      
    return S_OK(out)
    
  def __parseTerm(self,termList):
    """ From list input parse term. If error in parsing, return None
        The term is formated: metaName operator value [, value]*
    """
    # meta name is always the first
    metaName = termList[0]
    
    # get operator and check its validity
    operator = termList[1]
    if operator not in OPOSITES.keys():
      return None
    
    # get the value type from local metaTypeDict
    mtype = DEFAULT_TYPE
    if metaName in self.__metaTypeDict:
      mtype = self.__metaTypeDict[metaName]
      if mtype[:3].lower() == "int":
        mtype = "int"
      elif mtype[:5].lower() == "float":
        mtype = "float"

    isList = False
    # get the right value
    try:
      if "," in termList: # the value is a list
        if operator not in ['=','!=']:
          return None
        isList = True
        if mtype == "int":
          value = [int(val) for val in termList[2:] if ',' not in val]
        elif mtype == "float":
          value = [float(val) for val in termList[2:] if ',' not in val]
        else:
          value = [val for val in termList[2:] if ',' not in val]
      else: # value is simple
        if mtype == "int":
          value = int(termList[2])
        elif mtype == "float":
          value = float(termList[2])
        else:
          value = termList[2]
    except ValueError: # if someone tries to insert a invalid (not parsable) value
      return None
        
    if not isList and operator == "=":
      return {metaName:value}
    else:
      return {metaName:{operator:value}}
    
  def __addToConj(self,conj,newTerm):
    """ Add another term to temporary conjunction buffer
    """
    #print "call conj: %s newTerm %s" % (conj, newTerm) 
    out = []
    if not conj:
      return newTerm
    # combine conjunction items from the buffer and the new increment
    for itemOld in conj:
      for itemNew in newTerm:
        # if the two conjunctions concern metas, just mechanically combine them
        commonKeys = list(set(itemOld.keys()).intersection(itemNew.keys()))
        if not commonKeys:
          tmp = copy.deepcopy(itemOld)
          tmp.update(itemNew)
          out.append(tmp)
        # the two conjunctions overlap
        else:
          resDict = {}
          # preserve keys that are not in commonKeys
          for key,value in itemOld.items():
            if key not in commonKeys:
              resDict[key] = value
          for key,value in itemNew.items():
            if key not in commonKeys:
              resDict[key] = value
          
          for key in commonKeys:
            lDict = itemOld[key]
            rDict = itemNew[key]
            if isinstance(lDict,dict):
              lKey = lDict.keys()[0]
              lVal = lDict[lKey]
            else:
              lKey = 'eq'
              lVal = itemOld[key]
            if isinstance(rDict, dict):
              rKey = rDict.keys()[0]
              rVal = rDict[rKey]
            else:
              rKey = 'eq'
              rVal = itemNew[key]
            
            # now try all the combination and combine the possible operators. For sake of code shortness, the combining is
            # done regarding order and if not matched, the operators are switched and it is tried again 
            tried = False
            done = False
            while not done:
              
              if lKey == 'eq':
                
                if rKey == '=':
                  if lVal in rVal:
                    out.append({key:rDict})
                    done = True
                  else:
                    raise RuntimeError("__addToConj error: trying to AND: "+MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery())
                
                elif rKey == '!=' and isinstance(rVal, list):
                  if lVal not in rVal:
                    out.append({key:lDict})
                    done = True
                  else:
                    raise RuntimeError("__addToConj error: trying to AND: "+MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery())
                  
                else: # eq, >, >=, <, <=
                  if self.compareFunct[rKey](lVal, rVal):
                    out.append({key:lDict})
                    done = True
                  else:
                    raise RuntimeError("__addToConj error: trying to AND: "+MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery())

              elif lKey == '=': # for arrays
                
                if rKey == '=': # for two arrays, combine the array
                  out.append({key:{'=': set(lVal).add(set(rVal))}})
                  done = True
                  
                elif rKey == '!=' and isinstance(rVal, list):
                  if not list(set(lVal) & set(rVal)): # the two lists have no intersection
                    out.append({key:lDict})
                    done = True
                  else:
                    raise RuntimeError("__addToConj error: trying to AND: "+MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery())
                  
                elif rKey != 'eq':
                  newVal = [val for val in lVal if self.compareFunct[rKey](val, rVal)]
                  out.append( {key: {'=': newVal}} )
                  done = True
                  
              elif lKey == '!=' and isinstance(lVal, list):
                
                if rKey == '!=':
                  if isinstance(rVal,list):
                    newVal = rVal
                  else:
                    newVal = [rVal]
                  out.append( {key : {'!=' : list(set(lVal).add(rVal))}} )
                  done = True
                
                elif rKey in ['>', '>=', '<=', '<']:
                  newLVal = [val for val in lVal if self.compareFunct[rKey](val,rVal)]
                  out.append( {key : {'!=' : newLVal , rKey : rVal}} )
                  done = True
                  
              elif lKey == '!=': # with non-list value
                
                if rKey == '!=':
                  if isinstance(rVal,list):
                    newVal = rVal
                  else:
                    newVal = [rVal]
                  out.append( {key : {'!=' : list(set(lVal).add(rVal))}} )
                  done = True
                  
                elif rKey in ['>', '>=', '<=', '<']:
                  if self.compareFunct[rKey](lVal,rVal):
                    out.append( { key : { lKey:lVal, rKey:rVal }} )
                  else:
                    out.append(rDict)
                  done = True
                  
              elif lKey == '>' or lKey == '>=':
                
                if rKey in ['>', '>=']:
                  if lVal > rVal:
                    out.append({key:lDict})
                  else:
                    out.append({key:rDict})
                  done = True
                
                elif rKey in ['<', '<=']:
                  if lVal > rVal:
                    raise RuntimeError("__addToConj error: trying to AND: "+MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery())
                  elif lVal == rVal:
                    if '=' in lKey or '=' in rKey:
                      out.append({key:lVal})
                      done = True
                    else: # values are the same, but intervals are sharp -> no solution 
                      raise RuntimeError("__addToConj error: trying to AND: "+MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery())
                  else: # lVal <[=] rVal -> there is a solution 
                    out.append( {key: {lKey:lVal, rKey:rVal}} )
                    done = True
                
              elif lKey == '<' or lKey == '<=':
                if rKey == '<' or rKey == '<=':
                  if lVal < rVal:
                    out.append({key:lDict})
                  else:
                    out.append({key:rDict})
                  done = True

              
              if not tried:
                tried = True
                # swapping values for another run
                lKey, rKey = rKey, lKey
                lDict, rDict = rDict, lDict
                lVal, rVal = rVal, lVal
              elif not done: # and tried
                raise RuntimeError("__addToConj error, combination" + MetaQuery(itemNew).prettyPrintMetaQuery() + " with " + MetaQuery(itemOld).prettyPrintMetaQuery() +". not supported, please contact developer")
          # merge with non-common keys
          if resDict:
            out[-1].update(resDict)   
    #print "Returning: ", out
    return out      