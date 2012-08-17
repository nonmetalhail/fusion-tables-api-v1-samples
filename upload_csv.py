#!/usr/bin/python

'''
Created Jun 21, 2012

Library to upload a csv file or folder of csv files to Fusion Tables
* Relies on Kathryn's CSVImporter and batch uplaod relies on my FileFolder class
* Class ColTypes checks column types so uploaded columns will have correct type (hopefully)
* Class FileUploader has methonds to upload a csv file
* Class UploadCSV is is a child class of FileUploader which basically instantiates 
    and runs FileUploader methods
* Class BatchUpload wraps FileUploader in a for loop to iterate through directory files
'''

__author__ = "nahman@google.com (Elliot Nahman)"

import csv
import os
import sys
from fileimport.fileimporter import CSVImporter
from FileFolder import *
from sql.sqlbuilder import SQL

class ColType:
  '''
  This class processes the data columns and returns the column type
  when creating a FT, this type can then be applied to the data cols
  '''
  #defines the enumeration of column type
  String = 0
  Number = 1
  Date = 2
  Null = 3

  def detect_ColType(self,item):
    # return column type of input string
    if (len(item) == 0):
      return self.Null
    try: 
      df = float(item)
      return self.Number
    except:
      return self.String

  def ft_ColType(self,col_stat):
    # return Fusion Table column type based on overall column type stats
    # if more than 90% of non null entries are numeric define as NUMBER
    if (col_stat[1] > 0.9 * (col_stat[0]+col_stat[1])):
      return 'NUMBER'
    else:
      return 'STRING'

class FileUploader(object):
  '''
  this class contains methods to upload a csv file/create a FT
  '''
  batchsize = 250
  csv_file = ''
  csv_reader = None
  col_types = []
  skip_rows = 0

  def openFile(self,file_name):
    """
    Opens the csv file and creates a csv reader object
    Supply it a file path
    Returns an csv reader object
    """
    print('trying file: {0}'.format(file_name.split('/')[-1]))
    try:
      cfile = open(file_name, 'rb')
      return csv.reader(cfile)
    except:
      print('failed to open ' + csv_file)
      return False

  def skipMetadataRows(self, skip_rows,csv_reader):
    """
    Option to skip metadata rows above the header
    Supply csv reader object and an integer for number or rows to skip
    """
    for i in range(skip_rows):
      csv_reader.next()

  def findColTypes(self,csv_reader):
    """
    finds the col data types by using the ColTypes class methods
    Supply the csv reader object
    Returns list of col types
    """
    ct = ColType()
    hdr = csv_reader.next()  
    # first iterate through rows collecting column type statistics
    # initialize column type statistics array
    col_stats = [[0,0,0,0] for i in range(len(hdr))]

    for row in csv_reader:
      for col in range(len(row)):
        col_stats[col][ct.detect_ColType(row[col])] += 1

    # create type array
    return [ct.ft_ColType(cstat) for cstat in col_stats]

  def uploadCSV(self,ft_client,csv_file,col_types,batchsize=250,table_name = None):
    """
    uploads the csv employing the fileimporter.CSVImporter module
    Supply the OAuth Object, file path, col types list
    optional batch size int and table name string
    Returns the Table ID
    """
    tableid = CSVImporter(ft_client,batchsize).importFile(csv_file, 
      table_name or csv_file.split('/')[-1], "windows-1252", col_types)
    print 'Created new table with id {0}'.format(tableid)
    return tableid

class UploadFile(FileUploader):
  """
  Instantiation of FileUploader to upload a single file
  Supply the file path, 
  """
  def __init__(self,csv_file,ft_client,table_name = None, skip_rows = 0, batchsize = 250):
    self.csv_file = csv_file
    self.csv_reader = self.openFile(self.csv_file)
    self.batchsize = batchsize
    self.tableid = None

    if not self.csv_reader:
      return
    if skip_rows:
      self.skip_rows = skip_rows
      self.skipMetadataRows(self.skip_rows,self.csv_reader)
    # do initial read of csv file to determine columns types
    try:
      self.col_types = self.findColTypes(self.csv_reader)
      self.tableid = self.uploadCSV(ft_client,
        self.csv_file,self.col_types,self.batchsize,table_name)
    except:
      print('something went wrong with: {0}'.format(self.csv_file))
      print str(sys.exc_info()[1])

class BatchUpload(FileUploader):
  """
  Instantiation of FileUploader to upload a directory of files
  supply the file folder path, the OAuth object
  Optional: list of table names, Int of header rows to skip, Int of batch size
  """
  def __init__(self,filefolder,ft_client,table_names = None, 
    skip_rows = 0, batchsize = 250):
    self.path = filefolder
    self.filefolder = FileFolder(filefolder)
    self.ft_client = ft_client
    self.table_names = table_names
    self.skip_rows = skip_rows
    self.batchsize = batchsize
  
  def uploadFolder(self):
    """
    wraps the file upload in a for loop for file in directory
    returns a list of table ids
    """
    tableid_list = []
    for f in self.filefolder.files:
      print(f)
      path_file = '{0}{1}'.format(self.path,f)
      ufo = UploadFile(path_file,self.ft_client,self.table_names, 
        self.skip_rows, self.batchsize)
      if ufo.tableid:
        tableid_list.append(ufo.tableid)
    return tableid_list

class GetTableInfo:
  """
  Method to call a DESCRIBE on a table
  supply OAuth object and tableid
  Return result
  """
  def getTableInfo(self,ftclient,table_id):
    info = ftclient.service.table().get(tableId = table_id).execute(ftclient.http)
    return info

class MergeTable(object):
  """
  Method to merge two tables
  Supply the Oauth object, Table IDs for both tables, Column names to merge on 
    for both tables, and an optional new table name
  """
  def mergeTable(self,ftclient,base_table_id,second_table_id, 
    base_column_name, second_column_name, merge_table_name = None):
    # if no name is supplied for the new table, 
    # get the existing table name and create the standard "Merge of {}" name
    if not merge_table_name:
      table_info = GetTableInfo().getTableInfo(ftclient,base_table_id)
      name = table_info['name']
      merge_table_name = 'Merge of {0}'.format(name)

    merge_sql = SQL().mergeTable(base_table_id,second_table_id,base_column_name,
      second_column_name,merge_table_name)

    print(merge_sql)

    results = self.ftclient.service.query().sql(sql=merge_sql).execute(self.ftclient.http)
    print(results)

    return results


class TableMerger(MergeTable):
  """
  Instantiation of MergeTable Class
  """
  def __init__(self,ftclient,base_table_id,second_table_id, 
    base_column_name, second_column_name, merge_table_name = None):
    self.ftclient = ftclient
    self.base_table_id = base_table_id
    self.second_table_id = second_table_id
    self.base_column_name = base_column_name
    self.second_column_name = second_column_name
    self.merge_table_name = merge_table_name

    self.merge_tid = self.mergeTable(self.ftclient,self.base_table_id,
      self.second_table_id,self.base_column_name,self.second_column_name,
      self.merge_table_name)

if __name__ == "__main__":
  pass