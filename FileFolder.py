#!/usr/bin/python

'''
Created Jun 21, 2012

give it a path, it will get files in that directory, 
create a new subfolder
'''

__author__ = "nahman@google.com (Elliot Nahman)"

import os

class FileFolder(object):
  path = ''
  files = []
  # out_folder = ''
  
  def __init__(self,path):
    self.validatePath(path)
    self.path = path
    self.path = self.checkPathFormat(self.path)
    self.files = self.getFiles(self.path)
    # self.out_folder = createNewFolder(self.path)

  #Make sure it is a valid path
  def validatePath(self,path):
    if not os.path.exists(path):
      sys.exit('bad path')
  
  #fix path if need be
  def checkPathFormat(self,path):
    if path[-1] != '/':
      return '{0}{1}'.format(path,'/')
    else:
      return path
  
  #Get files in that path
  def getFiles(self, path):
    return os.listdir(path)

  def createNewFolder(self,path,new_folder_name='clean/'):
    new_folder_name = self.checkPathFormat(new_folder_name)
    out_folder = '{0}{1}'.format(path,new_folder_name)
    # should add a checker if folder exists...
    os.mkdir(out_folder)
    return out_folder
