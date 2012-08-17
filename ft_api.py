#!/usr/bin/python2.4
# Copyright (C) 2012 Google Inc.
#
"""
  Fusion Table API.
  Confusing flag system, but hopefully will be ok  :)
    There are a set of bool flags which indicate which operations to run
    Then a set of string flags which are your inputs
  Example: python ft_api.py --d --id 1EAdKyzl5QQGhnF3IJzuDYO0nKqJK2a6hHqGC33E
    This says: run a describe on this tableID

  4 operations are supported:
    1) --d flag: running a describe on a table. 
        Supply the --id: encrypted tableID
    2) --u flag: upload a file
        Supply the --f: file name with path
    3) --b flag: batch upload a directory of csv files
        Supply the --dir: file directory
    4) --m flag: merge two tables
        Merge will want: 4 parts --id & --id2:encrypted tablesIDs
        and --col & --col2: columns in those tables to join on

        There are 3 types of merge as I see it:
        1) straight merge--provide two table tablesIDs (WHY would you not use GUI?)
        2) upload and merge--provide file name and tid
        3) batch upload and merge--provide dir path and tid
        
        Although you could run #1, why? I did for testing purposes and it is commented out.
        #2 and #3 supported here and --id are taken directly from their output
        
        Example of upload and merge:
        python ft_api.py --u --m --f foo.csv --id2 1EAdKyzl5QQGhnF3IJzuDYO0nKqJK2a6hHqGC33E --col Geography --col2 Name
        Says: upload file --f, run a merge with --f tableID, id2, based on --col and --col2

        This of course assumes that your merge key col names are the same for all your files.
        Good luck if they are not :P



  IMPORTANT: at the time of this upload (and the end of my internship) there is
    a bug in the API where the insert row command (which is a part of the 
    upload file method) is being passed in the url rather than the body. Thus, 
    batch size is set to 1. When that is fixed, it will be much faster to 
    increase that number and/or might require a slight change in the 
    fileimport.fileimporter from CSVImporter module. Also, this will soon be a
    legacy way to upload a file once direct upload of csvs is supported.

"""

__author__ = 'nahman@google.com (Elliot Nahman)'

import gflags
import logging
import os
import pprint
import sys

# from fileimport.fileimporter import CSVImporter
from ftoauth import FT_OAuth
from oauth2client.client import AccessTokenRefreshError
from sql.sqlbuilder import SQL
from upload_csv import *

FLAGS = gflags.FLAGS
gflags.DEFINE_string('id',None,'encrypted tableID')
gflags.DEFINE_string('id2',None,'encrypted tableID for table 2')
gflags.DEFINE_string('col',None,'table1 join column')
gflags.DEFINE_string('col2',None,'table2 join column')
gflags.DEFINE_string('dir',None,'file directory')
gflags.DEFINE_string('f',None,'file name')
gflags.DEFINE_string('f2',None,'file name')
gflags.DEFINE_bool('d',False, 'DESCRIBE tableID')
gflags.DEFINE_bool('u',False, 'Upload File')
gflags.DEFINE_bool('b',False, 'Batch Upload Dir')
gflags.DEFINE_bool('m',False, 'Merge two tablesIDs based on col and col2')


# The gflags module makes defining command-line options easy for
# applications. Run this program with the '--help' argument to see
# all the flags that it understands.
gflags.DEFINE_enum('logging_level', 'ERROR',
    ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    'Set the level of logging detail.')

    #service.table()
    #service.column()
    #service.template()
    #service.style()
    #service.query()
    #  .sql(sql=foo)
    #  .sqlGet(sql=foo)


def main(argv):
  tids = []
  # Let the gflags module process the command-line arguments
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError, e:
    print '%s\\nUsage: %s ARGS\\n%s' % (e, argv[0], FLAGS)
    sys.exit(1)

  # Set the logging according to the command-line flag
  logging.getLogger().setLevel(getattr(logging, FLAGS.logging_level))

  ftclient = FT_OAuth()

  try:
    if FLAGS.d and FLAGS.id is not None:
      # seems to be two ways of doing this now; what is the diff?
      # call query and pass Kathryn SQL string:
      # describe = ftclient.service.query().sql(sql=SQL().describeTable(FLAGS.id)).execute(ftclient.http)
      # print(describe)

      # alternatively, using the upload_csv module and table rather than query:
      desc = GetTableInfo()
      describe1 = desc.getTableInfo(ftclient,FLAGS.id)
      print(describe1)

    #upload a single file; tid of resultant table returned in case of merge.
    if FLAGS.u and FLAGS.f is not None:
      table_name=''
      
      upload = UploadFile(FLAGS.f,ftclient, batchsize=1)
      print(upload.tableid)
      tids.append(upload.tableid)
    
    # batch upload a dir. List of tids returned in case of merge
    if FLAGS.b and FLAGS.dir is not None:
      table_name=''

      batch = BatchUpload(FLAGS.dir, ftclient,skip_rows = 0, batchsize = 1)
      batch_table_list = batch.uploadFolder()
      print(batch_table_list)
      tids += batch_table_list

    # if you uploaded a file or dir, then merge with returned tid
    if FLAGS.m and tids: 
      for tid in tids:
        merge = TableMerger(ftclient,tid,FLAGS.id2,FLAGS.col,FLAGS.col2)
        print(merge.merge_tid)

    # for testing purpose. this will always run right now if --m flag
    # if FLAGS.m: 
    #   merge = TableMerger(ftclient,'11uzeiRjVpXrzZxAXH5nzejvQNJ1EhcBI59TTc9I',
    #     '1zFhy_llcaShAvCIKjMTxsT3TA1yRXtSnh6GNkdw','Geography','Name')
    #   # merge = TableMerger(ftclient,FLAGS.id,FLAGS.id2,FLAGS.col,FLAGS.col2)
    #   print(merge.merge_tid)

  except AccessTokenRefreshError:
    print ("The credentials have been revoked or expired, please re-run"
      "the application to re-authorize")

if __name__ == '__main__':
  main(sys.argv)
