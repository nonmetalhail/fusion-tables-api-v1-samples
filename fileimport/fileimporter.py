#!/usr/bin/python
#
# Copyright (C) 2010 Google Inc.

""" Imports files.

Imports CSV files into Fusion Tables.

2 July 2010: edited by Elliot Nahman for new API
"""

__author__ = 'kbrisbin@google.com (Kathryn Hurley)'


from sql.sqlbuilder import SQL
import csv, time, sys, codecs


class Importer:
  def importFile(self, filename):
    pass

  def importMoreRows(self, filename):
    pass

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class CSVImporter(Importer):
  def __init__(self, ftclient, batchsize=1):
    self.ftclient = ftclient
    self.batch_size = batchsize

  def importFile(self, filename, table_name=None, encoding="utf-8", data_types=None):
    """ Creates new table and imports data from CSV file """
    filehandle = UnicodeReader(open(filename, "rb"), encoding=encoding)
    cols = filehandle.next()
    if data_types: columns_and_types = dict(zip(cols, data_types))
    else: columns_and_types = dict([(c, "STRING") for c in cols])

    table = {}
    table[table_name or filename] = columns_and_types
    # results = self.ftclient.query(SQL().createTable(table))
    results = self.ftclient.service.query().sql(sql=SQL().createTable(table)).execute(self.ftclient.http)
    # table_id = int(results.split()[1])
    table_id = results['rows'][0][0]

    self._importRows(filehandle, table_id, cols, 0)

    return table_id

  def importMoreRows(self, filename, table_id, encoding="utf-8", skip_rows=0):
    """ Imports more rows in a CSV file to an existing table. First row is a header """

    filehandle = UnicodeReader(open(filename, "rb"), encoding=encoding)
    return self._importRows(filehandle, table_id, filehandle.next(), skip_rows)

  def _importRows(self, filehandle, table_id, cols, skip_rows):
    """ Helper function to upload rows of data in a CSV file to a table """
    current_row = 0
    queries = []
    rows = range(skip_rows)
    total_rows = skip_rows
    # if skip_rows > 0 skip the rows
    for r in rows:
      filehandle.next()

    for line in filehandle:
      values = dict(zip(cols, line))
      query = SQL().insert(table_id, values)
      queries.append(query)
      current_row += 1
      total_rows+=1
      if current_row == self.batch_size:
        full_query = ';'.join(queries)
        num_tries = 1
        while (num_tries < 10):
          try:
            # print(full_query)
            self.ftclient.service.query().sql(sql=full_query).execute(self.ftclient.http)
            # print(rows)
            # rows += self.ftclient.query(full_query).split("\n")[1:-1]
            num_tries = 10
          except:
            print "Exception on attempt {0}".format(num_tries)
            print str(sys.exc_info()[1])
            #print full_query + "\n"
            num_tries += 1
            time.sleep(2)

        print (total_rows)

        current_row = 0
        queries = []

    if len(queries) > 0:
      full_query = ';'.join(queries)
      try:
        self.ftclient.service.query().sql(sql=full_query).execute(self.ftclient.http)
        # print(rows)
      except:
        print str(sys.exc_info()[1])
        print full_query

    return total_rows

if __name__ == "__main__":
  pass
