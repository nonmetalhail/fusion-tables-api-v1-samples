#!/usr/bin/python
#
# Copyright (C) 2010 Google Inc.

""" Builds SQL strings.

Builds SQL strings to pass to FTClient query method.
"""

import re

__author__ = 'kbrisbin@google.com (Kathryn Hurley)'


class SQL:
  """ Helper class for building SQL queries """

  def showTables(self):
    """ Build a SHOW TABLES sql statement.

    Returns:
      the sql statement
    """
    return 'SHOW TABLES'

  def describeTable(self, table_id):
    """ Build a DESCRIBE <tableid> sql statement.

    Args:
      table_id: the ID of the table to describe

    Returns:
      the sql statement
    """
    return 'DESCRIBE {0}'.format(table_id)

  def createTable(self, table):
    """ Build a CREATE TABLE sql statement.

    Args:
      table: a dictionary representing the table. example:
        {
          "tablename":
            {
            "col_name1":"STRING",
            "col_name2":"NUMBER",
            "col_name3":"LOCATION",
            "col_name4":"DATETIME"
            }
        }

    Returns:
      the sql statement
    """

    table_name = table.keys()[0]
    cols_and_datatypes = ",".join(["'{0}': {1}".format(col[0], col[1]) 
                                   for col in table.get(table_name).items()])
    return "CREATE TABLE '{0}' ({1})".format(table_name, cols_and_datatypes)


  def select(self, table_id, cols=None, condition=None):
    """ Build a SELECT sql statement.

    Args:
      table_id: the id of the table
      cols: a list of columns to return. If None, return all
      condition: a statement to add to the WHERE clause. For example,
        "age > 30" or "Name = 'Steve'". Use single quotes as per the API.

    Returns:
      the sql statement
    """
    stringCols = "*"
    if cols: stringCols = ("'{0}'".format("','".join(cols))) \
                          .replace("\'rowid\'", "rowid") \
                          .replace("\'ROWID\'", "ROWID")

    if condition: select = 'SELECT {0} FROM {1} WHERE {2}'.format(stringCols, table_id, condition)
    else: select = 'SELECT {0} FROM {1}'.format(stringCols, table_id)
    return select


  def update(self, table_id, cols, values=None, row_id=None):
    """ Build an UPDATE sql statement.

    Args:
      table_id: the id of the table
      cols: list of columns to update
      values: list of the new values
      row_id: the id of the row to update

      OR if values is None and type cols is a dictionary -

      table_id: the id of the table
      cols: dictionary of column name to value pairs
      row_id: the id of the row to update

    Returns:
      the sql statement
    """
    if row_id == None: return None

    if type(cols) == type({}):
      updateStatement = ""
      count = 1
      for col,value in cols.iteritems():
        if type(value).__name__ == 'int':
          updateStatement = '{0}{1}={2}'.format(updateStatement, col, value)
        elif type(value).__name__ == 'float':
          updateStatement = '{0}{1}={2}'.format(updateStatement, col, value)
        else:
          updateStatement = "{0}{1}='{2}'".format(updateStatement, col,
              value.encode('string-escape'))

        if count < len(cols): updateStatement = "{0},".format(updateStatement)
        count += 1
  
      return "UPDATE {0} SET {1} WHERE ROWID = '{2}'".format(table_id,
          updateStatement, row_id)

    else:
      if len(cols) != len(values): return None
      updateStatement = ""
      count = 1
      for i in range(len(cols)):
        updateStatement = "{0}'{1}' = ".format(updateStatement, cols[i])
        if type(values[i]).__name__ == 'int':
          updateStatement = "{0}{1}".format(updateStatement, values[i])
        elif type(values[i]).__name__ == 'float':
          updateStatement = "{0}{1}".format(updateStatement, values[i])
        else:
          updateStatement = "{0}'{1}'".format(updateStatement, 
              values[i].encode('string-escape'))
  
        if count < len(cols): updateStatement = "{0},".format(updateStatement)
        count += 1
  
      return "UPDATE {0} SET {1} WHERE ROWID = '{2}'".format(table_id, updateStatement, row_id)

  def delete(self, table_id, row_id):
    """ Build DELETE sql statement.

    Args:
      table_id: the id of the table
      row_id: the id of the row to delete

    Returns:
      the sql statement
    """
    return "DELETE FROM {0} WHERE ROWID = '{1}'".format(table_id, row_id)


  def insert(self, table_id, values):
    """ Build an INSERT sql statement.

    Args:
      table_id: the id of the table
      values: dictionary of column to value. Example:
        {
        "col_name1":12,
        "col_name2":"mystring",
        "col_name3":"Mountain View",
        "col_name4":"9/10/2010"
        }

    Returns:
      the sql statement
    """
    stringValues = ""
    count = 1
    cols = values.keys()
    values = values.values()
    for value in values:
      if type(value).__name__=='int':
        stringValues = '{0}{1}'.format(stringValues, value)
      elif type(value).__name__=='float':
        stringValues = '{0}{1}'.format(stringValues, value)
      else:
        value = value.replace("\\","\\\\")
        value = value.replace("'","\\'")
        stringValues = "{0}'{1}'".format(stringValues, value)
      if count < len(values): stringValues = "{0},".format(stringValues)
      count += 1

    return 'INSERT INTO {0} ({1}) VALUES ({2})'\
      .format(table_id, ','.join(["'{0}'".format(col) for col in cols]), stringValues)

  def dropTable(self, table_id):
    """ Build DROP TABLE sql statement.

    Args:
      table_id: the id of the table

    Returns:
      the sql statement
    """
    return "DROP TABLE {0}".format(table_id)

  def mergeTable(self,base_table_id,second_table_id,base_col,second_col,merge_table_name):
    """ Build MERGE TABLE sql statement.

    Args:
      base_table_id: the id of the first table
      second_table_id: the id of the second table
      base_col: the name of the merge col in the base table
      second_col: the name of the merge col in the second table
      merge_table_name: new name of the merged table

    Returns:
      the sql statement
    """

    query = []
    query.append("CREATE VIEW '{0}' AS (".format(merge_table_name))
    query.append("SELECT * ")
    # Use the two lines below instead if you want to specify cols to include
    # query.append("SELECT MyBaseTable.{0} AS myFirstColumn, ".format(base_col))
    # query.append("MySecondBaseTable.{0} AS mySecondColumn ".format(second_col))
    query.append("FROM {0} AS MyBaseTable ".format(base_table_id))
    query.append("LEFT OUTER JOIN {0} AS MySecondBaseTable ".format(second_table_id))
    # if use alias, can use those alias1 = alias2
    query.append("ON MyBaseTable.{0} = MySecondBaseTable.{1})".format(base_col,second_col))

    return ''.join(query)


if __name__ == '__main__':
    pass
