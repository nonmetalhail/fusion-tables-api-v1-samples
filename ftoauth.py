#!/usr/bin/python2.4
# -*- coding: utf-8 -*-
#
# Copyright (C) 2012 Google Inc.
#

"""Fusion Table OAuth.

  Based on API samples by 'jcgregorio@google.com (Joe Gregorio)'
"""

__author__ = 'nahman@google.com (Elliot Nahman)'

import httplib2
import os

from apiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run

class FT_OAuth(object):

  def __init__(self):
    # CLIENT_SECRETS, name of a file containing the OAuth 2.0 information for this
    # application, including client_id and client_secret, which are found
    # on the API Access tab on the Google APIs
    # Console <http://code.google.com/apis/console>
    CLIENT_SECRETS = 'client_secrets.json'

    # Helpful message to display in the browser if the CLIENT_SECRETS file
    # is missing.
    MISSING_CLIENT_SECRETS_MESSAGE = """
    WARNING: Please configure OAuth 2.0

    To make this sample run you will need to populate the client_secrets.json file
    found at:

       %s

    with information from the APIs Console <https://code.google.com/apis/console>.

    """ % os.path.join(os.path.dirname(__file__), CLIENT_SECRETS)

    # Set up a Flow object to be used if we need to authenticate.
    FLOW = flow_from_clientsecrets(CLIENT_SECRETS,
        scope='https://www.googleapis.com/auth/fusiontables',
        message=MISSING_CLIENT_SECRETS_MESSAGE)


    # If the Credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # Credentials will get written back to a file.
    storage = Storage('fusiontables.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
      credentials = run(FLOW, storage)

    # Create an httplib2.Http object to handle our HTTP requests and authorize it
    # with our good Credentials.
    self.http = httplib2.Http()
    self.http = credentials.authorize(self.http)
    
    self.service = build("fusiontables", "v1", http=self.http)

if __name__ == "__main__":
  pass
