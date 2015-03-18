Sample Python code and modules for connecting to the Fusion Tables API v1.0 with OAuth.

In order to makes these work, you must:
1) enable the Fusion Tables API Service at code.google.com/apis/console/
2) create a client ID at that url under Access
3) edit the client\_secrets.json file and update it with the client ID and secret.

The main file if ft\_api.py The flag system for this may be a bit weird. See the comments in that file for (hopefully clear) explanations.

Another important note:
at the time of this upload (and the end of my internship) there is a bug in the API where the insert row command (which is a part of the upload file method) is being passed in the url rather than the body. Thus, batch size is set to 1. When that is fixed, it will be much faster to increase that number and/or might require a slight change in the fileimport.fileimporter from CSVImporter module. Also, this will soon be a legacy way to upload a file once direct upload of csvs is supported.