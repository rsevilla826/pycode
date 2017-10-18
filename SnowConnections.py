'''
Created on 9/12/2017 - Robert S
'''
import os
import sys
import snowflake.connector
import json

DEFAULT_FILE = 'snowflake_conns.json'
LOCAL_FILE = 'snow_local_conns.json'

class SnowConnections:
    _connections=None
    
    def __init__(self):
        file_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.abspath(os.path.join(file_dir, DEFAULT_FILE))) as json_data:
            self._connections = json.load(json_data)
            
        local_file_path = os.path.abspath(os.path.join(file_dir, LOCAL_FILE))
        if os.path.exists(local_file_path):
            with open(local_file_path) as local_json_data:
                self._connections.update(json.load(local_json_data))
    
    ########################################### DB
    def openConnection(self,connector_key, **kwargs):
        if connector_key not in self._connections:
            raise Exception(connector_key + " not a connection")
        
        config = self._connections[connector_key]
        config.update(kwargs)
        try:
            return snowflake.connector.connect(**config)
        except snowflake.connector.errors.ProgrammingError as err:
            msg = "Connection to DB: {account} {user} failed\r\n\r\n".format(**config)
            msg += "key: %s\r\n\r\n" %(connector_key)
            msg += str(err)
            raise Exception(msg)
