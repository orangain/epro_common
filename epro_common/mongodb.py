# coding: utf-8

"""
A wrapper of pymongo to connect to database with uri
"""

import pymongo

def connect_to_db(uri):
    parts = pymongo.uri_parser.parse_uri(uri)
    host, port = parts['nodelist'][0]
    database = parts['database']

    client = pymongo.MongoClient(host, port)
    return client[database]
