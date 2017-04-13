import config

#neo4j
from neo4j.v1 import GraphDatabase,basic_auth
auth_token1 = basic_auth(config.user, config.password)
driver1 = GraphDatabase.driver("bolt://"+config.server,auth=auth_token)