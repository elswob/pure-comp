import mysql.connector
import config
import neo4j_functions

def connect():
	cnx = mysql.connector.connect(user=config.mysql_user, password=config.mysql_password,host=config.mysql_host,database=config.mysql_database,port=config.mysql_port)
	return cnx

def test_connection():
	curA = cnx.cursor(buffered=True)

	query = ("select * from browser_concept limit 5;")

	curA.execute(query)
	for (id,name,type) in curA:
		print id,name,type

def copy_graph_to_mysql():
	session = neo4j_functions.connect()
	cnx = connect()
	curA = cnx.cursor(buffered=True)

	#person table
	#name,user_name,institute,position,sex
	neo4j_com = "match (o:Org)--(s:Staff) return s.published_name as n, s.person_id as pid, o.short_name as s;"
	mysql_com = ("INSERT IGNORE INTO browser_person (name, user_name, institute, position, sex) " "VALUES (%s, %s, %s, %s, %s)")
	for res in session.run(neo4j_com):
		name = res['n']
		print name
		username = res['pid']
		org = res['s']
		pos='n/a'
		sex='n/a'
		curA.execute(mysql_com, (name,username,org,pos,sex))

	#concepts
	neo4j_com = "match (c:Concept) return c.name as n, c.type as t"
	mysql_com = ("INSERT IGNORE INTO browser_concept (name, type) " "VALUES (%s, %s)")
	for res in session.run(neo4j_com):
		name = res['n']
		print name
		type = res['t']
		curA.execute(mysql_com, (name, type))
	cnx.close()

if __name__ == '__main__':
	copy_graph_to_mysql()