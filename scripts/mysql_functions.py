import mysql.connector
import config
import neo4j_functions

def connect():
	cnx = mysql.connector.connect(user=config.mysql_user, password=config.mysql_password,host=config.mysql_host,database=config.mysql_database,port=config.mysql_port)
	return cnx

def test_connection():
	cnx = connect()
	curA = cnx.cursor(buffered=True)

	query = ("select * from browser_concept limit 5;")

	curA.execute(query)
	for (id,name,type) in curA:
		print id,name,type

def copy_graph_to_mysql():
	session = neo4j_functions.connect()
	cnx = connect()
	curA = cnx.cursor(buffered=True)

	#add orgs
	print "Adding orgs"
	neo4j_com = "match (o:Org) return o.code as c, o.short_name as s, o.full_name as f, o.url as u, o.type as t;"
	mysql_com = ("INSERT IGNORE INTO browser_org (code, short_name, full_name, url, type) " "VALUES (%s, %s, %s, %s, %s)")
	for res in session.run(neo4j_com):
		code = res['c']
		short = res['s']
		full = res['f']
		url=res['u']
		type=res['t']
		#print code,short,full,url,type
		try:
			curA.execute(mysql_com, (code,short,full,url,type))
		except mysql.connector.Error as err:
			print("failed to insert values %s, %s")

	#person table
	#name,user_name,institute,position,sex
	print "Adding people"
	neo4j_com = "match (o:Org)--(s:Staff) return s.published_name as n, s.person_id as pid, o.code as s;"
	mysql_com = ("INSERT IGNORE INTO browser_person (org_id, user_name, name, position, sex) "
				 "VALUES ((SELECT id from browser_org where code = %s), %s, %s, %s, %s)")
	for res in session.run(neo4j_com):
		name = res['n']
		#print name
		user_name = res['pid']
		org = res['s']
		pos='n/a'
		sex='n/a'
		#curA.execute(mysql_com, (org,user_name,name,pos,sex))

	#concepts
	#name,type
	print "Adding concepts"
	neo4j_com = "match (c:Concept) return c.name as n, c.type as t"
	mysql_com = ("INSERT IGNORE INTO browser_concept (name, type) " "VALUES (%s, %s)")
	for res in session.run(neo4j_com):
		name = res['n']
		#print name
		type = res['t']
		#curA.execute(mysql_com, (name, type))

	#enrichments
	#person_id,globalCount,cpval,concept_id,globalTotal,localCount,localTotal,year
	print "Adding people-concepts"
	neo4j_com = "match (s:Staff)-[e]-(c:Concept) return s.person_id as pid,c.name, e.localCount,e.localTotal,e.globalCount,e.globalTotal,e.year,e.cpval;"
	mysql_com = ("INSERT IGNORE INTO browser_enrichedp (person_id,concept_id,localCount,localTotal,globalCount,globalTotal,year,cpval) "
				 "VALUES ((SELECT id from browser_person where user_name = %s), (SELECT id from browser_concept where name = %s), %s, %s, %s, %s, %s, %s )")
	for res in session.run(neo4j_com):
		pid = res['pid']
		cName = res['c.name']
		localCount = res['e.localCount']
		localTotal = res['e.localTotal']
		globalCount = res['e.globalCount']
		globalTotal = res['e.globalTotal']
		year = res['e.year']
		cpval = res['e.cpval']
		#print pid,cName
		#curA.execute(mysql_com, (pid,cName,localCount,localTotal,globalCount,globalTotal,year,cpval))

	print "Adding org-concepts"
	neo4j_com = "match (o:Org)-[e]-(c:Concept) return o.code as c,c.name, e.localCount,e.localTotal,e.globalCount,e.globalTotal,e.year,e.cpval;"
	mysql_com = ("INSERT IGNORE INTO browser_enrichedo (org_id,concept_id,localCount,localTotal,globalCount,globalTotal,year,cpval) "
	 			 "VALUES ((SELECT id from browser_org where code = %s), (SELECT id from browser_concept where name = %s), %s, %s, %s, %s, %s, %s )")
	for res in session.run(neo4j_com):
		pid = res['c']
		cName = res['c.name']
		localCount = res['e.localCount']
		localTotal = res['e.localTotal']
		globalCount = res['e.globalCount']
		globalTotal = res['e.globalTotal']
		year = res['e.year']
		cpval = res['e.cpval']
		#print pid, cName
		#curA.execute(mysql_com, (pid, cName, localCount, localTotal, globalCount, globalTotal, year, cpval))


	cnx.close()

if __name__ == '__main__':
	copy_graph_to_mysql()