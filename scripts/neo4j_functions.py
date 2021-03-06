import config, filter_data
import sys
from csv import reader

#neo4j
from neo4j.v1 import GraphDatabase,basic_auth
auth_token = basic_auth(config.neo4j_user, config.neo4j_password)
driver = GraphDatabase.driver("bolt://"+config.neo4j_server+":"+config.neo4j_port,auth=auth_token)
#session = driver.session()

def neo4j_check():
	try:
		session = driver.session()
		print "session ok"
		cmd = "match (s) return count(s);"
		result = session.run(cmd)
		for r in result:
			print r

	except:
		print "session not ok"

def connect():
	session = driver.session()
	return session

def string_format(line):
	#deal with unicode things
	line = [l.replace("\\","/") for l in line]
	#deal with apostrophes
	line = [l.replace("'", "\\'") for l in line]
	return line

def load_org():
	session = connect()
	#org key
	#ORGANISATION_CODE,ORGANISATION_TYPE,FULL_NAME,SHORT_NAME,URL
	orgDic = {}
	with open('data/org_key.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			line = string_format(line)
			org_code,org_type,full_name,short_name,url = line
			if org_code not in orgDic:
				com = "MERGE (o:Org {code:'"+org_code+"',type:'"+org_type+"',full_name:'"+full_name+"',short_name:'"+short_name+"',url:'"+url+"'});"
				print com
				session.run(com)
			orgDic[org_code]=''

	i="CREATE index on :Org(code);"
	session.run(i)

	#org hierarchy
	#PARENT_ORG_CODE,CHILD_ORG_CODE
	with open('data/org_hierarchy.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			parent,child = line
			#		pCom2 = 'MATCH (pe:Person {name: "'+person.user_name+'"}) MERGE (s:'+methodC+' {name: "'+res+'"}) MERGE (pe)-[:ENRICHED{year:'+year+',localCount:'+str(localCount)+',localTotal:'+str(localTotal)+',globalCount:'+str(globalCount)+',globalTotal:'+str(globalTotal)+',cpval:'+str(cpval)+'}]-(s);'
			com = "MATCH (o1:Org {code:'"+parent+"'}) , (o2:Org {code:'"+child+"'}) merge (o1)-[:PARENT_OF]-(o2);"
			print com
			session.run(com)

def load_staff():
	session = connect()
	#PERSON_ID,PUBLISHED_NAME,FORENAME,SURNAME,ORGANISATION_CODE,TYPE,JOB_TITLE,START_DATE,END_DATE
	staffDic = {}
	counter=0
	with open('data/staff.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			line = string_format(line)
			person_id,published_name,forename,surname,organisation_code,type,job_title,start_date,end_date = line
			if person_id not in staffDic:
				counter+=1
				com = "MERGE (s:Staff {person_id: "+person_id+",published_name: '"+published_name+"', " \
				"forename: '"+forename+"',surname: '"+surname+"',organisation_code:'"+organisation_code+"'," \
				"type:'"+type+"',job_title:'"+job_title+"',start_date:'"+start_date+"',end_date:'"+end_date+"'}) " \
				"return s.published_name;"
				#print counter
				for res in session.run(com):
					print com
			staffDic[person_id]=''

	i="CREATE index on :Staff(person_id);"
	session.run(i)

	counter=0
	with open('data/staff.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			line = string_format(line)
			person_id,published_name,forename,surname,organisation_code,type,job_title,start_date,end_date = line
			counter+=1
			com = "MATCH (o:Org {code:'"+organisation_code+"'}) MATCH (s:Staff {person_id: "+person_id+",published_name: '"+published_name+"', " \
			"forename: '"+forename+"',surname: '"+surname+"',organisation_code:'"+organisation_code+"'," \
			"type:'"+type+"',job_title:'"+job_title+"',start_date:'"+start_date+"',end_date:'"+end_date+"'}) " \
			"MERGE (s)-[:MEMBER_OF]-(o) return s.published_name;"
			#print counter
			for res in session.run(com):
				print com

	i="CREATE index on :Staff(person_id);"
	session.run(i)

	#check loading
	com = "match (s:Staff) return s.person_id as s;"
	for res in session.run(com):
		staffDic[str(res['s'])]=1
	for s in staffDic:
		if staffDic[s]!=1:
			print 'missing',s

def load_outputs():
	session = connect()

	#load same title info
	ignorePubs = filter_data.ignore_pubs()

	#PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,KEYWORDS,ABSTRACT
	pubDic = {}
	counter=0
	with open('data/outputs.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			counter+=1
			line = string_format(line)
			PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,KEYWORDS,ABSTRACT = line
			if PUBLICATION_ID not in ignorePubs:
				if TYPE != 'workingpaper/workingpaper':
					if PUBLICATION_ID not in pubDic:
						if counter % 1000 == 0:
							print str(counter)
							session.close()
							session = connect()
						if PUBLICATION_YEAR == '':
							PUBLICATION_YEAR = '0'
						if PUBLICATION_MONTH == '':
							PUBLICATION_MONTH = '0'
						if PUBLICATION_DAY == '':
							PUBLICATION_DAY = '0'
						com = "MERGE (p:Publication {pub_id:"+PUBLICATION_ID+",title:'"+TITLE+"',type:"+TYPE_NO+",pub_day:"+PUBLICATION_DAY+"," \
						"pub_month:"+PUBLICATION_MONTH+",pub_year:"+PUBLICATION_YEAR+",abstract:'"+ABSTRACT+"'});"

						for res in session.run(com):
							print com
					pubDic[PUBLICATION_ID]=''

	i="CREATE index on :Publication(pub_id);"
	session.run(i)

def load_authors():
	session = connect()
	counter=0
	#PERSON_ID,PUBLICATION_ID,PUBLISHED_NAME
	with open('data/authors.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			if counter % 1000 == 0:
				print str(counter)
				session.close()
				session = connect()
			counter+=1
			person,publication,name = line
			com = "MATCH (s:Staff {person_id:"+person+"}) , (p:Publication {pub_id:"+publication+"}) merge (s)-[:PUBLISHED]-(p);"
			#print com
			for r in session.run(com):
				print r['p.pub_id']

def load_enriched(com,type):
	print "Loading enriched data"


def load_data():
	print 'Loading data'
	#neo4j_check()
	#load_org()
	#load_staff()
	#load_outputs()
	load_authors()

if __name__ == '__main__':
	load_data()

