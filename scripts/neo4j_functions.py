import config
import sys
from csv import reader

#neo4j
from neo4j.v1 import GraphDatabase,basic_auth
auth_token = basic_auth(config.user, config.password)
driver = GraphDatabase.driver("bolt://"+config.server+":"+config.port,auth=auth_token)
session = driver.session()

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

def string_format(line):
	#deal with unicode things
	line = [l.replace("\\","/") for l in line]
	#deal with apostrophes
	line = [l.replace("'", "\\'") for l in line]
	return line

def load_org():
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
	#staff
	#PERSON_ID,PUBLISHED_NAME,FORENAME,SURNAME,ORGANISATION_CODE,TYPE,JOB_TITLE,START_DATE,END_DATE
	staffDic = {}
	with open('data/staff.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			line = string_format(line)
			person_id,published_name,forename,surname,organisation_code,type,job_title,start_date,end_date = line
			if person_id not in staffDic:
				com = "MERGE (s:Staff {person_id: "+person_id+",published_name: '"+published_name+"', " \
				"forename: '"+forename+"',surname: '"+surname+"',organisation_code:'"+organisation_code+"'," \
				"type:'"+type+"',job_title:'"+job_title+"',start_date:'"+start_date+"',end_date:'"+end_date+"'}) " \
				"MERGE (o:Org {code:'"+organisation_code+"'}) MERGE (s)-[:MEMBER_OF]-(o);"
				print com
				session.run(com)
			staffDic[person_id]=''

	i="CREATE index on :Staff(person_id);"
	session.run(i)




if __name__ == '__main__':
	#load_org()
	load_staff()

