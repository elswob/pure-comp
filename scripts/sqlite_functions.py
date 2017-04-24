import sqlite3,csv,sys

sys.path.append('/Users/be15516/projects/pure-comp/')

def main():
	create_db()

def create_db():
	conn = sqlite3.connect('pure_comp.db')
	c = conn.cursor()

	#drop tables
	c.execute('''DROP TABLE if exists staff''')
	c.execute('''DROP TABLE if exists org_key''')
	c.execute('''DROP TABLE if exists org_hierarchy''')
	c.execute('''DROP TABLE if exists authors''')
	c.execute('''DROP TABLE if exists outputs''')


	# Create tables
	c.execute('''CREATE TABLE if not exists staff (person_id int PRIMARY KEY, published_name text, forename text, surname text, organisation_code text, type text, job_title text, start_date text, end_date text,FOREIGN KEY(organisation_code) REFERENCES org_key(organisation_code))''')
	c.execute('''CREATE TABLE if not exists org_key (organisation_code text PRIMARY KEY,organisation_type text,full_name text,short_name text,url text)''')
	c.execute('''CREATE TABLE if not exists org_hierarchy (parent_org_code text,child_org_code text,FOREIGN KEY(parent_org_code) REFERENCES org_key(organisation_code), FOREIGN KEY(child_org_code) REFERENCES org_key(organisation_code))''')
	c.execute('''CREATE TABLE if not exists authors (person_id int, publication_id int, published_name text, FOREIGN KEY(publication_id) REFERENCES outputs(publication_id), FOREIGN KEY(person_id) REFERENCES staff(person_id))''')
	c.execute('''CREATE TABLE if not exists outputs (publication_id int PRIMARY KEY, title text, type_no int, type text, publication_day int, publication_month int, publication_year int, keywords text, abstract text)''')

	with open('data/staff.csv', 'rb') as a:
		dr = csv.DictReader(a)
		to_db = [(i['PERSON_ID'], i['PUBLISHED_NAME'], i['FORENAME'], i['SURNAME'], i['ORGANISATION_CODE'], i['TYPE'], i['JOB_TITLE'], i['START_DATE'], i['END_DATE']) for i in dr]
		#print to_db
	c.executemany("INSERT OR IGNORE INTO staff (PERSON_ID,PUBLISHED_NAME,FORENAME,SURNAME,ORGANISATION_CODE,TYPE,JOB_TITLE,START_DATE,END_DATE) VALUES (?,?,?,?,?,?,?,?,?);", to_db)
	conn.commit()

	with open('data/org_key.csv', 'rb') as a:
		dr = csv.DictReader(a)
		to_db = [(i['ORGANISATION_CODE'], i['ORGANISATION_TYPE'], i['FULL_NAME'], i['SHORT_NAME'], i['URL']) for i in dr]
		#print to_db
	c.executemany("INSERT OR IGNORE INTO org_key (ORGANISATION_CODE,ORGANISATION_TYPE,FULL_NAME,SHORT_NAME,URL) VALUES (?,?,?,?,?);", to_db)
	conn.commit()

	with open('data/org_hierarchy.csv', 'rb') as a:
		dr = csv.DictReader(a)
		to_db = [(i['PARENT_ORG_CODE'], i['CHILD_ORG_CODE']) for i in dr]
		#print to_db
	c.executemany("INSERT OR IGNORE INTO org_hierarchy (PARENT_ORG_CODE,CHILD_ORG_CODE) VALUES (?,?);", to_db)
	conn.commit()

	with open('data/authors.csv', 'rb') as a:
		dr = csv.DictReader(a)
		to_db = [(i['PERSON_ID'], i['PUBLICATION_ID'], i['PUBLISHED_NAME']) for i in dr]
		#print to_db
	c.executemany("INSERT OR IGNORE INTO authors (PERSON_ID, PUBLICATION_ID, PUBLISHED_NAME) VALUES (?,?,?);", to_db)
	conn.commit()

	with open('data/outputs.csv', 'rb') as a:
		dr = csv.DictReader(a)
		to_db = [(i['PUBLICATION_ID'], i['TITLE'], i['TYPE_NO'], i['TYPE'], i['PUBLICATION_DAY'], i['PUBLICATION_MONTH'], i['PUBLICATION_YEAR'], i['ABSTRACT']) for i in dr]
		#print to_db
	c.executemany("INSERT OR IGNORE INTO outputs (PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,ABSTRACT) VALUES (?,?,?,?,?,?,?,?);", to_db)
	conn.commit()

	conn.close()

if __name__ == '__main__':
	main()