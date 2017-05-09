import requests, os, json
import xmltodict
from csv import reader
from difflib import SequenceMatcher
import difflib
import itertools

dataDir = '/Users/be15516/projects/pure-comp/data'

staff = dataDir+'/staff.csv'
staff_2013 = dataDir+'/staff_2013.csv'
authors = dataDir+'/authors.csv'
authors_2013 = dataDir+'/authors_2013.csv'
outputs = dataDir+'/outputs.csv'
outputs_2013 = dataDir+'/outputs_2013.csv'

ref_cutoff = 2014

def check_data():
	cDic = {}
	with open(outputs) as f:
		next(f)
		for line in reader(f, delimiter=','):
			cDic[len(line)]=''
	print cDic

def filter_people():
	print "Finding people after ref"
	w = open(staff_2013,'w')
	with open(staff) as f:
		next(f)
		for line in reader(f, delimiter=','):
			start_year = line[7].split('/')[2]
			if int(start_year)<ref_cutoff:
				#print line
				w.write(','.join(line)+'\n')

def filter_authors():
	print "Filtering authors file"

	staff_ids = {}

	#get ids from filtered staff file
	with open(staff_2013) as f:
		next(f)
		for line in reader(f, delimiter=','):
			staff_ids[line[0]] = line[1]

	w = open(authors_2013,'w')
	with open(authors) as f:
		next(f)
		for line in reader(f, delimiter=','):
			if line[0] in staff_ids:
				w.write(','.join(line)+'\n')

def filter_output():
	print "Filtering output file"
	pub_ids = {}

	#get pub ids from filtered authors file
	with open(authors_2013) as f:
		next(f)
		for line in reader(f, delimiter=','):
			pub_ids[line[1]] = line[0]

	w = open(outputs_2013,'w')
	with open(outputs) as f:
		next(f)
		for line in f:
			pub_id = line.split(',')[0]
			if pub_id in pub_ids:
				w.write(line)

def get_xml():
	print "Getting pure xml"
	pub_dic = {}
	with open(outputs_2013) as f:
		for line in f:
			pub_id = line.split(',')[0]
			pub_dic[pub_id]=''

	counter=0
	pub_dic_test = {}
	for i in pub_dic:
		if counter<5:
			pub_dic_test[i]=''
			print i
			if os.path.exists(dataDir+'/xml/'+i+'.xml'):
				print 'XML file already downloaded'
			else:
				w = open(dataDir+'/xml/'+i+'.xml','w')
				url = 'https://research-information.bris.ac.uk/ws/rest/publication?searchString='+i
				print url
				res = requests.get(url)
				w.write(res.text)
				w.close()
			counter+=1
	return pub_dic_test

def create_json(pub_dic):
	for i in pub_dic:
		if os.path.exists(dataDir+'/json/'+i+'.json'):
			print 'JSON file already created'
		else:
			print 'Converting '+i+'.xml to JSON'
			w = open(dataDir+'/json/'+i+'.json','w')
			with open(dataDir+'/xml/'+i+'.xml') as fd:
				doc = xmltodict.parse(fd.read())
				w.write(json.dumps(doc))
			#print json.dumps(doc)

def parse_json(pub_dic):
	for i in pub_dic:
		with open(dataDir+'/json/'+i+'.json') as data_file:
			json_data = json.load(data_file)
		if 'publication-base_uk:dois' in json_data['publication-template:GetPublicationResponse']['core:result']['core:content']:
			print json_data['publication-template:GetPublicationResponse']['core:result']['core:content']['publication-base_uk:dois']
		else:
			print i+ ".json has no DOI"

def similar(a, b):
	return SequenceMatcher(None, a, b).ratio()

def find_similar():
	print "Finding similar text"
	#pDic = {}
	pList = []
	with open('data/outputs_1000.csv', 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,KEYWORDS,ABSTRACT = line
			if len(TITLE)>10:
				# pDic[PUBLICATION_ID]=TITLE
				pList.append(TITLE)
	# counter=0
	# for i in pDic:
	# 	if counter % 100 == 0:
	# 		print counter,len(pDic)
	# 	counter+=1
	# 	for j in pDic:
	# 		if i != j:
	# 			s = similar(pDic[i],pDic[j])
	# 			if s > 0.8:
	# 				print i,j,s

	threshold_ratio = 0.75
	counter=0
	for str_1, str_2 in itertools.combinations(pList, 2):
		if counter % 1000 == 0:
			print counter,len(pList)
		counter+=1
		ratio = difflib.SequenceMatcher(None, str_1, str_2).ratio()
		if (ratio > threshold_ratio):
			print '%f\t%s\t\t could be \t\t%s' % (ratio, str_1, str_2)

def main():
	#check_data()
	#filter_people()
	#filter_authors()
	#filter_output()
	#pub_dic = get_xml()
	#create_json(pub_dic)
	#parse_json(pub_dic)
	find_similar()

if __name__ == '__main__':
	main()