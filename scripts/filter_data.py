import requests, os, json
import xmltodict
from csv import reader,writer
import config
from difflib import SequenceMatcher
import difflib
import itertools
import collections
from time import gmtime, strftime
from collections import defaultdict

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
	w = writer(open(staff_2013,'w'))
	with open(staff) as f:
		next(f)
		for line in reader(f, delimiter=','):
			start_year = line[7].split('/')[2]
			if int(start_year)<ref_cutoff:
				#print line
				w.writerow(line)

def filter_authors():
	print "Filtering authors file"

	staff_ids = {}

	#get ids from filtered staff file
	with open(staff_2013) as f:
		next(f)
		for line in reader(f, delimiter=','):
			staff_ids[line[0]] = line[1]

	w = writer(open(authors_2013,'w'))
	with open(authors) as f:
		next(f)
		for line in reader(f, delimiter=','):
			if line[0] in staff_ids:
				w.writerow(line)

def filter_output():
	print "Filtering output file"
	pub_ids = {}

	#get pub ids from filtered authors file
	with open(authors_2013) as f:
		next(f)
		for line in reader(f, delimiter=','):
			pub_ids[line[1]] = line[0]

	w = writer(open(outputs_2013,'w'))
	with open(outputs) as f:
		next(f)
		for line in reader(f, delimiter=','):
			pub_id = line[0]
			if pub_id in pub_ids:
				w.writerow(line)

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
	print "### Finding similar text"
	print "Reading authors.csv"
	pubAuthorDic = {}
	if os.path.exists('output/outputs_same_authors.csv'):
		print "Same author data already generated"
	else:
		w = open('output/outputs_same_authors.csv','w')
		with open('data/authors.csv', 'rb') as a:
			next(a)
			for line in reader(a, delimiter=','):
				PERSON_ID,PUBLICATION_ID,PUBLISHED_NAME = line
				if PUBLICATION_ID in pubAuthorDic:
					pubAuthorDic[PUBLICATION_ID].append(PERSON_ID)
				else:
					pubAuthorDic[PUBLICATION_ID] = [PERSON_ID]
		pubSameAuthorDic = {}
		counter=0
		for pub1 in pubAuthorDic:
			if counter % 1000 == 0:
				t = strftime("%H:%M:%S", gmtime())
				print t,counter,len(pubAuthorDic)
			counter+=1
			for pub2 in pubAuthorDic:
				if pub1 < pub2:
					if sorted(pubAuthorDic[pub1])==sorted(pubAuthorDic[pub2]):
						#w.write(str(pub1)+'\t'+str(pub2)+'\t'+str(",".join(pubAuthorDic[pub1]))+'\n')
						s = sorted([pub1,pub2])
						pubSameAuthorDic[s[0]+":"+s[1]]=pubAuthorDic[pub1]
						#print pub1,pubAuthorDic[pub1],pub2,pubAuthorDic[pub2]

		for p in pubSameAuthorDic:
			w.write(p+'\t'+str(",".join(pubSameAuthorDic[p]))+'\n')

	if os.path.exists('output/conflicting_titles.txt'):
		print "Already created conflict files"
	else:
		#load outputs
		pubDic = {}
		with open('data/outputs.csv', 'rb') as a:
			next(a)
			for line in reader(a, delimiter=','):
				PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,KEYWORDS,ABSTRACT = line
				#pubDic[PUBLICATION_ID]=TITLE
				#only want pubs with abstracsts > some length
				#if len(ABSTRACT)>config.minAbsLength:
				if TYPE != 'workingpaper/workingpaper':
					pubDic[PUBLICATION_ID]=[TITLE,ABSTRACT]

		threshold_ratio = 0.9
		num_lines = sum(1 for line in open('output/outputs_same_authors.csv'))
		counter=0
		tCheck = open('output/conflicting_titles.txt','w')
		aCheck = open('output/conflicting_abstracts.txt','w')
		with open('output/outputs_same_authors.csv', 'rb') as a:
			for line in a:
				if counter % 10000 == 0:
					t = strftime("%H:%M:%S", gmtime())
					print t,counter,num_lines
				counter+=1
				line = line.rstrip()
				pubs,authors = line.split('\t')
				p1,p2 = pubs.split(':')

				#check for people listed in authors but not in outputs
				if p1 in pubDic and p2 in pubDic:
					title1 = pubDic[p1][0]
					title2 = pubDic[p2][0]
					abs1 = pubDic[p1][1]
					abs2 = pubDic[p2][1]

					#compare titles
					ratio = difflib.SequenceMatcher(None, title1, title2).ratio()
					if ratio>threshold_ratio:
						tCheck.write(str(ratio)+'||'+str(p1)+'||'+str(p2)+'||'+title1+'||'+title2+'\n')
					#compare abstracts
					if len(abs1) > config.minAbsLength and len(abs2) > config.minAbsLength:
						ratio = difflib.SequenceMatcher(None, abs1, abs2).ratio()
						if ratio>threshold_ratio:
							aCheck.write(str(ratio)+'||'+str(p1)+'||'+str(p2)+'||'+abs1+'||'+abs2+'\n')

	print "Parsing conflict files"
	tDic = {}
	with open('output/conflicting_titles.txt', 'rb') as a:
		for line in a:
			print line
			ratio,p1,p2,pt1,pt2 = line.split('||')
			if pt1 in tDic:
				tDic[pt1].append(p1)
			else:
				tDic[pt1] = [p1]
			if pt2 in tDic:
				tDic[pt2].append(p2)
			else:
				tDic[pt2] = [p2]
	print tDic

def find_similar_by_author():
	#read authors file
	staffDic = defaultdict(dict)
	with open(authors_2013, 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			PERSON_ID,PUBLICATION_ID,PUBLISHED_NAME = line
			staffDic[PERSON_ID][PUBLICATION_ID]=''

	#read outputs
	#for pe in staffDic[PERSON_ID]

def find_similar_titles_by_length():
	tSame = open('output/same_titles.txt','w')
	pubDic = {}
	with open(outputs, 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,KEYWORDS,ABSTRACT = line
			pubDic[PUBLICATION_ID]=TITLE

	pubSameTitleDic = {}
	counter=0
	for p1 in pubDic:
		p1t = pubDic[p1]
		if counter % 1000 == 0:
			t = strftime("%H:%M:%S", gmtime())
			print t,counter,len(pubDic),len(pubSameTitleDic)
		counter+=1
		for p2 in pubDic:
			p2t = pubDic[p2]
			if p1<p2:
				if p1t==p2t:
					if p1t in pubSameTitleDic:
						if p1 not in pubSameTitleDic[p1t]:
							pubSameTitleDic[p1t].append(p1)
						if p2 not in pubSameTitleDic[p1t]:
							pubSameTitleDic[p1t].append(p2)
					else:
						pubSameTitleDic[p1t] = [p1,p2]

	#print pubSameTitleDic
	for p in pubSameTitleDic:
		tSame.write(p+'||'+",".join(pubSameTitleDic[p])+'\n')

def find_similar_abstracts_by_length():
	aSame = open('output/same_abstracts.txt','w')
	pubDic = {}
	with open(outputs, 'rb') as a:
		next(a)
		for line in reader(a, delimiter=','):
			PUBLICATION_ID,TITLE,TYPE_NO,TYPE,PUBLICATION_DAY,PUBLICATION_MONTH,PUBLICATION_YEAR,KEYWORDS,ABSTRACT = line
			pubDic[PUBLICATION_ID]=[ABSTRACT,PUBLICATION_YEAR]
	#for abstracts only compare when similar length, e.g. + or - 10 %?
	pubSameAbstractDic = {}
	threshold_ratio = 0.9
	counter=0
	for p1 in pubDic:
		p1a = pubDic[p1][0]
		p1y = pubDic[p1][1]
		if counter % 100 == 0:
			t = strftime("%H:%M:%S", gmtime())
			print t,counter,len(pubDic),len(pubSameAbstractDic)
		counter+=1
		if len(p1a)<config.minAbsLength:
			continue
		for p2 in pubDic:
			p2a = pubDic[p2][0]
			p2y = pubDic[p2][1]
			if len(p2a)<config.minAbsLength:
				continue
			if p1<p2:
				#check if same year
				if p1y == p2y:
					#check if within 5% length
					if len(p1a)*0.95<len(p2a)<len(p1a)*1.05:
						#check for similarity
						ratio = difflib.SequenceMatcher(None, p1a, p2a).ratio()
						if ratio>threshold_ratio:
							#check if in dic
							check=False
							for a in pubSameAbstractDic:
								ratio = difflib.SequenceMatcher(None, p2a, a).ratio()
								if ratio>threshold_ratio:
									check=True
									if p1 not in pubSameAbstractDic[a]:
										pubSameAbstractDic[a].append(p1)
									if p2 not in pubSameAbstractDic[a]:
										pubSameAbstractDic[a].append(p2)
							if check==False:
								pubSameAbstractDic[p1a] = [p1,p2]
							#print pubSameAbstractDic

	#print pubSameAbstractDic
	for p in pubSameAbstractDic:
		aSame.write(p+'||'+",".join(pubSameAbstractDic[p])+'\n')

def ignore_pubs():
	iDic = {}
	for line in open('output/same_titles.txt'):
		line = line.rstrip()
		pids = line.split('||')[1].split(',')
		for p in pids[1:]:
			iDic[p]=''
	for line in open('output/same_abstracts.txt'):
		line = line.rstrip()
		pids = line.split('||')[1].split(',')
		for p in pids[1:]:
			iDic[p]=''
	#print iDic
	return iDic

def main():
	#check_data()
	#filter_people()
	#filter_authors()
	#filter_output()
	#pub_dic = get_xml()
	#create_json(pub_dic)
	#parse_json(pub_dic)
	#find_similar()
	find_similar_titles_by_length()
	find_similar_abstracts_by_length()
	#`ignore_pubs()
if __name__ == '__main__':
	main()