import nltk_functions, neo4j_functions
import operator,os
from collections import defaultdict

def background_frequencies():
	print "Getting background frequencies"
	session = neo4j_functions.connect()
	o = open('output/background_types_frequencies.txt', 'w')
	o.write('type\tcount\n')
	com = "match (s:Staff)--(p:Publication) return distinct p.abstract as a, p.title as t, p.pub_id as pid;"
	backgroundDic = {}
	counter=0
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		abs = res['a']
		#print res['pid']
		types = set(nltk_functions.tokenise_and_lemm(abs))
		for s in types:
			if s in backgroundDic:
				backgroundDic[s]+=1
			else:
				backgroundDic[s] = 1
	for t in sorted(backgroundDic, key=backgroundDic.get, reverse=True):
		o.write(t+'\t'+str(backgroundDic[t])+'\n')
	b_sorted = sorted(backgroundDic.items(), key=operator.itemgetter(1),reverse=True)
	print len(b_sorted),list(b_sorted)[:10]
	session.close()

def person_frequencies():
	print "Getting person frequencies"
	session = neo4j_functions.connect()
	o = open('output/person_type_frequencies.txt', 'w')
	o.write('person\ttype\tcount\n')
	com = "match (s:Staff)--(p:Publication) return distinct s.published_name as p1, s.person_id as p2, p.abstract as a, p.title as t, p.pub_id as pid;"
	freqDic = defaultdict(dict)
	counter=0
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		abs = res['a']
		person_name = res['p1']
		person_id = res['p2']
		person = person_name+':'+str(person_id)
		#print res['pid']
		types = set(nltk_functions.tokenise_and_lemm(abs))
		for s in types:
			if s in freqDic[person]:
				freqDic[person][s]+=1
			else:
				freqDic[person][s] = 1
	for p in sorted(freqDic, key=freqDic.get, reverse=True):
		for t in sorted(freqDic[p], key=freqDic[p].get, reverse=True):
			o.write(str(p)+'\t'+t+'\t'+str(freqDic[p][t])+'\n')
	session.close()

if __name__ == '__main__':
	if os.path.exists('output/background_types_frequencies.txt'):
		print 'Background frequencies already created'
	else:
		background_frequencies()
	if os.path.exists('output/person_type_frequencies.txt'):
		print 'Person frequencies already created'
	else:
		person_frequencies()