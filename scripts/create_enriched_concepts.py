import nltk_functions, neo4j_functions, stats_functions
import operator,os
from collections import defaultdict

def background_frequencies():
	print "Getting background frequencies"
	session = neo4j_functions.connect()
	o = open('output/background_type_frequencies.txt', 'w')
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

def org_frequencies():
	print "Getting org frequencies"
	session = neo4j_functions.connect()
	o = open('output/org_type_frequencies.txt', 'w')
	o.write('org\ttype\tcount\n')
	com = "match (o:Org)--(s:Staff)--(p:Publication) return distinct o.short_name as o1, o.code as o2, p.abstract as a, p.title as t, p.pub_id as pid;"
	freqDic = defaultdict(dict)
	counter=0
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		abs = res['a']
		org = res['o1']+':'+res['o2']
		types = set(nltk_functions.tokenise_and_lemm(abs))
		for s in types:
			if s in freqDic[org]:
				freqDic[org][s]+=1
			else:
				freqDic[org][s] = 1
	for p in sorted(freqDic, key=freqDic.get, reverse=True):
		for t in sorted(freqDic[p], key=freqDic[p].get, reverse=True):
			o.write(str(p)+'\t'+t+'\t'+str(freqDic[p][t])+'\n')
	session.close()

def enrich():
	cor_pval = 1e-5
	print "Finding enriched tokens..."
	#get background frequencies
	bDic = {}
	with open('output/background_type_frequencies.txt', 'rb') as b:
		next(b)
		for line in b:
			line = line.rstrip()
			t,c = line.split('\t')
			bDic[t]=c
	#print bDic

	with open('output/person_type_frequencies.txt', 'rb') as p:
		next(p)
		#get num pubs per person
		pDic = stats_functions.pubs_per_person()
		#print pDic
		for line in p:
			line = line.rstrip()
			n,t,c = line.split('\t')
			pid = int(n.split(':')[1])
			#print pid
			a1 = c
			a2 = pDic[pid]
			b1 = bDic[t]
			b2 = len(bDic)
			#print('a1: '+str(a1)+' a2: '+str(a2)+' b1: '+str(b1)+ ' b2: '+str(b2))
			f = stats_functions.fet(int(a1), int(a2), int(b1), b2)
			if f[1] < cor_pval:
				# logger.debug('\nname:'+name+' a1:'+str(a1)+' a2:'+str(a2)+' b1:'+str(b1)+' b2:'+str(b2))
				# logger.debug(f)
				print line
				print a1,a2,b1,b2,f
				#d[name] = (a1, a2, b1, b2) + f

	# logger.debug(d)
	# logger.debug(len(d))
	#d_mc = stats_functions.multiple_test_correction(d, method)


if __name__ == '__main__':
	if os.path.exists('output/background_type_frequencies.txt'):
		print 'Background frequencies already created'
	else:
		background_frequencies()
	if os.path.exists('output/person_type_frequencies.txt'):
		print 'Person frequencies already created'
	else:
		person_frequencies()
	if os.path.exists('output/org_type_frequencies.txt'):
		print 'Org frequencies already created'
	else:
		org_frequencies()

	enrich()