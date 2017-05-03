import nltk_functions, neo4j_functions, stats_functions
import operator,os
from collections import defaultdict

def background_frequencies():
	print "Getting background frequencies"
	session = neo4j_functions.connect()
	o1 = open('output/background_type_frequencies.txt', 'w')
	o1.write('type\tcount\n')
	o2 = open('output/background_bigram_frequencies.txt', 'w')
	o2.write('type\tcount\n')
	o3 = open('output/background_trigram_frequencies.txt', 'w')
	o3.write('type\tcount\n')
	com = "match (s:Staff)--(p:Publication) return distinct p.abstract as a, p.title as t, p.pub_id as pid;"
	backgroundTypeDic = {}
	backgroundBigramDic = {}
	backgroundTrigramDic = {}
	counter=0
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		abs = res['a']
		#print res['pid']
		types = nltk_functions.tokenise_and_lemm(abs)
		bigrams,trigrams = nltk_functions.bigrams_and_trigrams(" ".join(types))
		for s in set(types):
			if s in backgroundTypeDic:
				backgroundTypeDic[s]+=1
			else:
				backgroundTypeDic[s] = 1
		for b in set(bigrams):
			if s in backgroundBigramDic:
				backgroundBigramDic[s]+=1
			else:
				backgroundBigramDic[s] = 1
		for b in set(trigrams):
			if s in backgroundTrigramDic:
				backgroundTrigramDic[s]+=1
			else:
				backgroundTrigramDic[s] = 1
	for t in sorted(backgroundTypeDic, key=backgroundTypeDic.get, reverse=True):
		o1.write(t+'\t'+str(backgroundTypeDic[t])+'\n')
	b_sorted = sorted(backgroundTypeDic.items(), key=operator.itemgetter(1),reverse=True)
	print len(b_sorted),list(b_sorted)[:10]

	for t in sorted(backgroundBigramDic, key=backgroundBigramDic.get, reverse=True):
		o2.write(t+'\t'+str(backgroundBigramDic[t])+'\n')
	b_sorted = sorted(backgroundBigramDic.items(), key=operator.itemgetter(1),reverse=True)
	print len(b_sorted),list(b_sorted)[:10]

	for t in sorted(backgroundTrigramDic, key=backgroundTrigramDic.get, reverse=True):
		o3.write(t+'\t'+str(backgroundTypeDic[t])+'\n')
	b_sorted = sorted(backgroundTrigramDic.items(), key=operator.itemgetter(1),reverse=True)
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

def enrich_person():
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

	#enrich person data
	e = defaultdict(dict)
	counter=0
	num_lines = sum(1 for line in open('output/person_type_frequencies.txt'))
	with open('output/person_type_frequencies.txt', 'rb') as p:
		next(p)
		#get num pubs per person
		pDic = stats_functions.pubs_per_person()
		#print pDic
		for line in p:
			if counter % 10000 == 0:
				print str(counter)+'/'+str(num_lines)
			counter+=1
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
				#print line
				#print a1,a2,b1,b2,f
				e[n][t] = (a1, a2, b1, b2) + f

	e_cor = defaultdict(dict)
	for p in e:
		e_cor[p] = stats_functions.multiple_test_correction(e[p])
		#print d_mc
		#print e
	o = open('output/person_type_enriched.txt','w')
	o.write('name\ttype\ta1\ta2\tb1\tb2\todds\tpval\tcor-pval\n')
	#print e_cor
	for p in e_cor:
		print p
		for t in e_cor[p]:
			#print t
			a1,a2,b1,b2,odds,pval,cor_pval = e_cor[p][t]
			o.write(p+'\t'+t+'\t'+str(a1)+'\t'+str(a2)+'\t'+str(b1)+'\t'+str(b2)+'\t'+str("%.4f" % odds)+'\t'+str("%03.02e" % pval)+'\t'+str("%03.02e" % cor_pval)+'\n')
	o.close()

def enrich_orgs():
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

	#enrich org data
	e = defaultdict(dict)
	counter=0
	num_lines = sum(1 for line in open('output/org_type_frequencies.txt'))
	with open('output/org_type_frequencies.txt', 'rb') as p:
		next(p)
		#get num pubs per person
		pDic = stats_functions.pubs_per_org()
		#print pDic
		for line in p:
			if counter % 10000 == 0:
				print str(counter)+'/'+str(num_lines)
			counter+=1
			line = line.rstrip()
			n,t,c = line.split('\t')
			pid = n.split(':')[1]
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
				#print line
				#print a1,a2,b1,b2,f
				e[n][t] = (a1, a2, b1, b2) + f

	e_cor = defaultdict(dict)
	for p in e:
		e_cor[p] = stats_functions.multiple_test_correction(e[p])
		#print d_mc
		#print e
	o = open('output/org_type_enriched.txt','w')
	o.write('name\ttype\ta1\ta2\tb1\tb2\todds\tpval\tcor-pval\n')
	#print e_cor
	for p in e_cor:
		print p
		for t in e_cor[p]:
			#print t
			a1,a2,b1,b2,odds,pval,cor_pval = e_cor[p][t]
			o.write(p+'\t'+t+'\t'+str(a1)+'\t'+str(a2)+'\t'+str(b1)+'\t'+str(b2)+'\t'+str("%.4f" % odds)+'\t'+str("%03.02e" % pval)+'\t'+str("%03.02e" % cor_pval)+'\n')
	o.close()

def add_enriched_to_graph():
	print "Adding enrichment data to graph..."
	session = neo4j_functions.connect()
	counter=0
	num_lines = sum(1 for line in open('output/person_type_enriched.txt'))

	#create concept nodes and index
	with open('output/person_type_enriched.txt', 'rb') as p:
		next(p)
		for line in p:
			line = line.rstrip()
			#print line
			if counter % 10000 == 0:
				print str(counter)+'/'+str(num_lines)
				session.close()
				session = neo4j_functions.connect()
			counter+=1
			name,type,a1,a2,b1,b2,odds,pval,cor_pval = line.split('\t')
			type = type.replace("'","\\'")
			person_id = name.split(':')[1]
			com = "MERGE (c:Concept {name:'"+type+"'})"
			#session.run(com)

	i="CREATE index on :Concept(name);"
	session.run(i)

	counter=0
	#create person-concept relationships
	print "Creating person-concept relatioships..."
	with open('output/person_type_enriched.txt', 'rb') as p:
		next(p)
		for line in p:
			line = line.rstrip()
			#print line
			if counter % 10000 == 0:
				print str(counter)+'/'+str(num_lines)
				session.close()
				session = neo4j_functions.connect()
			counter+=1
			name,type,a1,a2,b1,b2,odds,pval,cor_pval = line.split('\t')
			type = type.replace("'","\\'")
			person_id = name.split(':')[1]
			com = "MATCH (s:Staff {person_id: "+person_id+"}) " \
				  "MATCH (c:Concept {name:'"+type+"'}) " \
				  "MERGE (s)-[:ENRICHED{type:'pure-comp',year:2014,localCount:"+str(a1)+"," \
				  "localTotal:"+str(a2)+",globalCount:"+str(b1)+",globalTotal:"+str(b2)+",cpval:"+str(cor_pval)+"}]-(c);"
			#session.run(com)

	counter=0
	#create org-concept relationships
	print "Creating org-concept relatioships..."
	with open('output/org_type_enriched.txt', 'rb') as p:
		next(p)
		for line in p:
			if counter % 10000 == 0:
				print counter
			counter+=1
			line = line.rstrip()
			name,type,a1,a2,b1,b2,odds,pval,cor_pval = line.split('\t')
			code = name.split(':')[1]
			com = "MATCH (o:Org {code: '"+code+"'}) " \
				  "MATCH (c:Concept {name:'"+type+"'}) " \
				  "MERGE (o)-[:ENRICHED{type:'pure-comp',year:2014,localCount:"+str(a1)+"," \
				  "localTotal:"+str(a2)+",globalCount:"+str(b1)+",globalTotal:"+str(b2)+",cpval:"+str(cor_pval)+"}]-(c);"
			print com
			session.run(com)

	session.close()

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

	#run enrichment steps
	#enrich_person()
	#enrich_orgs()
	add_enriched_to_graph()