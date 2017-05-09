import nltk_functions, neo4j_functions, stats_functions
import operator,os
from collections import defaultdict

outDir='concepts/'

freqCutoff=1000

def ignore_tokens():
	#cat output/background_type_frequencies.txt | grep '/' | head -n 20 | cut -f1 | tr '\n' ','
	l = ['/00a9','br/','/sub','/sup','/00b1','/00d7','/03bcm','/00b0c','/03b2','/em','a/03b2','/03b1','.','ltd.','elsevier','inc.']
	other = ['one','two','three','four','five','six','seven','eight','nine','ten']
	return l

def background_frequencies():
	print "Getting background frequencies"
	session = neo4j_functions.connect()
	o1 = open(outDir+'/background_type_frequencies.txt', 'w')
	o1.write('type\tcount\n')
	o2 = open(outDir+'/background_bigram_frequencies.txt', 'w')
	o2.write('bigram\tcount\n')
	o3 = open(outDir+'/background_trigram_frequencies.txt', 'w')
	o3.write('trigram\tcount\n')

	com = "match (s:Staff)--(p:Publication) return distinct p.abstract as a, p.title as t, p.pub_id as pid;"
	typeDic = {}
	bigramDic = {}
	trigramDic = {}
	pubConceptDic = defaultdict(dict)
	counter=0
	ignoreList = ignore_tokens()
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		abs = res['a']
		pid = res['pid']
		types = nltk_functions.tokenise_and_lemm(abs)
		#bigrams,trigrams = nltk_functions.bigrams_and_trigrams(abs)
		bigrams,trigrams = nltk_functions.bigrams_and_trigrams(" ".join(types))
		for s in set(types):
			if s in ignoreList:
				pass
			else:
				#add to pub-concept dic
				pubConceptDic[pid][s]='type'
				if s in typeDic:
					typeDic[s]+=1
				else:
					typeDic[s] = 1
		for s in set(bigrams):
			if len(set(s).intersection(ignoreList))==0:
				ss = s[0]+':'+s[1]
				#add to pub-concept dic
				pubConceptDic[pid][ss]='bigram'
				if ss in bigramDic:
					bigramDic[ss]+=1
				else:
					bigramDic[ss] = 1
		for s in set(trigrams):
			if len(set(s).intersection(ignoreList))==0:
				ss = s[0]+':'+s[1]+':'+s[2]
				#add to pub-concept dic
				pubConceptDic[pid][ss]='trigram'
				if ss in trigramDic:
					trigramDic[ss]+=1
				else:
					trigramDic[ss] = 1

	for t in sorted(typeDic, key=typeDic.get, reverse=True):
		o1.write(t+'\t'+str(typeDic[t])+'\n')
	for t in sorted(bigramDic, key=bigramDic.get, reverse=True):
		o2.write(t+'\t'+str(bigramDic[t])+'\n')
	for t in sorted(trigramDic, key=trigramDic.get, reverse=True):
		o3.write(t+'\t'+str(trigramDic[t])+'\n')

	o4 = open(outDir+'/pub_concept.txt','w')
	l = len(pubConceptDic)
	counter=0
	for p in pubConceptDic:
		if counter % 1000 == 0:
			print counter,l
		counter+=1
		for c in pubConceptDic[p]:
			o4.write(str(p)+'\t'+c+"\t"+pubConceptDic[p][c]+'\n')

	session.close()

def typeFreqs():
	#read in type freqs for filtering
	typeDic = {}
	with open(outDir+'/background_type_frequencies.txt', 'rb') as a:
		next(a)
		for line in a:
			line = line.rstrip()
			t,c = line.split('\t')
			typeDic[t]=c
	return typeDic

def readPubConcepts():
	print "Recreating pubConcept dictionary..."
	pubConceptDic = defaultdict(dict)
	with open(outDir+'/pub_concept.txt', 'rb') as a:
		for line in a:
			line = line.rstrip()
			pid,concept,type = line.split('\t')
			pubConceptDic[pid][concept]=type
	firstpair = {k: pubConceptDic[k] for k in pubConceptDic.keys()[:1]}
	#print firstpair
	return pubConceptDic

def person_frequencies():
	print "Getting person frequencies"
	session = neo4j_functions.connect()
	o1 = open(outDir+'/person_type_frequencies.txt', 'w')
	o1.write('person\ttype\tcount\n')
	o2 = open(outDir+'/person_bigram_frequencies.txt', 'w')
	o2.write('person\tbigram\tcount\n')
	o3 = open(outDir+'/person_trigram_frequencies.txt', 'w')
	o3.write('person\ttrigram\tcount\n')

	#com = "match (s:Staff)--(p:Publication) where p.pub_id = 2942913 return distinct s.published_name as p1, s.person_id as p2, p.pub_id as pid;"
	com = "match (s:Staff)--(p:Publication) return distinct s.published_name as p1, s.person_id as p2, p.pub_id as pid;"
	typeDic = defaultdict(dict)
	bigramDic = defaultdict(dict)
	trigramDic = defaultdict(dict)
	ignoreList = ignore_tokens()
	counter=0
	#read in type freqs for filtering
	typeConceptDic = typeFreqs()
	#get pubConceptDic
	pubConceptDic = readPubConcepts()
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		person_name = res['p1']
		person_id = res['p2']
		pid = str(res['pid'])
		person = person_name+':'+str(person_id)
		#print res['pid']
		for s in pubConceptDic[pid]:
			if s in ignoreList:
				pass
			else:
				if pubConceptDic[pid][s] == 'type':
					if int(typeConceptDic[s])>freqCutoff:
						#print pid,s,pubConceptDic[pid][s],typeConceptDic[s]
						pass
					else:
						if s in typeDic[person]:
							typeDic[person][s]+=1
						else:
							typeDic[person][s] = 1
				elif pubConceptDic[pid][s] == 'bigram':
					if s in bigramDic[person]:
						bigramDic[person][s]+=1
					else:
						bigramDic[person][s] = 1
				elif pubConceptDic[pid][s] == 'trigram':
					if s in trigramDic[person]:
						trigramDic[person][s]+=1
					else:
						trigramDic[person][s] = 1

	for p in sorted(typeDic, key=typeDic.get, reverse=True):
		for t in sorted(typeDic[p], key=typeDic[p].get, reverse=True):
			o1.write(str(p)+'\t'+t+'\t'+str(typeDic[p][t])+'\n')
	for p in sorted(bigramDic, key=bigramDic.get, reverse=True):
		for t in sorted(bigramDic[p], key=bigramDic[p].get, reverse=True):
			o2.write(str(p)+'\t'+t+'\t'+str(bigramDic[p][t])+'\n')
	for p in sorted(trigramDic, key=trigramDic.get, reverse=True):
		for t in sorted(trigramDic[p], key=trigramDic[p].get, reverse=True):
			o3.write(str(p)+'\t'+t+'\t'+str(trigramDic[p][t])+'\n')

	session.close()

def org_frequencies():
	print "Getting org frequencies"
	session = neo4j_functions.connect()
	o1 = open(outDir+'/org_type_frequencies.txt', 'w')
	o1.write('org\ttype\tcount\n')
	o2 = open(outDir+'/org_bigram_frequencies.txt', 'w')
	o2.write('org\tbigram\tcount\n')
	o3 = open(outDir+'/org_trigram_frequencies.txt', 'w')
	o3.write('org\ttrigram\tcount\n')
	com = "match (o:Org)--(s:Staff)--(p:Publication) return distinct o.short_name as o1, o.code as o2, p.pub_id as pid;"
	typeDic = defaultdict(dict)
	bigramDic = defaultdict(dict)
	trigramDic = defaultdict(dict)
	counter=0
	ignoreList = ignore_tokens()
	#read in type freqs for filtering
	typeConceptDic = typeFreqs()
	#get pubConceptDic
	pubConceptDic = readPubConcepts()
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		org = res['o1']+':'+res['o2']
		pid = str(res['pid'])
		for s in pubConceptDic[pid]:
			if s in ignoreList:
				pass
			else:
				if pubConceptDic[pid][s] == 'type':
					if int(typeConceptDic[s])>freqCutoff:
						#print pid,s,pubConceptDic[pid][s],typeConceptDic[s]
						pass
					else:
						if s in typeDic[org]:
							typeDic[org][s]+=1
						else:
							typeDic[org][s] = 1
				elif pubConceptDic[pid][s] == 'bigram':
					if s in bigramDic[org]:
						bigramDic[org][s]+=1
					else:
						bigramDic[org][s] = 1
				elif pubConceptDic[pid][s] == 'trigram':
					if s in trigramDic[org]:
						trigramDic[org][s]+=1
					else:
						trigramDic[org][s] = 1

	for p in sorted(typeDic, key=typeDic.get, reverse=True):
		for t in sorted(typeDic[p], key=typeDic[p].get, reverse=True):
			o1.write(str(p)+'\t'+t+'\t'+str(typeDic[p][t])+'\n')
	for p in sorted(bigramDic, key=bigramDic.get, reverse=True):
		for t in sorted(bigramDic[p], key=bigramDic[p].get, reverse=True):
			o2.write(str(p)+'\t'+t+'\t'+str(bigramDic[p][t])+'\n')
	for p in sorted(trigramDic, key=trigramDic.get, reverse=True):
		for t in sorted(trigramDic[p], key=trigramDic[p].get, reverse=True):
			o3.write(str(p)+'\t'+t+'\t'+str(trigramDic[p][t])+'\n')
	session.close()

def enrich_person():
	cor_pval = 1e-5
	for i in ['type','bigram','trigram']:
		print "Finding enriched person data for type "+i+" ..."
		#get background frequencies
		bDic = {}
		with open(outDir+'/background_'+i+'_frequencies.txt', 'rb') as b:
			next(b)
			for line in b:
				line = line.rstrip()
				t,c = line.split('\t')
				bDic[t]=c

		#enrich person data
		e = defaultdict(dict)
		counter=0
		num_lines = sum(1 for line in open(outDir+'/person_'+i+'_frequencies.txt'))
		with open(outDir+'/person_'+i+'_frequencies.txt', 'rb') as p:
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
		o = open(outDir+'/person_'+i+'_enriched.txt','w')
		o.write('name\t'+i+'\ta1\ta2\tb1\tb2\todds\tpval\tcor-pval\n')
		#print e_cor
		for p in e_cor:
			for t in e_cor[p]:
				#print t
				a1,a2,b1,b2,odds,pval,cor_pval = e_cor[p][t]
				o.write(p+'\t'+t+'\t'+str(a1)+'\t'+str(a2)+'\t'+str(b1)+'\t'+str(b2)+'\t'+str("%.4f" % odds)+'\t'+str("%03.02e" % pval)+'\t'+str("%03.02e" % cor_pval)+'\n')
		o.close()

def enrich_orgs():
	cor_pval = 1e-5
	for i in ['type','bigram','trigram']:
		print "Finding enriched org data for type "+i+" ..."
		#get background frequencies
		bDic = {}
		with open(outDir+'/background_'+i+'_frequencies.txt', 'rb') as b:
			next(b)
			for line in b:
				line = line.rstrip()
				t,c = line.split('\t')
				bDic[t]=c

		#enrich org data
		e = defaultdict(dict)
		counter=0
		num_lines = sum(1 for line in open(outDir+'/org_'+i+'_frequencies.txt'))
		with open(outDir+'/org_'+i+'_frequencies.txt', 'rb') as p:
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
					e[n][t] = (a1, a2, b1, b2) + f

		e_cor = defaultdict(dict)
		for p in e:
			e_cor[p] = stats_functions.multiple_test_correction(e[p])
			#print d_mc
			#print e
		o = open(outDir+'/org_'+i+'_enriched.txt','w')
		o.write('name\t'+i+'\ta1\ta2\tb1\tb2\todds\tpval\tcor-pval\n')
		#print e_cor
		for p in e_cor:
			for t in e_cor[p]:
				#print t
				a1,a2,b1,b2,odds,pval,cor_pval = e_cor[p][t]
				o.write(p+'\t'+t+'\t'+str(a1)+'\t'+str(a2)+'\t'+str(b1)+'\t'+str(b2)+'\t'+str("%.4f" % odds)+'\t'+str("%03.02e" % pval)+'\t'+str("%03.02e" % cor_pval)+'\n')
		o.close()

def add_enriched_to_graph():
	print "Adding enrichment data to graph..."
	session = neo4j_functions.connect()
	for concept_type in ['type','bigram','trigram']:
		print "Adding data for data type '"+concept_type+"'"
		counter=0
		num_lines = sum(1 for line in open(outDir+'/person_'+concept_type+'_enriched.txt'))

		#create concept nodes and index
		with open(outDir+'/person_'+concept_type+'_enriched.txt', 'rb') as p:
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
				com = "MERGE (c:Concept {name:'"+type+"', type:'"+concept_type+"'})"
				session.run(com)

		i="CREATE index on :Concept(name);"
		session.run(i)
		#i="CREATE index on :Concept(type);"
		#session.run(i)

		counter=0
		#create person-concept relationships
		print "Creating person-concept relationships..."
		with open(outDir+'/person_'+concept_type+'_enriched.txt', 'rb') as p:
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
				if float(cor_pval)>0:
					cor_pval = ("%03.02e" % float(cor_pval))
				else:
					cor_pval = 0.0
				type = type.replace("'","\\'")
				person_id = name.split(':')[1]
				com = "MATCH (s:Staff {person_id: "+person_id+"}) " \
					  "MATCH (c:Concept {name:'"+type+"',type:'"+concept_type+"'}) " \
					  "MERGE (s)-[:ENRICHED{type:'pure-comp',year:2014,localCount:"+str(a1)+"," \
					  "localTotal:"+str(a2)+",globalCount:"+str(b1)+",globalTotal:"+str(b2)+",cpval:"+str(cor_pval)+"}]-(c);"
				session.run(com)

		counter=0
		#create org-concept relationships
		print "Creating org-concept relationships..."
		with open(outDir+'/org_'+concept_type+'_enriched.txt', 'rb') as p:
			next(p)
			for line in p:
				if counter % 10000 == 0:
					print counter
					session.close()
					session = neo4j_functions.connect()
				counter+=1
				line = line.rstrip()
				name,type,a1,a2,b1,b2,odds,pval,cor_pval = line.split('\t')
				if float(cor_pval)>0:
					cor_pval = ("%03.02e" % float(cor_pval))
				else:
					cor_pval = 0.0
				type = type.replace("'","\\'")
				code = name.split(':')[1]
				com = "MATCH (o:Org {code: '"+code+"'}) " \
					  "MATCH (c:Concept {name:'"+type+"',type:'"+concept_type+"'}) " \
					  "MERGE (o)-[:ENRICHED{type:'pure-comp',year:2014,localCount:"+str(a1)+"," \
					  "localTotal:"+str(a2)+",globalCount:"+str(b1)+",globalTotal:"+str(b2)+",cpval:"+str(cor_pval)+"}]-(c);"
				#print com
				session.run(com)
	session.close()

def add_pub_concepts():
	print "Adding pub-concept data"
	#read data
	pubConceptDic = defaultdict(dict)
	with open(outDir+'/pub_concept.txt', 'rb') as p:
		for line in p:
			line = line.rstrip()
			pid,name,type = line.split('\t')
			pubConceptDic[name+":"+type][pid]=''

	session = neo4j_functions.connect()
	com = "match (c:Concept) return c.name as n, c.type as t;"
	counter=0
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
			session.close()
			session = neo4j_functions.connect()
		counter+=1
		name = res['n']
		name = name.replace("'", "\\'")
		type = res['t']
		cName = name+":"+type
		#print len(pubConceptDic[cName]),cName
		for p in pubConceptDic[cName]:
			com = "match (p:Publication {pub_id:"+str(p)+"}) match (c:Concept {name:'"+name+"',type:'"+type+"'}) merge (p)-[:CONCEPT]-(c);"
			#print com
			session.run(com)

def distance_metrics():
	session = neo4j_functions.connect()
	#staff
	com="match (p1:Staff)-[e1:ENRICHED]->(s), (p2:Staff)-[e2:ENRICHED]->(s) where id(p1) < id(p2) with sqrt(sum((-log10(e1.cpval) - -log10(e2.cpval))^2)) as euc,p1,p2 merge (p1)-[d:DISTANCE]-(p2) set d.euclidean_2014 = euc;"
	print com
	session.run(com)
	#orgs
	com="match (p1:Org)-[e1:ENRICHED]->(s), (p2:Org)-[e2:ENRICHED]->(s) where id(p1) < id(p2) with sqrt(sum((-log10(e1.cpval) - -log10(e2.cpval))^2)) as euc,p1,p2 merge (p1)-[d:DISTANCE]-(p2) set d.euclidean_2014 = euc;"
	print com
	session.run(com)

if __name__ == '__main__':
	if os.path.exists(outDir+'/background_type_frequencies.txt'):
		print 'Background frequencies already created'
	else:
		background_frequencies()
	if os.path.exists(outDir+'/person_type_frequencies.txt'):
		print 'Person frequencies already created'
	else:
		person_frequencies()
	if os.path.exists(outDir+'/org_type_frequencies.txt'):
		print 'Org frequencies already created'
	else:
		org_frequencies()

	#run enrichment steps
#	#enrich_person()
	#enrich_orgs()
	#add_enriched_to_graph()
	add_pub_concepts()
	distance_metrics()