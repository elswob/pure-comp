from scipy import stats
import statsmodels.sandbox.stats.multicomp as sm
import neo4j_functions

def pubs_per_org():
	session = neo4j_functions.connect()
	com = 'match (o:Org)--(s:Staff)--(p:Publication) return o.code as o,count(distinct(p)) as c;'
	oDic = {}
	for res in session.run(com):
		oDic[res['o']] = res['c']
	return oDic

def pubs_per_person():
	session = neo4j_functions.connect()
	com = 'match (s:Staff)--(p:Publication) return s.person_id as p,count(p) as c;'
	pDic = {}
	for res in session.run(com):
		pDic[res['p']] = res['c']
	return pDic

def staff_per_org():
	session = neo4j_functions.connect()
	com = ' match (o:Org)--(s:Staff) return o.code as o,count(s) as c;'
	sDic = {}
	for res in session.run(com):
		sDic[res['o']] = res['c']
	return sDic

def fet(a1,a2,b1,b2):
	if b1 == 0:
		b1 = b1+1
	if a1>0 and a2-a1>0 and b1>0:
		oddsratio, pvalue = stats.fisher_exact([[a1,a2-a1],[b1,b2-b1]],alternative="greater")
		#print "fet = ",pvalue, oddsratio, "\n"
		#logger.debug("fet = "+str(pvalue)+":"+str(oddsratio))
		return(oddsratio,pvalue)
	else:
		#logger.debug("something is wrong with this FET:",a1,a2,b1,b2)
		return(0,1)

def multiple_test_correction(fet_dict):
	#do correction for multiple testing (pvalue is the 5th element in array)
	v=[item[5] for item in fet_dict.values()]
	if len(v)>0:
		#logger.debug('Correcting pvals with '+str(len(v))+' values')
		#vc=sm.multipletests(pvals=v,method='bonferroni')
		vc=sm.multipletests(pvals=v,method='fdr_bh')
		#logger.debug('vc : '+str(vc))
		c=0
		#add new pvals to dictionary
		for k in fet_dict:
			fet_dict[k]=fet_dict[k]+(vc[1][c],)
			c+=1
		filter_fet_dict = {}
		for res in fet_dict:
			#logger.debug(res+" : "+str(fet_dict[res]))
			if float(fet_dict[res][6]) < 1e-5:
				filter_fet_dict[res] = fet_dict[res]

		#print "Finished FET"
	else:
		filter_fet_dict = {}
	#logger.debug(filter_fet_dict)
	#logger.debug('Number of FET after cpval = '+str(len(filter_fet_dict)))
	return(filter_fet_dict)