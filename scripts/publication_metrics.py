import neo4j_functions, nltk_functions
from time import gmtime, strftime
import os

def get_metrics():

	#check for previous
	pubDic = {}
	if os.path.exists('output/pubMetrics.txt'):
		with open('output/pubMetrics.txt', 'rb') as p:
			for line in p:
				if len(line.split('\t'))==13:
					pid,cf,ld,ts_flesch_reading_ease,ts_smog_index,ts_flesch_kincaid_grade,ts_coleman_liau_index,ts_automated_readability_index,ts_dale_chall_readability_score,ts_difficult_words,ts_linsear_write_formula,ts_gunning_fog,ts_text_standard = line.split('\t')
					pubDic[int(pid)]=''
	#print pubDic
	print "Found "+str(len(pubDic))+" entries already processed"
	session = neo4j_functions.connect()
	com = "match (p:Publication) return distinct p.abstract as a, p.pub_id as pid;"
	counter=0
	o = open('output/pubMetrics.txt','a')
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter, strftime("%Y-%m-%d %H:%M:%S", gmtime())
			#session.close()
			#session = neo4j_functions.connect()
		counter+=1
		abs = res['a']
		pid = res['pid']
		#print pid
		if pid not in pubDic:
			#content fraction
			if len(abs)>0:
				cf = nltk_functions.content_fraction(abs)
				ld = nltk_functions.lexical_diversity(abs)
				ts = nltk_functions.run_textstat(abs)
				ts_flesch_reading_ease,ts_smog_index,ts_flesch_kincaid_grade,ts_coleman_liau_index,ts_automated_readability_index,ts_dale_chall_readability_score,ts_difficult_words,ts_linsear_write_formula,ts_gunning_fog,ts_text_standard = ts
				#print pid, cf, ld, ts_flesch_reading_ease
				o.write(str(pid)+"\t"+str("%.4f" % cf)+"\t"+str("%.4f" % ld)+"\t"+str("%.4f" % ts_flesch_reading_ease)+""
					   "\t"+str("%.4f" % ts_smog_index)+"\t"+str("%.4f" % ts_flesch_kincaid_grade)+"\t"+str("%.4f" % ts_coleman_liau_index)+""
					   "\t"+str("%.4f" % ts_automated_readability_index)+"\t"+str("%.4f" % ts_dale_chall_readability_score)+""
					   "\t"+str("%.4f" % ts_difficult_words)+"\t"+str("%.4f" % ts_linsear_write_formula)+"\t"+str("%.4f" % ts_gunning_fog)+""
					   "\t"+str(ts_text_standard)+"\n")

def add_metrics_to_graph():
	session = neo4j_functions.connect()
	with open('output/pubMetrics.txt', 'rb') as p:
		for line in p:
			line = line.rstrip()
			pid,cf,ld,ts_flesch_reading_ease,ts_smog_index,ts_flesch_kincaid_grade,ts_coleman_liau_index,ts_automated_readability_index,ts_dale_chall_readability_score,ts_difficult_words,ts_linsear_write_formula,ts_gunning_fog,ts_text_standard = line.split('\t')
			com = "match (p:Publication) where p.pub_id = "+str(pid)+" set p.cf = "+str("%.4f" % cf)+",p.ld ="+str("%.4f" % ld)+"" \
				   ",p.ts_flesch_reading_ease="+str("%.4f" % ts_flesch_reading_ease)+",p.ts_smog_index="+str("%.4f" % ts_smog_index)+",p.ts_flesch_kincaid_grade="+str("%.4f" % ts_flesch_kincaid_grade)+"" \
			       ",p.ts_coleman_liau_index="+str("%.4f" % ts_coleman_liau_index)+",p.ts_automated_readability_index="+str("%.4f" % ts_automated_readability_index)+"" \
				   ",p.ts_dale_chall_readability_score="+str("%.4f" % ts_dale_chall_readability_score)+",p.ts_difficult_words="+str("%.4f" % ts_difficult_words)+"" \
				   ",p.ts_linsear_write_formula="+str("%.4f" % ts_linsear_write_formula)+",p.ts_gunning_fog="+str("%.4f" % ts_gunning_fog)+",p.ts_text_standard='"+str(ts_text_standard)+"';"
			#print com
			session.run(com)
	session.close()
get_metrics()