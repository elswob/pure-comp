from __future__ import division
import neo4j_functions,nltk_functions
import operator
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
#from nltk.book import *
from nltk.collocations import *
from nltk import FreqDist
from nltk.corpus import stopwords
from collections import defaultdict
import matplotlib.pyplot as plt
#import matplotlib
#matplotlib.style.use('ggplot')
from csv import reader
import pandas as pd
import numpy as np
from textstat.textstat import textstat

bigram_measures = nltk.collocations.BigramAssocMeasures()
trigram_measures = nltk.collocations.TrigramAssocMeasures()

session = neo4j_functions.session

#Section 1 (lexical diversity)
def count_things():
	com = "match (o:Org)--(s:Staff)--(p:Publication) return distinct o.short_name as o, p.abstract as a, p.title as t, p.pub_id as pid;"
	print com
	pubText = defaultdict(dict)
	#pubCount = {}
	counter=0
	backgroundDic = {}
	print "Getting data from graph"
	for res in session.run(com):
		if counter % 1000 == 0:
			print counter
		counter+=1
		abstract = res['a']
		title = res['t']
		org = res['o']
		pid = res['pid']
		if len(abstract)>1:
			pubText[org][pid]=abstract

			#count unique tokens per abstract and create background frequencies
			aToken = word_tokenize(pubText[org][pid])
			types = set(aToken)
			types_filtered = nltk_functions.filter_stopwords_and_length(types,5)
			for s in types_filtered:
				if s in backgroundDic:
					backgroundDic[s]+=1
				else:
					backgroundDic[s] = 1

	#print "Creating single text object"
	#create single text variable
	#allText = ''
	#aCheck = {}
	#for org in pubText:
	#	print '\t'+org
	#	for pid in pubText[org]:
	#		#if pid not in aCheck:
	#			#allText+=pubText[org][pid]
	#		aCheck[pid]=''

	#print "Calculating background token frequencies"
	#get background token frequency
	#bTokens = word_tokenize(allText)
	#bTokens_no_short_and_no_stopwords = filter_stopwords_and_length(bTokens,5)
	#fdist = FreqDist(bTokens_no_short_and_no_stopwords)
	#print 'Top 10 common tokens in all text'
	#print fdist.most_common(10)

	o = open('output/nltk_counts.txt', 'w')
	o.write('org\tnum_text\tmean_num_tokens\tmean_num_types\tmean_lexical_diversity\tmean_content_fraction\n')
	masterDic = defaultdict(dict)
	for org in pubText:
		mean_num_tokens=0
		mean_num_types=0
		mean_lexical_diversity=0
		mean_content_fraction=0
		print '### processing '+org+' ###'
		aCount = len(pubText[org])
		aText=''
		for a in pubText[org]:
			aText+=pubText[org][a]
			aToken = word_tokenize(pubText[org][a])
			tokenCount = len(aToken)
			mean_num_tokens+=tokenCount
			types = set(aToken)
			typeCount = len(types)
			mean_num_types+=typeCount
			#token_per_abs = tokenCount/aCount
			if 	typeCount and tokenCount > 0:
				type_per_token = typeCount/tokenCount
			else:
				type_per_token = 0
			mean_lexical_diversity+=type_per_token

			#stopwords
			cf = nltk_functions.content_fraction(aToken)
			mean_content_fraction+=cf

			#counts of unique tokens for each abstract
			types_filtered = nltk_functions.filter_stopwords_and_length(types,5)
			for s in types_filtered:
				if s in masterDic[org]:
					masterDic[org][s]+=1
				else:
					masterDic[org][s] = 1

		#create top tokens - note this is counting multiple tokens per text which is not ideal
		#print aText
		#topWords = filter_stopwords_and_length(word_tokenize(aText),5)
		#fdist = FreqDist(topWords)
		#print fdist.most_common(10)

		#create means
		mean_num_tokens=mean_num_tokens/aCount
		mean_num_types=mean_num_types/aCount
		mean_lexical_diversity=mean_lexical_diversity/aCount
		mean_content_fraction=mean_content_fraction/aCount

		if aCount>10:
			o.write(org + '\t' + str(aCount) + '\t' + str("%.4f" % mean_num_tokens) + "\t" + str("%.4f" % mean_num_types) + "\t" + str("%.4f" % mean_lexical_diversity) + "\t" + str("%.4f" % mean_content_fraction) + "\n")

		#colocations
		#http://www.nltk.org/howto/collocations.html
		#c = aToken.collocations()
		#print c
		finder = BigramCollocationFinder.from_words(aToken)
		#ignore bigrams that feature less thatn x times
		finder.apply_freq_filter(5)
		#print finder.nbest(bigram_measures.pmi, 10)

	for org in masterDic:
		print '\n### '+org+' ###'
		c_sorted = sorted(masterDic[org].items(), key=operator.itemgetter(1),reverse=True)
		print len(c_sorted),list(c_sorted)[:10]
		#count unique types compared to background
		uCount={}
		for k in masterDic[org]:
			if masterDic[org][k] == backgroundDic[k]:
				uCount[k]=masterDic[org][k]
		print 'uniques = '+str(len(uCount))
		uCount_sorted = sorted(uCount.items(), key=operator.itemgetter(1),reverse=True)
		print list(uCount_sorted)[:10]

	print '\n### Background freqs ###'
	b_sorted = sorted(backgroundDic.items(), key=operator.itemgetter(1),reverse=True)
	print len(b_sorted),list(b_sorted)[:10]

#def synonyms():
	#http://www.nltk.org/book/ch02.html

def visualise():
	print "Creating plots"
	p = pd.read_csv('output/nltk_counts.txt',sep='\t')
	#sort and remove low texts
	p_filter = p.sort_values('mean_lexical_diversity',ascending=False)
	p_filter = p_filter[p_filter['num_text']>20]
	#print p
	#p1 = p_filter[['org','mean_lexical_diversity','mean_content_fraction','mean_num_tokens','mean_num_types']].plot.line(x='org',rot=90,fontsize=5,legend=False,secondary_y=['mean_num_tokens','mean_num_types'])
	p1 = p_filter[['org','mean_lexical_diversity','mean_content_fraction']].plot.line(x='org',rot=90,fontsize=5,legend=False)
	p2 = p_filter[['org','num_text']].plot.bar(x='org',rot=90,secondary_y=['num_text'],fontsize=5,color='green',ax=p1,legend=False)
	#move legend
	lines, labels = p1.get_legend_handles_labels()
	lines2, labels2 = p2.get_legend_handles_labels()
	p2.legend(lines + lines2, labels + labels2, loc='upper center',prop={'size':6})
	#h1, l1 = lines.get_legend_handles_labels()
	#lines.legend(loc='upper center')
	#pp.legend(loc='upper center')
	plt.gcf().subplots_adjust(bottom=0.4)
	fig = p2.get_figure()
	fig.savefig('output/lexical_diversity_and_num_texts.pdf')

	p3 = p_filter[['org','mean_lexical_diversity','mean_content_fraction','mean_num_tokens','mean_num_types']].plot.line(x='org',rot=90,fontsize=5,secondary_y=['mean_num_tokens','mean_num_types'])
	p3.legend(loc='upper center',prop={'size':6})
	plt.gcf().subplots_adjust(bottom=0.4)
	fig = p3.get_figure()
	fig.savefig('output/lexical_diversity_and_tokens.pdf')

def staff_per_org():
	print "staff_per_org"
	#plot output normalised by number of staff
	com = 'match (o:Org)--(s:Staff)--(p:Publication) return o.short_name as o,count(distinct s) as s ,count( distinct p) as p;'
	d = []
	for res in session.run(com):
		d.append({'org':res['o'],'staffCount':res['s'],'pubCount':res['p']})
	df = pd.DataFrame(d)

	#line of best fit - http://stackoverflow.com/questions/37234163/how-to-add-a-line-of-best-fit-to-scatter-plot
	z = np.polyfit(x=df.loc[:, 'staffCount'], y=df.loc[:, 'pubCount'], deg=1)
	p = np.poly1d(z)
	df['trendline'] = p(df.loc[:, 'staffCount'])

	print df
	df = df[df['staffCount']>10]
	#print df
	#p = df[['staffCount','pubCount']].plot(kind='scatter', x='staffCount', y='pubCount',logy=True, logx=True)
	ax = df[['staffCount','pubCount']].plot(kind='scatter', x='staffCount', y='pubCount')
	df.set_index('staffCount', inplace=True)
	df.trendline.sort_index(ascending=False).plot(ax=ax)
	plt.gca().invert_xaxis()
	fig = ax.get_figure()
	fig.savefig('output/normalised_pubs_per_org.pdf')

def cross_org_pubs():
	print 'cross_org_pubs'
	#calculate numbers of publications per organisation that have authors from other orgs
	countDic = defaultdict(dict)
	orgDic = {}
	staffDic = {}
	pubDic = defaultdict(dict)
	crossOrg=defaultdict(lambda: defaultdict(dict))
	com='match (o:Org)--(s:Staff)--(p:Publication) return o.code,o.short_name,s.person_id,s.published_name,p.pub_id;'
	#create dictionaries
	for res in session.run(com):
		org_code = res['o.code']
		org_name = res['o.short_name']
		staff_code = res['s.person_id']
		staff_name = res['s.published_name']
		pid = res['p.pub_id']
		#countDic[org_code][staff_code][pid]=''
		orgDic[org_code]=org_name
		staffDic[staff_code]=staff_name
		#crossOrg[org_code]['1']=''
		#crossOrg[org_code]['2']=''
		if pid in pubDic:
			if org_code in pubDic[pid]:
				pubDic[pid][org_code]+=1
			else:
				pubDic[pid][org_code] = 1
		else:
			pubDic[pid][org_code] = 1
	#parse again

	for res in session.run(com):
		org_code = res['o.code']
		org_name = res['o.short_name']
		staff_code = res['s.person_id']
		staff_name = res['s.published_name']
		pid = res['p.pub_id']
		if len(pubDic[pid])==1:
			crossOrg[org_code]['1'][pid]=''
		else:
			crossOrg[org_code]['2'][pid]=''

	#create counts of co-organisation publications per organisation
	orgs_per_org = defaultdict(dict)
	mean_orgs_per_org_full = defaultdict(dict)
	for org in crossOrg:
		for i in crossOrg[org]:
			if i == '2':
				for pid in crossOrg[org][i]:
					#print pid
					#get number of orgs per pid
					mean_orgs_per_org_full[org][pid]=len(pubDic[pid])
					for o in pubDic[pid]:
						orgs_per_org[org][o]=''
	#print orgs_per_org
	mean_orgs_per_org = {}
	for org in mean_orgs_per_org_full:
		m = 0
		for pid in mean_orgs_per_org_full[org]:
			m+=mean_orgs_per_org_full[org][pid]
		m = m/len(mean_orgs_per_org_full[org])
		mean_orgs_per_org[org]=m
	#print mean_orgs_per_org

	o = open('output/cross_pubs.txt', 'w')
	o.write('org\tsingle\tmulti\tmulti_over_single\torg_num\torg_num_per_multi\tmean_orgs_per_multi\n')
	for org in crossOrg:
		print '### '+orgDic[org]+' ###'
		o.write(orgDic[org])
		orgNum=len(crossOrg[org]['1'])
		crossNum = len(crossOrg[org]['2'])
		crossPerPub=0
		if crossNum>0 and orgNum>0:
			crossPerPub = crossNum/orgNum
		for i in crossOrg[org]:
			print i,len(crossOrg[org][i])
			o.write('\t'+str(len(crossOrg[org][i])))
		crossNumPerCross=0
		meanCrossNumPerCross=0

		if crossNum>0:
			crossNumPerCross = len(orgs_per_org[org])/crossNum
			meanCrossNumPerCross = mean_orgs_per_org[org]
		o.write('\t'+str("%.4f" % crossPerPub)+'\t'+str(len(orgs_per_org[org]))+'\t'+str("%.4f" % crossNumPerCross)+'\t'+str("%.4f" % meanCrossNumPerCross))
		o.write('\n')
	o.close()

def cross_org_pubs_plot():
	#plot
	p = pd.read_csv('output/cross_pubs.txt',sep='\t')
	p_filter = p.sort_values('multi_over_single',ascending=False)
	p_filter = p_filter[p_filter['single']>10]
	#print p
	#p1 = p_filter[['org','mean_lexical_diversity','mean_content_fraction','mean_num_tokens','mean_num_types']].plot.line(x='org',rot=90,fontsize=5,legend=False,secondary_y=['mean_num_tokens','mean_num_types'])
	p1 = p_filter[['org','multi_over_single','org_num_per_multi']].plot.bar(x='org',rot=90,fontsize=5)
	#p2 = p_filter[['org','single','multi','org_num']].plot.bar(x='org',rot=90,secondary_y=['single','multi','org_num'],fontsize=5,ax=p1,legend=False)
	#move legend
	#lines, labels = p1.get_legend_handles_labels()
	#lines2, labels2 = p2.get_legend_handles_labels()
	#p2.legend(lines + lines2, labels + labels2, loc='upper center',prop={'size':6})
	#h1, l1 = lines.get_legend_handles_labels()
	#lines.legend(loc='upper center')
	#pp.legend(loc='upper center')
	plt.gcf().subplots_adjust(bottom=0.4)
	fig = p1.get_figure()
	fig.savefig('output/cross_pubs.pdf')

	p_filter = p.sort_values('single',ascending=False)
	p_filter = p_filter[p_filter['single']>50]
	p1 = p_filter[['single','multi','org_num']].plot.scatter(x='single',y='multi',s=p_filter['org_num']*2)
	fig = p1.get_figure()
	fig.savefig('output/cross_pubs2.pdf')

	p2 = p_filter[['multi_over_single','org_num']].plot.scatter(title='Proportion of multiple organisation publications against number of organisations',y='multi_over_single',x='org_num',s=(p_filter['multi']+p_filter['single'])/10)
	fig = p2.get_figure()
	fig.savefig('output/cross_pubs3.pdf')

	p_filter = p.sort_values('mean_orgs_per_multi',ascending=False)
	p_filter = p_filter[p_filter['single']>50]
	p3 = p_filter[['org','mean_orgs_per_multi']].plot.bar(x='org',rot=90,fontsize=5)
	fig = p3.get_figure()
	fig.savefig('output/cross_pubs4.pdf')

#count_things()
#visualise()
#staff_per_org()
#cross_org_pubs()
#cross_org_pubs_plot()