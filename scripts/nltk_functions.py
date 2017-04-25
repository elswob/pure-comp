from __future__ import division
import neo4j_functions
import operator
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
#from nltk.book import *
from nltk.collocations import *
from nltk import FreqDist
from nltk.corpus import stopwords
from collections import defaultdict
#import matplotlib.pyplot as plt

bigram_measures = nltk.collocations.BigramAssocMeasures()
trigram_measures = nltk.collocations.TrigramAssocMeasures()

#analyse output
# awk -F '\t' '$2>50' output/nltk_counts.txt | sort -t$'\t' -k4 -nr | less

#http://www.nltk.org/book/ch01.html

def content_fraction(text):
	stopwords = nltk.corpus.stopwords.words('english')
	content = [w for w in text if w.lower() not in stopwords]
	return len(content) / len(text)

def filter_stopwords_and_length(text,wordLength):
	stopwords = nltk.corpus.stopwords.words('english')
	text_no_short_and_no_stopwords = [w for w in text if len(w)>wordLength and w.lower() not in stopwords]
	return text_no_short_and_no_stopwords

#Section 1 (lexical diversity)
def count_things():
	session = neo4j_functions.session
	com = "match (o:Org)--(s:Staff)--(p:Publication) return distinct o.short_name as o, p.abstract as a, p.title as t, p.pub_id as pid limit 1000;"
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
			types_filtered = filter_stopwords_and_length(types,5)
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
		print '\n### '+org+' ####'
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
			cf = content_fraction(aToken)
			mean_content_fraction+=cf

			#counts of unique tokens for each abstract
			types_filtered = filter_stopwords_and_length(types,5)
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
		print '### '+org+' ###'
		c_sorted = sorted(masterDic[org].items(), key=operator.itemgetter(1),reverse=True)
		print len(c_sorted),list(c_sorted)[:10]

	print '### Background freqs ###'
	b_sorted = sorted(backgroundDic.items(), key=operator.itemgetter(1),reverse=True)
	print len(b_sorted),list(b_sorted)[:10]

def visualise():
	print "Creating plots"

count_things()