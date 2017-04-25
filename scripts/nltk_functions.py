from __future__ import division
import neo4j_functions
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
#from nltk.book import *
from nltk.collocations import *
from nltk import FreqDist
from nltk.corpus import stopwords
from collections import defaultdict

bigram_measures = nltk.collocations.BigramAssocMeasures()
trigram_measures = nltk.collocations.TrigramAssocMeasures()

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
	#create single text variable
	allText = ''
	for res in session.run(com):
		abstract = res['a']
		title = res['t']
		org = res['o']
		pid = res['pid']
		allText+=abstract
		if len(abstract)>1:
			if org in pubText:
				if pid in pubText[org]:
					pubText[org][pid]+=abstract
					#pubText[org]+=title
				else:
					pubText[org][pid]=abstract
			else:
				pubText[org][pid]=abstract

	#get background token frequency
	bTokens = word_tokenize(allText)
	bTokens_no_short_and_no_stopwords = filter_stopwords_and_length(bTokens,5)
	fdist = FreqDist(bTokens_no_short_and_no_stopwords)
	print 'Top 10 common tokens in all text'
	print fdist.most_common(10)

	o = open('output/nltk_counts.txt', 'w')
	o.write('org\tnum_text\tmean_num_tokens\tmean_num_types\tlexical_diversity\tcontent_fraction\n')
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
			typeCount = len(set(aToken))
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

		#create top tokens
		#print aText
		topWords = filter_stopwords_and_length(word_tokenize(aText),5)
		fdist = FreqDist(topWords)
		print fdist.most_common(10)

		#create means
		mean_num_tokens=mean_num_tokens/aCount
		mean_num_types=mean_num_types/aCount
		mean_lexical_diversity=mean_lexical_diversity/aCount
		mean_content_fraction=mean_content_fraction/aCount

		o.write(org + '\t' + str(aCount) + '\t' + str("%.4f" % mean_num_tokens) + "\t" + str("%.4f" % mean_num_types) + "\t" + str("%.4f" % mean_lexical_diversity) + "\t" + str("%.4f" % mean_content_fraction) + "\n")

		#colocations
		#http://www.nltk.org/howto/collocations.html
		#c = aToken.collocations()
		#print c
		finder = BigramCollocationFinder.from_words(aToken)
		#ignore bigrams that feature less thatn x times
		finder.apply_freq_filter(5)
		#print finder.nbest(bigram_measures.pmi, 10)

	#cat output/nltk_counts.txt | sort -t$'\t' -k5 -nr | less

count_things()