import neo4j_functions
from nltk.tokenize import sent_tokenize, word_tokenize

#http://www.nltk.org/book/ch01.html

#Section 1 (lexical diversity)
def count_things():
	session = neo4j_functions.session
	com = "match (o:Org)--(s:Staff)--(p:Publication) return distinct o.short_name as o, p.abstract as a, p.title as t;"
	pubText = {}
	pubCount = {}
	for res in session.run(com):
		abstract = res['a']
		title = res['t']
		org = res['o']
		if org in pubText:
			if len(abstract)>1:
				pubCount[org]+=1
				pubText[org]+=abstract
			#pubText[org]+=title
		else:
			if len(abstract)>1:
				pubCount[org]=1
				pubText[org]=abstract
			#pubText[org]+=title
	#print aText
	o = open('output/nltk_counts.txt', 'w')
	o.write('org\tnum_abs\tnum_words\tnum_token\ttokens_per_abs\ttokens_per_words\n')
	for a in pubText:
		print a
		aToken = word_tokenize(pubText[a])
		aCount = pubCount[a]
		wordCount = len(aToken)
		tokenCount = len(set(aToken))
		token_per_abs = tokenCount/aCount
		if 	tokenCount and wordCount > 0:
			token_per_word = float(tokenCount)/float(wordCount)
		else:
			token_per_word = 0
		o.write(a + '\t' + str(aCount) + '\t' + str(wordCount) + "\t" + str(tokenCount) + "\t" + str(token_per_abs) + "\t" + str("%.4f" % token_per_word) + "\n")

	#cat output/nltk_counts.txt | sort -t$'\t' -k5 -nr | less

count_things()