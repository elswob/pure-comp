import neo4j_functions
from nltk.tokenize import sent_tokenize, word_tokenize

#http://www.nltk.org/book/ch01.html

def count_things():
	session = neo4j_functions.session
	com = "match (o:Org)--(s:Staff)--(p:Publication) return distinct o.short_name as o, p.abstract as p;"
	absText = {}
	absCount = {}
	for res in session.run(com):
		if res['o'] in absText:
			absCount[res['o']]+=1
			absText[res['o']]+=res['p']
		else:
			absCount[res['o']]=1
			absText[res['o']]=res['p']
	#print aText
	o = open('output/nltk_counts.txt', 'w')
	o.write('org\tnum_abs\tnum_words\tnum_token\twords_per_abs\ttokens_per_words\n')
	for a in absText:
		#print aText[a]
		aToken = word_tokenize(absText[a])
		aCount = absCount[a]
		wordCount = len(aToken)
		tokenCount = len(set(aToken))
		token_per_abs = tokenCount/aCount
		token_per_word = float(tokenCount)/float(wordCount)
		o.write(a + '\t' + str(aCount) + '\t' + str(wordCount) + "\t" + str(tokenCount) + "\t" + str(token_per_abs) + "\t" + str("%.4f" % token_per_word) + "\n")

count_things()