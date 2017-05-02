import neo4j_functions, nltk_functions

def get_metrics():
	session = neo4j_functions.connect()
	com = "match (p:Publication) return distinct p.abstract as a, p.title as t, p.pub_id as pid limit 10;"
	for res in session.run(com):
		abs = res['a']
		#content fraction
		if len(abs)>0:
			cf = nltk_functions.content_fraction(abs)
			ld = nltk_functions.lexical_diversity(abs)
			ts = nltk_functions.run_textstat(abs)
			print cf, ld, ts, abs


get_metrics()