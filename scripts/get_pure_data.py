import requests
import os
import json
import gzip
import xmltodict
from time import gmtime, strftime

dataDir = '/Users/be15516/projects/pure-comp/data/'

def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 35

def get_xml():
	print "Getting pure xml"
	fSize=1000
	totalPubs = 160000
	for i in range(0,totalPubs,fSize):
	#for i in range(0,100,10):
		t = strftime("%H:%M:%S", gmtime())
		print t,i
		fName = 'pure_'+str(i)+'.xml.gz'
		if is_non_zero_file(dataDir+'/xml/'+fName):
			print fName,'XML file already downloaded'
		else:
			w = gzip.open(dataDir+'/xml/'+fName,'w')
			url = 'https://research-information.bris.ac.uk/ws/rest/publication?window.size='+str(fSize)+'&offset='+str(i)
			#print url
			res = requests.get(url)
			w.write(res.text.encode('utf-8'))
			w.close()

def xml_to_json():
	fSize=1000
	totalPubs = 160000
	for i in range(0,totalPubs,fSize):
		fName_json = 'pure_'+str(i)+'.json.gz'
		fName_xml = 'pure_'+str(i)+'.xml.gz'
		if is_non_zero_file(dataDir+'/json/'+fName_json):
			print fName_json, 'JSON file already created'
		else:
			if is_non_zero_file(dataDir+'/xml/'+fName_xml):
				print 'Converting '+fName_xml+' to JSON'
				w = gzip.open(dataDir+'/json/'+fName_json,'w')
				with gzip.open(dataDir+'/xml/'+fName_xml) as fd:
					doc = xmltodict.parse(fd.read())
					w.write(json.dumps(doc))
				w.close()

def main():
	get_xml()
	#xml_to_json()

if __name__ == '__main__':
	main()