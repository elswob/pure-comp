import requests
import os
import json
import gzip
import re
import xmltodict
from time import gmtime, strftime

dataDir = '/Users/be15516/projects/pure-comp/data/'
fSize=1000
totalPubs = 160000

def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 35

def get_xml():
	print "Getting pure xml"
	for i in range(0,totalPubs,fSize):
	#for i in range(0,100,10):
		t = strftime("%H:%M:%S", gmtime())
		print t,i
		fName = 'pure_'+str(i)+'.xml.gz'
		if is_non_zero_file(dataDir+'/xml/'+fName):
			print fName,'XML file already downloaded'
		else:
			w = gzip.open(dataDir+'/xml/'+fName,'w')
			#url = 'https://research-information.bris.ac.uk/ws/rest/publication?window.size='+str(fSize)+'&offset='+str(i)+'&rendering=xml_long'
			url = 'https://research-information.bris.ac.uk/ws/rest/publication?window.size='+str(fSize)+'&window.offset='+str(i)+'&rendering=xml_short'
			print url
			res = requests.get(url)
			w.write(res.text.encode('utf-8'))
			w.close()

def get_long():
	print "Getting Pure long"
	fSize=10000
	for i in range(0,totalPubs,fSize):
	#for i in range(0,100,10):
		t = strftime("%H:%M:%S", gmtime())
		print t,i
		fName = 'pure_'+str(i)+'.long.gz'
		if is_non_zero_file(dataDir+'/long/'+fName):
			print fName,'long file already downloaded'
		else:
			w = gzip.open(dataDir+'/long/'+fName,'w')
			url = 'https://research-information.bris.ac.uk/ws/rest/publication?window.size='+str(fSize)+'&window.offset='+str(i)+'&rendering=long'
			print url
			res = requests.get(url)
			w.write(res.text.encode('utf-8'))
			w.close()

def parse_long():
	fSize=10000
	inAbs = False
	absData = ''
	uuid = ''
	absDic = {}
	o = gzip.open('data/absData.txt.gz','a')
	for i in range(0,totalPubs,fSize):
		fName = 'pure_'+str(i)+'.long.gz'
		print "Reading "+fName
		with gzip.open(dataDir+'/long/'+fName) as data_file:
			for line in data_file:
				lData = line.split('<')
				for l in lData:
					m = re.match(r'.*?renderedContentUUID="(.*?)".*',l)
					if m:
						#print m.group(1)
						uuid = m.group(1)
					aStart = re.match(r'div class="textblock">',l)
					if aStart:
						inAbs=True
					aEnd = re.match(r'/div>',l)
					if aEnd:
						#clean up absData
						absData = absData.replace('div class="textblock">','')
						absData = re.sub(r'^p>','',absData)
						absData = re.sub(r'/p>$','',absData)
						absData = absData.replace('\n',' ')
						#print absData
						#check if already done
						if uuid in absDic:
							if len(absDic[uuid])<len(absData):
								o.write(uuid+'\t'+absData+'\n')
								absDic[uuid]=absData
						else:
							o.write(uuid+'\t'+absData+'\n')
							absDic[uuid]=absData
						absData = ''
						inAbs=False
					if inAbs == True:
						absData+=l
	o.close()


def xml_to_json():
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

def parse_json():
	fName = 'pure_0.json.gz'
	with gzip.open(dataDir+'/json/'+fName) as data_file:
		json_data = json.load(data_file)
		for j in json_data['publication-template:GetPublicationResponse']['core:result']['core:content']:
			uuid = title = abstract = ''
			uuid = j['@uuid']
			if 'publication-base_uk:title' in j:
				title = j['publication-base_uk:title'].encode('utf-8')

			print uuid,title,abstract

def get_people():
	pDic={}
	for i in range(0,7):
		url = 'http://research-information.bristol.ac.uk/en/persons/search.html?filter=academic&page='+str(i)+'&pageSize=500'
		print url
		res = requests.get(url)
		uuid = re.findall('persons/(.*?)\((.*?)\).html', res.text)
		#print uuid
		for u in uuid:
			name = u[0].replace('-',' ').title()
			uuid = u[1]
			pDic[uuid]=name
	print len(pDic)

def main():
	#get_xml()
	#get_long()
	#parse_long()
	#xml_to_json()
	#parse_json()
	get_people()

if __name__ == '__main__':
	main()