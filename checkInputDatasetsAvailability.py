#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
from dbs.apis.dbsClient import DbsApi
from das_client import get_data

url='cmsweb.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
das_host='https://cmsweb.cern.ch'

def checkAcceptedSubscriptionRequest(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&type=xfer')
        r2=conn.getresponse()
        result = json.loads(r2.read())
        requests=result['phedex']
        if 'request' not in requests.keys():
                return [False, False]
        ourNode=False
        otherNode=False
        for request in result['phedex']['request']:
                for node in request['node']:
                        if 'Disk' in node['name']:
                                ourNode=True
                                if node['decision']=='approved':
                                        otherNode=True
        return[ourNode, otherNode]


def getSizesAndSites(dataset):
        count = 0
        query = "site dataset="+dataset
        validSites = ['T2_CH_CERN','T1_IT_CNAF_Disk','T1_DE_KIT_Disk','T1_FR_CCIN2P3_Disk','T1_ES_PIC_Disk','T1_UK_RAL_Disk','T1_US_FNAL_Disk']
        das_data = get_data(das_host,query,0,0,0)
        if isinstance(das_data, basestring):
           result = json.loads(das_data)
        else:
           result = das_data
           if result['status'] == 'fail' :
              print 'ERROR: DAS query failed with reason:',result['reason']
              sys.exit(0)
           else:
              preresult = result['data']
              for key in preresult:
                 if len(key['site']) > 0:
                    name = key['site'][0]['name']
                    if name in validSites:
                       if 'dataset_fraction' in key['site'][0]:
                          completion = key['site'][0]['dataset_fraction']
                          count = count + 1
                          print ' ',name,completion
        return count   

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-d', '--dataset', help='Dataset',dest='userDataset')
	parser.add_option('-w', '--workflow', help='Workflow',dest='userWorkflow')
	(options,args) = parser.parse_args()
	if not options.filename and not options.userDataset and options.userWorkflow:
		print "A filename, dataset or workflow is required"
		sys.exit(0)

        filename=options.filename

        if options.filename:
           f=open(filename,'r')
        elif options.userDataset:
           f=[options.userDataset]
        else:
           f=[options.userWorkflow]

        for line in f:
           line = line.rstrip('\n')
           pieces = line.split()
           [req,app] = checkAcceptedSubscriptionRequest(url,pieces[1])
           msg_req = 'NotSubscribed'
           msg_app = 'NotApproved'
           if req:
              msg_req = 'Subscribed'
           if app:
              msg_app = 'Approved'
           print pieces[0],pieces[1],msg_req,msg_app
           c0 = getSizesAndSites(pieces[1])
           if c0 == 0:
              c1 = getSizesAndSites(pieces[1])

	sys.exit(0)

if __name__ == "__main__":
	main()
