#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
from dbs.apis.dbsClient import DbsApi
from das_client import get_data
from xml.dom.minidom import getDOMImplementation
from collections import defaultdict

url='cmsweb.cern.ch'
dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
das_host='https://cmsweb.cern.ch'

def getInputDataSet(url, workflow):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
   r2=conn.getresponse()
   request = json.loads(r2.read())
   if 'InputDataset' in request:
      inputDataSets=request['InputDataset']
      if len(inputDataSets)<1:
         print "ERROR: No InputDataSet for workflow"
      else:
         return inputDataSets
   else:
      print "ERROR: No InputDataSet for workflow"
   return ''

def getWorkflows(state):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/couchdb/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after')
   r2=conn.getresponse()
   data = json.loads(r2.read())
   items = data['rows']

   workflows = []
   for item in items:
      if state in item['key'] and 'ReDigi' in item['key']:
         workflows.append(item['key'][0])

   return workflows

def getBlockSizeDataSet(dataset):
    """
    Returns the number of events in a dataset using DBS3

    """
    # initialize API to DBS3
    dbsapi = DbsApi(url=dbs3_url)
    # retrieve dataset summary
    reply = dbsapi.listBlockSummaries(dataset=dataset)
    return int(reply[0]['file_size'])/1000000000000.0

def createXML(datasets):
    """
    From a list of datasets return an XML of the datasets in the format required by Phedex
    """
    # Create the minidom document
    impl=getDOMImplementation()
    doc=impl.createDocument(None, "data", None)
    result = doc.createElement("data")
    result.setAttribute('version', '2')
    # Create the <dbs> base element
    dbs = doc.createElement("dbs")
    dbs.setAttribute("name", "https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
    result.appendChild(dbs)    
    #Create each of the <dataset> element
    for datasetname in datasets:
        dataset=doc.createElement("dataset")
        dataset.setAttribute("is-open","y")
        dataset.setAttribute("is-transient","y")
        dataset.setAttribute("name",datasetname)
        dbs.appendChild(dataset)
    return result.toprettyxml(indent="  ")

def phedexPost(url, request, params):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    encodedParams = urllib.urlencode(params)
    r1 = conn.request("POST", request, encodedParams)
    r2 = conn.getresponse()
    result = json.loads(r2.read())
    conn.close()
    return result

def makeReplicaRequest(url, site,datasets, comments):
    dataXML = createXML(datasets)
    params = { "node" : site,"data" : dataXML, "group": "DataOps", "priority":'normal',
                 "custodial":"n","request_only":"y" ,"move":"n","no_mail":"n","comments":comments}
    response = phedexPost(url, "/phedex/datasvc/json/prod/subscribe", params)
    return response

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return "No Site"
        if len(request['block'])==0:
                return "No Site"
        for replica in request['block'][0]['replica']:
                if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                        return replica['node']
        return "None"

def checkAcceptedSubscriptionRequest(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&type=xfer')
        r2=conn.getresponse()
        result = json.loads(r2.read())
        requests=result['phedex']
        if 'request' not in requests.keys():
                return [False, False]
        subscribed=False
        approved=False
        for request in result['phedex']['request']:
                for node in request['node']:
                        if 'Disk' in node['name']:
                                subscribed=True
                                if node['decision']=='approved':
                                        approved=True
        return[subscribed, approved]

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
        parser.add_option('-e', '--execute', help='Actually subscribe data',action="store_true",dest='execute')
	(options,args) = parser.parse_args()

        t1s = ['T1_IT_CNAF_Disk','T1_ES_PIC_Disk','T1_DE_KIT_Disk','T1_FR_CCIN2P3_Disk','T1_UK_RAL_Disk','T1_US_FNAL_Disk']

        data = {}
        sizes = {}
        workflows = getWorkflows('assignment-approved')
        for workflow in workflows:
           dataset = getInputDataSet(url, workflow)
           [req,app] = checkAcceptedSubscriptionRequest(url, dataset)
           if not req:
              siteCustodial = findCustodialLocation(url, dataset)
              size = getBlockSizeDataSet(dataset)
              siteDisk = siteCustodial.replace("MSS","Disk")
              if siteDisk == 'None':
                 siteDisk = 'T1_US_FNAL_Disk'
              if siteDisk not in data:
                 data[siteDisk] = []
              if siteDisk not in sizes:
                 sizes[siteDisk] = 0
              if dataset not in data[siteDisk]:
                 data[siteDisk].append(dataset)
                 sizes[siteDisk] = sizes[siteDisk] + size
        for site in data:
           print ''
           print 'Subscription to',site,'of size',sizes[site],'TB'
           print data[site]
           if options.execute:
              print makeReplicaRequest(url, site, data[site], 'prestaging')

	sys.exit(0)

if __name__ == "__main__":
	main()
