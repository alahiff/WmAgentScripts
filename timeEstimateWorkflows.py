#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import simplejson as json
import optparse
from das_client import get_data

das_host='https://cmsweb.cern.ch'

def getEvents(dataset):
        query = "dataset dataset="+dataset
        das_data = get_data(das_host,query,0,0,0)
        myStatus = ''
        if isinstance(das_data, basestring):
           result = json.loads(das_data)
        else:
           result = das_data
           if result['status'] == 'fail' :
              print 'ERROR: DAS query failed with reason:',result['reason']
              return 'Unknown'
           else:
              b = result['data'][0]['dataset']
              for key in b:
                 if 'nevents' in key:
                    events = key['nevents']
                    return events
        return 'Unknown'

def outputdatasetsWorkflow(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
        r2=conn.getresponse()
        datasets = json.loads(r2.read())
        if len(datasets)==0:
                print "ERROR: No output datasets for this workflow"
                sys.exit(0)
        return datasets

def getInputDataSet(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        if 'InputDataset' in request:
           inputDataSets=request['InputDataset']
        else:
           print "ERROR: No InputDataSet for workflow"
           return "NA"
        if len(inputDataSets)<1:
                print "ERROR: No InputDataSet for workflow"
                return "NA"
        else:   
                return inputDataSets

def getTimePerEvent(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        timePerEvent = 0
        if 'TimePerEvent' in request:
           timePerEvent=request['TimePerEvent']
        return timePerEvent

def getPriority(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        priority = -1
        if 'RequestPriority' in request:
           priority=request['RequestPriority']
        return priority

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

def getCampaign(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        campaign = "NA"
        if 'Campaign' in request:
           campaign = request['Campaign']
        return campaign

def getInputEvents(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        cacheID=request['RequestNumEvents']
        return cacheID

def getAssignedSite(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())

        site = 'Unknown'
        if 'Site Whitelist' in request:
           site = request['Site Whitelist']

        if 'T1_TW_ASGC' in site:
           return 'T1_TW_ASGC_MSS'
        if 'T1_IT_CNAF' in site:
           return 'T1_IT_CNAF_MSS'
        if 'T1_DE_KIT' in site:
           return 'T1_DE_KIT_MSS'
        if 'T1_ES_PIC' in site:
           return 'T1_ES_PIC_MSS'
        if 'T1_FR_CCIN2P3' in site:
           return 'T1_FR_CCIN2P3_MSS'
        if 'T1_UK_RAL' in site:
           return 'T1_UK_RAL_MSS'
        if 'T1_RU_JINR' in site:
           return 'T1_RU_JINR'
        if 'T1_US_FNAL' in site:
           return 'T1_US_FNAL_MSS'
        if 'T2_CH_CERN' in site:
           return 'T2_CH_CERN_MSS'
     
        for ssite in site:
           if 'T2' in ssite:
              return 'T2'

        return 'Unknown'

def generateAllInfo(url, workflow):
        timesPerEvent = { 'pAWinter13DR53X': 5.0, 'Summer12DR53X': 17.0, '2019GEMUpg14DR' : 150, '2023MuonUpg14DR' : 660, '2023TTIUpg14D': 360, 'Upg2023SHCAL14DR' : 660 }
        slotsPerSite = { 'T1_TW_ASGC_MSS': 1400, 'T1_IT_CNAF_MSS': 2000, 'T1_DE_KIT_MSS': 2000, 'T1_FR_CCIN2P3_MSS': 1500, 'T1_ES_PIC_MSS': 800, 'T1_UK_RAL_MSS': 1200, 'T1_US_FNAL_MSS': 10000, 'T2_CH_CERN_MSS': 4000, 'T2': 2000, 'T1_RU_JINR': 1100 }

        # Input dataset
        inputDataset = getInputDataSet(url, workflow)
        if inputDataset == "NA":
           return ["NA", 0, 0, 0, 0]

        # Get assigned site if workflow is acquired, running or complete
        assignedSite = getAssignedSite(url, workflow)
        print 'ass = ',assignedSite

        # Get custodial site
        custodialSite = findCustodialLocation(url, inputDataset)

        # Choose site to use
        if assignedSite != 'Unknown':
           useSite = assignedSite
        else:
           useSite = custodialSite

        # Get campaign name
        campaign = getCampaign(url, workflow)
        if campaign == "NA":
           return ["NA", 0, 0, 0, 0]

        # Get requested number of events
        eventsInput = getInputEvents(url, workflow)

        # Get output datasets
        outputDatasets = outputdatasetsWorkflow(url, workflow)

        # Get number of events in the output dataset (try to pick the right one)
        eventsOut = 0
        for dataset in outputDatasets:

           if 'DQM' not in dataset:
 
              outputEvents = getEvents(dataset)
              print dataset,'out = ',outputEvents
              if not str(outputEvents).isdigit():
                 if outputEvents == 'Unknown':
                    outputEvents = getEvents(dataset)

              if 'GEN-RAW' in dataset and outputEvents > 0:
                 eventsOut=outputEvents
              if 'GEN-RECO' in dataset and outputEvents > 0:
                 eventsOut=outputEvents
              if 'GEN-SIM-RECO' in dataset and outputEvents > 0 and outputEvents > eventsOut:
                 eventsOut=outputEvents
              if 'GEN-SIM-DIGI-RECO' in dataset and outputEvents > 0 and outputEvents > eventsOut:
                 eventsOut=outputEvents
              if 'GEN-SIM-RAW-RECO' in dataset and outputEvents > 0 and outputEvents > eventsOut:
                 eventsOut=outputEvents
              if 'AODSIM' in dataset and outputEvents > 0 and outputEvents > eventsOut:
                 eventsOut=outputEvents
              if 'GEN-SIM-RAW' in dataset and outputEvents > 0 and outputEvents > eventsOut:
                 eventsOut=outputEvents

        if not str(eventsOut).isdigit() or str(eventsOut) == 'N/A':
           eventsOut = 0

        timePerEvent = 0
        if campaign in timesPerEvent:
           timePerEvent = timesPerEvent[campaign]
        else:
           timePerEvent = getTimePerEvent(url, workflow)

        slots = 0
        if useSite in slotsPerSite:
           slots = slotsPerSite[useSite]

        eventsRemaining = eventsInput - eventsOut

        timeRemaining = -1
        if timePerEvent > 0 and slots > 0 and eventsRemaining > 0:
           timeRemaining = timePerEvent*(eventsInput - eventsOut)/slots/3600.0/24.0

        print 'in=',eventsInput,'out=',eventsOut

        priority = getPriority(url, workflow)

        if priority < 0:
           return ["NA", 0, 0, 0, 0]

        return [campaign, priority, useSite, eventsRemaining, timeRemaining]



def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-w', '--workflow', help='Workflow',dest='userWorkflow')
	(options,args) = parser.parse_args()
	if not options.filename and not options.userWorkflow:
		print "A filename or workflow is required"
		sys.exit(0)

        if options.filename:
           f=open(filename,'r')
        else:
           f=[options.userWorkflow]


        for workflow in f:
           workflow = workflow.rstrip('\n')
           [campaign, priority, site, eventsRemaining, timeRemaining] = generateAllInfo(url, workflow)
           print workflow, campaign, priority, site, eventsRemaining, timeRemaining

	sys.exit(0)

if __name__ == "__main__":
	main()
