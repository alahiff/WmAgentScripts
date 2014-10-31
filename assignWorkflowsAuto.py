#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os, json
import optparse
import reqMgrClient
from dbs.apis.dbsClient import DbsApi
from changePriorityWorkflow import changePriorityWorkflow

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

def getLFNbase(url, dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve file
        reply = dbsapi.listFiles(dataset=dataset)
        file = reply[0]['logical_file_name']
        # determine lfn
        lfn = '/store/mc'
        if '/store/himc' in file:
           lfn = '/store/himc'
        return lfn

def checkAcceptedSubscriptionRequest(url, dataset, site):
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
                        if node['name']==site and node['decision']=='approved':
                                ourNode=True
                        elif 'Disk' in node['name'] and node['decision']=='approved':
                                otherNode=True
        return[ourNode, otherNode]

def getDatasetStatus(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        return reply[0]['dataset_access_type']

def getDatasets(dataset):
       # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*')
        return reply

def getDatasetVersion(url, workflow):
        versionNum = 1
        era = getEra(url, workflow)
        partialProcVersion = getProcString(url, workflow)
        outputs = reqMgrClient.outputdatasetsWorkflow(url, workflow)
        for output in outputs:
           bits = output.split('/')
           outputCheck = '/'+bits[1]+'/'+era+'-'+partialProcVersion+'*/'+bits[len(bits)-1]

           datasets = getDatasets(outputCheck)
           for dataset in datasets:
              datasetName = dataset['dataset']
              matchObj = re.match(r".*-v(\d+)/.*", datasetName)
              if matchObj:
                 currentVersionNum = int(matchObj.group(1))
                 if versionNum <= currentVersionNum:
                    versionNum=versionNum+1

        return versionNum

def getPileupDataset(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')

        pileupDataset = 'None'

        for line in list:
           if 'request.schema.MCPileup' in line:
              pileupDataset = line[line.find("'")+1:line.find("'",line.find("'")+1)]

        return pileupDataset


def getPriority(url, workflow): 
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
        r2=conn.getresponse()
        workload=r2.read()
        list = workload.split('\n')
              
        priority = -1 
        
        for line in list:
           if 'request.schema.RequestPriority' in line:
              priority = line[line.find("=")+1:line.find("<br/")]

        priority = priority.strip()
        priority = re.sub(r'\'', '', priority)
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

def getEra(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        era=request['AcquisitionEra']
        return era

def getProcString(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        procString=request['ProcessingString']
        return procString

def assignRequest(url ,workflow ,team ,site ,era, procversion, procstring, activity, lfn, maxmergeevents, maxRSS, maxVSize, useX, siteCust):

    if "Upgrade" in workflow:
       softTimeout = 159600
    else:
       softTimeout = 144000
       
    params = {"action": "Assign",
              "Team"+team: "checked",
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "MergedLFNBase": lfn,
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "CustodialSites": siteCust,
              "Priority" : "Normal",
              "SoftTimeout": softTimeout,
              "GracePeriod": 300,
              "MaxMergeEvents": maxmergeevents,
	      "maxRSS": maxRSS,
              "maxVSize": maxVSize,
              "AcquisitionEra": era,
	      "dashboard": activity,
              "ProcessingVersion": procversion,
              "ProcessingString": procstring,
              "checkbox"+workflow: "checked"}
              
              
              
    # we don't want to subscribe these to tape and we certainly don't want move subscriptions ripping things out of T2's.
              
    if params["CustodialSites"] == 'None' or params["CustodialSites"] == '': 
       del params["CustodialSites"]
       siteCust='None'        
              
    if useX == 1:
       print "- Using xrootd for input dataset"
       params['useSiteListAsLocation'] = "true"

    encodedParams = urllib.urlencode(params, True)

    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("POST",  "/reqmgr/assign/handleAssignmentPage", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not assign request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
  	sys.exit(1)
    conn.close()
    print 'Assigned workflow:',workflow,'to site:',site,'custodial site:',siteCust,'acquisition era:',era,'team',team,'processing string:',procstring,'processing version:',procversion,'lfn:',lfn
    return

def main():
	url='cmsweb.cern.ch'	
	parser = optparse.OptionParser()
	parser.add_option('-f', '--filename', help='Filename',dest='filename')
	parser.add_option('-w', '--workflow', help='Workflow',dest='userWorkflow')
	parser.add_option('-t', '--team', help='Type of Requests',dest='team')
	parser.add_option('-s', '--site', help='Force workflow to run at this site. For HLT/AI just put use HLT.',dest='site')
	parser.add_option('-k', '--ignore-restrictions', help='Ignore site restrictions',action="store_true",dest='ignoresite')
	parser.add_option('-u', '--new-priority', help='Change workflow priority to #',dest='newpriority')
	parser.add_option('-c', '--custodial', help='Custodial site',dest='siteCust')
	parser.add_option('-p', '--procstring', help='Process String',dest='inprocstring')
	parser.add_option('-m', '--procversion', help='Process Version',dest='inprocversion')
	parser.add_option('-n', '--specialstring', help='Special Process String',dest='specialprocstring')
	parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
	parser.add_option('-x', '--restrict', help='Only assign workflows for this site',dest='restrict')
	parser.add_option('-r', '--rssmax', help='Max RSS',dest='maxRSS')
	parser.add_option('-v', '--vsizemax', help='Max VMem',dest='maxVSize')
	parser.add_option('-a', '--extension', help='Use _ext special name',dest='extension')
        parser.add_option('-o', '--xrootd', help='Read input using xrootd',action="store_true",dest='xrootd')
        parser.add_option('-i', '--ignore', help='Ignore any errors',action="store_true",dest='ignore')
	(options,args) = parser.parse_args()
	if not options.filename and not options.userWorkflow:
		print "A filename or workflow is required"
		sys.exit(0)
	activity='reprocessing'
        #activity='test'
        if not options.restrict:
                restrict='None'
        else:
                restrict=options.restrict
        maxRSS = 2800000
        if not options.maxRSS:
                maxRSS = 3000000
        else:
                maxRSS=options.maxRSS
	maxRSSdefault = maxRSS
        maxVSize = 4100000000
        if not options.maxVSize:
                maxVSize = 4100000000
        else:
                maxVSize=options.maxVSize
	filename=options.filename

        if not options.xrootd:
           useX = 0
        else:
           useX = 1
           
        ignore = 0
        if options.ignore:
           ignore = 1

        ignoresiterestrictions = 0
        if options.ignoresite:
           ignoresiterestrictions = 1
           
        if not options.newpriority:
           newpriority=0
        else: 
           newpriority=options.newpriority

        # Valid Tier-1 sites
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL', 'T2_CH_CERN', 'HLT']

        if options.filename:
           f=open(filename,'r')
        else:
           f=[options.userWorkflow]

        for workflow in f:
           workflow = workflow.rstrip('\n')
           siteUse=options.site
           if siteUse == 'T2_US':
              siteUse =  ['T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T3_US_Omaha', 'T2_US_Purdue', 'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin']
              if not options.siteCust:
                 print 'ERROR: A custodial site must be specified'
                 sys.exit(0)
              siteCust = options.siteCust

           team=options.team

           inputDataset = reqMgrClient.getInputDataSet(url, workflow)

           # Check status of input dataset
           inputDatasetStatus = getDatasetStatus(inputDataset)
           if inputDatasetStatus != 'VALID' and inputDatasetStatus != 'PRODUCTION':
              print 'ERROR: Input dataset is not PRODUCTION or VALID, value is',inputDatasetStatus
              sys.exit(0)

           if '-ext' in inputDataset and not options.extension:
              print 'WARNING: Input dataset is an extension and extension option is not specified'

           if not siteUse or siteUse == 'None':
              # Determine site where workflow should be run
              count=0
              for site in sites:
                 if site in workflow:
                    count=count+1
                    siteUse = site

              # Find custodial location of input dataset if workflow name contains no T1 site or multiple T1 sites
              if count==0 or count>1:
                 siteUse = findCustodialLocation(url, inputDataset)
                 if siteUse == 'None':
                    print 'ERROR: No custodial site found'
                    sys.exit(0)
                 siteUse = siteUse[:-4]
     
           # Set the custodial location if necessary
           if not options.site or options.site != 'T2_US':
              if not options.siteCust:
                 siteCust = siteUse
              else:
                 siteCust = options.siteCust
           if options.site == 'HLT':
              siteUse = ['T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CERN']

           # Check if input dataset subscribed to disk endpoint
           if 'T2_CH_CERN' not in siteUse:
              siteSE = siteUse + '_Disk'
           [subscribedOurSite, subscribedOtherSite] = checkAcceptedSubscriptionRequest(url, inputDataset, siteSE)

           if not subscribedOurSite and not options.xrootd and not ignore:
              print 'ERROR: input dataset not subscribed/approved to required Disk endpoint and xrootd option not enabled'
              sys.exit(0)
           if options.xrootd and not subscribedOtherSite and not ignore:
              print 'ERROR: input dataset not subscribed/approved to any Disk endpoint'
              sys.exit(0)

           # Check if pileup dataset subscribed to disk endpoint
           pileupDataset = getPileupDataset(url, workflow)
           if pileupDataset != 'None':
              [subscribedOurSite, subscribedOtherSite] = checkAcceptedSubscriptionRequest(url, pileupDataset, siteSE)
              if not subscribedOurSite:
                 print 'ERROR: pileup dataset (',pileupDataset,') not subscribed/approved to required Disk endpoint'
                 sys.exit(0)            
         
           # Decide which team to use if not already defined
           # - currently we only use reproc_lowprio for all workflows
           if not team:
              team = 'reproc_lowprio'

           # Get LFN base from input dataset
           lfn = getLFNbase(url, inputDataset)

           # ProcessingVersion
           if not options.inprocversion:
              procversion = getDatasetVersion(url, workflow)
           else:
              procversion = options.inprocversion

	   # Seset maxRSS to default, so it can't reuse the custom value from a previous workflow
	   maxRSS = maxRSSdefault
           if ('HiFall11' in workflow or 'HiFall13DR53X' in workflow) and 'IN2P3' in siteUse:
              maxRSS = 4000000

           # Set max number of merge events
           maxmergeevents = 50000
           if 'Fall11R1' in workflow:
              maxmergeevents = 6000
           if 'DR61SLHCx' in workflow:
              maxmergeevents = 5000

           if not lfn:
              print 'ERROR: lfn is not defined'
              sys.exit(0)

           # Get era & processing string
           era = getEra(url, workflow)
           procstring = getProcString(url, workflow)

           if siteUse not in sites and options.site != 'T2_US' and siteUse != ['T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CERN'] and not ignoresiterestrictions:
              print 'ERROR: invalid site'
              #sys.exit(0)

           if options.execute:
              if restrict == 'None' or restrict == siteUse:
	          assignRequest(url, workflow, team, siteUse, era, procversion, procstring, activity, lfn, maxmergeevents, maxRSS, maxVSize, useX, siteCust)
                  if (newpriority !=0 ):
                     changePriorityWorkflow(url,workflow,newpriority)
                     print "Priority reset to %i" % newpriority
              else:
                     print 'Skipping workflow ',workflow
           else:
              if restrict == 'None' or restrict == siteUse:
                 print 'Would assign ',workflow,' with ','Acquisition Era:',era,'ProcessingString:',procstring,'ProcessingVersion:',procversion,'lfn:',lfn,'Site(s):',siteUse,'Custodial Site:',siteCust,'team:',team
                 if (newpriority !=0 ):
                    print "Would reset priority to %i" % newpriority
              else:
                 print 'Would skip workflow ',workflow

	sys.exit(0)

if __name__ == "__main__":
	main()
