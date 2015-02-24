#!/usr/bin/env python -w
import sys,re,time,os
import json
import optparse
import httplib
import datetime
import shutil
import reqMgrClient
import setDatasetStatusDBS3
from dbs.apis.dbsClient import DbsApi

dbs3_url = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
url='cmsweb.cern.ch'

def getCampaign(url, workflow):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
        r2=conn.getresponse()
        request = json.loads(r2.read())
        campaign=request['Campaign']
        return campaign

def getDatasetStatus(dataset):
        # initialize API to DBS3
        dbsapi = DbsApi(url=dbs3_url)
        # retrieve dataset summary
        reply = dbsapi.listDatasets(dataset=dataset,dataset_access_type='*',detail=True)
        if len(reply) > 0:
           return reply[0]['dataset_access_type']
        return 'UNKNOWN'

def percentageCompletion(url, workflow, outputDataset, verbose=False):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    """
  
    inputEvents = reqMgrClient.getInputEvents(url, workflow)
    outputEvents = reqMgrClient.getOutputEvents(url, workflow, outputDataset)
    if inputEvents==0 or not inputEvents:
            return 0
    if not outputEvents:
        outputEvents = 0
    if verbose:
        print outputDataset
        print "Input events:", int(inputEvents)
        print "Output events:", int(outputEvents)
    
    percentage=100.0*outputEvents/float(inputEvents)
    return percentage

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
         if 'TEST' not in item['key'][0]:
            workflows.append(item['key'][0])

   return workflows
      
def main():
   # only workflows from these campaigns
   valids = ['Spring14dr', 'Fall13dr', 'Summer12DR53X', 'pAWinter13DR53X', 'Cosmic70DR', 'Phys14DR', 'Fall11R1', 'Fall11R2' ,'Fall14DR', 'Fall14DR73', 'Summer11LegDR','Spring14miniaod','TP2023SHCALDR', '2019GEMUpg14DR']
   #valids = ['Phys14DR']
   #valids = ['Spring14miniaod']

   # Get list of workflows
   workflows = getWorkflows('closed-out')

   for workflow in workflows:
      print 'Considering:',workflow
      # campaign
      campaign = getCampaign(url, workflow)

      if campaign in valids:
         # retrieve the output datasets
         outputDataSets=reqMgrClient.outputdatasetsWorkflow(url, workflow)

         statusAll = 0
         goodAll = 0
         typeAll = 0

         for dataset in outputDataSets:
            perc = percentageCompletion(url, workflow, dataset, verbose=False)
            status = getDatasetStatus(dataset)
            if 'test' in dataset or 'TEST' in dataset or 'None' in dataset:
               typeAll = typeAll + 1
            if status != 'PRODUCTION':
               statusAll = statusAll + 1
            if perc < 95.0 or perc > 100.0:
               if 'DQMIO' not in dataset and 'ALCA' not in dataset:
                  goodAll = goodAll + 1
         if statusAll == 0 and goodAll == 0 and typeAll == 0:
            print 'Announce: ',workflow
            for dataset in outputDataSets:
               print 'DATA:',dataset
               #setDatasetStatusDBS3.setStatusDBS3('https://cmsweb.cern.ch/dbs/prod/global/DBSWriter', dataset, 'VALID', '')
               #AnnounceWorkflow(url, workflow)
         if statusAll > 0:
            print 'Not announcing',workflow,'due to VALID output datasets'
         if goodAll > 0:
            print 'Not announcing',workflow,'due to output datasets < 95% or > 100% complete'
         if typeAll > 0:
            print 'Not announcing',workflow,'due to it being a test workflow'
      
   sys.exit(0)

if __name__ == "__main__":
        main()

