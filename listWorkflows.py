#!/usr/bin/env python -w
import sys,re,time,os
import json
import optparse
import httplib
import datetime
import shutil

url='cmsweb.cern.ch'

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
                if replica['custodial']=="y":
                        return replica['node']
        return "No Custodial Site found"


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
      
def main():
   args=sys.argv[1:]
   if len(args) == 1:
      state=args[0]
   else:
      state='assignment-approved'

   workflows = getWorkflows(state)

   for workflow in workflows:
      if 'TEST' not in workflow:
         if state == 'assignment-approved':
            inputDataset = getInputDataSet(url, workflow)
            custodialSite = findCustodialLocation(url, inputDataset)
            print workflow, inputDataset, custodialSite
         else:
            print workflow

   sys.exit(0)

if __name__ == "__main__":
        main()
