#!/usr/bin/env python -w
import sys,re,time,os
import json
import optparse
import httplib
import datetime
import shutil

url='cmsweb.cern.ch'

def getInputDataSet(url, workflow):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
   r2=conn.getresponse()
   request = json.loads(r2.read())
   inputDataSets=request['InputDataset']
   if len(inputDataSets)<1:
      print "ERROR: No InputDataSet for workflow"
   else:
      return inputDataSets

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
      inputDataset = getInputDataSet(url, workflow)
      print workflow, inputDataset

   sys.exit(0)

if __name__ == "__main__":
        main()
