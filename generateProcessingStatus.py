#!/usr/bin/env python -w
import sys,re,time,os
import simplejson as json
import optparse
import httplib
import datetime
import shutil
import timeEstimateWorkflows

url='cmsweb.cern.ch'

def intWithCommas(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)

def getWorkflows(states):
   conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
   r1=conn.request("GET",'/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtype?stale=update_after')
   r2=conn.getresponse()
   data = json.loads(r2.read())
   items = data['rows']

   workflows = []
   for item in items:
      for state in states:
         if state in item['key'] and ('ReDigi' in item['key']):
            workflows.append(item['key'][0])

   return workflows
      
def main():
   args=sys.argv[1:]
   if args[0] == 'running':
      state = ['running-open', 'running-closed']
   elif args[0] == 'acquired':
      state = ['acquired']
   elif args[0] == 'assignment-approved':
      state = ['assignment-approved']
   elif args[0] == 'totals':
      state = ['running-open', 'running-closed', 'acquired', 'assignment-approved']

   workflows = getWorkflows(state)

# generate data

   t1_sites = ['CNAF', 'KIT', 'IN2P3', 'PIC', 'JINR', 'RAL', 'FNAL', 'CERN']
   allowed_campaigns = ['Fall11R1','Fall11R2','Summer12DR53X','Fall13dr','HiWinter13DR53X','pAWinter13DR53X','Summer11dr53X','Summer11LegDR','LowPU2010DR42', 'Spring14dr', 'Spring14premix', 'Spring14premixdr', 'ppSpring2014DRX53', 'HiFall13DR53X', '2019GEMUpg14DR', '2023MuonUpg14DR', '2023TTIUpg14D', 'Upg2023SHCAL14DR', 'Phys14DR', 'Cosmic70DR', 'Fall14DR', 'Fall14DR73', 'TP2023SHCALDR', 'RunIWinter15DR', '2023SHCALUpg14DR']

   priorities = []
   campaigns = []

   eventsPerCampaignPerSite = {}
   timePerCampaignPerSite = {}
   eventsPerCampaignPerSiteTotal = {}
   timePerCampaignPerSiteTotal = {}

   for workflow in workflows:
      print workflow
      [campaign, priority, ssite, eventsRemaining, timeRemaining] = timeEstimateWorkflows.generateAllInfo(url, workflow)
      if campaign != "NA":
         if timeRemaining < 0:
            timeRemaining = 0

         if eventsRemaining < 0:
            eventsRemaining = 0 

         for asite in t1_sites:
            if asite in ssite:
               site = asite
      
               print ' - ',campaign,priority, site, eventsRemaining, timeRemaining

               if campaign in allowed_campaigns and priority > 40000:

                  if priority not in priorities:
                     priorities.append(priority)
                  if campaign not in campaigns:
                     campaigns.append(campaign)

                  if not eventsPerCampaignPerSite.has_key((campaign, site, priority)):
                     eventsPerCampaignPerSite[(campaign, site, priority)] = 0
                  eventsPerCampaignPerSite[(campaign, site, priority)] = eventsPerCampaignPerSite[(campaign, site, priority)] + eventsRemaining

                  if not timePerCampaignPerSite.has_key((campaign, site, priority)):
                     timePerCampaignPerSite[(campaign, site, priority)] = 0
                  timePerCampaignPerSite[(campaign, site, priority)] = timePerCampaignPerSite[(campaign, site, priority)] + timeRemaining

                  if not eventsPerCampaignPerSiteTotal.has_key((campaign, site)):
                     eventsPerCampaignPerSiteTotal[(campaign, site)] = 0
                  eventsPerCampaignPerSiteTotal[(campaign, site)] = eventsPerCampaignPerSiteTotal[(campaign, site)] + eventsRemaining

                  if not timePerCampaignPerSiteTotal.has_key((campaign, site)):
                     timePerCampaignPerSiteTotal[(campaign, site)] = 0
                  timePerCampaignPerSiteTotal[(campaign, site)] = timePerCampaignPerSiteTotal[(campaign, site)] + timeRemaining

# fill gaps

   for site in t1_sites:
      for campaign in campaigns:
         if not eventsPerCampaignPerSiteTotal.has_key((campaign, site)):
            eventsPerCampaignPerSiteTotal[(campaign, site)] = 0
         if not timePerCampaignPerSiteTotal.has_key((campaign, site)):
            timePerCampaignPerSiteTotal[(campaign, site)] = 0
         for priority in priorities:
            if not eventsPerCampaignPerSite.has_key((campaign, site, priority)):
               eventsPerCampaignPerSite[(campaign, site, priority)] = 0
            if not timePerCampaignPerSite.has_key((campaign, site, priority)):
               timePerCampaignPerSite[(campaign, site, priority)] = 0

# generate table

   tmpoutputfile1 = "reprocessingcampaigns_%s.html.tmp" % (args[0]);
   outputfile1 = "reprocessingcampaigns_%s.html" % (args[0]);
   output1 = open(tmpoutputfile1, 'w')

   output1.write('<html><head><title>Processing Campaigns</title>')
   output1.write('<style type="text/css">')
   output1.write('#nicetable')
   output1.write('{')
   output1.write('font-family:"Trebuchet MS", Arial, Helvetica, sans-serif;')
   output1.write('width:100%;')
   output1.write('border-collapse:collapse;')
   output1.write('}')
   output1.write('#nicetable td, #nicetable th')
   output1.write('{')
   output1.write('font-size:1em;')
   output1.write('border:1px solid #000000;')
   output1.write('padding:3px 7px 2px 7px;')
   output1.write('}')
   output1.write('#nicetable th')
   output1.write('{')
   output1.write('font-size:1.0em;')
   output1.write('text-align:left;')
   output1.write('padding-top:5px;')
   output1.write('padding-bottom:4px;')
   output1.write('background-color:#3366FF;')
   output1.write('color:#ffffff;')
   output1.write('}')
   output1.write('#nicetable td[scope=col]')
   output1.write('{')
   output1.write('font-size:1.0em;')
   output1.write('text-align:left;')
   output1.write('padding-top:5px;')
   output1.write('padding-bottom:4px;')
   output1.write('background-color:#3366FF;')
   output1.write('color:#ffffff;')
   output1.write('}')
   output1.write('#nicetable tr.alt td')
   output1.write('{')
   output1.write('color:#000000;')
   output1.write('background-color:#EAF2D3;')
   output1.write('}')
   output1.write('</style>')
   output1.write('<meta http-equiv="refresh" content="600" >')
   output1.write('</head><body style=\'font-family:sans-serif;\'>')
   s = 'State: <b>'+args[0]+'</b> (<a href="http://www.gridpp.rl.ac.uk/cms/reprocessingcampaigns_running.html">running</a>, <a href="http://www.gridpp.rl.ac.uk/cms/reprocessingcampaigns_acquired.html">acquired</a>, <a href="http://www.gridpp.rl.ac.uk/cms/reprocessingcampaigns_assignment-approved.html">assignment-approved</a>, <a href="http://www.gridpp.rl.ac.uk/cms/reprocessingcampaigns_totals.html">totals</a>)<br/><br/>'
   output1.write(s)

   output1.write('<table id=nicetable border=1 style=\'border-width:1px;border-spacing:0px;border-collapse:collapse;font-size:14px;width:45%;\'>')

   bigtotal = 0

   for site in t1_sites:
      s="<tr><td colspan=\"7\"; scope=\"col\">%s</td></tr>" % (site)
      output1.write(s)
      daysSiteTotal = 0
      for campaign in campaigns:
         daysSiteTotal = daysSiteTotal + timePerCampaignPerSiteTotal[(campaign, site)]
      j=0
      for campaign in campaigns:
         i=0
         eventsTotal = eventsPerCampaignPerSiteTotal[(campaign, site)]
         daysTotal = timePerCampaignPerSiteTotal[(campaign, site)]
         eventsTotalSite = eventsPerCampaignPerSiteTotal[(campaign, site)]
         daysTotalSite = timePerCampaignPerSiteTotal[(campaign, site)]
         for priority in priorities:
            events = eventsPerCampaignPerSite[(campaign, site, priority)]
            bigtotal = bigtotal + events
            days = timePerCampaignPerSite[(campaign, site, priority)]
            cevents = intWithCommas(int(events))
            cpriority = intWithCommas(int(priority))
            if events > 0:
               if i==0 and j==0:
                  s="<tr><td></td><td>%s</td><td>%s</td><td>%s</td><td>%6.2f</td><td>%6.2f</td><td>%6.2f</td></tr>" % (campaign, cpriority, cevents, days, daysTotal, daysSiteTotal)
               elif i==0:
                  s="<tr><td></td><td>%s</td><td>%s</td><td>%s</td><td>%6.2f</td><td>%6.2f</td><td></td></tr>" % (campaign, cpriority, cevents, days, daysTotal)
               else:
                  s="<tr><td></td><td></td><td>%s</td><td>%s</td><td>%6.2f</td><td></td><td></td></tr>" % (cpriority, cevents, days)
               output1.write(s)
               i=i+1
               j=j+1

   output1.write("</table>")
   output1.write("<br/><br/>")

   output1.write("Total events ="+str(bigtotal)+"<br/><br/>")

   #s="Slots per site: "
   #for site in t1_sites:
   #   slots = slotsPerSite[site]
   #   s=s+"%s: %d, " % (site, int(slots))
   #   output1.write(s)

   #output1.write("<br/>")

   #s="Time per event: "
   #for campaign in campaigns:
   #   ctime = timesPerEvent[campaign]
   #   s=s+"%s: %5.1f, " % (campaign, ctime)
   #   output1.write(s)

#   if args[0] == 'assignment-approved':
#      output1.write("<br/>")
#      output1.write('<a href="http://www.gridpp.rl.ac.uk/cms/prestaging.html">Prestaging status</a> (workflows with << 100% cannot be assigned)')
#      output1.write("<br/><br/>")
#   else:
#      output1.write("<br/><br/>")

   output1.write("<br/><br/>")
   output1.write("<small><i>Updated: %s [UTC]</i></small>" % str(datetime.datetime.utcnow()))
   output1.write('</body></html>')
   output1.close

   shutil.move(tmpoutputfile1,outputfile1)

   sys.exit(0)

if __name__ == "__main__":
        main()
