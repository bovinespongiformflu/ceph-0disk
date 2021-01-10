import json
import subprocess
import time
import os

def MonitorRemainingPG(osdid):
    print("osd", osdid, "has been reweighted")
    pgremain = 100
    while pgremain > 0:
        result = subprocess.Popen(['ceph', 'osd', 'df', '-f', 'json-pretty'], stdout=subprocess.PIPE)
        resultstdout = result.communicate()[0]
        dictPGOutput=json.loads(resultstdout)
        for i in range(len(dictPGOutput['nodes'])):
            if dictPGOutput['nodes'][i]['id']==osdid:
                pgremain=dictPGOutput['nodes'][i]['pgs']
                print("pgs remaining on osd", osdid, "-", pgremain)
                time.sleep(10)
    print("done with osd", osdid)

def EvacOSD(systemname): #sub that performs the evacuation procedure
        print("running evac procedure on", systemname)
        osdlist=[]
        for i in range(len(dictOSDTree['nodes'])):
            if dictOSDTree['nodes'][i]['name']==systemname:
                for g in range(len(dictOSDTree['nodes'][i]['children'])):
                        osdlist.append(dictOSDTree['nodes'][i]['children'][g])
        print("the following OSDs will be reweighted to 0.0", osdlist, "press enter to proceed")
        input()
        for i in range(len(osdlist)):
            osdstr="osd." + str(osdlist[i])
            subprocess.Popen(['ceph', 'osd', 'crush', 'reweight', osdstr, '0.0'])
            MonitorRemainingPG(osdlist[i])

result = subprocess.Popen(['ceph', 'osd', 'tree', '-f', 'json-pretty'], stdout=subprocess.PIPE)
resultstdout = result.communicate()[0]
dictOSDTree=json.loads(resultstdout) #run ceph osd tree to get current host and disk info store in this dictionary
m=0
hostname=[]
for i in range(len(dictOSDTree['nodes'])):
    if dictOSDTree['nodes'][i]['type'] == "host":
      hostname.append(dictOSDTree['nodes'][i]['name'])
      print(m, hostname[m])
      m=m+1

hostnum=int(input("type host number to vacate. "))
if hostnum > m:
        print("error, selection out of bounds ")
else:
        EvacOSD(hostname[hostnum])
