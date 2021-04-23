import json
import subprocess
import time
def main():
    result = subprocess.Popen(['ceph', 'osd', 'tree', '-f', 'json-pretty'], stdout=subprocess.PIPE) #run shell command and receive output into shell buffer
    resultstdout = result.communicate()[0]
    dictOSDTree=json.loads(resultstdout.decode('utf-8')) #take json output from shell buffer and put into this dictionary
    m=0 #variable to count the number of hosts found in the json output
    hostname=[] # list to store hostnames
    for i in range(len(dictOSDTree['nodes'])):
        if dictOSDTree['nodes'][i]['type'] == "host":
            hostname.append(dictOSDTree['nodes'][i]['name']) #appends to list "hostname" the name of any node with "type" of "host"
            print(m, hostname[m])
            m=m+1

    hostnum=(int(input("type host number to vacate. "))) # host selection menu and minor validation
    if int(hostnum) > m:
        print ("error, selection out of bounds")
    else:
        EvacOSD(hostname[hostnum], dictOSDTree)
   
def MonitorRemainingPG(osdid): # this sub monitors the number of PGs remaining on a disk and stays in loop until its completely vacated
    print("osd", osdid, "has been reweighted")
    pgremain = 100 # variable declaration and non zero value so the loop doesnt exit immediately
    while pgremain > 0:
        result = subprocess.Popen(['ceph', 'osd', 'df', '-f', 'json-pretty'], stdout=subprocess.PIPE)
        resultstdout = result.communicate()[0]
        dictPGOutput=json.loads(resultstdout.decode('utf-8'))
        for i in range(len(dictPGOutput['nodes'])):
            if dictPGOutput['nodes'][i]['id']==osdid: #check we are reading the info for the correct OSD
                pgremain=dictPGOutput['nodes'][i]['pgs'] #update pgremain to the current number of PGs on disk
                print("pgs remaining on osd", osdid, "-", pgremain)
                time.sleep(60)
    print("done with osd", osdid)

def EvacOSD(systemname, dictOSDTree): #sub that performs the evacuation procedure
        print("running evac procedure on", systemname)
        osdlist=[] #list to store osd numbers
        for i in range(len(dictOSDTree['nodes'])):
            if dictOSDTree['nodes'][i]['name']==systemname: #look for systemname
                for g in range(len(dictOSDTree['nodes'][i]['children'])): #loop to enumerate osds on the system
                        osdlist.append(dictOSDTree['nodes'][i]['children'][g])
        print("the following OSDs will be reweighted to 0.0", osdlist, "press enter to proceed")
        input() #keyboard confirmation input
        for i in range(len(osdlist)):
            osdstr="osd." + str(osdlist[i]) #concatenate the string "osd." and the osd number
            subprocess.Popen(['ceph', 'osd', 'crush', 'reweight', osdstr, '0.0']) #osd reweight command, confirmation is printed in the shell
            MonitorRemainingPG(osdlist[i]) #call the PG counting sub

if __name__ == "__main__":

    main()