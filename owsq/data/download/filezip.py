#!/usr/bin/env python

from zipfile import ZipFile
from urllib2 import urlopen
import socket
import os,json


def notify_email(toaddress, subject, bodytext):
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(bodytext)
    msg['Subject'] = subject
    msg['From'] = "DoNotReply@ou.edu"
    msg['To'] = toaddress
    s = smtplib.SMTP('smtp.ou.edu')
    s.sendmail("DoNotReply@ou.edu", [toaddress], msg.as_string())
    return "Notification sent"

def zipurls(files,out_path):
    ''' Takes a list of URL locations, fetches files and returns a zipfile ''' 
    if type(files) is list:
        OutputFile = open(out_path,'w')
        zipFile = ZipFile(OutputFile, 'w', allowZip64=True)
        for filename in files:
            zipFile.writestr(os.path.basename(filename), urlopen(filename).read())
        zipFile.close()
        OutputFile.seek(0)
        return out_path
    else:
        return "ERROR: expected a list of URLs"

def makezip(urls, outname, outpath, overwrite=False):
    ''' Make a zipfile from a set of urls '''
    full_path = os.path.join(outpath,outname)
    #try:    
    if not os.path.exists(outname) and overwrite:
        os.remove(full_path)
    zipurls(urls,full_path)
    if os.path.exists(full_path):
        return 'http://%s/%s/%s' % ( socket.gethostname(),'request', outname) 
    else:
        return 'Couldn\'t write zipfile'
    #except:
    #    return "Error writing zip file"
def rdb2json(url):
    temp='#'
    head=''
    f1=urllib2.urlopen(url)
    while (temp[0]=="#"):
        temp=f1.readline()
        if temp[0]!='#':
            head = temp.strip('\r\n').split('\t')
    f1.readline()
    data=[]
    for row in f1:
        temp=row.strip('\r\n').split('\t')
        data.append(dict(zip(head,temp)))
    return json.dumps(data)
    
    


    

