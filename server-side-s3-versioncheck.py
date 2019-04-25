#!/usr/bin/python3

import os
import time
import boto3
import datetime

#boto.s3 variables
session = boto3.session.Session(profile_name='schema-handler')
s3 = session.resource('s3')
bucketname = 'sql-buck-name'
bucket = s3.Bucket(bucketname)

#local file name variables
schemafile = time.strftime("%y-%m-%d-dump.sql")
dev = 'devcurrent.sql'


#functions to set up check for s3 interfacing
def todays_count(file):
    tday = '<{0} wc -l'.format(file)
    texec = os.popen(tday)
    twc = texec.read()
    return twc

def devcurrent_count(file):
    last = '<{0} wc -l'.format(file)
    lexec = os.popen(last)
    lastwc = lexec.read()
    return lastwc

def filecompare(tcount, lcount):
    if tcount != lcount:
        print(datetime.datetime.now())
        print("S3 and local dump files do not match. Attempting upload of most current schema.")
        return 1
    else:
        return 0

diff = filecompare(todays_count(schemafile), devcurrent_count(dev))

if diff == 1:
    bucket.upload_file(schemafile, dev)
    os.remove(dev)
    os.rename(schemafile, dev)
    print("... Current schema successfully uploaded to S3.\n")
else:
    os.remove(schemafile)

