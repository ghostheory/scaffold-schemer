#!/usr/bin/python3 

import os
import sys
import hashlib
import time
import datetime
import boto3
import subprocess
from zipfile import ZipFile

path = ***current path***
log = open(path'/sretrieve.log','a+')

proddb = 'prod_schema'
localdb = 'current_local_schema'
dev_database = 'devdb'

patchfile = time.strftime("{0}.%Y%m%d.patch.sql".format(localdb))
revertfile = time.strftime("{0}.%Y%m%d.revert.sql".format(localdb))
sql_file = 'devcurrent.sql'

session = boto3.session.Session(profile_name='schema-handler')
s3 = session.resource('s3')
bucket = s3.Bucket('sql-buck-name')
client = session.client('s3')
headerpull = client.head_object(Bucket='sql-bucket-name', Key=sql_file)


def hash_reprise():

    print('\n\ncomparing the checksum hashes of the S3 and local sql dumps:')
    print('... S3 and local hashes are different')
    print('... removing local file')
    os.remove(sql_file)
    print('... downloading S3 object')
    bucket.download_file(sql_file, sql_file)

    relocalhash = hashlib.md5(open(sql_file, 'rb').read()).hexdigest()
    bfhead = headerpull['ETag']
    rebuckethash = bfhead.strip('\"')

    if relocalhash != rebuckethash:
        print('!!! after trying to update the local {0} file, we are still seeing md5 mismatching with S3. Please investigate issue further. Because of this the schema update has failed.'.format(sql_file))
        return 1
    else:
        print('... hashes check out. The local {0} file now matches S3\'s'.format(sql_file))
        return 0


def hash_comparison():

    localhash = hashlib.md5(open(sql_file, 'rb').read()).hexdigest()
    bfh = headerpull['ETag']
    buckethash = bfh.strip('\"')
 
    if localhash != buckethash:
        return 0
    else:
        return 1


def apply_sql_to_db(dbname, srcsql):
    print('\n\nattempting to import the {0} file into {1} database:'.format(srcsql, dbname))
    
    src_cmd = 'mysql --user=root -proot --force -D {0} < {1}'.format(dbname, srcsql)
    source = subprocess.run(src_cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if source.returncode == 0:
        print("... importing the {0} file into {1} database succeeded".format(srcsql, dbname))
        return 0
    else:
        print("... importing the {0} file into {1} database failed".format(srcsql, dbname))
        return 1


def create_patch_file(local, production):
    print('\n\nattempting to create the patch file from the two template databases using Schemasync:')
    
    ptch_cmd = '/usr/local/bin/schemasync mysql://root:root@localhost:3306/{0} mysql://root:root@localhost:3306/{1}'.format(production, local)
    patch = subprocess.run(ptch_cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if patch.returncode == 0:
        print('... patch file {0} successfully created'.format(patchfile))
        return 0
    else:
        print('... Schemasync failed to successfully create patch file')
        return 1


def apply_patch_file():
    if create_patch_file(localdb, proddb) == 0:
        print('\n\nupdating patch file with the correct database name:')
        print('... reading patch file into memory')
        with open(patchfile, 'r') as file: 
            filedata = file.read()

        print('... replacing database name in memory')
        filedata = filedata.replace(localdb, dev_database)
        
        print('... writing over original patch file with updates from memory\n')
        with open(patchfile, 'w') as file:
            file.write(filedata)
        
        print('**** recalling import function to apply the patch file to {0} database ...'.format(dev_database))
        apply_sql_to_db(dev_database, patchfile)            

        print('\n**** recalling import function to apply the {0} file to {1} database ...'.format(sql_file, localdb))
        apply_sql_to_db(localdb, sql_file)            


def zip_patch_revert(patch, revert):

    print('\n\nzipping {0} and {1}:'.format(patch, revert))
    f_patchrevert = [ patch, revert ]
    zipf = time.strftime("revert-and-patch.%Y%m%d-%H%M.zip")

    with ZipFile(zipf, 'w') as zip:
        for file in f_patchrevert:
            zip.write(file)

    print('... zip file {0} successfully created'.format(zipf))
    print('... removing {0} and {1}:'.format(patch, revert))
    os.remove(patch)
    os.remove(revert)



# Start of implementing runtime logic


if hash_comparison() == 0:
    print("\n")
    print(datetime.datetime.now())
    if hash_reprise() == 0:
        if apply_sql_to_db(proddb, sql_file) == 0:
            apply_patch_file()
            zip_patch_revert(patchfile, revertfile)
            print("\n")
            print(datetime.datetime.now())
            print('\n\n ### --- ### --- ### \n\n')

# https://github.com/mmatuson/SchemaSync
