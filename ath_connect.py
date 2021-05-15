#!/usr/bin/env python
# coding: utf-8

"""
Author: dusvyat
"""

import os
import pandas as pd
import boto3
from pyathena import connect
from subprocess import call

class Athena_connection():   
    
    def __init__(self,profile_name,region_name,s3_staging_dir,local_dir):
        
        self.profile_name=profile_name
        self.session = boto3.Session(profile_name=self.profile_name)
        self.credentials=self.session.get_credentials()
        self.region_name=region_name
        self.s3_staging_dir=s3_staging_dir
        self.local_dir=local_dir
        
    def authenticate(self):
        conn = connect(#jvm_path='/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/MacOS/libjli.dylib',
            profile_name=self.profile_name,
            aws_access_key_id=self.credentials.access_key,
            aws_secret_access_key=self.credentials.secret_key,
            aws_session_token=self.credentials.token,
            region_name = self.region_name,
            s3_staging_dir=self.s3_staging_dir)
        
        return conn
    
    def large_query(self,sql):
        cur =Athena_connection().authenticate().cursor()
        cur.execute(sql)
        output = cur.query_id
        location = '{s3_staging_dir}{output}.csv'.format(s3_staging_dir=self.s3_staging_dir,output=output)
        local_file = '{local_dir}{output}.csv'.format(local_dir=self.local_dir,output=output)
        call('aws s3 cp {location} {local_file} --profile ophan'.format(location=location,local_file=local_file),shell=True)
        df = pd.read_csv(local_file)
        return df
    
    def create_table(self,sql):
        cur =Athena_connection().authenticate().cursor()
        cur.execute(sql)
    
    def clean_local(self):
        
        call('rm {local_dir}*'.format(local_dir=self.local_dir),shell=True)
        print("all local query CSVs have been deleted from: {local_dir}".format(local_dir=self.local_dir))
        
    def clean_s3(self):
        
        call('aws s3 rm {s3_staging_dir}*'.format(s3_staging_dir=self.s3_staging_dir),shell=True)
        print("all CSVs have been deleted from: {s3_staging_dir}".format(s3_staging_dir=self.s3_staging_dir))
        
if __name__ == "__main__":

    athena_conn = Athena_connection()

    df = pd.read_sql('select * from clean.content_new limit 5',athena_conn.authenticate())

    df = athena_conn.proust("""SELECT * from clean.pageview limit 5""")
