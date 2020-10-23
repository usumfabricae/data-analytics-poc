# Step 3 - 


## Overview  
The components we are goint to explore in this session are:  
![SERVERLESS](./pictures/ModernDWH.PNG)  


## Load Json Files
### Convert json file from index to record formats
Create a glue job:
```bash
Name: json-dict-to-json-records
Role: LakeformationWorkflowRole
This job runs: A new Script to be authored by you
Script file name: jsonDict-to-jsonRecords

Open Security Configuration:
    Worker Type: G.1X
    Number of Workers: 2
    Catalog Options
    
```

With the current script:  
```bash
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

import boto3
import pandas as pd

## @params: [JOB_NAME]
#RIMUOVERE COMMENTO PER USARE IN GLUE
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
#FINE RIMUOVERE COMMENTO PER USARE IN GLUE

bucketname = "fede-analytics-694275606777"
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucketname)
source = "datalake/consensi"
target="datalake/consensi_json"

for obj in my_bucket.objects.filter(Prefix=source):
    source_filename = (obj.key).split('/')[-1]
    body = obj.get()['Body'].read()
    dataframe=pd.read_json(body,orient='index').reset_index()
    result=dataframe.to_json(orient="records",lines=True)
    output=s3.Object(bucketname, '{}/{}'.format(target,source_filename))
    output.put(Body=result)
    
job.commit()    
```      

### Configure Glue Crawler to parse Json files and update catalog  
```bash
Name: grtTransformedFormat
Description: load formats related to transformed tables
Datastore: S3
Include Path: s3://<bucket_name>/datalake/consensi_json
Role: LakeFormationWorkflowRole
Frequency: Run on Demand
Database: datalake
TablePrefix:j
```    


## Configure redshift

Crate a custom role for redshift with following inline policy to allow access to datalake
RoleName: 

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "glue:GetTable",
                "glue:GetTables",
                "glue:SearchTables",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        }
    ]
}

Crate a redshift cluster:

```bash
create external schema if not exists datalake from DATA CATALOG database 'datalake' iam_role 'arn:aws:iam::<account-id>:role/<role-name>' region '<region>';
```  

Test some queries
```bash
select count(*) from datalake.customer_view_churn_analisys;

create table  local_customer_view_churn_analisys as select * from datalake.customer_view_churn_analisys;

select count(*) from local_customer_view_churn_analisys a , datalake.customer_view_churn_analisys b 
where
a.key_soggetti=b.key_soggetti;

## Accessing data using Quicksight leveraging Athena
Enable Quicksight Enterprise (so we can also access Oracle database directly)

Enable Athena in security panel
Enable access to S3 buckets (data and where athena saves the queries)
Grant access to database and tables to IAMAllowedPrincipals role.

