# Step 2 - 

## Generate datamodel for data loaded via DMS (CdC)

### Check the output of CDC process
Go to s3: https://s3.console.aws.amazon.com/s3/home?region=eu-west-1  

There's a folder for each table:  
<target S3>/<selected output folder>/ADMIN/CONTRATTI  

in each folder there are multiple csv files related to Full LOAD (LOADnnnnnnnn.csv) and incremental data (timestamp.csv)
Let's add a few more rows to one table by connecting to Workspace and Sql Developer and:
```bash
insert into contratti (select key_contratti+100000000), nome_commerciale, vettore, key_punti_di_fornitura, 
    data_attivazione_fornitura, data_cessazione_fornitura, anno_prima_attivazione_fornitura, canale_di_vendita,
    codice_contratto,key_soggetti from contratti where rownum < 100000);

commit;

exec rdsadmin.rdsadmin_util.switch_logfile;
```

Now we can check in DMS if those records has been transferred to the datalake:  https://eu-west-1.console.aws.amazon.com/dms/v2/home?region=eu-west-1#tasks

### Create a Datalake and a database to store tables information

Go to Lakeformation: https://eu-west-1.console.aws.amazon.com/lakeformation/home?region=eu-west-1#databases  

Add Administrators to Datalake including current IAMUser or Role used to access to the console

Create a Database to store cdc data: 
```bash
Name: cdc
Description: database containing cdc output
```

Create a Role to be used with Lakeformation:
https://console.aws.amazon.com/iam/home?region=eu-west-1#/roles  

Create a Role with the following parameters
```bash
Name: LakeFormationWorkflowRole
Service: Glue
Policcies: AWSGlueServiceRole 
Inline Policy:
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::<s3bucket>/*",
                "arn:aws:s3:::<s3bucket>"
                
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": [
                "arn:aws:iam::<AccountID>:role/LakeFormationWorkflowRole"
            ]
        }
    ]
    }
```


### Create a Glue Crawler to analyze Oracle database content and generate datamodel

Add a new crawler: https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#addCrawler:  
```bash
Name: grtCDCFormat
Description: database containing cdc output
Datastore: S3
Include Path: s3://<backet>/dms_target/ADMIN/
Role: LakeFormationWorkflowRole
Frequency: Run on Demand
Database: cdc

```
Now we can execute the crawler and once it is finished we can log onto Athena to have a look at the content of those tables.

Here after some query examples:
```bash
WITH deduplica_soggetti as (
select * from (
SELECT 
rank() OVER (partition by nome,cognome order by key_soggetti desc) as rnk,
a.* 
FROM "cdc"."soggetti" a) deduplica_soggetti 
where deduplica_soggetti.rnk=1)
select * from deduplica_soggetti limit 10;
```  
And also more complex queries:
```bash
with deduplica_soggetti as (
select * from (
SELECT 
rank() OVER (partition by nome,cognome order by key_soggetti desc) as rnk,
a.* 
FROM "cdc"."soggetti" a) deduplica_soggetti 
where deduplica_soggetti.rnk=1),
contratti_attivi as (
select * from "cdc"."contratti" where
  data_attivazione_fornitura <=date_format(current_timestamp,'%Y-%m-%d %k:%i:%s') and
  data_cessazione_fornitura >=date_format(current_timestamp,'%Y-%m-%d %k:%i:%s')
  )
select * from deduplica_soggetti a, contratti_attivi b
where 
a.key_soggetti=b.key_soggetti limit 10;    

```

### Include external database tables into the catalog
Create another Database to store oracle tables references:  
https://eu-west-1.console.aws.amazon.com/lakeformation/home?region=eu-west-1#create-database  

```bash
Name: oracle_source
Description: database containing oracle source tables
```

Create a Glue Connection to allow Glue to access Oracle Database:  
https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#addEditConnection:
```bash
Connection Name: orcl
Connection Type: 
Jdbc URL: jdbc:oracle:thin://@<RDS ENDPOINT>:1521/ORCL
Username: admin
Pwd: XXX
VPC: Select the vpc that includes RDS
Security Group: Select security group that shall be used by glue

```


Add a new crawler: https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#addCrawler:  
```bash
Name: getOracleTables
Description: database containing cdc output
Datastore: JDBC
Connection: orcl
Inlcude Path: ORCL/%
Role: LakeFormationWorkflowRole
Frequency: Run on Demand
Database: oracle_source

```

### Create a Glue Developer Endpoint (0.44 USD/hour per DPU about 31 USD/day) (circa 10 min)

```bash
Name: analytics-developer-endpoint
Role: LakeFormationWorkflowRole
DPU: 3
Choose a VPC, subnet, and security groups:
    Select VPC:
    Subnet: Chose a subnet
    Select Security group : the same used for Oracle connection
SSH Key: Skip

```

And a Notebook linked to the endpoint ($0.0638 USD/hour) (circa 6 minuti)


```bash
Notebook Name: analytics-poc
Create a IAM Role: LakeFormationWorkflowRole
VPC: select VPC
Subnet: pick one
Security Group: select glue security group
```

Once the notebook is available you can open it git clone the repo within the notebook.
https://eu-west-1.console.aws.amazon.com/sagemaker/home?region=eu-west-1#/notebook-instances


## Use Lakeformation Blueprint to load oracle data into the datalake

Create another Database to store oracle tables references:  
https://eu-west-1.console.aws.amazon.com/lakeformation/home?region=eu-west-1#create-database  

```bash
Name: datalake
Description: database containing datalake objects
```

Create a Blueprint:  
https://eu-west-1.console.aws.amazon.com/lakeformation/home?region=eu-west-1#create-workflow

```bash
Blueprint Type: database snapshot
Database connection: orcl
Source data path: ORCL/%

Import Target: datalake

Data Format: Parquet

Workflow name: upload-oracle-database
IAM Role: LakeformationWorkflowRole
TablePrefix: l
```

Execute the workflow and monitor the status.


Check performance difference with Athena leveraging Parquet files instead of csv.


### Build the transform job and use AWS Glue to run the transformation
Continue with Jupyther Notebook to build and test the transform module.  

Deploy the transform module in AWS Glue and run the transform Job.
https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#addJob:
```bash
Name: transformationJob
Role: LakeformationWorkflowRole
This job runs: A new Script to be authored by you
Script file name: transformationJob

Open Security Configuration:
    Worker Type: G.1X
    Number of Workers: 4
    Catalog Options
    
```
Next and Save job and edit script.
Copy Paste the script content by using Trasformation.ipynb as guideline

Execute the transformation Job.

Add a new crawler to automatically upload the catalog with the new transform output
```bash
Name: grtTransformedFormat
Description: load formats related to transformed tables
Datastore: S3
Include Path: s3://<bucket_name>/transformed
Role: LakeFormationWorkflowRole
Frequency: Run on Demand
Database: datalake

```


## Accessing data using Quicksight leveraging Athena
Enable Quicksight Enterprise (so we can also access Oracle database directly)

Enable Athena in security panel
Enable access to S3 buckets (data and where athena saves the queries)
Grant access to database and tables to IAMAllowedPrincipals role.



## Using EMR to access catalog tables
Set-up an EMR cluster:  
https://eu-west-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-1#quick-create:  

Change the following options and leave the other as they are:  
```bash
Go to advanced options
Select at least: Spark, Livy
AWS Datacatalog settings: Enable all
Cluster Name: My Cluster

```  
Allow EMR to access Datalake resources:  
https://eu-west-1.console.aws.amazon.com/lakeformation/home?region=eu-west-1#catalog-settings  
Allow full access to datalake inserting EMR_EC2_DefaultRole among datalake administrators  

Start-up a EMR Notebook:  
https://eu-west-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-1#create-notebook:  

```bash
Notebook Name: MyNotebook
Chose the EMR cluster we would like to associate to the notebook

```  
Leave other options as they are.  

Once the notebook is ready we can use EMR_Example.ipynb as guideline.  


