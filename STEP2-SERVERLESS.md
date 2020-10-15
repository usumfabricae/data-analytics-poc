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


### Create a Glue Crawler to analyze CDC content and generate datamodel

Add a new crawler: https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#addCrawler:  
```bash
Name: crawl-cdc
Description: database containing cdc output
Datastore: JDBC
Connection: orcl
Inlcude Path: ORCL/%
Role: LakeFormationWorkflowRole
Frequency: Run on Demand
Database: cdc

```
Now we can execute the crawler and once it is finished we can log onto Athena to have a look at the content of those tables.





