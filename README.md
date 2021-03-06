# Fire Incidents Analysis

## About Fire Incidents Dataset

Fire Incidents includes a summary of each (non-medical) incident to which the SF Fire Department responded. Each incident record includes the call number, incident number, address, number and type of each unit responding, call type (as determined by dispatch), prime situation (field observation), actions taken, and property loss.

<img src="images/photo.jpeg" width="700">

The dataset url is:
<a href="https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric"> Link </a>


##  Data Arquitecture

The Data team, needs dimensional tables based on the information provided from a .csv file which is updated daily.
The solution proposed, is developed in AWS, using their different servieces.<br>
The arquitecture developed is the following:

<img src="images/arch.jpeg" width="700">


There are differents stages in the datalake: <br>

<ul>
   <li> Raw: Where the .csv file is uploaded everyday. It has the raw data coming from differents sources.
   <li> Refined: Where the information is clean and partitioned by year, month, day. Stored in parquet format
   <li> Redshift: where coexists 3 schemas. prev, stg and dwh
</ul>


The raw data will be uploaded to the raw bucket named: fire-incidents-raw-dev.<br>
A glue job, named fire-incidents-raw-refined-dev will read the .csv using PySpark, and write it partitioned by year,month,day in the bucket named 	fire-incidents-refined-dev with parquet format.<br>
The data in raw and refined stages, will be crawled and available in Athena, where Data Quality processes will be able to analyze and compare the data, in order not to miss records during the process.<br>
Once in refined, the data will be loaded to Redshift, using a "prev" schema, where the tables: fire_incidents, district and battalion will be written. <br>
The table battalion will have the fields: ['battalion', 'station_area'] + id_battalion (generated with md5 to identify an unique record).<br>
The table district will have the fields: ['neighborhood_district', 'city', 'zipcode'] + id_district (generated with md5 to identify an unique record)<br>
The table fire_incidents will have all the fields plus id_district and id_battalion. This will be usefull to obtain the SK from the datawarehouse dimensions when creating a fact table.
The prev tables are temporal tables, which will be truncated and written to the schema "stg" using upserts/merge methods. The tables in "stg" will be updated daily and will have all the history. While the schema "prev" will have the data of the day (the incremental load). The tables in "stg" will have the same structure as in "prev". <br>
Once in "stg", another glue job will create the dimensions and the fact table in "dwh". Setting the transformation outside Redshift (using Glue) will help us not to overload the datawarehouse, and avoid us of using stored procedures. 
This Glue Job, will create the dimensions: dim_district, dim_battalion, copying the tables from "stg" to "dwh" and adding them the SK (autoincremental), which are from the datawarehouse. 
Once the dims are created in "dwh", the job will create a fact table named: fact_fire_incidents, consuming fire_incidents from "stg" and joining it to the dimension to obtain the sk_district, and sk_battalion.<br>
<br>The dimension dim_date, will be a once time triggered process that will load like a calendar table in the "dwh" schema. It is prepared to run only one time, with the necesary sequence of the dates.
<br>
Once finished the proccess, the data will be available in "dwh" to be consumed. 
<br>The whole proccess could be orchestrated via Step Functions. <br>
All the resources will be deployed via SAM Cli and Cloud Formation, using a template.yaml file, where the resources are defined. (Role,Policy,GlueJobs,StepFunction,Databases,Crawlers)



# Fire Incidents Library

This library contains all the common code used by the Fire Incidents team.

## How to contribute

### Version control

Your commits should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) with these [types](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#type).

- feat
- fix
- ci
- docs
- chore
- tests
- perf
- refactor

You should branch from **dev**, and your branch name should follow this rule **< type >/< task_description >** i.e. **feat/frontend**

1. Clone the Repository
   ```bash
   git clone https://github.com/tovarfranco/fire-incidents-case-study.git
   ```
   > Note: You can obtain the url from Code.
2. Check the branch you are positioned:
   ```bash
   git status
   ```
   > Note: You will be placed in branch Main. You should start from Dev
3. Go to dev:
   ```bash
   git checkout dev
   ```
   > Note: You will be placed in branch Dev, where you'll start from.
4. For a new feature/bug/fix/refactor, you'll create a branch:
   ```bash
   git checkout -b <type>/<branch_name>
   ```
   > Note: Replace <type> and <branch_name> for your actual information. i.e: feat/frontend
5. Work on your changes.
6. Add your changes:
   ```bash
   git add --all
   ```
7. Commit your changes:
   ```bash
   git commit -m <type:message>
   ```
   > Note: Replace <type:message> to describe your changes. i.e: feat: New readme added.
8. Push your changes:
   ```bash
   git push
   ```
   > Note: You will may be required to set upstream. i.e: git push --set-upstream origin feat/fronted

9. After you finish your feature you must submit a merge request pointing to **dev** branch:
   > Note: You will specify the reviewers that will approve this merge.


# Get Started

Requirements:

<ul>
   <li> Amazon Account
   <li> AWS CLI
   <li> SAM CLI
</ul>

Steps:

1. Create Virtualenv
   ```bash
   virtualenv venv1
   ```
   > Note: if not installed, pip install virtualenv
2. Activate the virtualenv and install requirements.txt in it
   ```bash
   source venv1/bin/activate
   pip install requirements.txt 
   ```
   > Note: requirements will have the neccesaries libraries for the env.
3. Configure AWS and provide the keys.
   ```bash
   aws configure
   ```
   > Note: You will need to install AWS CLI before.
4. You will need to configure the access token and provide your MFA code.
   ```bash
   sudo cp ~/.aws/credentials ~/.aws/credentials_back
   python incidents_token.py
   ```
   > Note: You will need MFA in your AWS account activated before. 
5. Install the AWS Infrastructure and create the wheel for your project
   ```bash
   ./start.sh
   ```
   > Note: You will need SAM cli. 
6. Your AWS resources will be deployed in your account
7. The Role deployed needs to be attached to the Redshift Cluster
   > Note: A Redshift Cluster is required
8. Run the DDL scripts inside the database in Redshift.
9. The Job will be configure via .yamls located in src/config. All of them should uploaded to s3.
   > Note: s3://fire-incidents-config-dev/config/
10. In case of any troubleshoot using Glue and Redshift, please refer to the Troubleshooting section. 
11. To delete the stack:
   ```bash
   ./stop.sh
   ```

# Setting up Redshift Cluster

Useful links:
Setting up cluster: <a href=https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html> AWS Docs </a> <br>

## Troubleshooting

Possibles troubleshoots when setting Redshift Cluster and Glue:

<ul>
   <li> Nat Gateway activation: <a href=https://aws.amazon.com/es/premiumsupport/knowledge-center/glue-s3-endpoint-validation-failed/> AWS Docs </a>
   <li> Routing Tables updated: <a href=https://aws.amazon.com/es/premiumsupport/knowledge-center/glue-s3-endpoint-validation-failed/> AWS Docs </a>
   <li> Attached the role deployed to the cluster: <a href=https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum-add-role.html> AWS Docs </a>
   <li> Create a Catalog Connection from Glue: <a href=https://docs.aws.amazon.com/glue/latest/dg/console-connections.html> AWS Docs </a>
   <li> Attach the connection to the GLue Job: <a href=https://aws.amazon.com/es/premiumsupport/knowledge-center/connection-timeout-glue-redshift-rds/> AWS Docs </a>
</ul>


# S3 Folder structures:

Buckets: 


<img src="images/Buckets.png" width="700">

Bucket: 	fire-incidents-config-dev

<img src="images/Bucket_config.png" width="700">

<ul>
   <li> config: will contain the neccesary yaml configurations for the jobs
   <li> lib: will contain the generated wheel for the jobs
   <li> sam_templates: will containt the deployed templates from sam
   <li> athena_results: will be used to stored the athena queries
   <li> spark_logs: will contain the spark history logs of the jobs
   <li> redshift: will contain temporal files when write to redshift cluster
</ul>

Bucket:  fire-incidents-raw-dev

<img src="images/Bucket_raw.png" width="700">

This folder will have the csv file.

Bucket:  fire-incidents-refined-dev

<img src="images/Bucket_refined.png" width="700">

This folder will have the refined data in parquet


# Spark History Logs

1. Download the image
   ```bash
   docker build -t glue/sparkui:latest
   ```

2. Export Credentials
   ```bash
   export AWS_ACCESS_KEY_ID="???"
   export AWS_SECRET_ACCESS_KEY="???"
   export AWS_SESSION_TOKEN="???"
   ```


3. Run the spark host 
   
   docker run -it -e SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS \
   -Dspark.history.fs.logDirectory=s3a://fire-incidents-config-dev/logs/spark \
   -Dspark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID
   -Dspark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
   -Dspark.hadoop.fs.s3a.session.token=$AWS_SESSION_TOKEN \
   -Dspark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" \
   -p 18080:18080 --name glue_sparkUI glue/sparkui:latest "/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
   ```

# Setting Up Airflow

1. Go to Airflow Folder
   ```bash
   cd airflow
   ```

2. Update the stepfunction arn and the glue job names en each DAG.

3. Create the services
   ```bash
   docker-compose up
   ```

4. Access to Airflow console. In the "Admin" section in "Connections", create a connection to aws:
   ```bash
   login: access_key
   password: secret_access_key
   {"region_name": "us-west-2", "aws_session_token": "??"}
   ```
    
5. You will see both Dags live, and able to be triggered. 

<img src="images/Airflow.png" width="700">

## License
[MIT](https://choosealicense.com/licenses/mit/)