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
The data in raw and refined stages, will be crawled and available in Athena, where Data Quality proccesses will be able to analyze and compare the data, in order not to miss records during the process.
Once in refined, the data will be loaded to Redshift, using a "prev" schema, where the tables: fire_incidents, district and battalion will be written. <br>
The table battalion will have the fields: ['battalion', 'station_area'] + id_battalion (generated with md5 to identify an unique record).<br>
The table district will have the fields: ['neighborhood_district', 'city', 'zipcode'] + id_district (generated with md5 to identify an unique record)<br>
The table fire_incidents will have all the fields plus id_district and id_battalion. This will be usefull to obtain the SK from the datawarehouse dimensions when creating a fact table.
The prev tables are temporal tables, which will be truncated and written to the schema "stg" using upserts/merge methods. The tables in "stg" will be updated daily and will have all the history. While the schema "prev" will have the data of the day (the incremental load). The tables in "stg" will have the same structure as in "prev". <br>
Once in "stg", another glue job will create the dimensions and the fact table. 
This, will create the dimensions: dim_district, dim_battalion, copying the tables from "stg" and adding them the SK, which are from the datawarehouse. 
Once the dims are created in "dwh", the job will create a fact table named: fact_fire_incidents, consuming fire_incidents from "stg" and joining it to the dimension to obtain the sk_district, and sk_battalion.<br>
Once finished the proccess, the data will be available in "dwh" to be consumed. 


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


## License
[MIT](https://choosealicense.com/licenses/mit/)