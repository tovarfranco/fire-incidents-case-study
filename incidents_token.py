import boto3
import csv
import os

"""
First you need to set up your credentials from your command line (terminal) as follows:
------------------------------------------------------------------------------------------------------------------------
aws configure
[default]
aws_access_key_id = <your aws access key id>
aws_secret_access_key = <your aws secret access key>
------------------------------------------------------------------------------------------------------------------------
Once it's done, you need to create a backup of your original credentials:
------------------------------------------------------------------------------------------------------------------------
sudo cp ~/.aws/credentials ~/.aws/credentials_back
------------------------------------------------------------------------------------------------------------------------
"""
# Documentation:
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.get_session_token
# Settings
# Put your AWS IAM User here
IAM_user = "FrancoTovar"
# If you already finished all the steps above, you can run this script in your terminal
# The terminal will ask you type your MFA code, make sure you have it.

# os.system("cp ~/.aws/credentials_back ~/.aws/credentials")

arn = f"arn:aws:iam::104674085951:mfa/{IAM_user}"
mfa_code = input("Enter the MFA code: ")

sts = boto3.client("sts")
response = sts.get_session_token(
    SerialNumber=arn, TokenCode=mfa_code, DurationSeconds=129600
)
with open(f"{os.getenv('HOME')}/.aws/credentials", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["[default]"])
    writer.writerow(["aws_access_key_id = " + response["Credentials"]["AccessKeyId"]])
    writer.writerow(
        ["aws_secret_access_key = " + response["Credentials"]["SecretAccessKey"]]
    )
    writer.writerow(["aws_session_token = " + response["Credentials"]["SessionToken"]])
print("New aws temporary tokens were generated, they expire in 36 hours")
