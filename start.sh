cd "$(dirname "$0")"

python setup.py bdist_wheel

aws s3 cp dist/fire_incidents-1.0-py3-none-any.whl s3://fire-incidents-config-dev/lib/
aws s3 cp src/config/raw_refined.yaml s3://fire-incidents-config-dev/config/
aws s3 cp src/config/refined_stg.yaml s3://fire-incidents-config-dev/config/
aws s3 cp src/config/stg_dwh.yaml s3://fire-incidents-config-dev/config/

sam build
sam deploy -t template.yaml --debug --capabilities "CAPABILITY_NAMED_IAM" --parameter-overrides Stage="dev" --region "us-west-2" --stack-name "fire-incidents-v1-dev" --s3-bucket "fire-incidents-config-dev" --s3-prefix "sam_templates/fire-incidents" --no-confirm-changeset --force-upload 

cd airflow
docker-compose up
cd ..