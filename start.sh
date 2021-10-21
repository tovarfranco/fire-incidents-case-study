# #!/bin/bash

# # change cwd
# cd "$(dirname "$0")"

# # create volumes directories
# mkdir -p volumes/postgres
# mkdir -p volumes/pubsub

# # start infra
# clear \
#   && docker-compose down \
#   && docker-compose up -d --build \
#   && echo "Done!"


cd "$(dirname "$0")"

python setup.py bdist_wheel

aws s3 cp dist/fire_incidents-1.0-py3-none-any.whl s3://fire-incidents-config-dev/lib/

sam build
sam deploy -t template.yaml --debug --capabilities "CAPABILITY_NAMED_IAM" --parameter-overrides Stage="dev" --region "us-west-2" --stack-name "fire-incidents-v1-dev" --s3-bucket "fire-incidents-config-dev" --s3-prefix "sam_templates/fire-incidents" --no-confirm-changeset --force-upload 