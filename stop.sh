#!/bin/bash

# change cwd
cd "$(dirname "$0")"

# clear \
#   && docker-compose down \
#   && echo "Done!"

echo "Stack Will be Deleted!"
rm -r build
rm -r dist
rm -r fire_incidents.egg-info
aws cloudformation delete-stack --stack-name "fire-incidents-v1-dev"