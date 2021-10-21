cd "$(dirname "$0")"

echo "Stack Will be Deleted!"
rm -r build
rm -r dist
rm -r fire_incidents.egg-info
aws cloudformation delete-stack --stack-name "fire-incidents-v1-dev"