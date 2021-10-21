#!/bin/bash

# change cwd
cd "$(dirname "$0")"

# start infra
clear \
  && docker-compose ps
