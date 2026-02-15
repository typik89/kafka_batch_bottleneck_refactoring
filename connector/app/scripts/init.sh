#!/usr/bin/env bash

# turn on bash's job control
set -m

#Dynamically set variables before start
. /app/scripts/set_variables.sh

# Start the primary process and put it in the background
/etc/confluent/docker/run &

#source /app/scripts/handle_templates.sh
#source /app/scripts/handle_tasks.sh
source /app/scripts/wait_for_ready.sh
source /app/scripts/delete_tasks.sh
source /app/scripts/install_tasks.sh

# now we bring the primary process back into the foreground
# and leave it there
fg %1