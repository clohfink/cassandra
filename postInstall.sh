#!/bin/bash

ln -s /var/lib/nflx-configs/cassandra/cassandra.yaml /etc/cassandra/ods-cass-conf.yaml || echo "Warning: Failed to create symbolic link /etc/cassandra/ods-cass-conf.yaml"
# this is needed for priam when it starts up it tries to create backup directories
mkdir /mnt/data/cassandra || echo "Warning: Failed to create directory /mnt/data/cassandra"
chmod www-data:www-data /mnt/data/cassandra || echo "Warning: Failed to change ownership of /mnt/data/cassandra"
