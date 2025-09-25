#!/bin/bash

ln -s /var/lib/nflx-configs/cassandra/cassandra.yaml /etc/cassandra/ods-cass-conf.yaml || echo "Warning: Failed to create symbolic link /etc/cassandra/ods-cass-conf.yaml"
