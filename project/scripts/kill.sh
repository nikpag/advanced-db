#!/bin/bash

MASTER_PORT="65509"
SLAVE_PORT="65510"

kill $(netstat -tlnp | grep $MASTER_PORT | sed "s|.*LISTEN      \([0-9]\+\)/java|\1|g")
ssh user@snf-33421 'kill $(netstat -tlnp | grep '$SLAVE_PORT' | sed "s|.*LISTEN      \([0-9]\+\)/java|\1|g")'