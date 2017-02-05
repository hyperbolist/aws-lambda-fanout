#!/bin/bash

#TUNNEL_ADDRESS=54.194.8.98
#DEBUG_MODE=true node -e 'require("./test-hosts/memcached.js")().then((c) => consolg.log("${c.configPort}"));'
#echo -e 'config get cluster\r\n' | nc localhost 59000
#
#
#MEMCACHED_CLUSTER_NAME_PORT=9000
#
#MEMCACHED_CLUSTER_NAME_COMMAND="ssh -N $(aws elasticache describe-cache-clusters --cache-cluster-id test-memcached --query 'CacheClusters[0].ConfigurationEndpoint.[Address, Port]' --output text | awk 'BEGIN { FS="\t" } { printf "-L 9000:%s:%s ", $1, $2 }') ec2-user@$TUNNEL_ADDRESS"
#$MEMCACHED_CLUSTER_NAME_COMMAND &
#MEMCACHED_CLUSTER_NAME_PID=$!
#
#MEMCACHED_CLUSTER_BASE_PORT=$((MEMCACHED_CLUSTER_NAME_PORT+1))
#
#MEMCACHED_CLUSTER_MEMBERS=$(echo "config get cluster" | nc localhost 9000 | awk 'BEGIN { RS="\r\n"; FS="\n"; OK=0 } /^CONFIG/ { OK=1 } OK == 1 && NR == 2 { print $3 }')
#MEMCACHED_CLUSTER_MEMBERS_COUNT=$(echo $MEMCACHED_CLUSTER_MEMBERS | awk 'BEGIN { RS=" " } END { print NR }')
#MEMCACHED_CLUSTER_MEMBERS_COMMAND="ssh -N $(echo "config get cluster" | nc localhost 9000 | awk 'BEGIN { RS="\r\n"; FS="\n"; OK=0 } /^CONFIG/ { OK=1 } OK == 1 && NR == 2 { print $3 }' | awk 'BEGIN { RS=" "; FS="|" ; port='$MEMCACHED_CLUSTER_BASE_PORT' } { printf "-L %d:%s:%s ", port, $1, $3; port = port + 1 }') ec2-user@$TUNNEL_ADDRESS"
#$MEMCACHED_CLUSTER_MEMBERS_COMMAND &
#MEMCACHED_CLUSTER_MEMBERS_PID=$!
#
#REDIS_PORT=$((MEMCACHED_CLUSTER_BASE_PORT+MEMCACHED_CLUSTER_MEMBERS_COUNT))
#
#REDIS_COMMAND="ssh -N $(aws elasticache describe-cache-clusters --cache-cluster-id test-redis --query 'CacheClusters[0].ConfigurationEndpoint.[Address, Port]' --output text | awk 'BEGIN { FS="\t" } { printf "-L 9000:%s:%s ", $1, $2 }') ec2-user@$TUNNEL_ADDRESS"
#$REDIS_COMMAND &
#REDIS_PID=$!
#
##MEMCACHED_TUNNEL_MODE=true MEMCACHED_TUNNEL_HOST=localhost MEMCACHED_TUNNEL_COUNT=$MEMCACHED_CLUSTER_MEMBERS_COUNT MEMCACHED_TUNNEL_BASE=$MEMCACHED_CLUSTER_BASE_PORT node_modules/istanbul/lib/cli.js cover node_modules/mocha/bin/_mocha test/crc16.js -- -R spec
#DEBUG_MODE=true MEMCACHED_TUNNEL_MODE=true MEMCACHED_TUNNEL_HOST=localhost MEMCACHED_TUNNEL_COUNT=$MEMCACHED_CLUSTER_MEMBERS_COUNT MEMCACHED_TUNNEL_BASE=$MEMCACHED_CLUSTER_BASE_PORT node_modules/istanbul/lib/cli.js cover node_modules/mocha/bin/_mocha -- -R spec
#
#kill $MEMCACHED_CLUSTER_MEMBERS_PID $MEMCACHED_CLUSTER_NAME_PID $REDIS_PID

DEBUG_MODE=true node -e 'const fs=require("fs");require("./test-hosts/memcached.js")().then((c) => { fs.writeFileSync(`/tmp/memcached.${process.pid}`, `${c.config.configPort}`); c.stopped = () => fs.unlink(`/tmp/memcached.${process.pid}`); });' &

node_modules/istanbul/lib/cli.js cover node_modules/mocha/bin/_mocha -- -R spec