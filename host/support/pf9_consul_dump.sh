#!/usr/bin/env bash


if [ -d "/opt/pf9/etc/pf9-consul" ]; then
    echo "listing /opt/pf9/etc/pf9-consul"
    ls -lah /opt/pf9/etc/pf9-consul
else
    echo "folder not exist /opt/pf9/etc/pf9-consul"
fi

if [ -d "/opt/pf9/etc/pf9-consul/conf.d" ]; then
    echo "listing /opt/pf9/etc/pf9-consul/conf.d"
    ls -lah /opt/pf9/etc/pf9-consul/conf.d
else
    echo "folder not exist /opt/pf9/etc/pf9-consul/conf.d"
fi

if [ -f "/opt/pf9/etc/pf9-consul/conf.d/client.json" ]; then
    echo "cat file /opt/pf9/etc/pf9-consul/conf.d/client.json"
    cat /opt/pf9/etc/pf9-consul/conf.d/client.json
fi

if [ -f "/opt/pf9/etc/pf9-consul/conf.d/server.json" ]; then
    echo "cat file /opt/pf9/etc/pf9-consul/conf.d/server.json"
    cat /opt/pf9/etc/pf9-consul/conf.d/server.json
fi

if [ -f /usr/bin/consul ]; then
    echo "list consul members"
    consul members
    echo "list consul kv store"
    consul kv get --recurse
    echo "list consul peers"
    consul operator raft list-peers
    echo "list consul info"
    consul info
else
    echo "consul not found on host"
fi



