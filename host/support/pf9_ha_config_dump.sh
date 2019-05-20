#!/usr/bin/env bash

if [ -d "/opt/pf9/etc" ]; then
    echo "listing /opt/pf9/etc"
    ls -lah /opt/pf9/etc
else
    echo "folder not exist /opt/pf9/etc"
fi

if [ -d "/opt/pf9/etc/pf9-ha" ]; then
    echo "listing /opt/pf9/etc/pf9-ha"
    ls -lah /opt/pf9/etc/pf9-ha
else
    echo "folder not exist /opt/pf9/etc/pf9-ha"
fi

if [ -d "/opt/pf9/etc/pf9-ha/conf.d" ]; then
    echo "list /opt/pf9/etc/pf9-ha/conf.d"
    ls -lah /opt/pf9/etc/pf9-ha/conf.d
else
    echo "folder not exist /opt/pf9/etc/pf9-ha/conf.d"
fi

if [ -f "/opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf" ]; then
    echo "cat /opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf"
    cat /opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf
else
    echo "file not exist /opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf"
fi
