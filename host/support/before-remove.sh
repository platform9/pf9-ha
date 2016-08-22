PIDFILE=/var/run/pf9-consul/pf9-consul.pid
LOCKFILE=/var/lock/subsys/pf9-consul
DATADIR=/opt/pf9/consul-data-dir
HA_CONF=/opt/pf9/etc/pf9-ha/conf.d/pf9-ha.conf
CONSUL_CONF=/opt/pf9/etc/pf9-consul/conf.d/*.json
pid=`cat ${PIDFILE}`
kill -TERM ${pid}
rm -f ${PIDFILE}
rm -rf ${DATADIR}
rm -f ${HA_CONF}
rm -f ${CONSUL_CONF}
rm -f ${LOCKFILE}
rm -f /var/consul-status/last_update
