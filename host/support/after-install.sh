mkdir -p /opt/pf9/consul-data-dir
mkdir -p /var/run/pf9-consul/
mkdir -p /var/run/pf9-ha/
mkdir -p /var/consul-status/
chown -R pf9:pf9group /var/run/pf9-consul
chown -R pf9:pf9group /var/run/pf9-ha
chown -R pf9:pf9group /opt/pf9/consul-data-dir
chown -R pf9:pf9group /opt/pf9/pf9-ha
chown -R pf9:pf9group /opt/pf9/etc/pf9-consul
chown -R pf9:pf9group /opt/pf9/etc/pf9-ha
chown -R pf9:pf9group /var/consul-status
