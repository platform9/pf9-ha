/var/log/pf9/pf9-consul.log
/var/log/pf9/pf9-ha.log
/var/log/pf9/pf9-ha-slave-out.log
{
    daily
    rotate 4
    missingok
    compress
    minsize 50M
}