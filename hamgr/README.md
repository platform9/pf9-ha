# High Availability Manager #
This service sets up the high availability (HA) node cluster

### Data Format ###
All Data is exchanged in JSON format

## API ##
GET /v1/ha

Returns the HA configuration state for the all availability zones

Example:
```
{
    "status":
    [
     { "enabled": true, "name": "nova", "task_state": "completed" },
     { "enabled": true, "name": "compute-az", "task_state": "completed" }
    ]
}
```

HA configuration requires a minimum of three hosts. It is said to be
non-available if the availability zone has less than three hosts.

GET /v1/ha/<availability_zone>

Returns HA configuration state for one availability zone

Example:
```
{
  "status": [
    {
      "enabled": true,
      "name": "nova",
      "task_state": "completed"
    }
  ]
}
```


PUT /v1/ha/<availability_zone>/enable

Activates the HA configuration on all hosts in the given availability zone.
1. If HA is not yet enabled for given availability zone:  The eligible hosts are determined by looking at their
host availability zone metadata. All hosts in the availability zone are treated as one HA cluster
2. If HA is already enabled for given availability zone: No action is performed.
3. If HA cannot be enabled for given availability zone: This can happen if there are
less than three hosts in the availability zone. The API fails in such a case.

PUT /v1/ha/<availability_zone>/disable

Disables HA for an availability zone. With following possible outcomes --
1. If given availability zone is HA enabled, it is disabled. The clustering
services are removed from all participating hosts.
2. If the availability zone has HA disabled, no action is taken.
3. If the availability zone was not found, the API reports failure.

Once an availability zone is HA enabled, the hosts added to such availability zone
are automatically added to HA cluster services.




