# High Availability Manager #
This service sets up the high availability (HA) node cluster

### Data Format ###
All Data is exchanged in JSON format

## API ##
GET /v1/ha/<aggregate_id>

Returns the HA configuration state for the host aggregate

Example:
```
{
    "status":
    [
     { "id": 1, "state": "enabled" },
    ]
}
```

HA configuration requires a minimum of three hosts. It is said to be
non-available if the aggregate has less than three hosts.

GET /v1/ha/\<aggregate_id>

Returns HA configuration state for one host aggregate

Example:
```
{
    "status":
    [
     { "id": 1, "state": "enabled" },
     { "id": 11, "state": "disabled" },
     { "id": 14, "state": "not-available" }
    ]
}
```


PUT /v1/ha/\<aggregate_id>/enable

Activates the HA configuration on all hosts in the given aggregate.
1. If HA is not yet enabled for given aggregate:  The eligible hosts are determined by looking at their
host aggregate metadata. All hosts in the host aggregate are treated as one HA cluster
2. If HA is already enabled for given aggregate: No action is performed.
3. If HA cannot be enabled for given aggregate: This can happen if there are
less than three hosts in the host aggregate. The API fails in such a case.

PUT /v1/ha/\<aggregate_id>/disable

Disables HA for an aggregate. With following possible outcomes --
1. If given aggregate is HA enabled, it is disabled. The clustering
services are removed from all participating hosts.
2. If the aggregate has HA disabled, no action is taken.
3. If the aggregate was not found, the API reports failure.

Once an aggregate is HA enabled, the hosts added to such aggregate
are automatically added to HA cluster services.




