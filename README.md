# pf9-ha
HA : High Availability cluster management for OpenStack.

HA repository contains couple of simple services to deploy and manage a distributed clustering service like Consul (https://www.consul.io/) on a set of KVM nodes.
By running KVM nodes as distributed cluster, host failures can be detected. This ability serves as a base for higher level OpenStack capabilities like Virtual Machine HA.

## Details
The HA cluster management is split into two simple services

### HA Manager
This RESTful service provides a simple control API to manage cluster membership. The Cloud admins can create an availability zone (AZ) in OpenStack and enable HA on that AZ. This action pushes required clustering services to all nodes in the AZ.
The HA manager service also co-ordinates the initialization of distributed clustering services. Specifically, it chooses the bootstrap node and instructs follower nodes to join the cluster.

### HA Helper
This service is deployed on a node in HA cluster along with distributed clustering service like Consul. The main job of helper is to programatically manage the distributed clustering service. It is the workhorse which controls the lifecycle, cluster properties and bootstrap of distributed cluster.
The helper is controlled by the manager piece. Manager passes on the distributed cluster configuration to helper and it is acted upon.

