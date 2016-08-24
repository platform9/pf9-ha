# HA Helper

This service runs on all the nodes that are part of a HA cluster. This service performs following tasks -
1. Start the distributed clustering service and join the cluster as configured by HA Manager.
2. Monitor all the nodes in a cluster for failures. If the service is running on the leader node in the cluster, report failures to the controller.
3. Maintain and cleanup relevant information in the distributed key-value store.
