/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

public class PoolScopedRoutingNodes extends RoutingNodes {
    public PoolScopedRoutingNodes(RoutingNodes routingNodes) {
        super(true);
//        unassignedShards = new RoutingNodes.UnassignedShards(this);
//        assignedShards = routingNodes.getAssignedShards();
//        inactivePrimaryCount = routingNodes.getInactivePrimaryCount();
//        inactiveShardCount = routingNodes.getInactiveShardCount();
//        relocatingShards = routingNodes.getRelocatingShardCount();
//        recoveriesPerNode = routingNodes.getRecoveriesPerNode();            // Maintains recoveries per node
//        initialPrimaryRecoveries = routingNodes.getInitialPrimaryRecoveries();
//        initialReplicaRecoveries = routingNodes.getInitialReplicaRecoveries();
//        remoteInitializingShardCount = routingNodes.getRemoteInitializingShardCount();
//        remoteSearcherNodes = routingNodes.getRemoteSearcherNodes();
    }
}
