/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.IndexSettings;

public enum RoutingPool {
    LOCAL_ONLY,
    REMOTE_CAPABLE;

    public static RoutingPool getNodePool(RoutingNode node) {
        if (node.node().isRemoteSearcherNode()) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }

    public static RoutingPool getNodePool(DiscoveryNode node) {
        if (node.isRemoteSearcherNode()) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }

    public static RoutingPool getShardPool(ShardRouting shard, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shard.index());
        if (IndexSettings.SNAPSHOT_REPOSITORY.exists(indexMetadata.getSettings())) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }

    public static RoutingPool getIndexPool(IndexMetadata indexMetadata) {
        if (IndexSettings.SNAPSHOT_REPOSITORY.exists(indexMetadata.getSettings())) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }
}
