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
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;

public enum RoutingPool {
    LOCAL_ONLY,
    REMOTE_CAPABLE;

    public static RoutingPool getNodePool(RoutingNode node) {
        return getNodePool(node.node());
    }

    public static RoutingPool getNodePool(DiscoveryNode node) {
        if (node.isRemoteSearcherNode()) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }

    public static RoutingPool getShardPool(ShardRouting shard, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shard.index());
       return getIndexPool(indexMetadata);
    }

    public static RoutingPool getIndexPool(IndexMetadata indexMetadata) {
        Settings indexSettings = indexMetadata.getSettings();
        if (IndexModule.Type.REMOTE_SNAPSHOT.match(indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()))) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }
}
