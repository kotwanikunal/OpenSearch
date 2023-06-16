/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.AbstractIndexShardComponent;
import org.opensearch.index.shard.ShardId;

public class LocalOnlyStore extends AbstractIndexShardComponent implements Store {

    private final StoreDirectory directory;
    private final ShardLock shardLock;
    private final OnClose onClose;

    // used to ref count files when a new Reader is opened for PIT/Scroll queries
    // prevents segment files deletion until the PIT/Scroll expires or is discarded
    private final ReplicaFileTracker replicaFileTracker;

    private final AbstractRefCounted refCounter = new AbstractRefCounted("store") {
        @Override
        protected void closeInternal() {
            // close us once we are done
            LocalOnlyStore.this.closeInternal();
        }
    };

    public LocalOnlyStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock) {
        this(shardId, indexSettings, directory, shardLock, OnClose.EMPTY);
    }

    public LocalOnlyStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock, OnClose onClose) {
        super(shardId, indexSettings);
        final TimeValue refreshInterval = indexSettings.getValue(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING);
        logger.debug("store stats are refreshed with refresh_interval [{}]", refreshInterval);
        ByteSizeCachingDirectory sizeCachingDir = new ByteSizeCachingDirectory(directory, refreshInterval);
        this.directory = new StoreDirectory(sizeCachingDir, Loggers.getLogger("index.store.deletes", shardId));
        this.shardLock = shardLock;
        this.onClose = onClose;
        this.replicaFileTracker = indexSettings.isSegRepEnabled() ? new ReplicaFileTracker() : null;

        assert onClose != null;
        assert shardLock != null;
        assert shardLock.getShardId().equals(shardId);
    }

    @Override
    public ShardLock shardLock() {
        return shardLock;
    }

    @Override
    public OnClose onClose() {
        return onClose;
    }

    @Override
    public ReplicaFileTracker replicaFileTracker() {
        return replicaFileTracker;
    }

    @Override
    public AbstractRefCounted refCounter() {
        return refCounter;
    }

    @Override
    public IndexSettings indexSettings() {
        return indexSettings;
    }

    @Override
    public Logger logger() {
        return logger;
    }

    @Override
    public Directory directory() {
        return directory;
    }
}
