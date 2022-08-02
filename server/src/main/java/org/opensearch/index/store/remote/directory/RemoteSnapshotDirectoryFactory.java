/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.util.TransferManager;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.threadpool.ThreadPool;

public class RemoteSnapshotDirectoryFactory {

    public static final String REMOTE_STORE_LOCATION = "RemoteStore";

    public Directory newDirectory(IndexSettings indexSettings, ShardPath path, Repository repository, ThreadPool threadPool) throws IOException {
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobPath blobPath = new BlobPath().add("indices")
            .add(IndexSettings.SNAPSHOT_INDEX_ID.get(indexSettings.getSettings()))
            .add(Integer.toString(path.getShardId().getId()));
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(blobPath);
        final SnapshotId snapshotId = new SnapshotId(
            IndexSettings.SNAPSHOT_ID_NAME.get(indexSettings.getSettings()),
            IndexSettings.SNAPSHOT_ID_UUID.get(indexSettings.getSettings())
        );
        Path shardActualPath = path.getDataPath().resolve(REMOTE_STORE_LOCATION);
        final BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);
        FSDirectory fsDirectory = FSDirectory.open(shardActualPath);

        TransferManager transferManager = new TransferManager(blobContainer, threadPool);

        return new RemoteSnapshotDirectory(blobContainer, snapshot, fsDirectory, transferManager);

    }
}
