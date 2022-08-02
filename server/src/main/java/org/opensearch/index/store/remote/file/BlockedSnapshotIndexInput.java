/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.remote.util.TransferManager;
import org.opensearch.threadpool.ThreadPool;

/**
 * A virtual file index input backed by a s3 file with ES snapshot format.
 * It only downloads those blocks of the file needed by queries to local disk.
 */
public class BlockedSnapshotIndexInput extends BlockedIndexInput {

    /**
     * snapshot file info
     */
    protected final FileInfo fileInfo;


    public BlockedSnapshotIndexInput(BlobStoreIndexShardSnapshot.FileInfo fileInfo, FSDirectory directory, BlobContainer blobContainer, TransferManager transferManager) {
        super(fileInfo, directory, blobContainer, transferManager);
        this.fileInfo = fileInfo;
    }

    public BlockedSnapshotIndexInput(
        String resourceDescription,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        long off,
        long length,
        boolean isClone,
        FSDirectory directory,
        BlobContainer blobContainer,
        TransferManager transferManager
    ) {
        super(resourceDescription, fileInfo, off, length, isClone, directory, blobContainer, transferManager);
        this.fileInfo = fileInfo;
    }

    @Override
    public BlockedSnapshotIndexInput clone() {
        BlockedSnapshotIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        if (currentBlock != null) {
            clone.currentBlock = currentBlock.clone();
            clone.currentBlockId = currentBlockId;
            clone.currentBlockHolder.set(clone.currentBlock);
        }

        return clone;
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    @Override
    protected BlockedSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new BlockedSnapshotIndexInput(sliceDescription, fileInfo, this.offset + offset, length, true, directory, blobContainer, transferManager);
    }

}
