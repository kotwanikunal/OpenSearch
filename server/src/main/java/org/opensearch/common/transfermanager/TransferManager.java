/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.transfermanager;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class TransferManager {

    private final ThreadPool threadPool;
    private final PriorityBlockingQueue<Runnable> recoveryQueue;
    private final PriorityBlockingQueue<Runnable> replicationQueue;
    private final RemoteStoreFileDownloader remoteStoreFileDownloader;
    private final RepositoryFileDownloader repositoryFileDownloader;
    private final RepositoryBlockDownloader repositoryBlockDownloader;

    public TransferManager(ThreadPool threadPool, RecoverySettings recoverySettings, FileCache fileCache) {
        this.threadPool = threadPool;
        this.recoveryQueue = new PriorityBlockingQueue<>();
        this.replicationQueue = new PriorityBlockingQueue<>();
        remoteStoreFileDownloader = new RemoteStoreFileDownloader(threadPool, recoverySettings);
        repositoryFileDownloader = new RepositoryFileDownloader(threadPool);
        repositoryBlockDownloader = new RepositoryBlockDownloader(threadPool, fileCache);
    }

    public void downloadSegmentsFromRemoteStore(
        Directory source,
        Directory destination,
        Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) throws InterruptedException, IOException {
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        List<Runnable> downloadSupplier = remoteStoreFileDownloader.downloadV2(source, destination, secondDestination, toDownloadSegments, onFileCompletion);
        replicationQueue.addAll(downloadSupplier);
        PrioritizedOpenSearchThreadPoolExecutor prioritizedOpenSearchThreadPoolExecutor = (PrioritizedOpenSearchThreadPoolExecutor) threadPool.executor(ThreadPool.Names.DOWNLOAD);
        prioritizedOpenSearchThreadPoolExecutor.execute(downloadSupplier.get(0), TimeValue.ZERO, null);
        doDownload();
        listener.actionGet();
    }

    public void downloadSegmentFromRemoteStoreAsync(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        Collection<String> toDownloadSegments,
        ActionListener<Void> listener
    ) {
        List<Runnable> downloadSupplier = remoteStoreFileDownloader.downloadAsyncV2(cancellableThreads, source, destination, toDownloadSegments, listener);
        replicationQueue.addAll(downloadSupplier);
        doDownload();
    }

    public void downloadSegmentFromSnapshot(
        Store store,
        BlobContainer container,
        BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files,
        ActionListener<Void> allFilesListener
    ) throws InterruptedException {
        Runnable downloadSupplier = repositoryFileDownloader.executeOneFileRestore(store, container, files, allFilesListener);
        recoveryQueue.add(downloadSupplier);
        doDownload();
    }

    public IndexInput downloadBlockFromSnapshot(BlobContainer blobContainer, BlobFetchRequest blobFetchRequest) throws IOException {
        PlainActionFuture<IndexInput> indexInputActionListener = new PlainActionFuture<>();
        Runnable indexInputSupplier = repositoryBlockDownloader.downloadBlock(blobContainer, blobFetchRequest, indexInputActionListener);
        recoveryQueue.add(indexInputSupplier);
        doDownload();
        return indexInputActionListener.actionGet();
    }

    public void doDownload() {
    }

}
