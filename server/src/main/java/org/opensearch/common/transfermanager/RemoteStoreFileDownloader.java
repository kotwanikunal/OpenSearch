/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.transfermanager;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Helper class to downloads files from a {@link RemoteSegmentStoreDirectory}
 * instance to a local {@link Directory} instance in parallel depending on thread
 * pool size and recovery settings.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.11.0")
public final class RemoteStoreFileDownloader {
    private final Logger logger;
    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;

    public RemoteStoreFileDownloader(ThreadPool threadPool, RecoverySettings recoverySettings) {
        this.logger = Loggers.getLogger(RemoteStoreFileDownloader.class);
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory.
     * @param source The remote directory to copy segment files from
     * @param destination The local directory to copy segment files to
     * @param toDownloadSegments The list of segment files to download
     * @param listener Callback listener to be notified upon completion
     */
    public List<Runnable> downloadAsyncV2(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        Collection<String> toDownloadSegments,
        ActionListener<Void> listener
    ) {
        final List<Runnable> fileCallables = new ArrayList<>();

        for (String file: toDownloadSegments) {
            fileCallables.add(downloadInternalV2(cancellableThreads, source, destination, null, file, () -> {}, listener));
        }
        return fileCallables;
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory, while also copying the segments _to_ another remote directory.
     * @param source The remote directory to copy segment files from
     * @param destination The local directory to copy segment files to
     * @param secondDestination The second remote directory that segment files are
     *                          copied to after being copied to the local directory
     * @param toDownloadSegments The list of segment files to download
     * @param onFileCompletion A generic runnable that is invoked after each file download.
     *                         Must be thread safe as this may be invoked concurrently from
     *                         different threads.
     */
    public List<Runnable> downloadV2(
        Directory source,
        Directory destination,
        Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        final List<Runnable> fileCallables = new ArrayList<>();

        for (String file: toDownloadSegments) {
            fileCallables.add(downloadInternalV2(cancellableThreads, source, destination, secondDestination, file, onFileCompletion, listener));
        }

        return fileCallables;
    }

    public Runnable downloadInternalV2(CancellableThreads cancellableThreads,
                                   Directory source,
                                   Directory destination,
                                   @Nullable Directory secondDestination,
                                   String file,
                                   Runnable onFileCompletion,
                                   ActionListener<Void> listener) {

        return () -> {
            try {
                cancellableThreads.executeIO(() -> {
                    destination.copyFrom(source, file, file, IOContext.DEFAULT);
                    onFileCompletion.run();
                    if (secondDestination != null) {
                        secondDestination.copyFrom(destination, file, file, IOContext.DEFAULT);
                    }
                });
            } catch (Exception e) {
                listener.onFailure(e);
            }
        };
    }
}
