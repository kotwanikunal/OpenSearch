/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.transfermanager;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.ActionRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.opensearch.index.snapshots.blobstore.SlicedInputStream;
import org.opensearch.index.store.Store;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.opensearch.repositories.blobstore.BlobStoreRepository.VIRTUAL_DATA_BLOB_PREFIX;

public class RepositoryFileDownloader {

    private final Executor executor;
    private final Logger logger;

    public RepositoryFileDownloader(ThreadPool threadPool) {
        this.executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        this.logger = Loggers.getLogger(RepositoryFileDownloader.class);
    }

    public Runnable executeOneFileRestore(
        Store store,
        BlobContainer container,
        BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files,
        ActionListener<Void> allFilesListener
    ) throws InterruptedException {
        return () -> {
            final BlobStoreIndexShardSnapshot.FileInfo fileToRecover;
            try {
                fileToRecover = files.poll(0L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (fileToRecover == null) {
                allFilesListener.onResponse(null);
            } else {
                executor.execute(ActionRunnable.wrap(allFilesListener, filesListener -> {
                    store.incRef();
                    try {
                        restoreFile(fileToRecover, container, store);
                    } finally {
                        store.decRef();
                    }
                    executeOneFileRestore(store, container, files, filesListener);
                }));
            }
        };
    }

    private void restoreFile(BlobStoreIndexShardSnapshot.FileInfo fileInfo, BlobContainer container, Store store) throws IOException {
        ensureNotClosing(store);
        boolean success = false;
        try (
            IndexOutput indexOutput = store.createVerifyingOutput(
                fileInfo.physicalName(),
                fileInfo.metadata(),
                IOContext.DEFAULT
            )
        ) {
            if (fileInfo.name().startsWith(VIRTUAL_DATA_BLOB_PREFIX)) {
                final BytesRef hash = fileInfo.metadata().hash();
                indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
                // TODO: Metrics handler
//                recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), hash.length);
            } else {
                // TODO: Add rate limiting to InputStream
                try (InputStream stream = maybeRateLimitRestores(new SlicedInputStream(fileInfo.numberOfParts()) {
                    @Override
                    protected InputStream openSlice(int slice) throws IOException {
                        ensureNotClosing(store);
                        return container.readBlob(fileInfo.partName(slice));
                    }
                })) {
                    //TODO: Configurable buffer size
                    int bufferSize = 1024;
                    final byte[] buffer = new byte[Math.toIntExact(Math.min(bufferSize, fileInfo.length()))];
                    int length;
                    while ((length = stream.read(buffer)) > 0) {
                        ensureNotClosing(store);
                        indexOutput.writeBytes(buffer, 0, length);
                        // TODO: Metrics handler
//                        recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), length);
                    }
                }
            }
            Store.verify(indexOutput);
            indexOutput.close();
            store.directory().sync(Collections.singleton(fileInfo.physicalName()));
            success = true;
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            try {
                store.markStoreCorrupted(ex);
            } catch (IOException e) {
                logger.warn("store cannot be marked as corrupted", e);
            }
            throw ex;
        } finally {
            if (success == false) {
                store.deleteQuiet(fileInfo.physicalName());
            }
        }
    }

    void ensureNotClosing(final Store store) throws AlreadyClosedException {
        assert store.refCount() > 0;
        if (store.isClosing()) {
            throw new AlreadyClosedException("store is closing");
        }
    }

    private static InputStream maybeRateLimit(InputStream stream, Supplier<RateLimiter> rateLimiterSupplier, CounterMetric metric) {
        return new RateLimitingInputStream(stream, rateLimiterSupplier, metric::inc);
    }

    public InputStream maybeRateLimitRestores(InputStream stream) {
        // TODO: Rate Limiter Config
//        return maybeRateLimit(
//            maybeRateLimit(stream, () -> restoreRateLimiter, restoreRateLimitingTimeInNanos),
//            recoverySettings::rateLimiter,
//            restoreRateLimitingTimeInNanos
//        );
        return stream;
    }

}
