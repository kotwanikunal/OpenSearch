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
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.stream.read.listener.FilePartWriter;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

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
    private final ShardPath shardPath;
    private final RemoteSegmentStoreDirectory directory;
    private final BlobContainer blobContainer;

    static final long PART_SIZE = 16 * 1024 * 1024;

    public RemoteStoreFileDownloader(ShardId shardId, ThreadPool threadPool, RecoverySettings recoverySettings) {
        this(shardId, threadPool, recoverySettings, null, null);
    }

    public RemoteStoreFileDownloader(
        ShardId shardId,
        ThreadPool threadPool,
        RecoverySettings recoverySettings,
        ShardPath shardPath,
        RemoteSegmentStoreDirectory directory
    ) {
        this.logger = Loggers.getLogger(RemoteStoreFileDownloader.class, shardId);
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.shardPath = shardPath;
        this.directory = directory;
        this.blobContainer = directory == null ? null : directory.getSegmentBlobContainer();
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory.
     * @param source The remote directory to copy segment files from
     * @param destination The local directory to copy segment files to
     * @param toDownloadSegments The list of segment files to download
     * @param listener Callback listener to be notified upon completion
     */
    public void downloadAsync(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        Collection<String> toDownloadSegments,
        ActionListener<Void> listener
    ) {
        downloadInternal(cancellableThreads, source, destination, null, toDownloadSegments, () -> {}, listener);
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
    public void download(
        Directory source,
        Directory destination,
        Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) throws InterruptedException, IOException {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        downloadInternal(cancellableThreads, source, destination, secondDestination, toDownloadSegments, onFileCompletion, listener);
        try {
            listener.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            // If the blocking call on the PlainActionFuture itself is interrupted, then we must
            // cancel the asynchronous work we were waiting on
            cancellableThreads.cancel(e.getMessage());
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private void downloadInternal(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        @Nullable Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion,
        ActionListener<Void> listener
    ) {
        logger.error("Performing FILE based download: {}", toDownloadSegments);
        final Queue<String> queue = new ConcurrentLinkedQueue<>(toDownloadSegments);
        // Choose the minimum of:
        // - number of files to download
        // - max thread pool size
        // - "indices.recovery.max_concurrent_remote_store_streams" setting
        final int threads = Math.min(
            toDownloadSegments.size(),
            Math.min(threadPool.info(ThreadPool.Names.REMOTE_RECOVERY).getMax(), recoverySettings.getMaxConcurrentRemoteStoreStreams())
        );
        logger.trace("Starting download of {} files with {} threads", queue.size(), threads);
        final ActionListener<Void> allFilesListener = new GroupedActionListener<>(ActionListener.map(listener, r -> null), threads);
        for (int i = 0; i < threads; i++) {
            copyOneFile(cancellableThreads, source, destination, secondDestination, queue, onFileCompletion, allFilesListener);
        }
    }

    private void copyOneFile(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        @Nullable Directory secondDestination,
        Queue<String> queue,
        Runnable onFileCompletion,
        ActionListener<Void> listener
    ) {
        final String file = queue.poll();
        if (file == null) {
            // Queue is empty, so notify listener we are done
            listener.onResponse(null);
        } else {
            threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY).submit(() -> {
                logger.trace("Downloading file {}", file);
                try {
                    cancellableThreads.executeIO(() -> {
                        destination.copyFrom(source, file, file, IOContext.DEFAULT);
                        onFileCompletion.run();
                        if (secondDestination != null) {
                            secondDestination.copyFrom(destination, file, file, IOContext.DEFAULT);
                        }
                    });
                } catch (Exception e) {
                    // Clear the queue to stop any future processing, report the failure, then return
                    queue.clear();
                    listener.onFailure(e);
                    return;
                }
                copyOneFile(cancellableThreads, source, destination, secondDestination, queue, onFileCompletion, listener);
            });
        }
    }

    // Virtual Thread Implementation
    public void downloadAsync(
        CancellableThreads cancellableThreads,
        List<FileInfo> toDownloadSegments,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<Void> listener
    ) {
        downloadFilesInternal(cancellableThreads, toDownloadSegments, fileProgressTracker, () -> {}, listener);
    }

    public void download(List<FileInfo> toDownloadSegments, BiConsumer<String, Long> fileProgressTracker, Runnable onFileCompletion)
        throws InterruptedException, IOException {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        downloadFilesInternal(cancellableThreads, toDownloadSegments, fileProgressTracker, onFileCompletion, listener);
        try {
            listener.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            // If the blocking call on the PlainActionFuture itself is interrupted, then we must
            // cancel the asynchronous work we were waiting on
            cancellableThreads.cancel(e.getMessage());
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private void downloadFilesInternal(
        CancellableThreads cancellableThreads,
        List<FileInfo> toDownloadSegments,
        BiConsumer<String, Long> fileProgressTracker,
        Runnable onFileCompletion,
        ActionListener<Void> listener
    ) {
        logger.error("Performing part based download: {}", toDownloadSegments.stream().map(FileInfo::getFileName).toList());
        toDownloadSegments.forEach(fileInfo -> fileInfo.setFilePath(shardPath.resolveIndex().resolve(fileInfo.fileName)));
        Map<String, AtomicInteger> partsTracker = new ConcurrentHashMap<>();
        List<PartInfo> toDownloadSegmentParts = createParts(toDownloadSegments, partsTracker);
        downloadFileInternal(cancellableThreads, toDownloadSegmentParts, partsTracker, onFileCompletion, fileProgressTracker, listener);
    }

    List<PartInfo> createParts(List<FileInfo> toDownloadSegments, Map<String, AtomicInteger> partTracker) {
        List<PartInfo> fileParts = new ArrayList<>();
        for (FileInfo toDownloadSegment : toDownloadSegments) {
            long remainingLength = toDownloadSegment.getLength();
            int partNumber = 0;
            while (remainingLength > 0) {
                long offset = partNumber * PART_SIZE;
                long partSize = Math.min(toDownloadSegment.length, PART_SIZE * (partNumber + 1)) - offset;
                fileParts.add(new PartInfo(toDownloadSegment.fileName, partSize, offset, toDownloadSegment.getFilePath()));
                ++partNumber;
                remainingLength = Math.max(0, remainingLength - PART_SIZE);
            }
            partTracker.put(toDownloadSegment.getFileName(), new AtomicInteger(partNumber));
        }
        return fileParts;
    }

    private void downloadFileInternal(
        CancellableThreads cancellableThreads,
        List<PartInfo> toDownloadSegmentParts,
        Map<String, AtomicInteger> partTracker,
        Runnable onFileCompletion,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<Void> listener
    ) {
        final Queue<PartInfo> queue = new ConcurrentLinkedQueue<>(toDownloadSegmentParts);
        // Choose the minimum of:
        // - number of files to download
        // - max thread pool size
        // - "indices.recovery.max_concurrent_remote_store_streams" setting
        int threads = recoverySettings.limitRemoteStreams()
            ? Math.min(
                toDownloadSegmentParts.size(),
                Math.min(threadPool.info(ThreadPool.Names.REMOTE_RECOVERY).getMax(), recoverySettings.getMaxConcurrentRemoteStoreStreams())
            )
            : Math.min(toDownloadSegmentParts.size(), recoverySettings.getMaxConcurrentRemoteStoreStreams());
        final int optionalVirtualThreadQueueSize = recoverySettings.useVirtualThreads() ? threads * 10 : threads;
        logger.error("Starting download of {} files with {} threads", queue.size(), optionalVirtualThreadQueueSize);

        final ActionListener<Void> allPartsListener = new GroupedActionListener<>(
            ActionListener.map(listener, r -> null),
            optionalVirtualThreadQueueSize
        );
        for (int i = 0; i < optionalVirtualThreadQueueSize; i++) {
            copyOnePart(cancellableThreads, queue, partTracker, onFileCompletion, fileProgressTracker, allPartsListener);
        }
    }

    @SuppressWarnings("any")
    private void copyOnePart(
        CancellableThreads cancellableThreads,
        Queue<PartInfo> queue,
        Map<String, AtomicInteger> partTracker,
        Runnable onFileCompletion,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<Void> listener
    ) {
        final PartInfo part = queue.poll();
        if (part == null) {
            // Queue is empty, so notify listener we are done
            listener.onResponse(null);
        } else {
            ExecutorService executor = recoverySettings.useVirtualThreads()
                ? threadPool.virtual()
                : threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY);
            executor.submit(() -> AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    String blobName = directory.getExistingRemoteFilename(part.getFileName());
                    logger.error(
                        "Downloading file {} from {} to {} corresponding to {} {}",
                        part.getFileName(),
                        part.getOffset(),
                        part.getOffset() + part.getPartSize(),
                        blobName,
                        Thread.currentThread()
                    );
                    try {
                        cancellableThreads.executeIO(() -> {
                            InputStream inputStream = blobContainer.readBlob(blobName, part.getOffset(), PART_SIZE);
                            InputStreamContainer inputStreamContainer = new InputStreamContainer(
                                inputStream,
                                inputStream.available(),
                                part.getOffset()
                            );

                            FilePartWriter.write(part.getFilePath(), inputStreamContainer, directory.getRateLimiter());
                            fileProgressTracker.accept(part.getFileName(), part.getPartSize());

                            if (partTracker.get(part.getFileName()).decrementAndGet() == 0) {
                                IOUtils.fsync(part.getFilePath(), false);
                                onFileCompletion.run();
                            }

                        });
                    } catch (Exception e) {
                        // Clear the queue to stop any future processing, report the failure, then return
                        queue.clear();
                        listener.onFailure(e);
                        return null;
                    }
                    copyOnePart(cancellableThreads, queue, partTracker, onFileCompletion, fileProgressTracker, listener);
                    return null;
                }
            }));
        }
    }

    /**
     * Helper class to downloads files from a {@link RemoteSegmentStoreDirectory}
     * instance to a local {@link Directory} instance in parallel depending on thread
     * pool size and recovery settings.
     *
     * @opensearch.api
     */
    public static class FileInfo {
        private String fileName;
        private long length;
        private Path filePath;

        public FileInfo(String fileName, long length) {
            this.fileName = fileName;
            this.length = length;
        }

        public String getFileName() {
            return fileName;
        }

        public long getLength() {
            return length;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public void setLength(long length) {
            this.length = length;
        }

        public Path getFilePath() {
            return filePath;
        }

        public void setFilePath(Path filePath) {
            this.filePath = filePath;
        }

        @Override
        public String toString() {
            return "FileInfo{" + "fileName='" + fileName + '\'' + ", length=" + length + ", filePath=" + filePath + '}';
        }
    }

    /**
     * Helper class to downloads files from a {@link RemoteSegmentStoreDirectory}
     * instance to a local {@link Directory} instance in parallel depending on thread
     * pool size and recovery settings.
     *
     * @opensearch.api
     */
    public static class PartInfo {
        private String fileName;
        private long partSize;
        private long offset;
        private Path filePath;

        public PartInfo(String fileName, long partSize, long offset, Path filePath) {
            this.fileName = fileName;
            this.partSize = partSize;
            this.offset = offset;
            this.filePath = filePath;
        }

        public long getPartSize() {
            return partSize;
        }

        public void setPartSize(long partSize) {
            this.partSize = partSize;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public Path getFilePath() {
            return filePath;
        }

        public void setFilePath(Path filePath) {
            this.filePath = filePath;
        }
    }
}
