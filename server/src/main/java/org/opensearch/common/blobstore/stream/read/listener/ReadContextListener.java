/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.io.Channels;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * ReadContextListener orchestrates the async file fetch from the {@link org.opensearch.common.blobstore.BlobContainer}
 * using a {@link ReadContext} callback. On response, it spawns off the download using multiple streams.
 */
@InternalApi
public class ReadContextListener implements ActionListener<ReadContext> {

    private final String fileName;
    private final Path fileLocation;
    private final ActionListener<String> completionListener;
    private final ThreadPool threadPool;
    private static final Logger logger = LogManager.getLogger(ReadContextListener.class);

    public ReadContextListener(String fileName, Path fileLocation, ActionListener<String> completionListener, ThreadPool threadPool) {
        this.fileName = fileName;
        this.fileLocation = fileLocation;
        this.completionListener = completionListener;
        this.threadPool = threadPool;
    }

    @Override
    public void onResponse(ReadContext readContext) {
        logger.trace("Streams received for blob {}", fileName);
        final int numParts = readContext.getNumberOfParts();
        final AtomicBoolean anyPartStreamFailed = new AtomicBoolean(false);
        final FileCompletionListener fileCompletionListener = new FileCompletionListener(numParts, fileName, completionListener);
        final Queue<Supplier<CompletableFuture<InputStreamContainer>>> queue = new ConcurrentLinkedQueue<>(readContext.getPartStreams());
        final PartStreamProcessor processor = new PartStreamProcessor(queue, anyPartStreamFailed, fileLocation, fileCompletionListener, threadPool.executor(
            ThreadPool.Names.REMOTE_RECOVERY));
        processor.start();
    }

    @Override
    public void onFailure(Exception e) {
        completionListener.onFailure(e);
    }

    private static class PartStreamProcessor {
        private static final int BUFFER_SIZE = 8 * 1024 * 2024;

        private final Queue<Supplier<CompletableFuture<InputStreamContainer>>> queue;
        private final AtomicBoolean anyPartStreamFailed;
        private final Path fileLocation;
        private final ActionListener<Integer> completionListener;
        private final Executor executor;

        private PartStreamProcessor(Queue<Supplier<CompletableFuture<InputStreamContainer>>> queue, AtomicBoolean anyPartStreamFailed,
            Path fileLocation,
            ActionListener<Integer> completionListener,
            Executor executor) {
            this.queue = queue;
            this.anyPartStreamFailed = anyPartStreamFailed;
            this.fileLocation = fileLocation;
            this.completionListener = completionListener;
            this.executor = executor;
        }

        void start() {
            // TODO: starting 50 processors concurrently is an arbitrary choice. Need something better here.
            for (int i = 0; i < 50; i++) {
                process(queue.poll());
            }
        }

        void process(Supplier<CompletableFuture<InputStreamContainer>> supplier) {
            if (supplier == null) {
                return;
            }
            supplier.get().whenCompleteAsync((blobPartStreamContainer, throwable) -> {
                if (throwable != null) {
                    if (throwable instanceof Exception) {
                        processFailure((Exception) throwable);
                    } else {
                        processFailure(new Exception(throwable));
                    }
                    return;
                }
                // Ensures no writes to the file if any stream fails.
                if (anyPartStreamFailed.get() == false) {
                    try (FileChannel outputFileChannel = FileChannel.open(fileLocation, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE))
                    {
                        logger.info("{} Writing {}[{}-{}]", Thread.currentThread().getName(), fileLocation, blobPartStreamContainer.getOffset(), blobPartStreamContainer.getOffset() + blobPartStreamContainer.getContentLength());
                        try (InputStream inputStream = blobPartStreamContainer.getInputStream()) {
                            long streamOffset = blobPartStreamContainer.getOffset();
                            final byte[] buffer = new byte[BUFFER_SIZE];
                            int bytesRead;
                            while ((bytesRead = inputStream.read(buffer)) != -1) {
                                Channels.writeToChannel(buffer, 0, bytesRead, outputFileChannel, streamOffset);
                                streamOffset += bytesRead;
                            }
                        }
                    } catch (IOException e) {
                        completionListener.onFailure(e);
                        return;
                    }
                    completionListener.onResponse(0);
                    process(queue.poll());
                }
            }, executor);
        }

        void processFailure(Exception e) {
            try {
                Files.deleteIfExists(fileLocation);
            } catch (IOException ex) {
                // Die silently
                logger.info("Failed to delete file {} on stream failure: {}", fileLocation, ex);
            }
            if (anyPartStreamFailed.getAndSet(true) == false) {
                completionListener.onFailure(e);
            }
        }
    }
}
