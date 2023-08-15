/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart.mocks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.AfterClass;
import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.VerifyingMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MockFsVerifyingBlobContainer extends FsBlobContainer implements VerifyingMultiStreamBlobContainer {

    private static final int TRANSFER_TIMEOUT_MILLIS = 30000;

    private final boolean triggerDataIntegrityFailure;

    private static final Logger logger = LogManager.getLogger(MockFsVerifyingBlobContainer.class);

    public MockFsVerifyingBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, boolean triggerDataIntegrityFailure) {
        super(blobStore, blobPath, path);
        this.triggerDataIntegrityFailure = triggerDataIntegrityFailure;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        int nParts = 10;
        long partSize = writeContext.getFileSize() / nParts;
        StreamContext streamContext = writeContext.getStreamProvider(partSize);
        final Path file = path.resolve(writeContext.getFileName());
        byte[] buffer = new byte[(int) writeContext.getFileSize()];
        AtomicLong totalContentRead = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(streamContext.getNumberOfParts());
        for (int partIdx = 0; partIdx < streamContext.getNumberOfParts(); partIdx++) {
            int finalPartIdx = partIdx;
            Thread thread = new Thread(() -> {
                try {
                    InputStreamContainer inputStreamContainer = streamContext.provideStream(finalPartIdx);
                    InputStream inputStream = inputStreamContainer.getInputStream();
                    long remainingContentLength = inputStreamContainer.getContentLength();
                    long offset = partSize * finalPartIdx;
                    while (remainingContentLength > 0) {
                        int readContentLength = inputStream.read(buffer, (int) offset, (int) remainingContentLength);
                        totalContentRead.addAndGet(readContentLength);
                        remainingContentLength -= readContentLength;
                        offset += readContentLength;
                    }
                    inputStream.close();
                } catch (IOException e) {
                    completionListener.onFailure(e);
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        try {
            if (!latch.await(TRANSFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IOException("Timed out waiting for file transfer to complete for " + writeContext.getFileName());
            }
        } catch (InterruptedException e) {
            throw new IOException("Await interrupted on CountDownLatch, transfer failed for " + writeContext.getFileName());
        }
        try (OutputStream outputStream = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
            outputStream.write(buffer);
        }
        if (writeContext.getFileSize() != totalContentRead.get()) {
            throw new IOException(
                    "Incorrect content length read for file "
                            + writeContext.getFileName()
                            + ", actual file size: "
                            + writeContext.getFileSize()
                            + ", bytes read: "
                            + totalContentRead.get()
            );
        }

        try {
            // bulks need to succeed for segment files to be generated
            if (isSegmentFile(writeContext.getFileName()) && triggerDataIntegrityFailure) {
                completionListener.onFailure(
                        new RuntimeException(
                                new CorruptIndexException(
                                        "Data integrity check failure for file: " + writeContext.getFileName(),
                                        writeContext.getFileName()
                                )
                        )
                );
            } else {
                writeContext.getUploadFinalizer().accept(true);
                completionListener.onResponse(null);
            }
        } catch (Exception e) {
            completionListener.onFailure(e);
        }

    }

    @Override
    public void readBlobAsync(String blobName, long position, long length, ActionListener<InputStream> listener) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        try {
            executorService.submit(() -> {
                try {
                    InputStream inputStream = readBlob(blobName, position, length);
                    listener.onResponse(inputStream);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } finally {
            executorService.shutdown();
        }
    }

    private boolean isSegmentFile(String filename) {
        return !filename.endsWith(".tlog") && !filename.endsWith(".ckp");
    }
}
