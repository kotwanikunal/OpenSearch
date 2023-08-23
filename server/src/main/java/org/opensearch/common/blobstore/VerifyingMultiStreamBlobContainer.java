/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.stream.read.listener.ReadContextListener;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * An extension of {@link BlobContainer} that adds {@link VerifyingMultiStreamBlobContainer#asyncBlobUpload} to allow
 * multipart uploads and performs integrity checks on transferred files
 *
 * @opensearch.internal
 */
public interface VerifyingMultiStreamBlobContainer extends BlobContainer {

    /**
     * Reads blob content from multiple streams, each from a specific part of the file, which is provided by the
     * StreamContextSupplier in the WriteContext passed to this method. An {@link IOException} is thrown if reading
     * any of the input streams fails, or writing to the target blob fails
     *
     * @param writeContext         A WriteContext object encapsulating all information needed to perform the upload
     * @param completionListener   Listener on which upload events should be published.
     * @throws IOException if any of the input streams could not be read, or the target blob could not be written to
     */
    void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException;

    /**
     * Creates an async callback of an {@link java.io.InputStream} for the specified blob within the container.
     * An {@link IOException} is thrown if requesting the input stream fails.
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @param listener  Async listener for {@link InputStream} object which serves the input streams and other metadata for the blob
     */
    void readBlobAsync(String blobName, ActionListener<ReadContext> listener);

    default void asyncBlobDownload(
        String blobName,
        Path segmentFileLocation,
        ThreadPool threadPool,
        ActionListener<String> segmentCompletionListener
    ) {
        ReadContextListener readContextListener = new ReadContextListener(
            blobName,
            segmentFileLocation,
            threadPool,
            segmentCompletionListener
        );
        readBlobAsync(blobName, readContextListener);
    }
}
