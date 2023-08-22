/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An extension of {@link BlobContainer} that adds {@link VerifyingMultiStreamBlobContainer#asyncBlobUpload} to allow
 * multipart uploads and performs integrity checks on transferred files
 *
 * @opensearch.internal
 */
public interface VerifyingMultiStreamBlobContainer extends BlobContainer {

    Logger logger = LogManager.getLogger(VerifyingMultiStreamBlobContainer.class);

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

    default void asyncBlobDownload(String blobName, Path segmentFileLocation, ActionListener<String> segmentCompletionListener) {
        try {
            ActionListener<ReadContext> readBlobListener = new ActionListener<>() {
                @Override
                public void onResponse(ReadContext readContext) {
                    int numParts = readContext.getNumberOfParts();
                    for (int partNumber = 0; partNumber < numParts; partNumber++) {
                        logger.error(
                            "[MultiStream] Started part {} download  for {}",
                            partNumber,
                            segmentFileLocation.getFileName().toString()
                        );
                        try (
                            FileChannel fileChannel = FileChannel.open(
                                segmentFileLocation,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE
                            )
                        ) {
                            InputStreamContainer inputStreamContainer = readContext.provideStream(partNumber);
                            long offset = inputStreamContainer.getOffset();
                            long partSize = inputStreamContainer.getContentLength();
                            try (InputStream inputStream = inputStreamContainer.getInputStream()) {
                                fileChannel.transferFrom(Channels.newChannel(inputStream), offset, partSize);
                                logger.error(
                                    "[MultiStream] Completed part {} download  for {}",
                                    partNumber,
                                    segmentFileLocation.getFileName().toString()
                                );
                            }
                        } catch (IOException e) {
                            segmentCompletionListener.onFailure(e);
                        }
                    }
                    segmentCompletionListener.onResponse(blobName);
                }

                @Override
                public void onFailure(Exception e) {
                    segmentCompletionListener.onFailure(e);
                }
            };

            readBlobAsync(blobName, readBlobListener);
        } catch (Exception e) {
            segmentCompletionListener.onFailure(e);
        }
    }
}
