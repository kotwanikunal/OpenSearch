/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.stream.listener.FileCompletionListener;
import org.opensearch.common.blobstore.stream.listener.StreamCompletionListener;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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

    default String blobChecksum(String blobName) throws IOException {
        throw new UnsupportedOperationException();
    }

    default void asyncBlobDownload(String blobName, Path segmentFileLocation, ActionListener<String> segmentCompletionListener) {
        try {
            final long segmentSize = listBlobs().get(blobName).length();
            final long optimalStreamSize = readBlobPreferredLength();
            final int numStreams = (int) Math.ceil(segmentSize * 1.0 / optimalStreamSize);

            final AtomicBoolean anyStreamFailed = new AtomicBoolean();
            final List<String> partFileNames = Collections.synchronizedList(new ArrayList<>());

            final String segmentFileName = segmentFileLocation.getFileName().toString();
            final Path segmentDirectory = segmentFileLocation.getParent();

            final FileCompletionListener fileCompletionListener = new FileCompletionListener(
                numStreams,
                segmentFileName,
                segmentDirectory,
                partFileNames,
                anyStreamFailed,
                segmentCompletionListener
            );

            for (int streamNumber = 0; streamNumber < numStreams; streamNumber++) {
                String partFileName = UUID.randomUUID().toString();
                long start = streamNumber * optimalStreamSize;
                long end = Math.min(segmentSize, ((streamNumber + 1) * optimalStreamSize));
                long length = end - start;
                partFileNames.add(partFileName);

                final StreamCompletionListener streamCompletionListener = new StreamCompletionListener(
                    partFileName,
                    segmentDirectory,
                    anyStreamFailed,
                    fileCompletionListener
                );
                readBlobAsync(blobName, start, length, streamCompletionListener);
            }
        } catch (Exception e) {
            segmentCompletionListener.onFailure(e);
        }

    }
}
