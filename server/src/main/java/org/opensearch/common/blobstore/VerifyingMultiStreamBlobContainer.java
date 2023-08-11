/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

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
     * Creates and populates a list of {@link java.io.InputStream} from the blob stored within the repository, returned
     * within an async callback using the {@link ReadContext} object.
     * Defaults to using multiple streams, when feasible, unless a single stream is forced using @param forceSingleStream.
     * An {@link IOException} is thrown if requesting any of the input streams fails, or reading metadata for the
     * requested blob fails
     * @param blobName          Name of the blob to be read using the async mechanism
     * @param forceSingleStream Value to denote forced use of a single stream within the returned object
     * @param listener  Async listener for {@link ReadContext} object which serves the input streams and other metadata for the blob
     * @throws IOException if any of the input streams could not be requested, or reading metadata for requested blob fails
     */
    void asyncBlobDownload(String blobName, boolean forceSingleStream, ActionListener<ReadContext> listener) throws IOException;
}
