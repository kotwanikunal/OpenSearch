/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.InputStreamContainer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * ReadContext is used to encapsulate all data needed by <code>BlobContainer#readBlobAsync</code>
 */
@ExperimentalApi
public class ReadContext {
    private static final Logger logger = LogManager.getLogger(ReadContext.class);
    private final long blobSize;
    private final List<CompletableFuture<InputStreamContainer>> asyncPartStreams;
    private final String blobChecksum;

    public ReadContext(long blobSize, List<CompletableFuture<InputStreamContainer>> asyncPartStreams, String blobChecksum) {
        this.blobSize = blobSize;
        this.asyncPartStreams = asyncPartStreams;
        this.blobChecksum = blobChecksum;
    }

    public ReadContext(ReadContext readContext) {
        this.blobSize = readContext.blobSize;
        this.asyncPartStreams = readContext.asyncPartStreams;
        this.blobChecksum = readContext.blobChecksum;
    }

    public String getBlobChecksum() {
        return blobChecksum;
    }

    public int getNumberOfParts() {
        return asyncPartStreams.size();
    }

    public long getBlobSize() {
        return blobSize;
    }

    public List<CompletableFuture<InputStreamContainer>> getPartStreams() {
        logger.error("[Kunal] ReadContext part streams: {}", asyncPartStreams);
        return asyncPartStreams;
    }
}
