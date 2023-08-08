/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import java.io.InputStream;
import java.util.List;

/**
 * WriteContext is used to encapsulate all data needed by <code>BlobContainer#readStreams</code>
 *
 * @opensearch.internal
 */
public class ReadContext {
    private final List<InputStream> blobInputStreams;

    private final String blobChecksum;

    private final int numStreams;

    private final long blobSize;

    public ReadContext(List<InputStream> blobInputStreams, String blobChecksum, int numStreams, long blobSize) {
        this.blobInputStreams = blobInputStreams;
        this.blobChecksum = blobChecksum;
        this.numStreams = numStreams;
        this.blobSize = blobSize;
    }

    public List<InputStream> getBlobInputStreams() {
        return blobInputStreams;
    }

    public String getBlobChecksum() {
        return blobChecksum;
    }

    public int getNumStreams() {
        return numStreams;
    }

    public long getBlobSize() {
        return blobSize;
    }
}
