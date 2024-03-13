/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.InputStream;

/**
 * Model composed of an input stream and the total content length of the stream
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InputStreamContainer {

    private final InputStream inputStream;
    private final long contentLength;
    private final long offset;
    private final Integer partNumber;

    /**
     * Construct a new stream object
     *
     * @param inputStream   The input stream that is to be encapsulated
     * @param contentLength The total content length that is to be read from the stream
     */
    public InputStreamContainer(InputStream inputStream, long contentLength, long offset) {
        this(inputStream, contentLength, offset, null);
    }

    public InputStreamContainer(InputStream inputStream, long contentLength, long offset, Integer partNumber) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
        this.offset = offset;
        this.partNumber = partNumber;
    }

    /**
     * @return The input stream this object is reading from
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * @return The total length of the content that has to be read from this stream
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * @return offset of the source content.
     */
    public long getOffset() {
        return offset;
    }

    public Integer getPartNumber() {
        return partNumber;
    }
}
