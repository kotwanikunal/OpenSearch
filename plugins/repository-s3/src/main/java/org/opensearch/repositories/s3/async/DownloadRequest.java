/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

/**
 * A model encapsulating all details for a download to S3
 */
public class DownloadRequest {
    private final String bucket;
    private final String key;
    private final long contentLength;
    private final long start;
    private final long end;

    public DownloadRequest(String bucket, String key, long contentLength, long start, long end) {
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.start = start;
        this.end = end;
    }

    public DownloadRequest(String bucket, String key, long contentLength) {
        this(bucket, key, contentLength, -1, -1);
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public long getContentLength() {
        return contentLength;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }
}
