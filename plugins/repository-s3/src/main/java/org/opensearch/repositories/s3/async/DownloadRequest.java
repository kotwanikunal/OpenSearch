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
    private final int partNumber;

    public DownloadRequest(String bucket, String key, int partNumber) {
        this.bucket = bucket;
        this.key = key;
        this.partNumber = partNumber;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public int getPartNumber() {
        return partNumber;
    }

}
