/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import org.apache.lucene.store.Directory;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Nullable;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.StreamContextSupplier;

import java.io.IOException;

/**
 * WriteContext is used to encapsulate all data needed by <code>BlobContainer#readStreams</code>
 *
 * @opensearch.internal
 */
public class ReadContext {

    private final String fileName;
    private final String remoteFileName;
    private final CheckedConsumer<Boolean, IOException> downloadFinalizer;
    private final boolean doRemoteDataIntegrityCheck;
    private Directory localDirectory;

    /**
     * Construct a new WriteContext object
     *
     * @param fileName                   The name of the file being downloaded
     * @param doRemoteDataIntegrityCheck A boolean to inform vendor plugins whether remote data integrity checks need to be done
     */
    public ReadContext(
        String remoteFileName,
        String fileName,
        CheckedConsumer<Boolean, IOException> downloadFinalizer,
        boolean doRemoteDataIntegrityCheck
    ) {
        this.remoteFileName = remoteFileName;
        this.fileName = fileName;
        this.downloadFinalizer = downloadFinalizer;
        this.doRemoteDataIntegrityCheck = doRemoteDataIntegrityCheck;
    }

    public void setLocalDirectory(Directory directory) {
        this.localDirectory = directory;
    }

    public Directory getLocalDirectory() {
        return this.localDirectory;
    }

    /**
     * @return The file name
     */
    public String getFileName() {
        return fileName;
    }

    public String getRemoteFileName() {
        return remoteFileName;
    }

    /**
     * @return The <code>UploadFinalizer</code> for this upload
     */
    public CheckedConsumer<Boolean, IOException> getDownloadFinalizer() {
        return downloadFinalizer;
    }

    /**
     * @return A boolean for whether remote data integrity check has to be done for this upload or not
     */
    public boolean doRemoteDataIntegrityCheck() {
        return doRemoteDataIntegrityCheck;
    }

}
