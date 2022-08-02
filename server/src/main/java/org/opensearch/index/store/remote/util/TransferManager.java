/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.util;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.threadpool.ThreadPool;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public class TransferManager {

    private final BlobContainer blobContainer;

    private final ThreadPool threadPool;

    private final ConcurrentMap<Path, Future<IndexInput>> downloadMap = new ConcurrentHashMap<>();

    public TransferManager(final BlobContainer blobContainer, final ThreadPool threadPool) {
        this.blobContainer = blobContainer;
        this.threadPool = threadPool;
    }

    public Future<IndexInput> download(DownloadRequest downloadRequest){
        Callable<IndexInput> indexInputCall = new Callable<IndexInput>() {
            @Override
            public IndexInput call() throws Exception {
                // TODO: Replace this mechanism with cache check
                if (Files.notExists(downloadRequest.filePath)) {
                    // TODO: Add a buffer implementation instead of copying over entire stream.
                    try (
                        InputStream snapshotFileInputStream = blobContainer.readBlob(downloadRequest.blobName,
                            downloadRequest.position, downloadRequest.length);
                        OutputStream localFileOutputStream = new BufferedOutputStream(new FileOutputStream(downloadRequest.filePath.toFile()));
                    ) {
                        localFileOutputStream.write(snapshotFileInputStream.readAllBytes());
                    }
                }
                return downloadRequest.directory.openInput(downloadRequest.fileName, IOContext.READ);
            }
        };
        return threadPool.executor(ThreadPool.Names.REMOTE_STORE).submit(indexInputCall);
    }

    // TODO: Make this a builder class instead.
    public static class DownloadRequest {

        private final long position;

        private final long length;

        private final String blobName;

        private final Path filePath;

        private final Directory directory;

        private final String fileName;

        public DownloadRequest(long position, long length, String blobName, FSDirectory directory, String fileName) {
            this.position = position;
            this.length = length;
            this.blobName = blobName;
            this.filePath = directory.getDirectory().resolve(fileName);
            this.directory = directory;
            this.fileName = fileName;
        }
    }

}
