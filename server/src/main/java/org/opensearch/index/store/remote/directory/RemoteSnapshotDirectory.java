/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.file.BlockedSnapshotIndexInput;
import org.opensearch.index.store.remote.file.VirtualFileIndexInput;
import org.opensearch.index.store.remote.file.VirtualSnapshotFileIndexInput;

public class RemoteSnapshotDirectory extends Directory {

    private final BlobContainer blobContainer;
    private final BlobStoreIndexShardSnapshot snapshot;
    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfoMap;

    private final FSDirectory fsDirectory;
    public static final Map<String, IndexInput> EXISTING_BLOCKS = new HashMap<>();

    protected final Logger logger;

    public RemoteSnapshotDirectory(BlobContainer blobContainer, BlobStoreIndexShardSnapshot snapshot, FSDirectory fsDirectory) {
        this.blobContainer = blobContainer;
        this.snapshot = snapshot;
        this.fileInfoMap = snapshot.indexFiles()
            .stream()
            .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, f -> f));
        this.fsDirectory = fsDirectory;
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public String[] listAll() throws IOException {
        return fileInfoMap.keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {}

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return NoopIndexOutput.INSTANCE;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final BlobStoreIndexShardSnapshot.FileInfo fi = fileInfoMap.get(name);

        // Virtual files are contained entirely in the metadata hash field
        if (fi.name().startsWith("v__")) {
            return VirtualSnapshotFileIndexInput.builder()
                .fileInfo(fileInfoMap.get(name))
                .fileBuilder(
                    new VirtualFileIndexInput.RemoteFileBuilder(name, fileInfoMap.get(name).length(), fileInfoMap.get(name).length())
                        .directory(fsDirectory)
                )
                .build();
        }
        return new BlockedSnapshotIndexInput(fi, fsDirectory, blobContainer);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long fileLength(String name) throws IOException {
        return fileInfoMap.get(name).length();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        // throw new UnsupportedOperationException();
        return Collections.emptySet();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        // throw new UnsupportedOperationException();

    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return NoLockFactory.INSTANCE.obtainLock(null, null);
    }

    static class NoopIndexOutput extends IndexOutput {

        final static NoopIndexOutput INSTANCE = new NoopIndexOutput();

        NoopIndexOutput() {
            super("noop", "noop");
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public long getFilePointer() {
            return 0;
        }

        @Override
        public long getChecksum() throws IOException {
            return 0;
        }

        @Override
        public void writeByte(byte b) throws IOException {

        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {

        }
    }

}
