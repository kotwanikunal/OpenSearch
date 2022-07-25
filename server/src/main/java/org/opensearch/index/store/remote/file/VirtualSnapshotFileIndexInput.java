/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.nio.file.Path;

import static org.opensearch.core.internal.io.IOUtils.closeWhileHandlingException;

public final class VirtualSnapshotFileIndexInput extends VirtualFileIndexInput {

    /**
     * FileInfo contains snapshot metadata references for this IndexInput
     */
    private FileInfo fileInfo;

    VirtualSnapshotFileIndexInput(Builder builder) {
        super(builder.fileBuilder);
        this.fileInfo = builder.fileInfo;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this
            );
        }

        // The slice is started at the beginning position.
        return buildSlice(sliceDescription, offset, length);
    }

    @Override
    public VirtualSnapshotFileIndexInput clone() {
        VirtualSnapshotFileIndexInput clone = buildSlice("clone", 0, length());
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        if (currentInput != null) {
            clone.currentInput = currentInput.clone();
            clone.currentInputHolder.set(clone.currentInput);
        }

        return clone;
    }

    /**
     * Builds the actual sliced IndexInput.
     **/
    private VirtualSnapshotFileIndexInput buildSlice(String sliceDescription, long offset, long length) {
        VirtualSnapshotFileIndexInput slice = builder().fileInfo(this.fileInfo)
            .fileBuilder(
                new RemoteFileBuilder(this.fileName, fileInfo.length(), length).directory(this.directory)
                    .offset(this.offset + offset)
                    .clone(true)
                    .resourceDescription(getFullSliceDescription(sliceDescription))
            )
            .build();

        return slice;
    }

    @Override
    public void downloadTo(final Path filePath) throws IOException {
        IndexOutput output = null;
        try {
            output = createVerifyingOutput(directory, filePath.toString(), fileInfo.metadata(), IOContext.DEFAULT);
            final BytesRef hash = fileInfo.metadata().hash();
            output.writeBytes(hash.bytes, hash.offset, hash.length);
            Store.verify(output);
        } catch (Exception e) {
            throw e;
        } finally {
            // in both success/failure case we want to close the IndexOutput
            if (output != null) {
                closeWhileHandlingException(output);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private VirtualFileIndexInput.RemoteFileBuilder fileBuilder;

        // File info about the s3 blob
        private FileInfo fileInfo;

        private Builder() {

        }

        public FileInfo getFileInfo() {
            return fileInfo;
        }

        public Builder fileInfo(FileInfo fileInfo) {
            this.fileInfo = fileInfo;
            return this;
        }

        public VirtualFileIndexInput.RemoteFileBuilder getFileBuilder() {
            return fileBuilder;
        }

        public Builder fileBuilder(VirtualFileIndexInput.RemoteFileBuilder fileBuilder) {
            this.fileBuilder = fileBuilder;
            return this;
        }

        public VirtualSnapshotFileIndexInput build() {
            return new VirtualSnapshotFileIndexInput(this);
        }
    }

    /*
        Access modifiers class and methods within the Store class had to be updated to enable VerifyingOutput
        TODO: Find an alternative which can create the IndexOutput without higher level changes.
     */
    public static IndexOutput createVerifyingOutput(
        Directory directory,
        String fileName,
        final StoreFileMetadata metadata,
        final IOContext context
    ) throws IOException {
        IndexOutput output = null;
        try {
            output = directory.createOutput(fileName, context);
            assert metadata.writtenBy() != null;
            output = new Store.LuceneVerifyingIndexOutput(metadata, output);
        } catch (Exception ex) {
            if (output != null) {
                closeWhileHandlingException(output);
            }
            throw ex;
        }
        return output;
    }
}
