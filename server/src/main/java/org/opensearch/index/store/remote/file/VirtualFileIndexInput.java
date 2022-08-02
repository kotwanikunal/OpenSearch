/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class acts as a virtual file mechanism for the accessed files and only downloads the file only when its read.
 */
public abstract class VirtualFileIndexInput extends IndexInput implements RandomAccessInput {

    /**
     * start offset of the file : non-zero index input the slice case
     */
    protected final long offset;
    /**
     * length of the index input, smaller than underlying file size if it's a slice
     */
    protected final long length;

    /**
     * size of the file, larger than length if it's a slice
     */
    protected final long originalFileSize;

    /**
     * underlying lucene directory to open file
     */
    protected final FSDirectory directory;

    /**
     * file name
     */
    protected final String fileName;

    /**
     * working index input for read, it should be a cloned index input always
     */
    protected IndexInput currentInput;

    /**
     * a holder which holds the working index input
     */
    protected final AtomicReference<IndexInput> currentInputHolder = new AtomicReference<>();

    /**
     * whether this index input is a clone or otherwise a master
     */
    protected final boolean isClone;

    VirtualFileIndexInput(RemoteFileBuilder fileBuilder) {
        super(fileBuilder.getResourceDescription());
        this.fileName = fileBuilder.name;
        this.offset = fileBuilder.offset;
        this.length = fileBuilder.length;
        this.originalFileSize = fileBuilder.fileSize;
        this.directory = fileBuilder.directory;
        this.isClone = fileBuilder.isClone;
    }

    @Override
    public void close() throws IOException {
        if (currentInput != null) {
            currentInput.close();
            currentInput = null;
            currentInputHolder.set(null);
        }
    }

    @Override
    public long getFilePointer() {
        if (currentInput == null) return 0L;
        return currentInput.getFilePointer() - offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        return currentInput.readByte();
    }

    @Override
    public short readShort() throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        return currentInput.readShort();
    }

    @Override
    public int readInt() throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        return currentInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        return currentInput.readLong();
    }

    @Override
    public final int readVInt() throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        return currentInput.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        return currentInput.readVLong();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (currentInput == null) {
            // download the file and seek to the beginning
            seek(0);
        }
        currentInput.readBytes(b, offset, len);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }

        ensureDownloaded();
        currentInput.seek(pos + offset);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        ensureDownloaded();
        return ((RandomAccessInput) currentInput).readByte(pos + offset);
    }

    @Override
    public short readShort(long pos) throws IOException {
        ensureDownloaded();
        return ((RandomAccessInput) currentInput).readShort(pos + offset);
    }

    @Override
    public int readInt(long pos) throws IOException {
        ensureDownloaded();
        return ((RandomAccessInput) currentInput).readInt(pos + offset);
    }

    @Override
    public long readLong(long pos) throws IOException {
        ensureDownloaded();
        return ((RandomAccessInput) currentInput).readLong(pos + offset);
    }

    private void ensureDownloaded() throws IOException {
        if (currentInput != null) return;

        final Path filePath = directory.getDirectory().resolve(fileName);

        // TODO: Replace this mechanism with cache check
        if (Files.notExists(filePath)) {
            downloadTo(filePath);
        }

        IndexInput luceneIndexInput = directory.openInput(fileName, IOContext.READ);
        currentInput = luceneIndexInput.clone();
        currentInputHolder.set(currentInput);

    }

    public abstract void downloadTo(final Path filePath) throws IOException;

    public static class RemoteFileBuilder {

        private String name;

        private long length;

        private long fileSize;

        private FSDirectory directory;

        /**
         * default offset is 0
         */
        private long offset = 0L;

        private String resourceDescription;

        private boolean isClone = false;

        public RemoteFileBuilder(String fileName, long fileSize, long length) {
            this.name = fileName;
            this.fileSize = fileSize;
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public RemoteFileBuilder name(String name) {
            this.name = name;
            return this;
        }

        public long getLength() {
            return length;
        }

        public RemoteFileBuilder length(long length) {
            this.length = length;
            return this;
        }

        public long getFileSize() {
            return fileSize;
        }

        public RemoteFileBuilder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public FSDirectory getDirectory() {
            return directory;
        }

        /**
         * Underlying directory to open lucene index input
         */
        public RemoteFileBuilder directory(FSDirectory directory) {
            this.directory = directory;
            return this;
        }

        public long getOffset() {
            return offset;
        }

        public RemoteFileBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public String getResourceDescription() {
            return resourceDescription != null
                ? resourceDescription
                : "RemoteFileIndexInput(path=\"" + directory.getDirectory().toString() + "/" + name + "\")";
        }

        public RemoteFileBuilder resourceDescription(String resourceDescription) {
            this.resourceDescription = resourceDescription;
            return this;
        }

        public boolean isClone() {
            return isClone;
        }

        RemoteFileBuilder clone(boolean isClone) {
            this.isClone = isClone;
            return this;
        }
    }
}
