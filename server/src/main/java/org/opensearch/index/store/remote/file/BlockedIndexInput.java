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
import org.opensearch.action.ActionRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory;
import org.opensearch.index.store.remote.util.TransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class acts as a virtual file mechanism for the accessed files and only downloads the required blocks of the actual file.
 * Utilizes the BlobContainer interface to read files located within a repository.
 */
abstract class BlockedIndexInput extends IndexInput implements RandomAccessInput {

    public static final int DEFAULT_BLOCK_SIZE_SHIFT = 8;

    protected static final Map<String, IndexInput> EXISTING_BLOCKS = new HashMap<>();

    /**
     * Start offset of the virtual file : non-zero in the slice case
     */
    protected final long offset;
    /**
     * Length of the virtual file, smaller than actual file size if it's a slice
     */
    protected final long length;
    /**
     * Size of the file, larger than length if it's a slice
     */
    protected final long originalFileSize;
    /**
     * Underlying lucene directory to open blocks
     */
    protected final FSDirectory directory;
    /**
     * File Name
     */
    protected final String fileName;

    /**
     * BlobContainer interface for fetching blocks from repository
     */
    protected BlobContainer blobContainer;
    /**
     * Current block for read, it should be a cloned block always
     */
    protected IndexInput currentBlock;

    /**
     * ID of the current block
     */
    protected int currentBlockId;

    /**
     * A holder which holds the current block
     */
    protected final AtomicReference<IndexInput> currentBlockHolder = new AtomicReference<>();

    /**
     * Whether this index input is a clone or otherwise a master
     */
    protected final boolean isClone;

    // Variables needed for block calculation and fetching logic
    /**
     * Block size shift (default value is 13 = 8KB)
     */
    protected final int blockSizeShift;

    /**
     * Fixed block size
     */
    protected final int blockSize;

    /**
     * Block mask
     */
    protected final int blockMask;

    protected final long partSize;

    protected final TransferManager transferManager;

    /**
     * FileInfo contains snapshot metadata references for this IndexInput
     */
    protected final BlobStoreIndexShardSnapshot.FileInfo fileInfo;

    public BlockedIndexInput(BlobStoreIndexShardSnapshot.FileInfo fileInfo, FSDirectory directory, BlobContainer blobContainer, TransferManager transferManager) {
        this(
            "BlockedIndexInput(path=\"" + directory.getDirectory().toString() + "/" + fileInfo.physicalName() + "\")",
            fileInfo,
            0L,
            fileInfo.length(),
            false,
            directory,
            blobContainer,
            transferManager
        );
    }

    BlockedIndexInput(
        String resourceDescription,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        long offset,
        long length,
        boolean isClone,
        FSDirectory directory,
        BlobContainer blobContainer,
        TransferManager transferManager
    ) {
        super(resourceDescription);
        this.fileInfo = fileInfo;
        this.offset = offset;
        this.fileName = fileInfo.physicalName();
        this.originalFileSize = fileInfo.length();
        this.length = length;
        this.isClone = isClone;
        this.directory = directory;
        this.partSize = fileInfo.partSize().getBytes();
        this.blockSizeShift = DEFAULT_BLOCK_SIZE_SHIFT;
        this.blockSize = 1 << blockSizeShift;
        this.blockMask = blockSize - 1;
        this.blobContainer = blobContainer;
        this.transferManager = transferManager;
    }

    @Override
    public BlockedIndexInput clone() {
        BlockedIndexInput clone = buildSlice("clone", offset, length());
        // Ensures that clones may be positioned at the same point as the blocked file they were cloned from
        if (currentBlock != null) {
            clone.currentBlock = currentBlock.clone();
            clone.currentBlockId = currentBlockId;
            clone.currentBlockHolder.set(clone.currentBlock);
        }

        return clone;
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

        // The slice is seeked to the beginning.
        return buildSlice(sliceDescription, offset, length);
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    protected abstract BlockedIndexInput buildSlice(String sliceDescription, long offset, long length);

    @Override
    public void close() throws IOException {
        // current block
        if (currentBlock != null) {
            currentBlock.close();
            currentBlock = null;
            currentBlockId = 0;
            currentBlockHolder.set(null);
        }
    }

    @Override
    public long getFilePointer() {
        if (currentBlock == null) return 0L;
        return currentBlockStart() + currentBlockPosition() - offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (currentBlock == null) {
            // seek to the beginning
            seek(0);
        } else if (currentBlockPosition() >= blockSize) {
            int blockId = currentBlockId + 1;
            fetchBlock(blockId);
        }
        return currentBlock.readByte();
    }

    @Override
    public short readShort() throws IOException {
        if (currentBlock != null && Short.BYTES <= (blockSize - currentBlockPosition())) {
            return currentBlock.readShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (currentBlock != null && Integer.BYTES <= (blockSize - currentBlockPosition())) {
            return currentBlock.readInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (currentBlock != null && Long.BYTES <= (blockSize - currentBlockPosition())) {
            return currentBlock.readLong();
        } else {
            return super.readLong();
        }
    }

    @Override
    public final int readVInt() throws IOException {
        if (currentBlock != null && 5 <= (blockSize - currentBlockPosition())) {
            return currentBlock.readVInt();
        } else {
            return super.readVInt();
        }
    }

    @Override
    public final long readVLong() throws IOException {
        if (currentBlock != null && 9 <= (blockSize - currentBlockPosition())) {
            return currentBlock.readVLong();
        } else {
            return super.readVLong();
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }

        seekInternal(pos + offset);
    }

    /**
     * Seek to a block position, download the block if it's necessary
     * NOTE: the pos should be an adjusted position for slices
     */
    private void seekInternal(long pos) throws IOException {
        if (currentBlock == null || !isInCurrentBlockRange(pos)) {
            fetchBlock(getBlock(pos));
        }
        currentBlock.seek(getBlockOffset(pos));
    }

    /**
     * Check if pos in current block range
     * NOTE: the pos should be an adjusted position for slices
     */
    private boolean isInCurrentBlockRange(long pos) {
        long offset = pos - currentBlockStart();
        return offset >= 0 && offset < blockSize;
    }

    /**
     * Check if [pos, pos + len) in current block range
     * NOTE: the pos should be an adjusted position for slices
     */
    private boolean isInCurrentBlockRange(long pos, int len) {
        long offset = pos - currentBlockStart();
        return offset >= 0 && (offset + len) <= blockSize;
    }

    @Override
    public final byte readByte(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos)) {
            // the block contains the byte
            return ((RandomAccessInput) currentBlock).readByte(getBlockOffset(pos));
        } else {
            // the block does not have the byte, seek to the pos first
            seekInternal(pos);
            // then read the byte
            return currentBlock.readByte();
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos, Short.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) currentBlock).readShort(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readShort();
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos, Integer.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) currentBlock).readInt(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readInt();
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos, Long.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) currentBlock).readLong(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readLong();
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (currentBlock == null) {
            // lazy seek to the beginning
            seek(0);
        }

        int available = blockSize - currentBlockPosition();
        if (len <= available) {
            // the block contains enough data to satisfy this request
            currentBlock.readBytes(b, offset, len);
        } else {
            // the block does not have enough data. First serve all we've got.
            if (available > 0) {
                currentBlock.readBytes(b, offset, available);
                offset += available;
                len -= available;
            }

            // and now, read the remaining 'len' bytes:
            // len > blocksize example: FST <init>
            while (len > 0) {
                int blockId = currentBlockId + 1;
                int toRead = Math.min(len, blockSize);
                fetchBlock(blockId);
                currentBlock.readBytes(b, offset, toRead);
                offset += toRead;
                len -= toRead;
            }
        }

    }

    private void fetchBlock(int blockId) throws IOException {
        if (currentBlock != null && currentBlockId == blockId) return;

        // close the current block before jumping to the new block
        if (currentBlock != null) {
            currentBlock.close();
        }

        Future<IndexInput> indexInputAsync = downloadBlockAsync(blockId);
        try {
            currentBlock = indexInputAsync.get();
            currentBlockId = blockId;
            currentBlockHolder.set(currentBlock);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<IndexInput> downloadBlockAsync(int blockId) throws IOException {
        final String blockFileName = fileName + "." + blockId;

        final long blockStart = getBlockStart(blockId);
        final long blockEnd = blockStart + getActualBlockSize(blockId);
        final int part = (int) (blockStart / partSize);
        final long partStart = part * partSize;

        final long position = blockStart - partStart;
        final long offset = blockEnd - blockStart - partStart;

        TransferManager.DownloadRequest downloadRequest = new TransferManager.DownloadRequest(position, offset,
            fileInfo.partName(part), directory, blockFileName);
        return transferManager.download(downloadRequest);
    }

    protected int getBlock(long pos) {
        return (int) (pos >>> blockSizeShift);
    }

    protected int getBlockOffset(long pos) {
        return (int) (pos & blockMask);
    }

    protected long getBlockStart(int blockId) {
        return (long) blockId << blockSizeShift;
    }

    protected long currentBlockStart() {
        return getBlockStart(currentBlockId);
    }

    protected int currentBlockPosition() {
        return (int) currentBlock.getFilePointer();
    }

    protected int getBlockNum() {
        return (int) ((originalFileSize - 1) >>> blockSizeShift) + 1;
    }

    protected long getActualBlockSize(int blockId) {
        return (blockId != getBlock(originalFileSize - 1)) ? blockSize : getBlockOffset(originalFileSize - 1) + 1;
    }

}
