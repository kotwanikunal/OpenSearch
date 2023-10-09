/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * EncryptedBlobContainer is an encrypted BlobContainer that is backed by a
 * {@link AsyncMultiStreamBlobContainer}
 *
 * @opensearch.internal
 */
public class AsyncMultiStreamEncryptedBlobContainer<T, U> extends EncryptedBlobContainer<T, U> implements AsyncMultiStreamBlobContainer {

    private static final Logger logger = LogManager.getLogger(AsyncMultiStreamEncryptedBlobContainer.class);
    private final AsyncMultiStreamBlobContainer blobContainer;
    private final CryptoHandler<T, U> cryptoHandler;

    public AsyncMultiStreamEncryptedBlobContainer(AsyncMultiStreamBlobContainer blobContainer, CryptoHandler<T, U> cryptoHandler) {
        super(blobContainer, cryptoHandler);
        this.blobContainer = blobContainer;
        this.cryptoHandler = cryptoHandler;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        EncryptedWriteContext<T, U> encryptedWriteContext = new EncryptedWriteContext<>(writeContext, cryptoHandler);
        blobContainer.asyncBlobUpload(encryptedWriteContext, completionListener);
    }

    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        try {
            final U cryptoContext = cryptoHandler.loadEncryptionMetadata(getEncryptedHeaderContentSupplier(blobName));
            logger.error("[Kunal] Crypto context loaded: {}", cryptoContext);
            ActionListener<ReadContext> decryptingCompletionListener =
                    ActionListener.wrap(readContext -> {
                        ReadContext decryptedReadContext = new DecryptedReadContext<>(readContext, cryptoHandler, cryptoContext, this, blobName);
                        listener.onResponse(decryptedReadContext);
                    }, listener::onFailure);
            logger.error("[Kunal] Decrypting action listener created: {}", decryptingCompletionListener);

            blobContainer.readBlobAsync(blobName, decryptingCompletionListener);
            logger.error("[Kunal] Called read blob async: {}", blobName);

        } catch (Exception e) {
            logger.error("[Kunal] Exception: ", e);
            listener.onFailure(e);
        }
    }

    @Override
    public boolean remoteIntegrityCheckSupported() {
        return false;
    }

    static class EncryptedWriteContext<T, U> extends WriteContext {

        private final T encryptionMetadata;
        private final CryptoHandler<T, U> cryptoHandler;
        private final long fileSize;

        /**
         * Construct a new encrypted WriteContext object
         */
        public EncryptedWriteContext(WriteContext writeContext, CryptoHandler<T, U> cryptoHandler) {
            super(writeContext);
            this.cryptoHandler = cryptoHandler;
            this.encryptionMetadata = this.cryptoHandler.initEncryptionMetadata();
            this.fileSize = this.cryptoHandler.estimateEncryptedLengthOfEntireContent(encryptionMetadata, writeContext.getFileSize());
        }

        public StreamContext getStreamProvider(long partSize) {
            long adjustedPartSize = cryptoHandler.adjustContentSizeForPartialEncryption(encryptionMetadata, partSize);
            StreamContext streamContext = super.getStreamProvider(adjustedPartSize);
            return new EncryptedStreamContext<>(streamContext, cryptoHandler, encryptionMetadata);
        }

        /**
         * @return The total size of the encrypted file
         */
        public long getFileSize() {
            return fileSize;
        }
    }

    static class EncryptedStreamContext<T, U> extends StreamContext {

        private final CryptoHandler<T, U> cryptoHandler;
        private final T encryptionMetadata;

        /**
         * Construct a new encrypted StreamContext object
         */
        public EncryptedStreamContext(StreamContext streamContext, CryptoHandler<T, U> cryptoHandler, T encryptionMetadata) {
            super(streamContext);
            this.cryptoHandler = cryptoHandler;
            this.encryptionMetadata = encryptionMetadata;
        }

        @Override
        public InputStreamContainer provideStream(int partNumber) throws IOException {
            InputStreamContainer inputStreamContainer = super.provideStream(partNumber);
            return cryptoHandler.createEncryptingStreamOfPart(encryptionMetadata, inputStreamContainer, getNumberOfParts(), partNumber);
        }

    }

    /**
     * DecryptedReadContext decrypts the encrypted {@link ReadContext} by acting as a transformation wrapper around
     * the encrypted object
     * @param <T> Encryption Metadata / CryptoContext for the {@link CryptoHandler} instance
     * @param <U> Parsed Encryption Metadata / CryptoContext for the {@link CryptoHandler} instance
     */
    static class DecryptedReadContext<T, U> extends ReadContext {

        private final CryptoHandler<T, U> cryptoHandler;
        private final U cryptoContext;
        private Long blobSize;

        private final EncryptedBlobContainer blobContainer;

        private final String blobName;

        public DecryptedReadContext(ReadContext readContext, CryptoHandler<T, U> cryptoHandler, U cryptoContext, EncryptedBlobContainer blobContainer, String blobName) {
            super(readContext);
            this.cryptoHandler = cryptoHandler;
            this.cryptoContext = cryptoContext;
            this.blobContainer = blobContainer;
            this.blobName = blobName;
        }

        @Override
        public long getBlobSize() {
            // initializes the value lazily
            logger.error("[Kunal] getBlobSize called");

            if (blobSize == null) {
                this.blobSize = this.cryptoHandler.estimateDecryptedLength(cryptoContext, super.getBlobSize());
                logger.error("[Kunal] blob size value initialized: {}", blobSize);
            }
            return this.blobSize;
        }

        @Override
        public List<CompletableFuture<InputStreamContainer>> getPartStreams() {
            logger.error("[Kunal] Super part streams: {}", super.getPartStreams());
            List<CompletableFuture<InputStreamContainer>> encryptedPartFutures = super.getPartStreams();
            List<CompletableFuture<InputStreamContainer>> convertedStreams = IntStream.range(0, encryptedPartFutures.size())
                    .mapToObj(index -> encryptedPartFutures.get(index)
                            .thenApply(container -> decryptInputStreamContainer(container, index)))
                    .collect(Collectors.toUnmodifiableList());

            logger.error("[Kunal] Converted part streams: {}", convertedStreams);
            return convertedStreams;
        }

        /**
         * Transforms an encrypted {@link InputStreamContainer} to a decrypted instance
         * @param inputStreamContainer encrypted input stream container instance
         * @return decrypted input stream container instance
         */
        private InputStreamContainer decryptInputStreamContainer(InputStreamContainer inputStreamContainer, int index) {
            final long blobSize = getBlobSize();
            final int numberOfParts = getNumberOfParts();
            final long partSize = blobSize / numberOfParts;

            U encryptionMetadata;
            try {
                encryptionMetadata = cryptoHandler.loadEncryptionMetadata(blobContainer.getEncryptedHeaderContentSupplier(blobName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            long position = partSize * index;
            long end = Math.min(partSize * (index + 1), blobSize) - 1;
            long length = end - position + 1;

            DecryptedRangedStreamProvider decryptedStreamProvider = cryptoHandler.createDecryptingStreamOfRange(
                encryptionMetadata,
                position,
                position + length - 1
            );

            InputStream encryptedStream = inputStreamContainer.getInputStream();
            final InputStream decryptedStream = decryptedStreamProvider.getDecryptedStreamProvider().apply(encryptedStream);
            return new InputStreamContainer(decryptedStream, length, position);
        }
    }
}
