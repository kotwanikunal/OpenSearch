/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.io.Channels;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * FilePartWriter transfers the provided stream into the specified file path using a {@link FileChannel}
 * instance. It performs offset based writes to the file and notifies the {@link FileCompletionListener} on completion.
 */
@InternalApi
class FilePartWriter implements BiConsumer<InputStreamContainer, Throwable> {

    private final int partNumber;
    private final Path fileLocation;
    private final AtomicBoolean anyPartStreamFailed;
    private final ActionListener<Integer> fileCompletionListener;
    private static final Logger logger = LogManager.getLogger(FilePartWriter.class);

    // 8 MB buffer for transfer
    private static final int BUFFER_SIZE = 8 * 1024 * 2024;

    public FilePartWriter(
        int partNumber,
        Path fileLocation,
        AtomicBoolean anyPartStreamFailed,
        ActionListener<Integer> fileCompletionListener
    ) {
        this.partNumber = partNumber;
        this.fileLocation = fileLocation;
        this.anyPartStreamFailed = anyPartStreamFailed;
        this.fileCompletionListener = fileCompletionListener;
    }

    @Override
    public void accept(InputStreamContainer blobPartStreamContainer, Throwable throwable) {
        logger.error("[Kunal] Received FPW stream: {}, {}", fileLocation.getFileName(), partNumber);
        if (throwable != null) {
            if (throwable instanceof Exception) {
                processFailure((Exception) throwable);
            } else {
                processFailure(new Exception(throwable));
            }
            return;
        }
        // Ensures no writes to the file if any stream fails.
        if (anyPartStreamFailed.get() == false) {
            try (FileChannel outputFileChannel = FileChannel.open(fileLocation, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                logger.error("[Kunal] FPW Before input stream open: {}, {}", fileLocation.getFileName(), partNumber);
                try (InputStream inputStream = blobPartStreamContainer.getInputStream()) {
                    logger.error("[Kunal] FPW After input stream open: {}, {}", fileLocation.getFileName(), partNumber);
                    long streamOffset = blobPartStreamContainer.getOffset();
                    final byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        logger.error("[Kunal] FPW writing input stream open: {}, {}, bytes{}", fileLocation.getFileName(), partNumber, bytesRead);
                        Channels.writeToChannel(buffer, 0, bytesRead, outputFileChannel, streamOffset);
                        streamOffset += bytesRead;
                    }
                }
                if (Files.exists(fileLocation) == false) {
                    processFailure(new Exception("Stream could not be transferred"));
                    return;
                }
            } catch (IOException e) {
                logger.error("[Kunal] Exception FPW stream: ", e);
                processFailure(e);
                return;
            }
            logger.error("[Kunal] Exiting FPW stream: {}, {}", fileLocation.getFileName(), partNumber);
            fileCompletionListener.onResponse(partNumber);
        }
    }

    void processFailure(Exception e) {
        try {
            Files.deleteIfExists(fileLocation);
        } catch (IOException ex) {
            // Die silently
            logger.info("Failed to delete file {} on stream failure: {}", fileLocation, ex);
        }
        if (anyPartStreamFailed.getAndSet(true) == false) {
            fileCompletionListener.onFailure(e);
        }
    }
}
