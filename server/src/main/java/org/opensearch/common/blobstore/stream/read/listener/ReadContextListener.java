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
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReadContextListener orchestrates the async file fetch from the {@link org.opensearch.common.blobstore.BlobContainer}
 * using a {@link ReadContext} callback. On response, it spawns off the download using multiple streams which are
 * spread across a {@link ThreadPool} executor.
 */
@InternalApi
public class ReadContextListener implements ActionListener<ReadContext> {

    private final String fileName;
    private final Path fileLocation;
    private final ThreadPool threadPool;
    private final ActionListener<String> completionListener;
    private static final Logger logger = LogManager.getLogger(ReadContextListener.class);

    public ReadContextListener(String fileName, Path fileLocation, ThreadPool threadPool, ActionListener<String> completionListener) {
        this.fileName = fileName;
        this.fileLocation = fileLocation;
        this.threadPool = threadPool;
        this.completionListener = completionListener;
    }

    @Override
    public void onResponse(ReadContext readContext) {
        logger.info("Streams received for blob {}", fileName);
        final int numParts = readContext.getNumberOfParts();
        final AtomicBoolean anyPartStreamFailed = new AtomicBoolean();
        FileCompletionListener fileCompletionListener = new FileCompletionListener(numParts, fileName, completionListener);
        logger.info("THE threadpool name here is : "+Thread.currentThread().getName()+" no.of active threads before creating file part writers : "+Thread.activeCount() + " and no.of parts are : "+numParts);

        for (int partNumber = 0; partNumber < numParts; partNumber++) {
            FilePartWriter filePartWriter = new FilePartWriter(
                partNumber,
                readContext.getPartStreams().get(partNumber),
                fileLocation,
                anyPartStreamFailed,
                fileCompletionListener
            );
            logger.info("creating a new thread in sample test threadpool");
            threadPool.executor(ThreadPool.Names.SAMPLETEST).submit(filePartWriter);
        }
        logger.info("Streams completed for blob {}", fileName);
        logger.info("no.of active threads after creating file part writers : "+Thread.activeCount());
    }

    @Override
    public void onFailure(Exception e) {
        completionListener.onFailure(e);
    }
}
