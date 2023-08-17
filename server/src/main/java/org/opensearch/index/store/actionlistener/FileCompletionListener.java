/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.actionlistener;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.action.ActionListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FileCompletionListener implements ActionListener<String> {
    private final int numStreams;
    private final String segmentFileName;
    private final Directory segmentDirectory;
    private final List<String> toDownloadPartFileNames;
    private final AtomicInteger downloadedPartFiles;
    private final AtomicBoolean anyStreamFailed;
    private final ActionListener<String> segmentCompletionListener;

    public FileCompletionListener(
        int numStreams,
        String segmentFileName,
        Directory segmentDirectory,
        List<String> toDownloadPartFileNames,
        AtomicBoolean anyStreamFailed,
        ActionListener<String> segmentCompletionListener
    ) {
        this.downloadedPartFiles = new AtomicInteger();
        this.numStreams = numStreams;
        this.segmentFileName = segmentFileName;
        this.segmentDirectory = segmentDirectory;
        this.anyStreamFailed = anyStreamFailed;
        this.toDownloadPartFileNames = toDownloadPartFileNames;
        this.segmentCompletionListener = segmentCompletionListener;
    }

    @Override
    public void onResponse(String streamFileName) {
        if (downloadedPartFiles.incrementAndGet() == numStreams) {
            try (IndexOutput segmentOutput = segmentDirectory.createOutput(segmentFileName, IOContext.DEFAULT)) {
                for (String partFileName : toDownloadPartFileNames) {
                    try (IndexInput indexInput = segmentDirectory.openInput(partFileName, IOContext.DEFAULT)) {
                        for (int i = 0; i < indexInput.length(); i++) {
                            segmentOutput.writeByte(indexInput.readByte());
                        }
                    }
                    segmentDirectory.deleteFile(partFileName);
                }
            } catch (IOException e) {
                onFailure(e);
            }
            segmentCompletionListener.onResponse(segmentFileName);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            Set<String> tempFilesInDirectory = new HashSet<>(Arrays.asList(segmentDirectory.listAll()));
            if (tempFilesInDirectory.contains(segmentFileName)) {
                segmentDirectory.deleteFile(segmentFileName);
            }

            tempFilesInDirectory.retainAll(toDownloadPartFileNames);
            for (String tempFile : tempFilesInDirectory) {
                segmentDirectory.deleteFile(tempFile);
            }

        } catch (IOException ex) {
            // Die silently?
        }

        if (!anyStreamFailed.get()) {
            segmentCompletionListener.onFailure(e);
            anyStreamFailed.compareAndSet(false, true);
        }
    }
}
