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
import org.apache.lucene.store.IndexOutput;
import org.opensearch.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamCompletionListener implements ActionListener<InputStream> {
    private final String partFileName;
    private final Directory segmentDirectory;
    private final AtomicBoolean anyStreamFailed;
    private final ActionListener<String> fileCompletionListener;

    public StreamCompletionListener(
        String partFileName,
        Directory segmentDirectory,
        AtomicBoolean anyStreamFailed,
        ActionListener<String> fileCompletionListener
    ) {
        this.partFileName = partFileName;
        this.segmentDirectory = segmentDirectory;
        this.anyStreamFailed = anyStreamFailed;
        this.fileCompletionListener = fileCompletionListener;
    }

    @Override
    public void onResponse(InputStream inputStream) {
        try (inputStream) {
            // Do not write new segments if any stream for this file has already failed
            if (!anyStreamFailed.get()) {
                try (final IndexOutput segmentOutput = segmentDirectory.createOutput(partFileName, IOContext.DEFAULT)) {
                    byte[] buffer = new byte[inputStream.available()];
                    while ((inputStream.read(buffer)) != -1) {
                        segmentOutput.writeBytes(buffer, 0, buffer.length);
                    }
                }
                fileCompletionListener.onResponse(partFileName);
            }
        } catch (IOException e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        fileCompletionListener.onFailure(e);
    }
}
