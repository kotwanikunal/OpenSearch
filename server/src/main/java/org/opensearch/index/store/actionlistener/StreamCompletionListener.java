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
    private final ActionListener<String> listener;

    public StreamCompletionListener(
        String partFileName,
        Directory segmentDirectory,
        AtomicBoolean anyStreamFailed,
        ActionListener<String> listener
    ) {
        this.partFileName = partFileName;
        this.segmentDirectory = segmentDirectory;
        this.anyStreamFailed = anyStreamFailed;
        this.listener = listener;
    }

    @Override
    public void onResponse(InputStream inputStream) {
        if (!anyStreamFailed.get()) {
            try (final IndexOutput segmentOutput = segmentDirectory.createOutput(partFileName, IOContext.DEFAULT); inputStream) {
                byte[] buffer = new byte[inputStream.available()];
                while ((inputStream.read(buffer)) != -1) {
                    segmentOutput.writeBytes(buffer, 0, buffer.length);
                }
            } catch (IOException e) {
                listener.onFailure(e);
            }
            listener.onResponse(partFileName);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            segmentDirectory.deleteFile(partFileName);
        } catch (IOException ex) {
            // Die silently?
        }
        listener.onFailure(e);
    }
}
