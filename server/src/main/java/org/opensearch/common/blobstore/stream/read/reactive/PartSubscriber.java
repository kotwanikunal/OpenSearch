/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.reactive;

import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class PartSubscriber implements Subscriber<ByteBuffer> {
    private final AtomicLong position;
    private final AsynchronousFileChannel fileChannel;
    private final Path path;
    private final ActionListener<String> completionListener;
    private final Consumer<Throwable> onErrorMethod;
    private volatile boolean writeInProgress = false;
    private volatile boolean closeOnLastWrite = false;
    private Subscription subscription;

    public PartSubscriber(
        AsynchronousFileChannel fileChannel,
        Path path,
        ActionListener<String> completionListener,
        Consumer<Throwable> onErrorMethod,
        long startingPosition
    ) {
        this.fileChannel = fileChannel;
        this.path = path;
        this.completionListener = completionListener;
        this.onErrorMethod = onErrorMethod;
        this.position = new AtomicLong(startingPosition);
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (this.subscription != null) {
            s.cancel();
            return;
        }
        this.subscription = s;
        // Request the first chunk to start producing content
        s.request(1);
    }

    @Override
    public void onNext(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            throw new NullPointerException("Element must not be null");
        }

        performWrite(byteBuffer);
    }

    private void performWrite(ByteBuffer byteBuffer) {
        writeInProgress = true;

        fileChannel.write(byteBuffer, position.get(), byteBuffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                position.addAndGet(result);

                if (byteBuffer.hasRemaining()) {
                    performWrite(byteBuffer);
                } else {
                    synchronized (PartSubscriber.this) {
                        writeInProgress = false;
                        if (closeOnLastWrite) {
                            close();
                        } else {
                            subscription.request(1);
                        }
                    }
                }
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                subscription.cancel();
                completionListener.onFailure(new Exception(exc));
            }
        });
    }

    @Override
    public void onError(Throwable t) {
        onErrorMethod.accept(t);
    }

    @Override
    public void onComplete() {
        // if write in progress, tell write to close on finish.
        synchronized (this) {
            if (writeInProgress) {
                closeOnLastWrite = true;
            } else {
                close();
            }
        }
    }

    private void close() {
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
            completionListener.onResponse(null);
        } catch (RuntimeException | IOException exception) {
            completionListener.onFailure(exception);
        }
    }
}
