/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import static org.opensearch.index.store.RemoteStoreFileDownloader.PART_SIZE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreFileDownloaderTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Directory source;
    private Directory destination;
    private Directory secondDestination;
    private RemoteStoreFileDownloader fileDownloader;
    private ShardPath shardPath;
    private Map<String, Integer> files = new HashMap<>();

    @Before
    public void setup() throws IOException {
        final int streamLimit = randomIntBetween(1, 20);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder()
                .put("indices.recovery.max_concurrent_remote_store_streams", streamLimit)
                .put("indices.recovery.use_virtual_threads", true)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        threadPool = new TestThreadPool(getTestName());
        source = new NIOFSDirectory(createTempDir());
        destination = new NIOFSDirectory(createTempDir());
        secondDestination = new NIOFSDirectory(createTempDir());
        BlobContainer blobContainer = mock(BlobContainer.class);
        RemoteSegmentStoreDirectory remoteDirectory = mock(RemoteSegmentStoreDirectory.class);
        when(remoteDirectory.getSegmentBlobContainer()).thenReturn(blobContainer);

        Path toDataPath = createTempDir().resolve("test").resolve("1");
        shardPath = new ShardPath(false, toDataPath, toDataPath, new ShardId("test", "test", 1));
        Files.createDirectories(shardPath.resolveIndex());

        for (int i = 0; i < 10; i++) {
            final String filename = "file_" + i;
            final int content = randomInt();
            try (IndexOutput output = source.createOutput(filename, IOContext.DEFAULT)) {
                output.writeInt(content);
                when(remoteDirectory.getExistingRemoteFilename(filename)).thenReturn(filename);
                when(remoteDirectory.getRateLimiter()).thenReturn(UnaryOperator.identity());
                when(blobContainer.readBlob(eq(filename), anyLong(), anyLong())).thenReturn(
                    new ByteArrayInputStream(randomByteArrayOfLength(32))
                );
            }
            files.put(filename, content);
        }
        fileDownloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            recoverySettings,
            shardPath,
            remoteDirectory
        );
    }

    @After
    public void stopThreadPool() throws Exception {
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testDownload() throws IOException {
        final PlainActionFuture<Void> l = new PlainActionFuture<>();
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, files.keySet(), l);
        l.actionGet();
        assertContent(files, destination);
    }

    public void testDownloadWithSecondDestination() throws IOException, InterruptedException {
        fileDownloader.download(source, destination, secondDestination, files.keySet(), () -> {});
        assertContent(files, destination);
        assertContent(files, secondDestination);
    }

    public void testDownloadWithFileCompletionHandler() throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        fileDownloader.download(source, destination, null, files.keySet(), counter::incrementAndGet);
        assertContent(files, destination);
        assertEquals(files.size(), counter.get());
    }

    public void testDownloadWithFileCompletionHandlerOther() throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        BiConsumer<String, Long> updateTracker = (a, b) -> {};
        List<RemoteStoreFileDownloader.FileInfo> fileInfos = files.keySet()
            .stream()
            .map(fileName -> new RemoteStoreFileDownloader.FileInfo(fileName, 32))
            .toList();
        fileDownloader.download(fileInfos, updateTracker, counter::incrementAndGet);
        assertContentMultiPart(files, shardPath.resolveIndex());
        assertEquals(files.size(), counter.get());
    }

    public void testDownloadNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, Set.of("not real"), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(NoSuchFileException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testDownloadExtraNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> filesWithExtra = new ArrayList<>(files.keySet());
        filesWithExtra.add("not real");
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, filesWithExtra, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(NoSuchFileException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testCancellable() {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> blockingListener = new PlainActionFuture<>();
        final Directory blockingDestination = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                try {
                    Thread.sleep(60_000); // Will be interrupted
                    fail("Expected to be interrupted");
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed due to interrupt", e);
                }
            }
        };
        fileDownloader.downloadAsync(cancellableThreads, source, blockingDestination, files.keySet(), blockingListener);
        assertThrows(
            "Expected to timeout due to blocking directory",
            OpenSearchTimeoutException.class,
            () -> blockingListener.actionGet(TimeValue.timeValueMillis(500))
        );
        cancellableThreads.cancel("test");
        assertThrows(
            "Expected to complete with cancellation failure",
            CancellableThreads.ExecutionCancelledException.class,
            blockingListener::actionGet
        );
    }

    public void testBlockingCallCanBeInterrupted() throws Exception {
        final Directory blockingDestination = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                try {
                    Thread.sleep(60_000); // Will be interrupted
                    fail("Expected to be interrupted");
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed due to interrupt", e);
                }
            }
        };
        final AtomicReference<Exception> capturedException = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                fileDownloader.download(source, blockingDestination, null, files.keySet(), () -> {});
            } catch (Exception e) {
                capturedException.set(e);
            }
        });
        thread.start();
        thread.interrupt();
        thread.join();
        assertEquals(InterruptedException.class, capturedException.get().getClass());
    }

    public void testPartsCreator() {
        Map<String, AtomicInteger> partsTracker = new ConcurrentHashMap<>();
        List<RemoteStoreFileDownloader.FileInfo> segments = List.of(
            new RemoteStoreFileDownloader.FileInfo("test1", PART_SIZE * 2),
            new RemoteStoreFileDownloader.FileInfo("test2", 100),
            new RemoteStoreFileDownloader.FileInfo("test3", (PART_SIZE * 100) + 1)
        );

        List<RemoteStoreFileDownloader.PartInfo> parts = fileDownloader.createParts(segments, partsTracker);
        assertEquals(2 + 1 + 101, parts.size());
        assertEquals(3, partsTracker.size());
        assertEquals(2, partsTracker.get("test1").get());
        assertEquals(1, partsTracker.get("test2").get());
        assertEquals(101, partsTracker.get("test3").get());
    }

    public void testIOException() throws IOException, InterruptedException {
        final Directory failureDirectory = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
                throw new IOException("test");
            }
        };
        assertThrows(IOException.class, () -> fileDownloader.download(source, failureDirectory, null, files.keySet(), () -> {}));

        final CountDownLatch latch = new CountDownLatch(1);
        fileDownloader.downloadAsync(new CancellableThreads(), source, failureDirectory, files.keySet(), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(IOException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void assertContent(Map<String, Integer> expected, Directory destination) throws IOException {
        // Note that Lucene will randomly write extra files (see org.apache.lucene.tests.mockfile.ExtraFS)
        // so we just need to check that all the expected files are present but not that _only_ the expected
        // files are present
        final Set<String> actualFiles = Set.of(destination.listAll());
        for (String file : expected.keySet()) {
            assertTrue(actualFiles.contains(file));
            try (IndexInput input = destination.openInput(file, IOContext.DEFAULT)) {
                assertEquals(expected.get(file), Integer.valueOf(input.readInt()));
                assertThrows(EOFException.class, input::readByte);
            }
        }
    }

    private static void assertContentMultiPart(Map<String, Integer> expected, Path destination) throws IOException {
        // Note that Lucene will randomly write extra files (see org.apache.lucene.tests.mockfile.ExtraFS)
        // so we just need to check that all the expected files are present but not that _only_ the expected
        // files are present
        for (String file : expected.keySet()) {
            assertTrue(Files.exists(destination.resolve(file)));
            assertEquals(32, Files.size(destination.resolve(file)));
        }
    }
}
