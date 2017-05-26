/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractListeningExecutorService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for DataStore cache related tests.
 */
public class AbstractDataStoreCacheTest {
    static final Logger LOG = LoggerFactory.getLogger(AbstractDataStoreCacheTest.class);

    static class TestStagingUploader implements StagingUploader {
        private final File root;

        public TestStagingUploader(File dir) {
            this.root = new File(dir, "datastore");
            root.mkdirs();
        }

        @Override public void write(String id, File f) throws DataStoreException {
            try {
                File move = getFile(id, root);
                move.getParentFile().mkdirs();
                Files.copy(f, move);
                LOG.info("In TestStagingUploader after write [{}]", move);
            } catch (IOException e) {
                throw new DataStoreException(e);
            }
        }

        public File read(String id) {
            return getFile(id, root);
        }
    }


    static class TestCacheLoader<S, I> extends CacheLoader<String, FileInputStream> {
        private final File root;

        public TestCacheLoader(File dir) {
            this.root = new File(dir, "datastore");
            root.mkdirs();
        }

        public void write(String id, File f) throws DataStoreException {
            try {
                File move = getFile(id, root);
                move.getParentFile().mkdirs();
                Files.copy(f, move);
                LOG.info("In TestCacheLoader after write [{}], [{}]", id, move);
            } catch (IOException e) {
                throw new DataStoreException(e);
            }
        }

        @Override public FileInputStream load(@Nonnull String key) throws Exception {
            return FileUtils.openInputStream(getFile(key, root));
        }
    }

    static class TestPoolExecutor extends ThreadPoolExecutor {
        private final CountDownLatch beforeLatch;
        private final CountDownLatch afterLatch;

        TestPoolExecutor(int threads, CountDownLatch beforeLatch, CountDownLatch afterLatch) {
            super(threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("oak-async-thread"));
            this.beforeLatch = beforeLatch;
            this.afterLatch = afterLatch;
        }

        @Override public void beforeExecute(Thread t, Runnable command) {
            try {
                LOG.trace("Before execution....waiting for latch");
                beforeLatch.await();
                LOG.trace("Before execution....after acquiring latch");
                super.beforeExecute(t, command);
                LOG.trace("Completed beforeExecute");
            } catch (Exception e) {
                LOG.trace("Error in before execute", e);
            }
        }

        @Override protected void afterExecute(Runnable r, Throwable t) {
            try {
                LOG.trace("After execution....counting down latch");
                afterLatch.countDown();
                LOG.info("After execution....after counting down latch");
                super.afterExecute(r, t);
                LOG.trace("Completed afterExecute");
            } catch (Exception e) {
                LOG.trace("Error in after execute", e);
            }
        }
    }


    static class TestExecutor extends AbstractListeningExecutorService {
        private final CountDownLatch afterLatch;
        private final ExecutorService delegate;
        final List<ListenableFuture<Integer>> futures;

        public TestExecutor(int threads, CountDownLatch beforeLatch, CountDownLatch afterLatch,
            CountDownLatch afterExecuteLatch) {
            this.delegate = new TestPoolExecutor(threads, beforeLatch, afterExecuteLatch);
            this.futures = Lists.newArrayList();
            this.afterLatch = afterLatch;
        }

        @Override @Nonnull public ListenableFuture<?> submit(@Nonnull Callable task) {
            LOG.trace("Before submitting to super....");
            ListenableFuture<Integer> submit = super.submit(task);
            LOG.trace("After submitting to super....");

            futures.add(submit);
            Futures.addCallback(submit, new TestFutureCallback<Integer>(afterLatch));
            LOG.trace("Added callback");

            return submit;
        }

        @Override public void execute(@Nonnull Runnable command) {
            delegate.execute(command);
        }

        @Override public void shutdown() {
            delegate.shutdown();
        }

        @Override @Nonnull public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        static class TestFutureCallback<Integer> implements FutureCallback {
            private final CountDownLatch latch;

            public TestFutureCallback(CountDownLatch latch) {
                this.latch = latch;
            }

            @Override public void onSuccess(@Nullable Object result) {
                try {
                    LOG.trace("Waiting for latch in callback");
                    latch.await(100, TimeUnit.MILLISECONDS);
                    LOG.trace("Acquired latch in onSuccess");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override public void onFailure(@Nonnull Throwable t) {
                try {
                    LOG.trace("Waiting for latch onFailure in callback");
                    latch.await(100, TimeUnit.MILLISECONDS);
                    LOG.trace("Acquired latch in onFailure");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // A mock Backend implementation that uses a Map to keep track of what
    // records have been added and removed, for test purposes only.
    static class TestMemoryBackend extends AbstractSharedBackend {
        final Map<DataIdentifier, File> _backend = Maps.newHashMap();
        private final File root;

        public TestMemoryBackend(File root) {
            this.root = root;
        }

        @Override public InputStream read(DataIdentifier identifier) throws DataStoreException {
            try {
                return new FileInputStream(_backend.get(identifier));
            } catch (FileNotFoundException e) {
                throw new DataStoreException(e);
            }
        }

        @Override public void write(DataIdentifier identifier, File file)
            throws DataStoreException {
            File backendFile = getFile(identifier.toString(), root);
            if (file != null && file.exists()) {
                try {
                    FileUtils.copyFile(file, backendFile);
                } catch (IOException e) {
                    throw new DataStoreException(e);
                }
                _backend.put(identifier, backendFile);
            } else {
                throw new DataStoreException(
                    String.format("file %s of id %s", new Object[] {file, identifier.toString()}));
            }
        }

        @Override public DataRecord getRecord(DataIdentifier id) throws DataStoreException {
            if (_backend.containsKey(id)) {
                final File f = _backend.get(id);
                return new AbstractDataRecord(this, id) {
                    @Override public long getLength() throws DataStoreException {
                        return f.length();
                    }

                    @Override public InputStream getStream() throws DataStoreException {
                        try {
                            return new FileInputStream(f);
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }

                    @Override public long getLastModified() {
                        return f.lastModified();
                    }
                };
            }
            return null;
        }

        @Override public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
            return _backend.keySet().iterator();
        }

        @Override public Iterator<DataRecord> getAllRecords() throws DataStoreException {
            return null;
        }

        @Override public boolean exists(DataIdentifier identifier) throws DataStoreException {
            return _backend.containsKey(identifier);
        }

        @Override public void close() throws DataStoreException {
        }

        @Override public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            if (_backend.containsKey(identifier)) {
                _backend.remove(identifier);
            }
        }

        @Override public void addMetadataRecord(InputStream input, String name)
            throws DataStoreException {
        }

        @Override public void addMetadataRecord(File input, String name) throws DataStoreException {
        }

        @Override public DataRecord getMetadataRecord(String name) {
            return null;
        }

        @Override public List<DataRecord> getAllMetadataRecords(String prefix) {
            return null;
        }

        @Override public boolean deleteMetadataRecord(String name) {
            return false;
        }

        @Override public void deleteAllMetadataRecords(String prefix) {
        }

        @Override public void init() throws DataStoreException {

        }

        @Override
        public String getReferenceFromIdentifier(DataIdentifier identifier) {
            return super.getReferenceFromIdentifier(identifier);
        }
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    protected static File getFile(String id, File root) {
        File file = root;
        file = new File(file, id.substring(0, 2));
        file = new File(file, id.substring(2, 4));
        return new File(file, id);
    }

    static File copyToFile(InputStream stream, File file) throws IOException {
        FileUtils.copyInputStreamToFile(stream, file);
        return file;
    }

    static void serializeMap(Map<String,Long> pendingupload, File file) throws IOException {
        OutputStream fos = new FileOutputStream(file);
        OutputStream buffer = new BufferedOutputStream(fos);
        ObjectOutput output = new ObjectOutputStream(buffer);
        try {
            output.writeObject(pendingupload);
            output.flush();
        } finally {
            output.close();
            IOUtils.closeQuietly(buffer);
        }
    }
}
