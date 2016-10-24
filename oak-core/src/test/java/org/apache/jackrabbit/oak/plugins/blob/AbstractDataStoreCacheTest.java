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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
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
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractListeningExecutorService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
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
                LOG.trace("After execution....waiting for latch");
                afterLatch.countDown();
                LOG.trace("After execution....after acquiring latch");
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

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static File getFile(String id, File root) {
        File file = root;
        file = new File(file, id.substring(0, 2));
        file = new File(file, id.substring(2, 4));
        return new File(file, id);
    }

    static File copyToFile(InputStream stream, File file) throws IOException {
        FileUtils.copyInputStreamToFile(stream, file);
        return file;
    }
}
