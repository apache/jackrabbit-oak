/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.*;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.junit.Assert.*;

public class ConcurrentCopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private final Closer closer = Closer.create();
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    private Directory remote;

    @Before
    public void setup() throws Exception {
        executorService = Executors.newFixedThreadPool(2);
        closer.register(new ExecutorCloser(executorService, 1, TimeUnit.SECONDS));

        // normal remote directory
        remote = new RAMDirectory() {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                return new RemoteIndexInput(super.openInput(name, context));
            }
        };
        IndexOutput output = remote.createOutput("file", IOContext.DEFAULT);
        output.writeString("foo");
        output.close();

        IndexInput remoteInput = remote.openInput("file", IOContext.READ);
        assertTrue(remoteInput.length() > 1);
    }

    @After
    public void tearDown() throws Exception {
        closer.close();
    }

    @Ignore("OAK-8513")
    @Test
    public void concurrentPrefetch() throws Exception {
        // create filtering remote that would block on opening an input
        CountDownLatch copyWaiter = new CountDownLatch(1);
        CountDownLatch copyBlocker = new CountDownLatch(1);
        Directory blockingRemote = new BlockingInputDirectory(copyWaiter, copyBlocker, remote);

        IndexCopier copier = new IndexCopier(sameThreadExecutor(), temporaryFolder.newFolder(), true);

        NodeState root = InitialContentHelper.INITIAL_CONTENT;
        final LuceneIndexDefinition defn = new LuceneIndexDefinition(root, root, "/foo");

        // create a CoR instance to start pre-fetching in a separate thread as we'd be blocking it
        Future<String> concCoR = executorService.submit(() -> {
            try {
                openCoR(copier, blockingRemote, defn);
                return null;
            } catch (Throwable t) {
                return getThrowableAsString(t);
            }
        });

        try {
            // wait for CoR to start fetching which we're blocking its completion via copyBlocker latch
            copyWaiter.await();

            // get another directory instance with normal remote while the previous is blocked by us
            Directory dir = openCoR(copier, remote, defn);
            IndexInput input = dir.openInput("file", IOContext.READ);

            copyBlocker.countDown();

            String futureException = concCoR.get();

            assertNull("Concurrent CoR must not throw exception", futureException);

            assertFalse("Must not be reading from remote", input instanceof RemoteIndexInput);
        } finally {
            copyBlocker.countDown();
        }
    }

    private static Directory openCoR(IndexCopier copier, Directory remote, LuceneIndexDefinition defn) throws IOException {
        return copier.wrapForRead("/oak:index/foo", defn, remote, INDEX_DATA_CHILD_NAME);
    }

    private static String getThrowableAsString(Throwable t) {
        StringBuilder sb = new StringBuilder(t.getMessage() + "\n");
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        sb.append(sw.getBuffer());
        return sb.toString();
    }

    static class BlockingInputDirectory extends FilterDirectory {
        private final CountDownLatch copyWaiter;
        private final CountDownLatch copyBlocker;

        BlockingInputDirectory(CountDownLatch copyWaiter, CountDownLatch copyBlocker, Directory in) {
            super(in);
            this.copyWaiter = copyWaiter;
            this.copyBlocker = copyBlocker;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            IndexInput input;
            try {
                input = super.openInput(name, context);
            } finally {
                // signal that input has been opened
                copyWaiter.countDown();
            }

            // wait while we're signalled that we can be done with opening input
            boolean wait = true;
            while (wait) {
                try {
                    // block until we are signalled to call super
                    copyBlocker.await();
                    wait = false;
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            return input;
        }
    }

    static class RemoteIndexInput extends IndexInput {
        private final IndexInput delegate;

        RemoteIndexInput(IndexInput delegate) {
            super("Remote " + delegate.toString());
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void seek(long pos) throws IOException {
            delegate.seek(pos);
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public byte readByte() throws IOException {
            return delegate.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            delegate.readBytes(b, offset, len);
        }
    }
}