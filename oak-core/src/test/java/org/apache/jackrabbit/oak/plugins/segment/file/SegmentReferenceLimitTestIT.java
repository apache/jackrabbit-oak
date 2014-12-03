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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static java.lang.String.valueOf;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentOverflowException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentReferenceLimitTestIT {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentReferenceLimitTestIT.class);

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile("FileStoreTest", "dir", new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void cleanDir() {
        try {
            FileUtils.deleteDirectory(directory);
        } catch (IOException e) {
            LOG.error("Error cleaning directory", e);
        }
    }

    @Test
    public void corruption() throws IOException, CommitFailedException, ExecutionException, InterruptedException {
        FileStore fileStore = new FileStore(directory, 1, 0, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        NodeBuilder root = nodeStore.getRoot().builder();
        root.setChildNode("test");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        FutureTask<Void> w1 = run(new Worker(nodeStore, "w1"));
        FutureTask<Void> w2 = run(new Worker(nodeStore, "w2"));
        FutureTask<Void> w3 = run(new Worker(nodeStore, "w3"));
        FutureTask<Void> w4 = run(new Worker(nodeStore, "w4"));

        try {
            w1.get();
            w2.get();
            w3.get();
            w4.get();
        } catch (ExecutionException e) {
            assertTrue(valueOf(e.getCause()), e.getCause() instanceof CommitFailedException);
            assertTrue(valueOf(e.getCause()), e.getCause().getCause() instanceof SegmentOverflowException);
        }
    }

    private static <T> FutureTask<T> run(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<T>(callable);
        new Thread(task).start();
        return task;
    }

    private static class Worker implements Callable<Void> {
        private final NodeStore nodeStore;
        private final String name;

        private Worker(NodeStore nodeStore, String name) {
            this.nodeStore = nodeStore;
            this.name = name;
        }

        @Override
        public Void call() throws Exception {
            for (int k = 0; ; k++) {
                NodeBuilder root = nodeStore.getRoot().builder();
                root.getChildNode("test").setProperty(name + ' ' + k, name + " value " + k);
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
        }
    }

}
