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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentReferenceLimitTestIT {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentReferenceLimitTestIT.class);

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile("SegmentReferenceLimitTestIT", "dir", new File("target"));
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

    /**
     * OAK-2294 Corrupt repository after concurrent version operations
     * 
     * TODO Test is currently ignored because of how memory intensive it is
     *
     * @see <a
     *      href="https://issues.apache.org/jira/browse/OAK-2294">OAK-2294</a>
     */
    @Test
    @Ignore
    public void corruption() throws IOException, CommitFailedException, ExecutionException, InterruptedException {
        FileStore fileStore = new FileStore(directory, 1, 0, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        NodeBuilder root = nodeStore.getRoot().builder();
        root.setChildNode("test");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        List<FutureTask<Void>> l = new ArrayList<FutureTask<Void>>();
        for (int i = 0; i < 10; i++) {
            l.add(run(new Worker(nodeStore, "w" + i)));
        }

        try {
            for (FutureTask<Void> w : l) {
                w.get();
            }
        } finally {
            fileStore.close();
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
            for (int k = 0; k < 400; k++) {
                NodeBuilder root = nodeStore.getRoot().builder();
                root.getChildNode("test").setProperty(name + ' ' + k, name + " value " + k);
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            return null;
        }
    }

}
