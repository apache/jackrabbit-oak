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

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Tests verifying if the repository gets corrupted or not: {@code OAK-2294 Corrupt repository after concurrent version operations}</p>
 *
 * <p>These tests are disabled by default due to their long running time. On the
 * command line specify {@code -DSegmentReferenceLimitTestIT=true} to enable
 * them.</p>
 *
 *<p>If you only want to run this test:<br>
 * {@code mvn verify -Dsurefire.skip.ut=true -PintegrationTesting -Dit.test=SegmentReferenceLimitTestIT -DSegmentReferenceLimitTestIT=true}
 * </p>
 */
public class SegmentReferenceLimitTestIT {

    private static final Logger LOG = LoggerFactory
            .getLogger(SegmentReferenceLimitTestIT.class);
    private static final boolean ENABLED = Boolean
            .getBoolean(SegmentReferenceLimitTestIT.class.getSimpleName());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Before
    public void setUp() throws IOException {
        assumeTrue(ENABLED);
    }

    @Test
    public void corruption() throws IOException, CommitFailedException, ExecutionException, InterruptedException {
        FileStore fileStore = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1)
                .withNoCache().withMemoryMapping(true).build();
        SegmentNodeStore nodeStore = SegmentNodeStore.builder(fileStore).build();

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
