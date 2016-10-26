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

package org.apache.jackrabbit.oak.jcr;

import static java.io.File.createTempFile;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_NONE;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitorTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RefreshOnGCTest {
    private final Fixture fixture;

    private Callable<Void> compact;
    private Repository repository;
    private GCMonitorTracker gcMonitor;

    enum Fixture {
        SEGMENT_PERSISTED_MAP(true),
        SEGMENT_MEMORY_MAP(false),
        SEGMENT_TAR(false);

        private final boolean persistedMap;

        Fixture(boolean persistedMap) {
            this.persistedMap = persistedMap;
        }

        public boolean usePersistedMap() {
            return persistedMap;
        }
    }

    @Parameterized.Parameters
    public static List<Fixture[]> fixtures() {
        return ImmutableList.of(
                new Fixture[] {Fixture.SEGMENT_PERSISTED_MAP},
                new Fixture[] {Fixture.SEGMENT_MEMORY_MAP},
                new Fixture[] {Fixture.SEGMENT_TAR});
    }

    public RefreshOnGCTest(Fixture fixtures) {
        this.fixture = fixtures;
    }

    private NodeStore createSegmentStore(File directory, GCMonitor gcMonitor) throws Exception {
        CompactionStrategy strategy = new CompactionStrategy(
                false, false, CLEAN_NONE, 0, CompactionStrategy.MEMORY_THRESHOLD_DEFAULT) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
                setHead.call();
                return true;
            }
        };
        strategy.setPersistCompactionMap(fixture.usePersistedMap());
        final FileStore fileStore = FileStore.builder(directory)
                .withGCMonitor(gcMonitor)
                .build()
                .setCompactionStrategy(strategy);

        compact = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                fileStore.compact();
                return null;
            }
        };
        return SegmentNodeStore.builder(fileStore).build();
    }

    private NodeStore createSegmentTarStore(File directory, GCMonitor gcMonitor) throws Exception {
        final org.apache.jackrabbit.oak.segment.file.FileStore fileStore =
                fileStoreBuilder(directory).withGCMonitor(gcMonitor).build();
        compact = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                fileStore.compact();
                return null;
            }
        };
        return SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    @Before
    public void setup() throws Exception {
        File directory = createTempFile(getClass().getSimpleName(), "test", new File("target"));
        directory.delete();
        directory.mkdir();

        Whiteboard whiteboard = new DefaultWhiteboard();
        gcMonitor = new GCMonitorTracker();
        gcMonitor.start(whiteboard);

        Oak oak;
        if (fixture == Fixture.SEGMENT_TAR) {
            oak = new Oak(createSegmentTarStore(directory, gcMonitor));
        } else {
            oak = new Oak(createSegmentStore(directory, gcMonitor));
        }
        oak.with(whiteboard);
        repository = new Jcr(oak).createRepository();
    }

    @After
    public void tearDown() {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        gcMonitor.stop();
    }

    @Test
    public void compactionCausesRefresh() throws Exception {
        Session session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        try {
            Node root = session.getRootNode();
            root.addNode("one");
            session.save();

            addNode(repository, "two");

            compact.call();
            assertTrue(root.hasNode("one"));
            assertTrue("Node two must be visible as compaction should cause the session to refresh",
                    root.hasNode("two"));
        } finally {
            session.logout();
        }
    }

    private static void addNode(final Repository repository, final String name)
            throws ExecutionException, InterruptedException {
        // Execute on different threads to ensure same thread session
        // refreshing doesn't come into our way
        run(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Session session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
                try {
                    Node root = session.getRootNode();
                    root.addNode(name);
                    session.save();
                } finally {
                    session.logout();
                }
                return null;
            }
        });
    }

    private static void run(Callable<Void> callable) throws InterruptedException, ExecutionException {
        FutureTask<Void> task = new FutureTask<Void>(callable);
        new Thread(task).start();
        task.get();
    }


}
