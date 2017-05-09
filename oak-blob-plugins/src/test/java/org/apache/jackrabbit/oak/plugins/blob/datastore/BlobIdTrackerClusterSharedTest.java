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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.io.Closeables.close;
import static java.lang.String.valueOf;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.getBlobStore;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

/**
 * Test for BlobIdTracker simulating a cluster and a shared data store scenarios
 * to test addition, retrieval and removal of blob ids.
 */
public class BlobIdTrackerClusterSharedTest {
    private static final Logger log = LoggerFactory.getLogger(BlobIdTrackerClusterSharedTest.class);

    File root;
    Cluster cluster1;
    Cluster cluster2;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            assumeThat(getBlobStore(), instanceOf(SharedDataStore.class));
        } catch (Exception e) {
            assumeNoException(e);
        }
    }

    @Before
    public void setup() throws Exception {
        this.root = folder.newFolder();
    }

    /**
     * Test simulating add, remove, retrieve scenarios for blobId tracker on a 2 node cluster.
     *
     * @throws IOException
     */
    @Test
    public void addRetrieveCluster() throws Exception {
        String clusterRepoId = randomUUID().toString();
        cluster1 = new Cluster(clusterRepoId,
            folder.newFolder("cluster1").getAbsolutePath(), folder);
        cluster2 = new Cluster(clusterRepoId,
            folder.newFolder("cluster2").getAbsolutePath(), folder);
        Set<String> adds = newHashSet();

        // Add some on cluster 2 & simulate snapshot
        adds.addAll(cluster2.doAdd(range(5, 9)));
        cluster2.forceSnapshot();
        log.info("Done force snapshot for cluster2");

        // Add some on cluster1 & simulate snapshot
        adds.addAll(cluster1.doAdd(range(0, 4)));
        cluster1.forceSnapshot();
        log.info("Done force snapshot for cluster1");

        // Get on cluster 1
        Set<String> retrieves = cluster1.doRetrieve();
        assertEquals("Retrieves not correct", adds, retrieves);
        log.info("Done retrieve on cluster1");

        cluster1.doRemove(adds, range(4, 5));
        log.info("Done remove on cluster1");
        retrieves = cluster1.doRetrieve();
        log.info("Done retrieve on cluster1 again");
        assertEquals("Retrieves not correct after remove", adds, retrieves);
    }

    /**
     * Test simulating add, remove, retrieve scenarios for blobId tracker on a 2 node shared
     * repository.
     *
     * @throws IOException
     */
    @Test
    public void addRetrieveShared() throws Exception {
        cluster1 = new Cluster(randomUUID().toString(),
            folder.newFolder("cluster1").getAbsolutePath(), folder);
        cluster2 = new Cluster(randomUUID().toString(),
            folder.newFolder("cluster2").getAbsolutePath(), folder);
        Set<String> adds = newHashSet();

        // Add some on cluster1 & simulate snapshot
        adds.addAll(cluster1.doAdd(range(0, 4)));
        cluster1.forceSnapshot();
        log.info("Done force snapshot for cluster1");

        // Add some on cluster 2 & simulate shapshot
        adds.addAll(cluster2.doAdd(range(5, 9)));
        cluster2.forceSnapshot();
        log.info("Done force snapshot for cluster2");

        // Get on cluster 1
        Set<String> retrieves = cluster1.doRetrieve();
        assertEquals("Retrieves not correct",
            adds, retrieves);
        log.info("Done retrieve on cluster1");

        cluster1.doRemove(adds, range(4, 5));
        log.info("Done remove on cluster1");
        retrieves = cluster1.doRetrieve();
        log.info("Done retrieve on cluster1 again");
        assertEquals("Retrieves not correct after remove",
            adds, retrieves);
    }

    /**
     * Logical instance.
     */
    class Cluster {
        ScheduledExecutorService scheduler;
        BlobIdTracker tracker;
        TemporaryFolder folder;
        SharedDataStore dataStore;

        Cluster(String repoId, String path, TemporaryFolder folder) throws Exception {
            this.dataStore = getBlobStore(root);
            this.tracker = new BlobIdTracker(path, repoId, 100 * 60, dataStore);
            this.scheduler = newSingleThreadScheduledExecutor();
            this.folder = folder;
        }

        Set<String> doAdd(List<String> ints) throws IOException {
            return add(tracker, ints);
        }

        void doRemove(Set<String> adds, List<String> removes) throws IOException {
            remove(tracker, folder.newFile(), adds, removes);
        }

        void forceSnapshot() {
            try {
                ScheduledFuture<?> scheduledFuture =
                    scheduler.schedule(tracker.new SnapshotJob(), 0, MILLISECONDS);
                scheduledFuture.get();
            } catch (Exception e) {
                log.error("Error in snapshot", e);
            }
        }

        Set<String> doRetrieve() throws IOException {
            return retrieve(tracker);
        }

        public void close() throws IOException {
            new ExecutorCloser(scheduler).close();
            tracker.close();
            try {
                ((DataStoreBlobStore) dataStore).close();
            } catch (DataStoreException e) {
                log.warn("Error closing blobstore", e);
            }
        }
    }

    @After
    public void tearDown() throws IOException {
        cluster1.close();
        cluster2.close();
        folder.delete();
    }

    private static Set<String> add(BlobTracker tracker, List<String> ints) throws IOException {
        Set<String> s = newHashSet();
        for (String rec : ints) {
            tracker.add(rec);
            s.add(rec);
        }
        return s;
    }

    private static Set<String> retrieve(BlobTracker tracker) throws IOException {
        Set<String> retrieved = newHashSet();
        Iterator<String> iter = tracker.get();
        log.info("retrieving blob ids");
        while(iter.hasNext()) {
            retrieved.add(iter.next());
        }
        if (iter instanceof Closeable) {
            close((Closeable)iter, true);
        }
        return retrieved;
    }

    private static void remove(BlobIdTracker tracker, File temp, Set<String> initAdd,
        List<String> ints) throws IOException {
        writeStrings(ints.iterator(), temp, false);
        initAdd.removeAll(ints);
        tracker.remove(temp);
    }

    private static List<String> range(int min, int max) {
        List<String> list = newArrayList();
        for (int i = min; i <= max; i++) {
            list.add(valueOf(i));
        }
        return list;
    }
}

