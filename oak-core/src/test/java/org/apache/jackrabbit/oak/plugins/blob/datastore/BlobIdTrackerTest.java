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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.valueOf;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.getBlobStore;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils
    .SharedStoreRecordType.BLOBREFERENCES;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

/**
 * Test for BlobIdTracker to test addition, retrieval and removal of blob ids.
 */
public class BlobIdTrackerTest {
    File root;
    SharedDataStore dataStore;
    BlobIdTracker tracker;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));
    private String repoId;
    private ScheduledExecutorService scheduler;

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
        if (dataStore == null) {
            dataStore = getBlobStore(root);
        }
        this.repoId = randomUUID().toString();
        this.tracker = new BlobIdTracker(root.getAbsolutePath(), repoId, 100 * 60, dataStore);
        this.scheduler = newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() throws IOException {
        tracker.close();
        new ExecutorCloser(scheduler).close();
        folder.delete();
    }

    @Test
    public void addSnapshot() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 4));
        ScheduledFuture<?> scheduledFuture =
            scheduler.schedule(tracker.new SnapshotJob(), 0, TimeUnit.MILLISECONDS);
        scheduledFuture.get();

        Set<String> retrieved = retrieve(tracker);

        assertEquals("Extra elements after add", initAdd, retrieved);
        assertTrue(read(dataStore.getAllMetadataRecords(BLOBREFERENCES.getType())).isEmpty());
    }

    @Test
    public void addSnapshotRemove() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 4));
        ScheduledFuture<?> scheduledFuture =
            scheduler.schedule(tracker.new SnapshotJob(), 0, TimeUnit.MILLISECONDS);
        scheduledFuture.get();
        assertEquals("Extra elements after add", initAdd, retrieve(tracker));

        remove(tracker, folder.newFile(), initAdd, range(1, 2));

        assertEquals("Extra elements after remove", initAdd, retrieve(tracker));
    }

    @Test
    public void snapshotRetrieveIgnored() throws Exception {
        System.setProperty("oak.datastore.skipTracker", "true");
        this.tracker = new BlobIdTracker(root.getAbsolutePath(), repoId, 100 * 60, dataStore);
        this.scheduler = newSingleThreadScheduledExecutor();

        try {
            Set<String> initAdd = add(tracker, range(0, 10000));
            ScheduledFuture<?> scheduledFuture =
                scheduler.schedule(tracker.new SnapshotJob(), 0, TimeUnit.MILLISECONDS);
            scheduledFuture.get();
            assertEquals("References file not empty", 0, tracker.store.getBlobRecordsFile().length());

            Set<String> retrieved = retrieveFile(tracker, folder);
            assertTrue(retrieved.isEmpty());

            retrieved = retrieve(tracker);
            assertTrue(retrieved.isEmpty());
        } finally {
            //reset the skip tracker system prop
            System.clearProperty("oak.datastore.skipTracker");
            this.tracker = new BlobIdTracker(root.getAbsolutePath(), repoId, 100 * 60, dataStore);
            this.scheduler = newSingleThreadScheduledExecutor();
        }
    }

    private static Set<String> read(List<DataRecord> recs)
        throws IOException, DataStoreException {
        Set<String> ids = newHashSet();
        for (DataRecord b : recs) {
            ids.addAll(readStringsAsSet(b.getStream(), false));
        }
        return ids;
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
        while(iter.hasNext()) {
            retrieved.add(iter.next());
        }
        if (iter instanceof Closeable) {
            closeQuietly((Closeable)iter);
        }
        return retrieved;
    }

    private static Set<String> retrieveFile(BlobIdTracker tracker, TemporaryFolder folder) throws IOException {
        File f = folder.newFile();
        Set<String> retrieved = readStringsAsSet(
            new FileInputStream(tracker.get(f.getAbsolutePath())), false);
        return retrieved;
    }

    private static void remove(BlobTracker tracker, File temp, Set<String> initAdd,
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

