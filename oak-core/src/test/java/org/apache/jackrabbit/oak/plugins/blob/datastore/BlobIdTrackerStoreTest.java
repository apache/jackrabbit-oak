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
import java.util.concurrent.CountDownLatch;

import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker.BlobIdStore;
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
import static com.google.common.collect.Sets.symmetricDifference;
import static java.lang.String.valueOf;
import static java.util.UUID.randomUUID;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.getBlobStore;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

/**
 * Test for BlobIdTracker.BlobIdStore to test addition, retrieval and removal of blob ids.
 */
public class BlobIdTrackerStoreTest {
    private static final Logger log = LoggerFactory.getLogger(BlobIdTrackerStoreTest.class);

    File root;
    SharedDataStore dataStore;
    BlobIdTracker tracker;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));
    private String repoId;

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
        this.tracker = initTracker();
    }

    private BlobIdTracker initTracker() throws IOException {
        return new BlobIdTracker(root.getAbsolutePath(),
           repoId, 5 * 60, dataStore);
    }

    @After
    public void tearDown() throws IOException {
        tracker.close();
        folder.delete();
    }

    @Test
    public void addSnapshot() throws Exception {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 10000));
        store.snapshot();
        Set<String> retrieved = retrieve(store);

        assertEquals("Incorrect elements after add snapshot", initAdd, retrieved);
    }

    @Test
    public void addSnapshotRetrieve() throws Exception {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 10000));
        store.snapshot();
        Set<String> retrieved = retrieveFile(store, folder);

        assertEquals("Incorrect elements after add snapshot reading file", initAdd, retrieved);
    }

    @Test
    public void addSnapshotAdd() throws Exception {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 10000));
        store.snapshot();
        initAdd.addAll(add(store, range(10001, 10003)));
        Set<String> retrieved = retrieve(store);

        assertTrue("Incorrect elements with add before snapshot",
            symmetricDifference(initAdd, retrieved)
                .containsAll(newHashSet("10001", "10002", "10003")));
    }

    @Test
    public void addSnapshotAddSnapshot() throws Exception {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 10000));
        store.snapshot();
        initAdd.addAll(add(store, range(10001, 10003)));
        store.snapshot();
        Set<String> retrieved = retrieve(store);

        assertEquals("Incorrect elements with snapshot after add", initAdd, retrieved);
    }

    @Test
    public void addSnapshotRemove() throws Exception {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 10000));
        store.snapshot();
        remove(store, folder.newFile(), initAdd, range(2, 3));

        Set<String> retrieved = retrieve(store);
        assertEquals("Incorrect elements after remove", initAdd, retrieved);
    }

    @Test
    public void addRestart() throws IOException {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 100000));
        this.tracker = initTracker();
        Set<String> retrieved = retrieve(store);
        assertTrue("Extra elements retrieved", retrieved.isEmpty());
        store = tracker.store;
        store.snapshot();
        retrieved = retrieve(store);
        assertEquals("Incorrect elements after dirty restart", initAdd, retrieved);
    }

    @Test
    public void addCloseRestart() throws IOException {
        BlobIdStore store = tracker.store;

        Set<String> initAdd = add(store, range(0, 10000));
        store.close();
        this.tracker = initTracker();
        store = tracker.store;
        store.snapshot();
        Set<String> retrieved = retrieve(store);
        assertEquals("Incorrect elements after safe restart", initAdd, retrieved);
    }

    @Test
    public void addConcurrentSnapshot() throws IOException, InterruptedException {
        final BlobIdStore store = tracker.store;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(2);

        Thread addThread = addThread(store, start, done);
        Thread snapshotThread = snapshotThread(store, start, done);
        snapshotThread.start();
        addThread.start();

        start.countDown();
        done.await();

        // Do a snapshot to ensure that all the adds if snapshot finished first are collected
        store.snapshot();
        Set<String> retrieved = retrieve(store);
        assertEquals("Incorrect elements after concurrent snapshot",
            newHashSet(range(0, 100000)), retrieved);
    }

    @Test
    public void addSnapshotConcurrentRetrieve() throws IOException, InterruptedException {
        final BlobIdStore store = tracker.store;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(2);
        Set<String> initAdd = add(store, range(0, 100000));
        final Set<String> retrieves = newHashSet();

        Thread retrieveThread = retrieveThread(store, retrieves, start, done);
        Thread snapshotThread = snapshotThread(store, start, done);
        snapshotThread.start();
        retrieveThread.start();
        start.countDown();
        done.await();

        if (retrieves.isEmpty()) {
            // take a snapshot to ensure that all adds accounted if snapshot finished last
            store.snapshot();
            retrieves.addAll(retrieve(store));
        }
        assertEquals("Incorrect elements after concurrent snapshot/retrieve",
            initAdd, retrieves);
    }

    @Test
    public void snapshotConcurrentRemove() throws IOException, InterruptedException {
        final BlobIdStore store = tracker.store;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(2);
        final Set<String> initAdd = add(store, range(0, 100000));
        store.snapshot();

        Thread removeThread = removeThread(store, folder.newFile(), initAdd, start, done);
        Thread snapshotThread = snapshotThread(store, start, done);
        removeThread.start();
        snapshotThread.start();

        // add some more to check that snapshot is successfull
        initAdd.addAll(add(store, range(10001, 10003)));
        start.countDown();
        done.await();

        Set<String> retrieves = retrieve(store);
        assertEquals("Incorrect elements after concurrent snapshot/remove",
            initAdd, retrieves);
    }

    @Test
    public void addBulkAdd() throws IOException {
        final BlobIdStore store = tracker.store;
        final Set<String> initAdd = add(store, range(0, 4));

        // Add new ids from a file
        File temp = folder.newFile();
        List<String> newAdd = range(5, 9);
        initAdd.addAll(newAdd);
        writeStrings(newAdd.iterator(), temp, false);

        store.addRecords(temp);
        store.snapshot();

        Set<String> retrieved = retrieve(store);
        assertEquals("Incorrect elements after bulk add from file",
            initAdd, retrieved);

        newAdd = range(10, 14);
        initAdd.addAll(newAdd);

        store.addRecords(newAdd.iterator());
        store.snapshot();

        retrieved = retrieve(store);
        assertEquals("Incorrect elements after bulk add from iterator",
            initAdd, retrieved);
    }

    @Test
    public void bulkAddConcurrentCompact() throws IOException, InterruptedException {
        final BlobIdStore store = tracker.store;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(2);

        Thread addThread = addThread(store, true, start, done);
        Thread snapshotThread = snapshotThread(store, start, done);
        snapshotThread.start();
        addThread.start();

        start.countDown();
        done.await();

        // Do a snapshot to ensure that all the adds if snapshot finished first are collected
        store.snapshot();
        Set<String> retrieved = retrieve(store);
        assertEquals("Incorrect elements after concurrent snapshot",
            newHashSet(range(0, 100000)), retrieved);

    }

    private static Thread addThread(
        final BlobIdStore store, final CountDownLatch start, final CountDownLatch done) {
        return addThread(store, false, start, done);
    }

    private static Thread addThread(
        final BlobIdStore store, final boolean bulk, final CountDownLatch start, final CountDownLatch done) {
        return new Thread("AddThread") {
            @Override
            public void run() {
                try {
                    List<String> adds = range(0, 100000);
                    start.await();
                    if (!bulk) {
                        add(store, adds);
                    } else {
                        store.addRecords(adds.iterator());
                    }
                    done.countDown();
                } catch (IOException e) {
                    log.info("Exception in add", e);
                } catch (InterruptedException e) {
                    log.info("Interrupted in add", e);
                }
            }
        };
    }

    private static Thread retrieveThread(
        final BlobIdStore store, final Set<String> retrieves, final CountDownLatch start,
        final CountDownLatch done) {
        return new Thread("RetrieveThread") {
            @Override
            public void run() {
                try {
                    start.await();
                    retrieves.addAll(retrieve(store));
                    done.countDown();
                } catch (IOException e) {
                    log.info("Exception in retrieve", e);
                } catch (InterruptedException e) {
                    log.info("Interrupted in retrieve", e);
                }
            }
        };
    }

    private static Thread removeThread(final BlobIdStore store, final File temp,
        final Set<String> adds, final CountDownLatch start, final CountDownLatch done) {
        return new Thread("RemoveThread") {
            @Override
            public void run() {
                try {
                    start.await();
                    remove(store, temp, adds, range(1, 3));
                    done.countDown();
                } catch (IOException e) {
                    log.info("Exception in remove", e);
                } catch (InterruptedException e) {
                    log.info("Interrupted in remove", e);
                }
            }
        };
    }

    private static Thread snapshotThread(
        final BlobIdStore store, final CountDownLatch start, final CountDownLatch done) {
        return new Thread("SnapshotThread") {
            @Override
            public void run() {
                try {
                    start.await();
                    store.snapshot();
                    done.countDown();
                } catch (IOException e) {
                    log.info("Exception in snapshot", e);
                } catch (InterruptedException e) {
                    log.info("Interrupted in snapshot", e);
                }
            }
        };
    }

    private static Set<String> add(BlobIdStore store, List<String> ints) throws IOException {
        Set<String> s = newHashSet();
        for (String rec : ints) {
            store.addRecord(rec);
            s.add(rec);
        }
        return s;
    }

    private static Set<String> retrieve(BlobIdStore store) throws IOException {
        Set<String> retrieved = newHashSet();
        Iterator<String> iter = store.getRecords();
        while(iter.hasNext()) {
            retrieved.add(iter.next());
        }
        closeQuietly((Closeable)iter);
        return retrieved;
    }
    private static Set<String> retrieveFile(BlobIdStore store, TemporaryFolder folder) throws IOException {
        File f = folder.newFile();
        Set<String> retrieved = readStringsAsSet(
            new FileInputStream(store.getRecords(f.getAbsolutePath())), false);
        return retrieved;
    }

    private static void remove(BlobIdStore store, File temp, Set<String> initAdd,
            List<String> ints) throws IOException {
        writeStrings(ints.iterator(), temp, false);
        initAdd.removeAll(ints);
        store.removeRecords(temp);
    }

    private static List<String> range(int min, int max) {
        List<String> list = newArrayList();
        for (int i = min; i <= max; i++) {
            list.add(valueOf(i));
        }
        return list;
    }
}

