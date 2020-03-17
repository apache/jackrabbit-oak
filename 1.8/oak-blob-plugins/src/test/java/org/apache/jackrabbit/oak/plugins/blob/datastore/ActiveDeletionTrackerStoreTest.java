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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker.ActiveDeletionTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.valueOf;
import static java.util.UUID.randomUUID;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.getBlobStore;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

/**
 * Test for BlobIdTracker.ActiveDeletionTracker to test tracking removed blob ids.
 */
public class ActiveDeletionTrackerStoreTest {
    private static final Logger log = LoggerFactory.getLogger(ActiveDeletionTrackerStoreTest.class);

    File root;
    SharedDataStore dataStore;
    ActiveDeletionTracker tracker;

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

    private ActiveDeletionTracker initTracker() throws IOException {
        return new ActiveDeletionTracker(root, repoId);
    }

    @After
    public void tearDown() throws IOException {
        folder.delete();
    }

    @Test
    public void track() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 20), folder);
        Set<String> retrieved = retrieve(tracker, folder);

        assertEquals("Incorrect elements after add snapshot", initAdd, retrieved);
    }

    @Test
    public void filterWithNoActiveDeletion() throws Exception {
        File toFilter = create(range(7, 10), folder);
        Iterator<String> filtered = tracker.filter(toFilter);

        assertEquals("incorrect elements after filtering", Sets.newHashSet(range(7, 10)), Sets.newHashSet(filtered));
    }

    @Test
    public void filter() throws Exception {
        add(tracker, range(0, 20), folder);
        File toFilter = create(range(7, 10), folder);
        Iterator<String> filtered = tracker.filter(toFilter);

        assertTrue("More elements after filtering", Lists.newArrayList(filtered).isEmpty());
    }

    @Test
    public void noFilter() throws Exception {
        add(tracker, range(5, 20), folder);
        List<String> toFilter = combine(range(7, 10), range(0, 4));
        File toFilterFile = create(toFilter, folder);
        Iterator<String> filtered = tracker.filter(toFilterFile);

        assertEquals("Incorrect elements after filtering", range(0, 4), Lists.newArrayList(filtered));
    }

    @Test
    public void filterWithExtraElements() throws Exception {
        add(tracker, range(5, 25), folder);
        List<String> toFilter = combine(range(7, 10), range(0, 4));
        File toFilterFile = create(toFilter, folder);
        Iterator<String> filtered = tracker.filter(toFilterFile);

        assertEquals("Incorrect elements after filtering",
            range(0, 4), Lists.newArrayList(filtered));
    }

    @Test
    public void reconcileAll() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 20), folder);
        List toReconcile = Lists.newArrayList();

        File toFilter = create(toReconcile, folder);

        tracker.reconcile(toFilter);
        Set<String> retrieved = retrieve(tracker, folder);

        assertEquals("Incorrect elements after reconciliation", Sets.newHashSet(toReconcile), retrieved);
    }

    @Test
    public void reconcileNone() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 20), folder);
        List<String> toReconcile = range(0, 20);

        File toFilter = create(toReconcile, folder);

        tracker.reconcile(toFilter);
        Set<String> retrieved = retrieve(tracker, folder);

        assertEquals("Incorrect elements after reconciliation", Sets.newHashSet(toReconcile), retrieved);
    }

    @Test
    public void reconcile() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 20), folder);
        List<String> toReconcile = combine(range(7, 10), range(1, 4));

        File toFilter = create(toReconcile, folder);

        tracker.reconcile(toFilter);
        Set<String> retrieved = retrieve(tracker, folder);

        assertEquals("Incorrect elements after reconciliation", Sets.newHashSet(toReconcile), retrieved);
    }

    @Test
    public void reconcileExtraElements() throws Exception {
        Set<String> initAdd = add(tracker, range(0, 25), folder);
        List<String> toReconcile = combine(range(7, 10), range(1, 4));

        File toFilter = create(toReconcile, folder);

        tracker.reconcile(toFilter);
        Set<String> retrieved = retrieve(tracker, folder);

        assertEquals("Incorrect elements after reconciliation", Sets.newHashSet(toReconcile), retrieved);
    }

    @Test
    public void addCloseRestart() throws IOException {
        Set<String> initAdd = add(tracker, range(0, 10), folder);
        this.tracker = initTracker();
        Set<String> retrieved = retrieve(tracker, folder);
        assertEquals("Incorrect elements after safe restart", initAdd, retrieved);
    }

    private static Set<String> add(ActiveDeletionTracker store, List<String> ints, TemporaryFolder folder) throws IOException {
        File f = folder.newFile();
        FileIOUtils.writeStrings(ints.iterator(), f, false);
        store.track(f);
        return Sets.newHashSet(ints);
    }

    private static File create(List<String> ints, TemporaryFolder folder) throws IOException {
        File f = folder.newFile();
        FileIOUtils.writeStrings(ints.iterator(), f, false);
        return f;
    }

    private static Set<String> retrieve(ActiveDeletionTracker store, TemporaryFolder folder) throws IOException {
        File f = folder.newFile();
        Set<String> retrieved = readStringsAsSet(
            new FileInputStream(store.retrieve(f.getAbsolutePath())), false);
        return retrieved;
    }

    private static List<String> range(int min, int max) {
        List<String> list = newArrayList();
        for (int i = min; i <= max; i++) {
            list.add(Strings.padStart(valueOf(i), 2, '0'));
        }
        return list;
    }

    private static List<String> combine(List<String> first, List<String> second) {
        first.addAll(second);
        Collections.sort(first, new Comparator<String>() {
            @Override public int compare(String s1, String s2) {
                return Integer.valueOf(s1).compareTo(Integer.valueOf(s2));
            }
        });
        return first;
    }
}

