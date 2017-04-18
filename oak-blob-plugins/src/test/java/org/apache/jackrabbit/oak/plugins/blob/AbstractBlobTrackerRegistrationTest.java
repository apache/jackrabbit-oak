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

import java.io.File;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.ServiceRegistration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests OSGi registration for {@link BlobTrackingStore}.
 */
public abstract class AbstractBlobTrackerRegistrationTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public OsgiContext context = new OsgiContext();

    protected String repoHome;

    @Before
    public void setUp() throws  Exception {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        repoHome = folder.newFolder().getAbsolutePath();
    }

    @After
    public void tearDown() throws Exception {
        unregisterNodeStoreService();
        unregisterBlobStore();
    }

    @Test
    public void registerBlobTrackingStore() throws Exception {
        registerNodeStoreService();
        assertServiceNotActivated();
        registerTrackingBlobStore();
        assertServiceActivated();

        BlobStore blobStore = context.getService(BlobStore.class);
        assertTrue(blobStore instanceof BlobTrackingStore);

        BlobTrackingStore trackingStore = (BlobTrackingStore) blobStore;
        assertNotNull(trackingStore.getTracker());
    }

    @Test
    public void reRegisterBlobTrackingStore() throws Exception {
        registerNodeStoreService();
        assertServiceNotActivated();
        registerTrackingBlobStore();
        assertServiceActivated();

        BlobStore blobStore = context.getService(BlobStore.class);
        assertTrue(blobStore instanceof BlobTrackingStore);

        BlobTrackingStore trackingStore = (BlobTrackingStore) blobStore;
        assertNotNull(trackingStore.getTracker());

        BlobTracker oldTracker = trackingStore.getTracker();

        unregisterNodeStoreService();
        registerNodeStoreService();

        blobStore = context.getService(BlobStore.class);
        trackingStore = (BlobTrackingStore) blobStore;
        BlobTracker newTracker = trackingStore.getTracker();

        assertNotEquals(oldTracker, newTracker);
        assertTrackerReinitialized();
    }

    private void assertTrackerReinitialized() {
        File blobIdFiles = new File(repoHome, "blobids");
        ImmutableList<File> files =
                Files.fileTreeTraverser().postOrderTraversal(blobIdFiles).filter(new Predicate<File>() {
                    @Override public boolean apply(@Nullable File input) {
                        return input.getAbsolutePath().endsWith(".process");
                    }
                }).toList();
        assertEquals(1, files.size());
    }

    private void assertServiceNotActivated() {
        assertNull(context.getService(NodeStore.class));
    }

    private void assertServiceActivated() {
        assertNotNull(context.getService(NodeStore.class));
    }

    protected ServiceRegistration blobStore;

    protected void registerTrackingBlobStore() throws Exception {
        DataStoreBlobStore blobStore = DataStoreUtils.getBlobStore(repoHome);
        this.blobStore = context.bundleContext().registerService(BlobStore.class.getName(), blobStore, null);
    }

    protected void unregisterBlobStore() {
        blobStore.unregister();
    }

    protected abstract void registerNodeStoreService();

    protected abstract void unregisterNodeStoreService();
}
