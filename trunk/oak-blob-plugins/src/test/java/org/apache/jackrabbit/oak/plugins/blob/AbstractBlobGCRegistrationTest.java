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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public abstract class AbstractBlobGCRegistrationTest {
    @Rule
    public final TemporaryFolder target = new TemporaryFolder(new File("target"));
    @Rule
    public OsgiContext context = new OsgiContext();

    protected String repoHome;

    protected ServiceRegistration blobStore;

    @Before
    public void setUp() throws  Exception {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        repoHome = target.newFolder().getAbsolutePath();
    }

    @After
    public void tearDown() throws Exception {
        unregisterNodeStoreService();
        unregisterBlobStore();
    }

    @Test
    public void testBlobGcRegistered() throws Exception {
        registerNodeStoreService();
        assertServiceNotActivated();
        registerBlobStore();
        assertServiceActivated();

        BlobGCMBean mbean = context.getService(BlobGCMBean.class);
        assertNotNull(mbean);

        unregisterNodeStoreService();
        unregisterBlobStore();
    }

    protected abstract void registerNodeStoreService();

    protected abstract void unregisterNodeStoreService();

    protected void registerBlobStore() throws Exception {
        DataStoreBlobStore blobStore = DataStoreUtils.getBlobStore(repoHome);
        this.blobStore = context.bundleContext().registerService(BlobStore.class.getName(), blobStore, null);
    }

    protected void unregisterBlobStore() {
        blobStore.unregister();
    }

    private void assertServiceNotActivated() {
        assertNull(context.getService(NodeStore.class));
    }

    private void assertServiceActivated() {
        assertNotNull(context.getService(NodeStore.class));
    }
}
