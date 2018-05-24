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

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDataStoreProvider;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDelegates;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createEmptyCompositeDataStore;
import static org.junit.Assert.assertNotNull;

// Test the delegate management capabilities of the CompositeDataStore.
public class CompositeDataStoreDelegateManagementTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Set<DataStore> delegatesToDataStores(List<DataStoreProvider> delegates) {
        Set<DataStore> result = Sets.newHashSet();
        for (DataStoreProvider delegate : delegates) {
            result.add(delegate.getDataStore());
        }
        return result;
    }

    private void testAddDelegates(List<String> roles) throws IOException, RepositoryException {
        List<DataStoreProvider> delegates = createDelegates(folder.newFolder(), roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);

        for (DataStoreProvider delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);
    }

    private void verifyDelegatesInCompositeDataStore(List<DataStoreProvider> delegates, CompositeDataStore cds) {
        Set<DataStore> expectedDataStores = delegatesToDataStores(delegates);
        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        int delegateCount = 0;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            assertNotNull(ds);
            assertTrue(expectedDataStores.contains(ds));
            delegateCount++;
        }
        assertEquals(delegates.size(), delegateCount);
    }

    @Test
    public void testAddZeroDelegates() throws IOException, RepositoryException {
        testAddDelegates(Lists.newArrayList());
    }

    @Test
    public void testAddOneDelegate() throws IOException, RepositoryException {
        testAddDelegates(Lists.newArrayList("local1"));
    }

    @Test
    public void testAddTwoDelegates() throws IOException, RepositoryException {
        testAddDelegates(Lists.newArrayList("local1", "local2"));
    }

    @Test
    public void testAddThreeDelegates() throws IOException, RepositoryException {
        testAddDelegates(Lists.newArrayList("local1", "local2", "local3"));
    }

    @Test
    public void testAddDelegateWithMissingRole() throws IOException, RepositoryException {
        DataStoreProvider baseDataStoreProvider1 = createDataStoreProvider(folder.newFolder(), "local1", Maps.newHashMap());
        DataStoreProvider baseDataStoreProvider2 = createDataStoreProvider(folder.newFolder(), "local2", Maps.newHashMap());
        List<DataStoreProvider> delegates = Lists.newArrayList(
                new DataStoreProvider() {
                    @Override
                    public DataStore getDataStore() {
                        return baseDataStoreProvider1.getDataStore();
                    }

                    @Override
                    public String getRole() {
                        return null;
                    }

                    @Override
                    public Map<String, Object> getConfig() {
                        return baseDataStoreProvider1.getConfig();
                    }
                },
                new DataStoreProvider() {
                    @Override
                    public DataStore getDataStore() {
                        return baseDataStoreProvider2.getDataStore();
                    }

                    @Override
                    public String getRole() {
                        return "";
                    }

                    @Override
                    public Map<String, Object> getConfig() {
                        return baseDataStoreProvider2.getConfig();
                    }
                }
        );


        Map<String, Object> config = Maps.newHashMap();
        config.put(CompositeDataStore.ROLES, "local1,local2");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);

        for (DataStoreProvider delegate : delegates) {
            assertFalse(cds.addDelegate(delegate));

            Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testAddDelegateWithNonMatchingRole() throws IOException, RepositoryException {
        DataStoreProvider delegate = createDataStoreProvider(folder.newFolder(), "otherRole");

        Map<String, Object> config = Maps.newHashMap();
        config.put(CompositeDataStore.ROLES, "thisRole");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);
        assertFalse(cds.addDelegate(delegate));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveDelegate() throws IOException, RepositoryException {
        List<String> roles = Lists.newArrayList("local1");
        DataStoreProvider delegate = createDataStoreProvider(folder.newFolder(), roles.get(0));
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        assertTrue(cds.removeDelegate(delegate));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveOneOfManyDelegates() throws IOException, RepositoryException {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DataStoreProvider> delegates = createDelegates(folder.newFolder(), roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DataStoreProvider delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        assertTrue(cds.removeDelegate(delegates.get(1)));
        delegates.remove(1);

        Set<DataStore> dsps = delegatesToDataStores(delegates);

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertTrue(iter.hasNext());
        int delegateCount = 0;
        while (iter.hasNext()) {
            delegateCount++;
            DataStore ds = iter.next();
            assertTrue(dsps.contains(ds));
        }
        assertEquals(dsps.size(), delegateCount);
    }

    @Test
    public void testRemoveAllDelegates() throws IOException, RepositoryException {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DataStoreProvider> delegates = createDelegates(folder.newFolder(), roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DataStoreProvider delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        for (DataStoreProvider delegate : delegates) {
            assertTrue(cds.removeDelegate(delegate));
        }

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveNonMatchingDelegate() throws IOException, RepositoryException {
        List<String> roles = Lists.newArrayList("local1");
        DataStoreProvider delegate = createDataStoreProvider(folder.newFolder(), roles.get(0));
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        DataStoreProvider otherDsp = createDataStoreProvider(folder.newFolder(), "local1");
        assertFalse(cds.removeDelegate(otherDsp));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertTrue(iter.hasNext());
    }
}