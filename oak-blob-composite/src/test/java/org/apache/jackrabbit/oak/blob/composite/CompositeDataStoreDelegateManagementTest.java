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
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDataStoreProvider;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDelegates;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createEmptyCompositeDataStore;
import static org.junit.Assert.assertNotNull;

// Test the delegate management capabilities of the CompositeDataStore.
public class CompositeDataStoreDelegateManagementTest {

    private Set<DataStore> delegatesToDataStores(List<DelegateDataStore> delegates) {
        Set<DataStore> result = Sets.newHashSet();
        for (DelegateDataStore delegate : delegates) {
            result.add(delegate.getDataStore().getDataStore());
        }
        return result;
    }

    private void testAddDelegates(List<String> roles) {
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);

        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);
    }

    private void verifyDelegatesInCompositeDataStore(List<DelegateDataStore> delegates, CompositeDataStore cds) {
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
    public void testAddZeroDelegates() {
        testAddDelegates(Lists.newArrayList());
    }

    @Test
    public void testAddOneDelegate() {
        testAddDelegates(Lists.newArrayList("local1"));
    }

    @Test
    public void testAddTwoDelegates() {
        testAddDelegates(Lists.newArrayList("local1", "local2"));
    }

    @Test
    public void testAddThreeDelegates() {
        testAddDelegates(Lists.newArrayList("local1", "local2", "local3"));
    }

    @Test
    public void testAddDelegateWithMissingRole() {
        List<DelegateDataStore> delegates = Lists.newArrayList(
                new MissingRoleCompositeDataStoreDelegate(
                        createDataStoreProvider("local1"),
                        Maps.newHashMap()
                ),
                new EmptyRoleCompositeDataStoreDelegate(
                        createDataStoreProvider("local2"),
                        Maps.newHashMap()
                )
        );


        Map<String, Object> config = Maps.newHashMap();
        config.put(CompositeDataStore.ROLES, "local1,local2");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);

        for (DelegateDataStore delegate : delegates) {
            assertFalse(cds.addDelegate(delegate));

            Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testAddDelegateWithNonMatchingRole() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(createDataStoreProvider("otherRole"))
                .withConfig(Maps.newHashMap())
                .build();

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
    public void testRemoveDelegate() {
        List<String> roles = Lists.newArrayList("local1");
        DelegateDataStore delegate = createDelegate(roles.get(0));
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        assertTrue(cds.removeDelegate(delegate.getDataStore()));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveOneOfManyDelegates() {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        assertTrue(cds.removeDelegate(delegates.get(1).getDataStore()));
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
    public void testRemoveAllDelegates() {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.removeDelegate(delegate.getDataStore()));
        }

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveNonMatchingDelegate() {
        List<String> roles = Lists.newArrayList("local1");
        DelegateDataStore delegate = createDelegate(roles.get(0));
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        DataStoreProvider otherDsp = createDataStoreProvider("local1");
        assertFalse(cds.removeDelegate(otherDsp));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertTrue(iter.hasNext());
    }

    static class MissingRoleCompositeDataStoreDelegate extends DelegateDataStore {
        MissingRoleCompositeDataStoreDelegate(DataStoreProvider dsp, Map<String, Object> config) {
            super(dsp, config);
        }

        @Override
        public String getRole() {
            return null;
        }
    }

    static class EmptyRoleCompositeDataStoreDelegate extends DelegateDataStore {
        EmptyRoleCompositeDataStoreDelegate(DataStoreProvider dsp, Map<String, Object> config) {
            super(dsp, config);
        }

        @Override
        public String getRole() {
            return "";
        }
    }
}