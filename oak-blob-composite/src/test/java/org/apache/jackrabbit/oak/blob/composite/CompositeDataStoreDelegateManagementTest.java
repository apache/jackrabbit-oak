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
import org.apache.jackrabbit.core.data.InMemoryDataStore;
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
import static org.junit.Assert.assertNotNull;

public class CompositeDataStoreTest {
    private DataStoreProvider createDataStoreProvider(final String role) {
        return new DataStoreProvider() {
            DataStore ds = new InMemoryDataStore();

            @Override
            public DataStore getDataStore() {
                return ds;
            }

            @Override
            public String getRole() {
                return role;
            }
        };
    }

    private List<DelegateDataStore> createDelegates(List<String> roles) {
        List<DelegateDataStore> delegates = Lists.newArrayList();
        for (String role : roles) {
            delegates.add(createDelegate(role));
        }
        return delegates;
    }

    private DelegateDataStore createDelegate(String role) {
        return createDelegate(
                createDataStoreProvider(role),
                role,
                Maps.newHashMap()
        );
    }

    private DelegateDataStore createDelegate(DataStoreProvider dsp, String role, Map<String, Object> cfg) {
        return DelegateDataStore.builder(dsp)
                .withConfig(cfg)
                .build();
    }

    private CompositeDataStore createCompositeDataStore(List<String> roles) {
        Map<String, Object> config = Maps.newHashMap();

        for (String role : roles) {
            config.put(role, "");
        }

        Properties properties = new Properties();
        properties.putAll(config);
        return new CompositeDataStore(properties);
    }

    private Set<DataStoreProvider> delegatesToDataStores(List<DelegateDataStore> delegates) {
        Set<DataStoreProvider> result = Sets.newHashSet();
        for (DelegateDataStore delegate : delegates) {
            result.add(delegate.getDataStore());
        }
        return result;
    }

    private void testAddDelegates(List<String> roles) {
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createCompositeDataStore(roles);

        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);
    }

    private void verifyDelegatesInCompositeDataStore(List<DelegateDataStore> delegates, CompositeDataStore cds) {
        Set<DataStoreProvider> expectedDataStores = delegatesToDataStores(delegates);
        Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
        int delegateCount = 0;
        while (iter.hasNext()) {
            DataStoreProvider ds = iter.next();
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
        config.put("local1", "");
        config.put("local2", "");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);

        for (DelegateDataStore delegate : delegates) {
            assertFalse(cds.addDelegate(delegate));

            Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
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
        config.put("thisRole", "");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);
        assertFalse(cds.addDelegate(delegate));

        Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveDelegate() {
        List<String> roles = Lists.newArrayList("local1");
        DelegateDataStore delegate = createDelegate(roles.get(0));
        CompositeDataStore cds = createCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        assertTrue(cds.removeDelegate(delegate.getDataStore()));

        Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveOneOfManyDelegates() {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createCompositeDataStore(roles);
        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        assertTrue(cds.removeDelegate(delegates.get(1).getDataStore()));
        delegates.remove(1);

        Set<DataStoreProvider> dsps = delegatesToDataStores(delegates);

        Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
        assertTrue(iter.hasNext());
        int delegateCount = 0;
        while (iter.hasNext()) {
            delegateCount++;
            DataStoreProvider dsp = iter.next();
            assertTrue(dsps.contains(dsp));
        }
        assertEquals(dsps.size(), delegateCount);
    }

    @Test
    public void testRemoveAllDelegates() {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createCompositeDataStore(roles);
        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.removeDelegate(delegate.getDataStore()));
        }

        Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveNonMatchingDelegate() {
        List<String> roles = Lists.newArrayList("local1");
        DelegateDataStore delegate = createDelegate(roles.get(0));
        CompositeDataStore cds = createCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        DataStoreProvider otherDsp = createDataStoreProvider("local1");
        assertFalse(cds.removeDelegate(otherDsp));

        Iterator<DataStoreProvider> iter = cds.getDelegateIterator();
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