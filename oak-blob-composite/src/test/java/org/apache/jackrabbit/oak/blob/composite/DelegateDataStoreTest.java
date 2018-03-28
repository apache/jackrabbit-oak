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
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;

public class DelegateDataStoreTest {
    private DataStoreProvider ds;
    private String defaultRole = "local1";
    private Map<String, Object> defaultConfig = Maps.newConcurrentMap();

    @Before
    public void setup() {
        ds = createDataStoreProvider(defaultRole);
        defaultConfig.put("prop1", "val1");
    }

    DataStoreProvider createDataStoreProvider(final String role) {
        return new DataStoreProvider() {
            DataStore ds = new OakFileDataStore();
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

    void validate(DelegateDataStore delegate,
                  DataStoreProvider ds,
                  String role,
                  Map<String, ?> config) {
        assertNotNull(delegate);
        assertEquals(ds, delegate.getDataStore());
        assertEquals(role, delegate.getRole());
        if (null != config) {
            if (config.containsKey(DataStoreProvider.ROLE)) {
                assertEquals(config.size(), delegate.getConfig().size());
            }
            else {
                assertEquals(config.size()+1, delegate.getConfig().size());
            }
            for (String key : config.keySet()) {
                assertTrue(delegate.getConfig().containsKey(key));
                assertEquals(config.get(key), delegate.getConfig().get(key));
            }
        }
        else {
            assertEquals(1, delegate.getConfig().size()); // Should have the role only
        }
    }

    @Test
    public void testBuildCompositeDataStoreDelegate() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .withConfig(defaultConfig)
                        .build();

        validate(delegate, ds, defaultRole, defaultConfig);
    }

    @Test
    public void testBuildWithNullDataStoreProviderFails() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(null)
                        .withConfig(defaultConfig)
                        .build();
        assertNull(delegate);
    }

    @Test
    public void testBuildWithoutConfig() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .build();
        Map<String, Object> cfg = Maps.newHashMap();
        cfg.put(DataStoreProvider.ROLE, defaultRole);
        validate(delegate, ds, defaultRole, cfg);
    }

    @Test
    public void testBuildWithNullConfig() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .withConfig(null)
                        .build();

        validate(delegate, ds, defaultRole, null);
    }

    @Test
    public void testBuildWithEmptyConfig() {
        Map<String, Object> cfg = Maps.newHashMap();
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .withConfig(cfg)
                        .build();

        validate(delegate, ds, defaultRole, cfg);
    }

    @Test
    public void testIsReadOnly() {
        Map<String, Object> cfg = Maps.newHashMap();
        DelegateDataStore delegate;

        // Test forms of true
        List<String> formsOfTrue = Lists.newArrayList();
        formsOfTrue.add("true");
        formsOfTrue.add("True");
        formsOfTrue.add("TRUE");
        for (String ro : formsOfTrue) {
            cfg.put("readOnly", ro);
            delegate = DelegateDataStore.builder(ds)
                    .withConfig(cfg)
                    .build();
            assertTrue(delegate.isReadOnly());
        }

        // Test forms of false
        List<String> formsOfFalse = Lists.newArrayList();
        formsOfFalse.add("false");
        formsOfFalse.add("False");
        formsOfFalse.add("FALSE");
        formsOfFalse.add("something else");
        for (String ro : formsOfFalse) {
            cfg.put("readOnly", ro);
            delegate = DelegateDataStore.builder(ds)
                    .withConfig(cfg)
                    .build();
            assertFalse(delegate.isReadOnly());
        }
    }
}