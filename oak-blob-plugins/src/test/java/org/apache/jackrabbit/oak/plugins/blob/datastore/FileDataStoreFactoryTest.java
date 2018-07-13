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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.service.component.ComponentContext;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileDataStoreFactoryTest {
    @Rule
    public OsgiContext context = new OsgiContext();

    @Before
    public void setup() {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
    }

    private void activateService() {
        activateService(new FileDataStoreFactory(), null);
    }

    private void activateService(final AbstractDataStoreFactory factory) {
        activateService(factory, null);
    }

    private void activateService(final String role) {
        activateService(new FileDataStoreFactory(), role);
    }

    private void activateService(final AbstractDataStoreFactory factory, final String role) {
        Map<String, Object> config = Maps.newHashMap();
        if (null != role) {
            config.put(DataStoreProvider.ROLE, role);
        }
        context.registerInjectActivateService(factory, config);
    }

    @Test
    public void testActivate() throws IOException {
        activateService("local");
        DataStoreProvider dsp = context.getService(DataStoreProvider.class);
        assertNotNull(dsp);
        DataStore ds = dsp.getDataStore();
        assertNotNull(ds);
        if (! (ds instanceof OakFileDataStore) && ! (ds instanceof CachingFileDataStore)) {
            fail();
        }
    }

    @Test
    public void testActivateWithoutRole() throws IOException {
        activateService();
        assertNull(context.getService(DataStoreProvider.class));
    }

    @Test
    public void testActivateWithoutDataStore() throws IOException {
        FileDataStoreFactory factory = mock(FileDataStoreFactory.class);
        when(factory.createDataStore(any(ComponentContext.class), anyMapOf(String.class, Object.class))).thenReturn(null);

        activateService(factory, "local");
        assertNull(context.getService(DataStoreProvider.class));
    }

    @Test
    public void testActivateMultiple() throws IOException {
        Set<String> roles = Sets.newHashSet("local1", "local2", "cloud1");
        for (String role : roles) {
            activateService(role);
        }
        DataStoreProvider[] dsps = context.getServices(DataStoreProvider.class, null);
        assertEquals(3, dsps.length);
        for (DataStoreProvider dsp : dsps) {
            assertTrue(roles.contains(dsp.getRole()));
        }

        for (String role : roles) {
            dsps = context.getServices(DataStoreProvider.class, String.format("(role=%s)", role));
            assertEquals(1, dsps.length);
            DataStoreProvider dsp = dsps[0];
            assertEquals(role, dsp.getRole());
        }

        // Invalid role
        dsps = context.getServices(DataStoreProvider.class, "(role=notarole)");
        assertEquals(0, dsps.length);
    }
}
