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

package org.apache.jackrabbit.oak.composite;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MountInfoProviderServiceTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private MountInfoProviderService service = new MountInfoProviderService();

    @Test
    public void defaultSetup() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), Collections.<String, Object>emptyMap());

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertNotNull(provider);
        assertEquals(defaultMountInfoProvider(), provider);

        MockOsgi.deactivate(service, context.bundleContext());
        assertNull(context.getService(MountInfoProvider.class));
    }

    @Test
    public void mountWithConfig_Paths() throws Exception{
        MockOsgi.activate(service, context.bundleContext(),
                ImmutableMap.<String, Object>of("mountedPaths", new String[] {"/a", "/b"}));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoProviderService.PROP_MOUNT_NAME_DEFAULT);
        assertNotNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);
        assertFalse(m.isReadOnly());
        assertEquals(m, provider.getMountByPath("/a"));
        assertEquals(defMount, provider.getMountByPath("/x"));
    }

    @Test
    public void mountWithConfig_Name() throws Exception{
        MockOsgi.activate(service, context.bundleContext(),
                ImmutableMap.<String, Object>of(
                        "mountedPaths", new String[] {"/a", "/b"},
                        "mountName", "foo",
                        "readOnlyMount", true
                ));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoProviderService.PROP_MOUNT_NAME_DEFAULT);
        assertNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);

        m = provider.getMountByName("foo");
        assertEquals(m, provider.getMountByPath("/a"));
        assertEquals(defMount, provider.getMountByPath("/x"));
        assertTrue(m.isReadOnly());
    }

}