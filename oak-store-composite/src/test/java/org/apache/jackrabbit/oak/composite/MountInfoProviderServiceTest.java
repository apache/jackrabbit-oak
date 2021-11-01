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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MountInfoProviderServiceTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private final MountInfoProviderService service = new MountInfoProviderService();

    @Test
    public void defaultSetup() {
        MockOsgi.activate(service, context.bundleContext(), Collections.emptyMap());

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertNotNull(provider);
        assertEquals(defaultMountInfoProvider(), provider);

        MockOsgi.deactivate(service, context.bundleContext());
        assertNull(context.getService(MountInfoProvider.class));
    }

    @Test
    public void mountWithDefaultMountInfoConfig() {
        registerActivateMountInfoConfig(Collections.emptyMap());

        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext());

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertNotNull(provider);
        assertEquals(defaultMountInfoProvider(), provider);
    }

    @Test
    public void mountWithConfig_Paths() {
        registerActivateMountInfoConfig(ImmutableList.of("/a", "/b"));

        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.of("expectedMounts", new String[]{MountInfoConfig.PROP_MOUNT_NAME_DEFAULT}));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.PROP_MOUNT_NAME_DEFAULT);
        assertNotNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);
        assertTrue(m.isReadOnly());
        assertEquals(m, provider.getMountByPath("/a"));
        assertEquals(defMount, provider.getMountByPath("/x"));
    }

    @Test
    public void mountWithConfig_Multiple() {
        registerActivateMountInfoConfig("foo", "/a");
        registerActivateMountInfoConfig("bar", "/b");
        registerActivateMountInfoConfig("baz", "/c");

        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.of("expectedMounts", new String[]{"foo", "bar", "baz"}));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(3, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.PROP_MOUNT_NAME_DEFAULT);
        assertNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);

        m = provider.getMountByName("foo");
        assertNotNull(m);
        assertEquals(m, provider.getMountByPath("/a"));
        assertNotEquals(m, provider.getMountByPath("/b"));
        assertNotEquals(m, provider.getMountByPath("/c"));

        m = provider.getMountByName("bar");
        assertNotNull(m);
        assertNotEquals(m, provider.getMountByPath("/a"));
        assertEquals(m, provider.getMountByPath("/b"));
        assertNotEquals(m, provider.getMountByPath("/c"));

        m = provider.getMountByName("baz");
        assertNotNull(m);
        assertNotEquals(m, provider.getMountByPath("/a"));
        assertNotEquals(m, provider.getMountByPath("/b"));
        assertEquals(m, provider.getMountByPath("/c"));
    }

    @Test
    public void mountWithConfig_Name() {
        registerActivateMountInfoConfig("foo", ImmutableList.of("/a", "/b"));

        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.of("expectedMounts", "foo"));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.PROP_MOUNT_NAME_DEFAULT);
        assertNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);

        m = provider.getMountByName("foo");
        assertNotNull(m);
        assertEquals(m, provider.getMountByPath("/a"));
        assertEquals(m, provider.getMountByPath("/b"));
        assertEquals(defMount, provider.getMountByPath("/x"));
        assertTrue(m.isReadOnly());
    }

    @Test
    public void mountWithConfig_BackwardCompatible() {
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.of(
            "mountedPaths", new String[] {"/a", "/b"},
            "mountName", "foo",
            "readOnlyMount", true,
            "pathsSupportingFragments", new String[] {"/test/*$"}
        ));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.PROP_MOUNT_NAME_DEFAULT);
        assertNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);

        m = provider.getMountByName("foo");
        assertNotNull(m);
        assertEquals(m, provider.getMountByPath("/a"));
        assertEquals(m, provider.getMountByPath("/b"));
        assertEquals(defMount, provider.getMountByPath("/x"));
        assertTrue(m.isReadOnly());
        assertTrue(m.isSupportFragmentUnder("/test"));
    }

    private void registerActivateMountInfoConfig(Map<String, Object> mountProperties) {
        MountInfoConfig mountInfoConfig = new MountInfoConfig();
        context.bundleContext().registerService(MountInfoConfig.class.getName(), mountInfoConfig, new Properties());
        MockOsgi.activate(mountInfoConfig, context.bundleContext(), mountProperties);
    }

    private void registerActivateMountInfoConfig(String mountName, List<String> mountedPaths) {
        registerActivateMountInfoConfig(ImmutableMap.of(
            "mountedPaths", mountedPaths.toArray(),
            "mountName", mountName
        ));
    }

    private void registerActivateMountInfoConfig(String mountName, String mountedPath) {
        registerActivateMountInfoConfig(mountName, ImmutableList.of(mountedPath));
    }

    private void registerActivateMountInfoConfig(List<String> mountedPaths) {
        registerActivateMountInfoConfig(MountInfoConfig.PROP_MOUNT_NAME_DEFAULT, mountedPaths);
    }

}