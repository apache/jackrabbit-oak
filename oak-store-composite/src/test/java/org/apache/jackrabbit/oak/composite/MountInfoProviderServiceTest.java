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

import java.util.Hashtable;
import java.util.Map;
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
        service.activate(context.bundleContext(), propsBuilder().buildProviderServiceProps());

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertNotNull(provider);
        assertEquals(defaultMountInfoProvider(), provider);

        MockOsgi.deactivate(service, context.bundleContext());
        assertNull(context.getService(MountInfoProvider.class));
    }

    @Test
    public void mountWithDefaultMountInfoConfig() {
        registerActivateMountInfoConfig(propsBuilder().buildMountInfoProps());

        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext());

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertNotNull(provider);
        assertEquals(defaultMountInfoProvider(), provider);
    }

    @Test
    public void mountWithConfig_Paths() {
        registerActivateMountInfoConfig(propsBuilder().withMountPaths("/a", "/b").buildMountInfoProps());

        MockOsgi.injectServices(service, context.bundleContext());
        service.activate(context.bundleContext(), withExpectedMounts(MountInfoConfig.Props.DEFAULT_MOUNT_NAME));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.Props.DEFAULT_MOUNT_NAME);
        assertNotNull(m);
        Mount defMount = provider.getDefaultMount();
        assertNotNull(defMount);
        assertTrue(m.isReadOnly());
        assertEquals(m, provider.getMountByPath("/a"));
        assertEquals(defMount, provider.getMountByPath("/x"));
    }

    @Test
    public void mountWithConfig_Multiple() {
        registerActivateMountInfoConfig(propsBuilder().withMountName("foo").withMountPaths("/a").buildMountInfoProps());
        registerActivateMountInfoConfig(propsBuilder().withMountName("bar").withMountPaths("/b").buildMountInfoProps());
        registerActivateMountInfoConfig(propsBuilder().withMountName("baz").withMountPaths("/c").buildMountInfoProps());

        MockOsgi.injectServices(service, context.bundleContext());
        service.activate(context.bundleContext(), withExpectedMounts("foo", "bar", "baz"));
        MockOsgi.activate(service, context.bundleContext(), Map.of("expectedMounts", new String[]{"foo", "bar", "baz"}));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(3, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.Props.DEFAULT_MOUNT_NAME);
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
    public void mountWithConfig_Multiple_NotAllExpected() {
        registerActivateMountInfoConfig(propsBuilder().withMountName("foo").withMountPaths("/a").buildMountInfoProps());
        registerActivateMountInfoConfig(propsBuilder().withMountName("bar").withMountPaths("/b").buildMountInfoProps());

        MockOsgi.injectServices(service, context.bundleContext());
        service.activate(context.bundleContext(), withExpectedMounts("foo", "bar", "baz"));
        MockOsgi.activate(service, context.bundleContext(), Map.of("expectedMounts", new String[]{"foo", "bar", "baz"}));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertNull("Not all expected mounts have been provided", provider);
    }

    @Test
    public void mountWithConfig_Name() {
        registerActivateMountInfoConfig(propsBuilder().withMountName("foo").withMountPaths("/a", "/b").buildMountInfoProps());

        MockOsgi.injectServices(service, context.bundleContext());
        service.activate(context.bundleContext(), withExpectedMounts("foo"));

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.Props.DEFAULT_MOUNT_NAME);
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
        service.activate(context.bundleContext(), propsBuilder()
            .withMountPaths("/a", "/b")
            .withMountName("foo")
            .withReadonly(true)
            .withPathsSupportingFragments("/test/*$")
            .buildProviderServiceProps());

        MountInfoProvider provider = context.getService(MountInfoProvider.class);
        assertEquals(1, provider.getNonDefaultMounts().size());

        Mount m = provider.getMountByName(MountInfoConfig.Props.DEFAULT_MOUNT_NAME);
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

    private void registerActivateMountInfoConfig(MountInfoConfig.Props mountInfoProps) {
        MountInfoConfig mountInfoConfig = new MountInfoConfig();
        context.bundleContext().registerService(MountInfoConfig.class, mountInfoConfig, new Hashtable<String, Object>());
        mountInfoConfig.activate(context.bundleContext(), mountInfoProps);
    }

    private static MountInfoPropsBuilder propsBuilder() {
        return new MountInfoPropsBuilder();
    }

    private static MountInfoProviderService.Props withExpectedMounts(final String... expectedMounts) {
        return propsBuilder().withExpectedMounts(expectedMounts).buildProviderServiceProps();
    }
}