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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link PrivateStoreValidatorProvider}
 */
public class PrivateStoreValidatorProviderTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private ContentRepository repository;
    private PrivateStoreValidatorProvider privateStoreValidatorProvider = new PrivateStoreValidatorProvider();

    private void setUp(String... readOnlyPaths) {
        configureMountInfoProvider(readOnlyPaths);
        repository = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(privateStoreValidatorProvider)
                .createContentRepository();
    }

    @After
    public void tearDown() throws IOException {
        if (repository instanceof Closeable){
            ((Closeable) repository).close();
        }
    }

    @Test
    public void testDefaultMount() throws Exception {
        setUp();

        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree t = r.getTree("/").addChild("test");
        t.addChild("node1").setProperty("jcr:primaryType", "nt:base");
        t.addChild("node2").setProperty("jcr:primaryType", "nt:base");
        t.addChild("node3").setProperty("jcr:primaryType", "nt:base");
        r.commit();

        t.getChild("node1").removeProperty("jcr:primaryType");
        r.commit();

        t.getChild("node1").remove();
        r.commit();
    }

    @Test
    public void testReadOnlyMounts() throws Exception {
        // default mount info provider to setup some content
        setUp();
        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree t = r.getTree("/").addChild("content");
        t.addChild("node1").setProperty("jcr:primaryType", "nt:base");

        Tree readonlyRoot = t.addChild("readonly");
        readonlyRoot.setProperty("jcr:primaryType", "nt:base");
        readonlyRoot.addChild("readonlyChild").setProperty("jcr:primaryType", "nt:base");

        r.commit();

        // register a different mount info provider
        configureMountInfoProvider("/content/readonly");

        // commits under /content/readonly should now fail
        s = repository.login(null, null);

        // changes that are not under the read-only mount should work
        r = s.getLatestRoot();
        t = r.getTree("/").addChild("content");
        t.addChild("node2").setProperty("jcr:primaryType", "nt:base");
        r.commit();

        // changes under the read-only mount should fail
        readonlyRoot = t.getChild("readonly");
        readonlyRoot.setProperty("testProp", "test");
        try {
            r.commit();
            Assert.fail("Commit to read-only mount should fail!");
        } catch (CommitFailedException ignore) {
        }

        r.refresh();
        readonlyRoot = t.getChild("readonly");
        readonlyRoot.getChild("readonlyChild").remove();
        try {
            r.commit();
            Assert.fail("Commit to read-only mount should fail!");
        } catch (CommitFailedException ignore) {
        }
    }

    @Test
    public void testValidatorServiceRegistered() {
        // test service registration, there should be a service for the PrivateStoreValidatorProvider
        MountInfoProvider mountInfoProvider = createMountInfoProvider("/content/readonly");
        context.registerService(MountInfoProvider.class, mountInfoProvider);
        registerValidatorProvider(privateStoreValidatorProvider, true);

        EditorProvider validator = context.getService(EditorProvider.class);
        assertNotNull("No PrivateStoreValidatorProvider available!", validator);
        assertTrue(validator instanceof PrivateStoreValidatorProvider);
        assertTrue(((PrivateStoreValidatorProvider)validator).isFailOnDetection());

        MockOsgi.deactivate(privateStoreValidatorProvider, context.bundleContext());
        assertNull(context.getService(EditorProvider.class));
    }

    @Test
    public void testValidatorServiceNotRegistered() {
        // test service registration, for default mount there should be no service for the validator provider
        MountInfoProvider mountInfoProvider = createMountInfoProvider();
        context.registerService(MountInfoProvider.class, mountInfoProvider);
        registerValidatorProvider(privateStoreValidatorProvider, true);

        EditorProvider validator = context.getService(EditorProvider.class);
        assertNull("No PrivateStoreValidatorProvider should be registered for default mounts!", validator);
    }


    private void registerValidatorProvider(PrivateStoreValidatorProvider validatorProvider, boolean failOnDetection) {
        Map<String, Object> propMap = new HashMap<>();
        propMap.put("failOnDetection", failOnDetection);

        MockOsgi.injectServices(validatorProvider, context.bundleContext());
        MockOsgi.activate(validatorProvider, context.bundleContext(), propMap);
    }

    /**
     * Register a {@link MountInfoProvider} service
     * If the given path array is empty, the {@code MountInfoProvider.DEFAULT} will be registered.
     *
     * @param readOnlyPaths - contains the string paths mounted on a read-only store
     */
    private void configureMountInfoProvider(String... readOnlyPaths) {
        MountInfoProvider mountInfoProvider = createMountInfoProvider(readOnlyPaths);
        privateStoreValidatorProvider.setMountInfoProvider(mountInfoProvider);
        privateStoreValidatorProvider.setFailOnDetection(true);
    }

    private MountInfoProvider createMountInfoProvider(String... readOnlyPaths) {
        MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();
        if (readOnlyPaths.length > 0) {
            mountInfoProvider = Mounts.newBuilder().readOnlyMount("readOnly", readOnlyPaths).build();
        }
        return mountInfoProvider;
    }
}
