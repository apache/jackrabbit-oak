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

package org.apache.jackrabbit.oak.jcr.osgi;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RepositoryManagerTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void executorSetup() throws Exception {
        registerRequiredServices();

        context.registerInjectActivateService(new RepositoryManager());

        Executor executor = context.getService(Executor.class);
        assertNotNull("Repository initialization should have registered an Executor", executor);

        final AtomicBoolean invoked = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                invoked.set(true);
                latch.countDown();
            }
        });

        latch.await(5, TimeUnit.SECONDS);
        assertTrue(invoked.get());
    }

    @Test
    public void repositoryShutdown() throws Exception{
        registerRequiredServices();

        RepositoryManager mgr = context.registerInjectActivateService(new RepositoryManager());
        assertNotNull("MBean should be registered", context.getService(RepositoryManagementMBean.class));

        mgr.deactivate();

        assertNull("MBean should have been removed upon repository shutdown",
                context.getService(RepositoryManagementMBean.class));

    }


    private void registerRequiredServices() {
        context.registerService(SecurityProvider.class, new OpenSecurityProvider());
        context.registerService(NodeStore.class, new MemoryNodeStore());
        context.registerService(IndexEditorProvider.class, new PropertyIndexEditorProvider(),
                ImmutableMap.<String, Object>of("type", "property"));
        context.registerService(IndexEditorProvider.class, new ReferenceEditorProvider(),
                ImmutableMap.<String, Object>of("type", "reference"));
    }

}


