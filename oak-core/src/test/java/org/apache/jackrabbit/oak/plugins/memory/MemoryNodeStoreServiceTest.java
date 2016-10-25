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

package org.apache.jackrabbit.oak.plugins.memory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MemoryNodeStoreServiceTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private MemoryNodeStoreService service = new MemoryNodeStoreService();

    @Test
    public void registration() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), Collections.<String, Object>emptyMap());
        assertNotNull(context.getService(NodeStore.class));
    }

    @Test
    public void observers() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), Collections.<String, Object>emptyMap());

        final AtomicBoolean invoked = new AtomicBoolean();
        Observer o = new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                invoked.set(true);
            }
        };
        context.registerService(Observer.class, o);

        NodeStore store = context.getService(NodeStore.class);
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(invoked.get());

    }

}