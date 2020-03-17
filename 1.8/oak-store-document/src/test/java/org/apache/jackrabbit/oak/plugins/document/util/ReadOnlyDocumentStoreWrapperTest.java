/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadOnlyDocumentStoreWrapperTest {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void testPassthrough() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final List<String> disallowedMethods = Lists.newArrayList(
                "create", "update", "remove", "createOrUpdate", "findAndUpdate");
        InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String methodName = method.getName();

                if (disallowedMethods.contains(methodName)) {
                    Assert.fail(String.format("Invalid passthrough of method (%s) with params %s", method, Arrays.toString(args)));
                }

                if ("determineServerTimeDifferenceMillis".equals(methodName)) {
                    return new Long(0);
                } else {
                    return null;
                }
            }
        };
        DocumentStore proxyStore = (DocumentStore)Proxy.newProxyInstance(DocumentStore.class.getClassLoader(),
                new Class[]{DocumentStore.class},
                handler);

        DocumentStore readOnlyStore = ReadOnlyDocumentStoreWrapperFactory.getInstance(proxyStore);

        Collection<? extends Document> []collections = new Collection[] {
                Collection.CLUSTER_NODES, Collection.JOURNAL, Collection.NODES, Collection.SETTINGS
        };
        for (Collection collection : collections) {
            readOnlyStore.find(collection, null);
            readOnlyStore.find(collection, null, 0);

            readOnlyStore.query(collection, null, null, 0);
            readOnlyStore.query(collection, null, null, null, 0, 0);

            boolean uoeThrown = false;
            try {
                readOnlyStore.remove(collection, "");
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("remove must throw UnsupportedOperationException", uoeThrown);

            uoeThrown = false;
            try {
                readOnlyStore.remove(collection, Lists.<String>newArrayList());
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("remove must throw UnsupportedOperationException", uoeThrown);

            uoeThrown = false;
            try {
                readOnlyStore.remove(collection, Maps.<String, Long>newHashMap());
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("remove must throw UnsupportedOperationException", uoeThrown);
            uoeThrown = false;

            try {
                readOnlyStore.create(collection, null);
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("create must throw UnsupportedOperationException", uoeThrown);
            uoeThrown = false;

            try {
                readOnlyStore.createOrUpdate(collection, (UpdateOp) null);
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("createOrUpdate must throw UnsupportedOperationException", uoeThrown);
            uoeThrown = false;

            try {
                readOnlyStore.createOrUpdate(collection, Lists.<UpdateOp>newArrayList());
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("createOrUpdate must throw UnsupportedOperationException", uoeThrown);
            uoeThrown = false;

            try {
                readOnlyStore.findAndUpdate(collection, null);
            } catch (UnsupportedOperationException uoe) {
                //catch uoe thrown by read only wrapper
                uoeThrown = true;
            }
            assertTrue("findAndUpdate must throw UnsupportedOperationException", uoeThrown);

            readOnlyStore.invalidateCache(collection, null);
            readOnlyStore.getIfCached(collection, null);
        }

        readOnlyStore.invalidateCache();
        readOnlyStore.invalidateCache(null);

        readOnlyStore.dispose();
        readOnlyStore.setReadWriteMode(null);
        readOnlyStore.getCacheStats();
        readOnlyStore.getMetadata();
        readOnlyStore.determineServerTimeDifferenceMillis();
    }

    @Test
    public void backgroundRead() throws Exception {
        DocumentStore docStore = new MemoryDocumentStore();

        DocumentNodeStore store = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(docStore).setClusterId(2).getNodeStore();
        DocumentNodeStore readOnlyStore = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(docStore).setClusterId(1).setReadOnlyMode().getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("node");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.runBackgroundOperations();

        // at this point node must not be visible
        assertFalse(readOnlyStore.getRoot().hasChildNode("node"));

        readOnlyStore.runBackgroundOperations();

        // at this point node should get visible
        assertTrue(readOnlyStore.getRoot().hasChildNode("node"));
    }
}
