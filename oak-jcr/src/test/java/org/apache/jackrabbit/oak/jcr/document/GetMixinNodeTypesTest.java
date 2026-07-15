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
package org.apache.jackrabbit.oak.jcr.document;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.CountingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.closeIfCloseable;
import static org.junit.Assert.assertEquals;

/**
 * Test for OAK-7195 using a DocumentNodeStore to verify there are no calls
 * that check the existence of a jcr:mixinTypes child node when
 * getMixinNodeTypes() is called.
 */
public class GetMixinNodeTypesTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private CountingDocumentStore store = new CountingDocumentStore(new MemoryDocumentStore());

    private DocumentNodeStore ns;

    private Repository repository;

    private Session session;

    @Before
    public void before() throws Exception {
        ns = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(store).build();
        repository = new Jcr(ns).createRepository();
        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    @After
    public void after() throws Exception {
        session.logout();
        closeIfCloseable(repository);
    }

    @Test
    public void getMixinNodeTypes() throws Exception {
        Node foo = session.getRootNode().addNode("foo");
        foo.addNode("bar");
        session.save();
        foo.getPrimaryNodeType();
        ns.getNodeChildrenCache().invalidateAll();
        int numCalls = store.getNumFindCalls(NODES);
        // getMixinNodeTypes() must not trigger a read on the DocumentStore
        assertEquals(0, foo.getMixinNodeTypes().length);
        assertEquals(0, store.getNumFindCalls(NODES) - numCalls);
    }
}
