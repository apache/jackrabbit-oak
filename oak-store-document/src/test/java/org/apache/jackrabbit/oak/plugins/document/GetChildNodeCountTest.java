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
package org.apache.jackrabbit.oak.plugins.document;

import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.closeIfCloseable;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class GetChildNodeCountTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private CountingDocumentStore store = new CountingDocumentStore(new MemoryDocumentStore());

    private DocumentNodeStore ns;

    private ContentRepository repository;

    private ContentSession session;

    @Before
    public void before() throws Exception {
        ns = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(store).getNodeStore();
        repository = new Oak(ns)
                .with(new PropertyIndexEditorProvider())
                .with(new OpenSecurityProvider())
                .createContentRepository();
        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        Root root = session.getLatestRoot();
        Tree idx = root.getTree("/").addChild("oak:index").addChild("p");
        idx.setProperty("type", "property");
        idx.setProperty("propertyNames", newArrayList("p"), Type.NAMES);
        root.commit();
    }

    @After
    public void after() throws Exception {
        session.close();
        closeIfCloseable(repository);
        ns.dispose();
    }

    @Test
    public void removeIndexedNodes() throws Exception {
        Root root = session.getLatestRoot();
        Tree t = root.getTree("/").addChild("test");
        for (int i = 0; i < 200; i ++) {
            t.addChild("node-" + i).setProperty("p", "v");
        }
        root.commit();
        ns.getNodeChildrenCache().invalidateAll();
        store.resetCounters();
        // remove nodes
        root = session.getLatestRoot();
        root.getTree("/test").remove();
        root.commit();
        assertThat(store.getNumQueryCalls(NODES), lessThan(13));
    }
}
