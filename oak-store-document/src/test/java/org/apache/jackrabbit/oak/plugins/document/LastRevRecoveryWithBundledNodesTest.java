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

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundledTypesRegistry;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LastRevRecoveryWithBundledNodesTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private LogCustomizer log = LogCustomizer.forLogger(LastRevRecoveryAgent.class)
            .contains("find document").enable(Level.WARN).create();

    private DocumentStore store = new MemoryDocumentStore();
    private DocumentNodeStore ns;

    @Before
    public void setUpBundling() throws CommitFailedException {
        ns = builderProvider.newBuilder().setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();
        NodeState registryState = BundledTypesRegistry.builder()
                .forType(NT_FILE)
                .include(JCR_CONTENT)
                .build();

        NodeBuilder builder = ns.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        builder.getChildNode(JCR_SYSTEM)
                .getChildNode(DOCUMENT_NODE_STORE)
                .getChildNode(BUNDLOR)
                .setChildNode(NT_FILE, registryState.getChildNode(NT_FILE));
        merge(ns, builder);
        ns.runBackgroundOperations();
    }

    @Test
    public void lastRevRecoveryWithBundledNodes() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder file = builder.child("file");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE);
        file.child(JCR_CONTENT).child("metadata");
        merge(ns, builder);
        ns.runBackgroundOperations();

        log.starting();
        try {
            LastRevRecoveryAgent agent = ns.getLastRevRecoveryAgent();
            assertEquals(0, agent.recover(Utils.getAllDocuments(store), ns.getClusterId(), true));
            // must not log any warn messages
            assertThat(log.getLogs(), is(empty()));
        } finally {
            log.finished();
        }
    }

    @Test
    public void lastRevRecoveryWithMissingDocument() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder file = builder.child("file");
        file.child(JCR_CONTENT).child("metadata");
        merge(ns, builder);
        ns.runBackgroundOperations();

        String id = Utils.getIdFromPath(Path.fromString("/file/" + JCR_CONTENT));
        store.remove(Collection.NODES, id);

        log.starting();
        try {
            LastRevRecoveryAgent agent = ns.getLastRevRecoveryAgent();
            assertEquals(0, agent.recover(Utils.getAllDocuments(store), ns.getClusterId(), true));
            // must log a message about the deleted document
            assertThat(log.getLogs(), contains(containsString(id)));
        } finally {
            log.finished();
        }

    }
}
