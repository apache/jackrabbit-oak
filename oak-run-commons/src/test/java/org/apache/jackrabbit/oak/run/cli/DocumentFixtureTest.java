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

package org.apache.jackrabbit.oak.run.cli;

import java.io.IOException;
import java.util.List;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DocumentFixtureTest {

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Test
    public void documentNodeStore() throws Exception{
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(createMongoOptions(), false)) {
            NodeStore store = fixture.getStore();
            NodeBuilder builder = store.getRoot().builder();
            builder.setChildNode("foo");
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            assertNotNull(fixture.getBlobStore());
            assertClusterInvisible(store);
        }
    }

    @Test
    public void customizer() throws Exception{
        Options o = createMongoOptions();
        DocumentBuilderCustomizer customizer = mock(DocumentBuilderCustomizer.class);
        o.getWhiteboard().register(DocumentBuilderCustomizer.class, customizer, emptyMap());
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(o, false)) {

        }

        verify(customizer, times(1)).customize(any(DocumentNodeStoreBuilder.class));
    }

    private Options createMongoOptions() throws IOException {
        OptionParser parser = new OptionParser();
        Options opts = new Options().withDisableSystemExit();
        opts.parseAndConfigure(parser, new String[] {MongoUtils.URL});
        return opts;
    }

    private void assertClusterInvisible(NodeStore store) {
        List<ClusterNodeInfoDocument> clusterInfos =
            ClusterNodeInfoDocument.all(((DocumentNodeStore) store).getDocumentStore());
        assertNotNull(clusterInfos);
        assertTrue(clusterInfos.get(0).isInvisible());
    }
}
