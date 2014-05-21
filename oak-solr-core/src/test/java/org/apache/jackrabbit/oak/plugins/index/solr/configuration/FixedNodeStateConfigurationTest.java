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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.FixedNodeStateConfiguration}
 */
public class FixedNodeStateConfigurationTest {

    private NodeStore store;

    @Before
    public void setUp() throws Exception {
        store = new SegmentNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.setProperty("a", 1)
               .setProperty("b", 2)
               .setProperty("c", 3);

        builder.setChildNode("x");
        builder.setChildNode("y");
        builder.setChildNode("z");

        builder.setChildNode("oak:index").setChildNode("solrIdx")
               .setProperty("coreName", "cn")
               .setProperty("solrHomePath", "sh")
               .setProperty("solrConfigPath", "sc");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void testExistingPath() throws Exception {
        NodeState idxDef = store.getRoot().getChildNode("oak:index").getChildNode("solrIdx");
        FixedNodeStateConfiguration fixedNodeStateConfiguration = new FixedNodeStateConfiguration(idxDef);
        EmbeddedSolrServerConfiguration solrServerConfiguration = (EmbeddedSolrServerConfiguration) fixedNodeStateConfiguration.getSolrServerConfiguration();
        assertNotNull(solrServerConfiguration);
        assertEquals("sh", solrServerConfiguration.getSolrHomePath()); // property defined in the node state
        assertEquals("cn", solrServerConfiguration.getCoreName()); // property defined in the node state
        assertEquals("sc", solrServerConfiguration.getSolrConfigPath()); // property defined in the node state
        assertEquals("path_exact", fixedNodeStateConfiguration.getPathField()); // using default as this property is not defined in the node state
    }

    @Test
    public void testNonExistingPath() throws Exception {
        NodeState idxDef = store.getRoot().getChildNode("oak:index").getChildNode("a");
        FixedNodeStateConfiguration fixedNodeStateConfiguration = new FixedNodeStateConfiguration(idxDef);
        assertNotNull(fixedNodeStateConfiguration.getSolrServerConfiguration());
    }

    @Test
    public void testWrongNodeState() throws Exception {
        NodeState idxDef = store.getRoot().getChildNode("a");
        FixedNodeStateConfiguration fixedNodeStateConfiguration = new FixedNodeStateConfiguration(idxDef);
        assertFalse(fixedNodeStateConfiguration.getConfigurationNodeState().exists());
        assertNotNull(fixedNodeStateConfiguration.getSolrServerConfiguration()); // defaults are used
    }
}
