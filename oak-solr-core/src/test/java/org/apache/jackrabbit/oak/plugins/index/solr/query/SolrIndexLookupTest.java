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

package org.apache.jackrabbit.oak.plugins.index.solr.query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SolrIndexLookup}
 */
public class SolrIndexLookupTest {
    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void collectPathOnRootNode() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newSolrIndexDefinition(index, "l1", of("foo"));
        newSolrIndexDefinition(index, "l2", of("foo"));

        SolrIndexLookup lookup = new SolrIndexLookup(builder.getNodeState());
        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictPath("/", Filter.PathRestriction.EXACT);
        assertEquals(of("/oak:index/l1", "/oak:index/l2"),
                lookup.collectIndexNodePaths(f));
    }

    @Test
    public void collectPathOnSubNode() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newSolrIndexDefinition(index, "l1", of("foo"));

        index = builder.child("a").child(INDEX_DEFINITIONS_NAME);
        newSolrIndexDefinition(index, "l2", of("foo"));

        index = builder.child("a").child("b").child(INDEX_DEFINITIONS_NAME);
        newSolrIndexDefinition(index, "l3", of("foo"));

        SolrIndexLookup lookup = new SolrIndexLookup(builder.getNodeState());
        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictPath("/a", Filter.PathRestriction.EXACT);
        assertEquals(of("/oak:index/l1", "/a/oak:index/l2"),
                lookup.collectIndexNodePaths(f));

        f.restrictPath("/a/b", Filter.PathRestriction.EXACT);
        assertEquals(of("/oak:index/l1", "/a/oak:index/l2", "/a/b/oak:index/l3"),
                lookup.collectIndexNodePaths(f));
    }

    private static NodeBuilder newSolrIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> properties) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, "solr")
                .setProperty(ASYNC_PROPERTY_NAME, "async")
                .setProperty(REINDEX_PROPERTY_NAME, true);
        index.child("server").setProperty("solrServerType", "embedded");
        if (properties != null && !properties.isEmpty()) {
            index.setProperty(createProperty("usedProperties",
                    properties, STRINGS));
        }
        return index;
    }
}
