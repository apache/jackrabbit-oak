/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import static com.google.common.collect.Sets.newHashSet;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.solr.SolrBaseTest;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class SolrIndexHookIT extends SolrBaseTest {

    @Test
    public void testSingleNodeCreation() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("solr")
                .setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition")
                .setProperty("type", "solr");

        NodeState before = builder.getNodeState();
        builder.child("newnode").setProperty("prop", "val");
        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after);

        QueryIndex queryIndex = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(null, null);
        filter.restrictPath("/newnode", Filter.PathRestriction.EXACT);
        filter.restrictProperty("prop", Operator.EQUAL,
                PropertyValues.newString("val"));
        Cursor cursor = queryIndex.query(filter, indexed);
        assertNotNull(cursor);
        assertTrue("no results found", cursor.hasNext());
        IndexRow next = cursor.next();
        assertNotNull("first returned item should not be null", next);
        assertEquals("/newnode", next.getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testPropertyAddition() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("solr")
                .setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition")
                .setProperty("type", "solr");

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after);

        QueryIndex queryIndex = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(null, null);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(filter, indexed);
        assertNotNull(cursor);
        assertTrue("no results found", cursor.hasNext());
        IndexRow next = cursor.next();
        assertNotNull("first returned item should not be null", next);
        assertEquals("/", next.getPath());
        assertNotNull(next.getValue("foo"));
        assertEquals(PropertyValues.newString("[bar]"), next.getValue("foo"));
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testSomeNodesCreationWithFullText() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("solr")
                .setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition")
                .setProperty("type", "solr");

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after);

        QueryIndex queryIndex = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(null, null);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        filter.restrictFulltextCondition("bar");
        Cursor cursor = queryIndex.query(filter, indexed);

        Set<String> paths = newHashSet();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        assertTrue(paths.remove("/"));
        assertTrue(paths.remove("/a"));
        assertTrue(paths.remove("/a/b"));
        assertTrue(paths.remove("/a/b/c"));
        assertTrue(paths.isEmpty());
    }

}
