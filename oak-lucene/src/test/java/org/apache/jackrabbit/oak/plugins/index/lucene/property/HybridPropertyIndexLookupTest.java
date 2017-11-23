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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

public class HybridPropertyIndexLookupTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
    private String indexPath  = "/oak:index/foo";
    private PropertyIndexUpdateCallback callback = new PropertyIndexUpdateCallback(indexPath, builder, root);

    @Test
    public void simplePropertyRestriction() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync();

        propertyUpdated("/a", "foo", "bar");

        FilterImpl f = createFilter();
        f.restrictProperty("foo", Operator.EQUAL, newString("bar"));

        assertThat(query(f, "foo"), containsInAnyOrder("/a"));
    }

    @Test
    public void valuePattern() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync().valuePattern("(a.*|b)");

        propertyUpdated("/a", "foo", "a");
        propertyUpdated("/a1", "foo", "a1");
        propertyUpdated("/b", "foo", "b");
        propertyUpdated("/c", "foo", "c");

        assertThat(query("foo", "a"), containsInAnyOrder("/a"));
        assertThat(query("foo", "a1"), containsInAnyOrder("/a1"));
        assertThat(query("foo", "b"), containsInAnyOrder("/b"));

        // c should not be found as its excluded
        assertThat(query("foo", "c"), empty());
    }

    @Test
    public void relativeProperty() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync();

        propertyUpdated("/a", "foo", "bar");

        FilterImpl f = createFilter();
        f.restrictProperty("jcr:content/foo", Operator.EQUAL, newString("bar"));

        assertThat(query(f, "foo", "jcr:content/foo"), containsInAnyOrder("/a"));
    }

    @Test
    public void pathResultAbsolutePath() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync();

        propertyUpdated("/a", "foo", "bar");

        String propertyName = "foo";
        FilterImpl filter = createFilter();
        filter.restrictProperty("foo", Operator.EQUAL, newString("bar"));

        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(indexPath, builder.getNodeState());
        Iterable<String> paths = lookup.query(filter, propertyName,
                filter.getPropertyRestriction(propertyName));

        assertThat(ImmutableList.copyOf(paths), containsInAnyOrder("/a"));
    }

    @Test
    public void nonRootIndex() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync();
        indexPath = "/content/oak:index/fooIndex";

        propertyUpdated("/a", "foo", "bar");

        String propertyName = "foo";
        FilterImpl filter = createFilter();
        filter.restrictProperty("foo", Operator.EQUAL, newString("bar"));
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);

        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(indexPath, builder.getNodeState(),
                "/content", false);
        Iterable<String> paths = lookup.query(filter, propertyName,
                filter.getPropertyRestriction(propertyName));

        assertThat(ImmutableList.copyOf(paths), containsInAnyOrder("/a"));

        lookup = new HybridPropertyIndexLookup(indexPath, builder.getNodeState(),
                "/content", true);
        paths = lookup.query(filter, propertyName,
                filter.getPropertyRestriction(propertyName));

        assertThat(ImmutableList.copyOf(paths), containsInAnyOrder("/content/a"));
    }

    private void propertyUpdated(String nodePath, String propertyRelativeName, String value){
        callback.propertyUpdated(nodePath, propertyRelativeName, pd(propertyRelativeName),
                null, createProperty(PathUtils.getName(propertyRelativeName), value));
    }

    private List<String> query(String propertyName, String value) {
        FilterImpl f = createFilter();
        f.restrictProperty(propertyName, Operator.EQUAL, newString(value));
        return query(f, propertyName);
    }

    private List<String> query(Filter filter, String propertyName) {
        return query(filter, propertyName, propertyName);
    }

    private List<String> query(Filter filter, String propertyName, String propertyRestrictionName) {
        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(indexPath, builder.getNodeState());
        Iterable<String> paths = lookup.query(filter, propertyName,
                filter.getPropertyRestriction(propertyRestrictionName));
        return ImmutableList.copyOf(paths);
    }

    private PropertyDefinition pd(String propName){
        IndexDefinition defn = new IndexDefinition(root, defnb.build(), indexPath);
        return defn.getApplicableIndexingRule("nt:base").getConfig(propName);
    }

    private FilterImpl createFilter() {
        return createFilter(root, "nt:base");
    }

    private FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

}