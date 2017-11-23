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
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_PREVIOUS_BUCKET;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class HybridPropertyIndexStorageTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
    private String indexPath  = "/oak:index/foo";

    @Test
    public void nonSyncProp() throws Exception{
        defnb.indexRule("nt:base").property("foo");

        newCallback().propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar"));

        assertFalse(builder.isModified());
    }

    @Test
    public void simpleProperty() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync();

        newCallback().propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar"));
        newCallback().propertyUpdated("/b", "foo", pd("foo"),
                null, createProperty("foo", "bar2"));

        assertThat(query("foo", newString("bar")), containsInAnyOrder("/a"));
        assertThat(query("foo", newString("bar2")), containsInAnyOrder("/b"));
    }

    @Test
    public void relativeProperty() throws Exception{
        String propName = "jcr:content/foo";
        defnb.indexRule("nt:base").property(propName).sync();

        newCallback().propertyUpdated("/a", propName, pd(propName),
                null, createProperty("foo", "bar"));

        assertThat(query(propName, newString("bar")), containsInAnyOrder("/a"));
    }

    @Test
    public void valuePattern() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync().valueIncludedPrefixes("bar/");

        newCallback().propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar/a"));
        newCallback().propertyUpdated("/b", "foo", pd("foo"),
                null, createProperty("foo", "baz/a"));

        assertThat(query("foo", newString("bar/a")), containsInAnyOrder("/a"));

        //As baz pattern is excluded it should not be indexed
        assertThat(query("foo", newString("baz/a")), empty());
    }

    @Test
    public void pruningDisabledForSimpleProperty() throws Exception{
        defnb.indexRule("nt:base").property("foo").sync();

        newCallback().propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar"));
        newCallback().propertyUpdated("/b", "foo", pd("foo"),
                null, createProperty("foo", "bar"));

        assertThat(query("foo", newString("bar")), containsInAnyOrder("/a", "/b"));

        builder = builder.getNodeState().builder();
        newCallback().propertyUpdated("/b", "foo", pd("foo"),
                createProperty("foo", "bar"), null);

        // /b would still come as pruning is disabled
        assertThat(query("foo", newString("bar")), containsInAnyOrder("/a", "/b"));
    }

    //~----------------------------------------< unique props >

    @Test
    public void uniqueProperty() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        PropertyUpdateCallback callback = newCallback();

        callback.propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar"));
        callback.propertyUpdated("/b", "foo", pd("foo"),
                null, createProperty("foo", "bar2"));

        callback.done();

        assertThat(query("foo", newString("bar")), containsInAnyOrder("/a"));
    }

    @Test
    public void pruningWorkingForUnique() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        newCallback().propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar"));

        assertThat(query("foo", newString("bar")), containsInAnyOrder("/a"));

        builder = builder.getNodeState().builder();
        newCallback().propertyUpdated("/a", "foo", pd("foo"),
                createProperty("foo", "bar"), null);

        // /b should not come as pruning is enabled
        assertThat(query("foo", newString("bar")), empty());
    }

    //~---------------------------------------< buckets >

    @Test
    public void bucketSwitch() throws Exception{
        String propName = "foo";
        defnb.indexRule("nt:base").property(propName).sync();

        newCallback().propertyUpdated("/a", propName, pd(propName),
                null, createProperty(propName, "bar"));

        assertThat(query(propName, newString("bar")), containsInAnyOrder("/a"));

        switchBucket(propName);

        newCallback().propertyUpdated("/b", propName, pd(propName),
                null, createProperty(propName, "bar"));

        assertThat(query(propName, newString("bar")), containsInAnyOrder("/a", "/b"));

        switchBucket(propName);

        newCallback().propertyUpdated("/c", propName, pd(propName),
                null, createProperty(propName, "bar"));

        //Now /a should not come as its in 3rd bucket and we only consider head and previous buckets
        assertThat(query(propName, newString("bar")), containsInAnyOrder("/b", "/c"));
    }

    private void switchBucket(String propertyName) {
        NodeBuilder propertyIndex = builder.child(PROPERTY_INDEX);
        NodeBuilder idx = propertyIndex.child(HybridPropertyIndexUtil.getNodeName(propertyName));

        String head = idx.getString(HybridPropertyIndexUtil.PROP_HEAD_BUCKET);
        assertNotNull(head);

        int id = Integer.parseInt(head);
        idx.setProperty(PROP_PREVIOUS_BUCKET, head);
        idx.setProperty(PROP_HEAD_BUCKET, String.valueOf(id + 1));

        builder = builder.getNodeState().builder();
    }

    private List<String> query(String propertyName, PropertyValue value) {
        HybridPropertyIndexLookup lookup = new HybridPropertyIndexLookup(indexPath, builder.getNodeState());
        FilterImpl filter = createFilter(root, "nt:base");
        Iterable<String> paths = lookup.query(filter, propertyName, value);
        return ImmutableList.copyOf(paths);
    }

    private PropertyIndexUpdateCallback newCallback(){
        return new PropertyIndexUpdateCallback(indexPath, builder, root);
    }

    private PropertyDefinition pd(String propName){
        IndexDefinition defn = new IndexDefinition(root, defnb.build(), indexPath);
        return defn.getApplicableIndexingRule("nt:base").getConfig(propName);
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

}