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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.util.Iterator;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.After;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEPRECATED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.AGGREGATES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FIELD_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.COST_PER_ENTRY;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.COST_PER_EXECUTION;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_WEIGHT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState.stringProperty;
import static org.junit.Assert.*;

public class IndexDefinitionBuilderTest {
    private IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    private NodeBuilder nodeBuilder = EMPTY_NODE.builder();

    @After
    public void dumpState(){
        System.out.println(NodeStateUtils.toString(builder.build()));
    }

    @Test
    public void defaultSetup() throws Exception{
        NodeState state = builder.build();
        assertEquals(2, state.getLong("compatVersion"));
        assertEquals("async", state.getString("async"));
        assertEquals("lucene", state.getString("type"));
    }

    @Test
    public void indexRule() throws Exception{
        builder.includedPaths("/a", "/b");
        builder.queryPaths("/c", "/d");
        builder.supersedes("/e", "/f");
        builder.indexRule("nt:base")
                    .property("foo")
                        .ordered()
                .enclosingRule()
                    .property("bar")
                        .analyzed()
                        .propertyIndex()
                .enclosingRule()
                    .property("baz")
                    .propertyIndex();

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        assertTrue(state.getChildNode("indexRules").getChildNode("nt:base").exists());
        assertEquals(asList("/a", "/b"), state.getProperty(PathFilter.PROP_INCLUDED_PATHS).getValue(Type.STRINGS));
        assertEquals(asList("/c", "/d"), state.getProperty(IndexConstants.QUERY_PATHS).getValue(Type.STRINGS));
        assertEquals(asList("/e", "/f"), state.getProperty(IndexConstants.SUPERSEDED_INDEX_PATHS).getValue(Type.STRINGS));
    }

    @Test
    public void propertyDefIndexPropertySetIndexFalse() throws Exception {
        builder.indexRule("nt:base")
                .property("foo")
                .disable();

        PropertyState state = builder.build().
                getChildNode("indexRules").
                getChildNode("nt:base").
                getChildNode("properties").
                getChildNode("foo").
                getProperty("index");

        assertNotNull("index property must exist", state);
        assertFalse("Incorrect default value of index property", state.getValue(Type.BOOLEAN));
    }

    @Test
    public void aggregates() throws Exception{
        builder.aggregateRule("cq:Page").include("jcr:content").relativeNode();
        builder.aggregateRule("dam:Asset", "*", "*/*");

        NodeState state = builder.build();
        assertTrue(state.getChildNode("aggregates").exists());
        assertTrue(state.getChildNode("aggregates").getChildNode("dam:Asset").exists());
        assertTrue(state.getChildNode("aggregates").getChildNode("cq:Page").exists());
    }

    @Test
    public void duplicatePropertyName() throws Exception{
        builder.indexRule("nt:base")
                    .property("foo")
                        .ordered()
                .enclosingRule()
                    .property("jcr:content/foo")
                        .analyzed()
                        .propertyIndex()
                .enclosingRule()
                    .property("metadata/content/foo")
                        .propertyIndex();

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        assertTrue(state.getChildNode("indexRules").getChildNode("nt:base").exists());
        assertEquals(3, state.getChildNode("indexRules").getChildNode("nt:base")
                .getChildNode("properties").getChildNodeCount(10));
    }

    @Test
    public void ruleOrder() throws Exception{
        builder.indexRule("nt:unstructured");
        builder.indexRule("nt:base");

        Tree tree = TreeFactory.createTree(EMPTY_NODE.builder());
        builder.build(tree);

        //Assert the order
        Iterator<Tree> children = tree.getChild("indexRules").getChildren().iterator();
        assertEquals("nt:unstructured", children.next().getName());
        assertEquals("nt:base", children.next().getName());
    }

    @Test
    public void regexProperty() throws Exception{
        builder.indexRule("nt:base")
                .property(FulltextIndexConstants.REGEX_ALL_PROPS, true);

        NodeState state = builder.build();
        assertTrue(NodeStateUtils.getNode(state, "indexRules/nt:base/properties/prop")
                .getBoolean(FulltextIndexConstants.PROP_IS_REGEX));
    }

    @Test
    public void mergeExisting() throws Exception{
        nodeBuilder.setProperty("foo", "bar");
        builder = new IndexDefinitionBuilder(nodeBuilder);

        NodeState state = builder.build();
        assertEquals("bar", state.getString("foo"));
        assertEquals("async", state.getString("async"));
    }

    @Test
    public void mergeExisting_IndexRule() throws Exception{
        builder.indexRule("nt:unstructured").property("foo").propertyIndex();

        nodeBuilder = builder.build().builder();
        builder = new IndexDefinitionBuilder(nodeBuilder);

        assertTrue(builder.hasIndexRule("nt:unstructured"));
        assertFalse(builder.hasIndexRule("nt:base"));

        builder.indexRule("nt:unstructured").property("bar").propertyIndex();
        builder.indexRule("nt:base");

        assertTrue(builder.indexRule("nt:unstructured").hasPropertyRule("foo"));
        assertTrue(builder.indexRule("nt:unstructured").hasPropertyRule("bar"));
    }

    @Test
    public void mergeExisting_Aggregates() throws Exception{
        builder.aggregateRule("foo").include("/path1");
        builder.aggregateRule("foo").include("/path2");

        nodeBuilder = builder.build().builder();

        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.aggregateRule("foo").include("/path1");
        builder.aggregateRule("foo").include("/path3");

        NodeState state = builder.build();
        assertEquals(3, state.getChildNode(AGGREGATES).getChildNode("foo").getChildNodeCount(100));
    }

    @Test
    public void noReindexIfNoChange() throws Exception{
        builder.includedPaths("/a", "/b");
        builder.indexRule("nt:base")
                .property("foo")
                .ordered();

        nodeBuilder = builder.build().builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.includedPaths("/a", "/b");

        assertFalse(builder.isReindexRequired());
        NodeState state = builder.build();
        assertFalse(state.getBoolean(REINDEX_PROPERTY_NAME));
        assertFalse(state.getBoolean(PROP_REFRESH_DEFN));


        NodeState baseState = builder.build();
        nodeBuilder = baseState.builder();
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.indexRule("nt:file");

        assertTrue(builder.isReindexRequired());
        state = builder.build();
        assertTrue(state.getBoolean(REINDEX_PROPERTY_NAME));

        builder = new IndexDefinitionBuilder(baseState.builder(), false);
        builder.indexRule("nt:file");
        assertTrue(builder.isReindexRequired());
        state = builder.build();
        assertTrue(builder.isReindexRequired());
        assertFalse(state.getBoolean(REINDEX_PROPERTY_NAME));
    }

    @Test
    public void reindexAndAsyncFlagChange() throws Exception{
        builder.async("async", IndexConstants.INDEXING_MODE_NRT);

        nodeBuilder = builder.build().builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);

        NodeState oldState = nodeBuilder.getNodeState();

        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.async("async", IndexConstants.INDEXING_MODE_SYNC);
        assertFalse(builder.build().getBoolean(REINDEX_PROPERTY_NAME));

        builder = new IndexDefinitionBuilder(oldState.builder());
        builder.async("fulltext-async", IndexConstants.INDEXING_MODE_SYNC);
        assertTrue(builder.build().getBoolean(REINDEX_PROPERTY_NAME));
    }

    @Test
    public void noReindexWhenIfQueryPathsAddedOrChanged() {

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.queryPaths("/a","/b");

        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.queryPaths("/a","/c");
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().removeProperty(IndexConstants.QUERY_PATHS);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void noReindexWhenIfIndexTagsAddedOrChanged() {

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().setProperty(stringProperty(IndexConstants.INDEX_TAGS, of("foo1", "foo2")));

        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().setProperty(stringProperty(IndexConstants.INDEX_TAGS, of("foo2", "foo3")));
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().removeProperty(IndexConstants.INDEX_TAGS);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void noReindexWhenIfBlobSizeAddedOrChanged() {
        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().setProperty(BLOB_SIZE,32768);

        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().setProperty(BLOB_SIZE,35768);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().removeProperty(BLOB_SIZE);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    // This property is used in cost estimation - no reindexing required
    // on property change
    @Test
    public void noReindexIfWeightPropertyAddedOrChanged() throws Exception {

        builder.indexRule("nt:base").property("fooProp");

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        // Add the property weight to fooProp - this shouldn't cause reindex flag to set
        builder.indexRule("nt:base").property("fooProp").weight(10);

        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);

        // Now change the value for weight on fooProp - this also shouldn't lead to setting of reindex flag
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.indexRule("nt:base").property("fooProp").weight(20);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.indexRule("nt:base").property("fooProp").getBuilderTree().removeProperty(PROP_WEIGHT);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

    }
    // modifying boost value shouldn't require reindexing because we use
    // QueryTime Boosts and not index time boosts. Refer OAK-3367 for details
    @Test
    public void noReindexIfBoostPropAddedOrChanged() throws  Exception {
        builder.indexRule("nt:base").property("fooProp");

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        // Add the property  boost - this shouldn't cause reindex flag to set
        builder.indexRule("nt:base").property("fooProp").boost(1.0f);

        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);

        // Now change the value for boost - this also shouldn't lead to setting of reindex flag
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.indexRule("nt:base").property("fooProp").boost(2.0f);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.indexRule("nt:base").property("fooProp").getBuilderTree().removeProperty(FIELD_BOOST);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

    }

    // This is a node for configuration on how faceted search works
    // Everything impacts querty time evauation - so no need of reindexing in case of changes
    @Test
    public void noReindexWhenFacetNodeAddedOrRemoved() throws Exception {
        builder.indexRule("nt:base")
                .property("foo1").facets();

        NodeState currentNodeState  = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        //Add the facets child node now
        builder.getBuilderTree().addChild(PROP_FACETS);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        // Now test deleting the facets node should also not set the reindexing flag
        nodeBuilder = currentNodeState.builder();

        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().getChild(PROP_FACETS).remove();

        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void noReindexWhenFacetConfigChanged_topChildren() throws Exception {
        builder.indexRule("nt:base")
                .property("foo1").facets();

        builder.getBuilderTree().addChild(PROP_FACETS);
        NodeState currentNodeState  = builder.build();
        nodeBuilder = currentNodeState.builder();
        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        //Add top Children prop on facets node
        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_FACETS_TOP_CHILDREN,100);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        nodeBuilder = currentNodeState.builder();

        //Now test with changing the value - this too shouldn't set the reindexing flag
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_FACETS_TOP_CHILDREN,200);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().getChild(PROP_FACETS).removeProperty(FulltextIndexConstants.PROP_FACETS_TOP_CHILDREN);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void noReindexWhenFacetConfigChanged_secure() throws Exception {
        builder.indexRule("nt:base")
                .property("foo1").facets();

        builder.getBuilderTree().addChild(PROP_FACETS);

        NodeState currentNodeState  = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        //Add top secure prop on facets node
        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_SECURE_FACETS,FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_SECURE);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);

        //Now test with changing the value - this too shouldn't set the reindexing flag
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_SECURE_FACETS,FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_INSECURE);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().getChild(PROP_FACETS).removeProperty(FulltextIndexConstants.PROP_SECURE_FACETS);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void noReindexWhenFacetConfigChanged_sampleSize() throws Exception {
        builder.indexRule("nt:base")
                .property("foo1").facets();

        builder.getBuilderTree().addChild(PROP_FACETS);

        NodeState currentNodeState  = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        //Add top sample size prop on facets node
        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE,1000);
        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);

        //Now test with changing the value - this too shouldn't set the reindexing flag
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE,2000);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().getChild(PROP_FACETS).removeProperty(FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void noReindexWhenIfCostPerExecAddedOrChanged() {
        builder.indexRule("nt:base");

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild("indexRules").getChild("nt:base").setProperty(COST_PER_EXECUTION, 2.0);
        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);

        //Now test with changing the value - this too shouldn't set the reindexing flag
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild("indexRules").getChild("nt:base").setProperty(COST_PER_EXECUTION, 3.0);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().getChild("indexRules").getChild("nt:base").removeProperty(COST_PER_EXECUTION);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

    }

    @Test
    public void noReindexWhenIfCostPerEntryAddedOrChanged() {
        builder.indexRule("nt:base");

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild("indexRules").getChild("nt:base").setProperty(COST_PER_ENTRY, 2.0);
        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);

        //Now test with changing the value - this too shouldn't set the reindexing flag
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild("indexRules").getChild("nt:base").setProperty(COST_PER_ENTRY, 3.0);
        currentNodeState = builder.build();
        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        //Now check for property delete use case
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().getChild("indexRules").getChild("nt:base").removeProperty(COST_PER_ENTRY);
        currentNodeState = builder.build();

        assertFalse(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertTrue(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
    }

    @Test
    public void reindexFlagSetWhenRequired() {

        NodeState currentNodeState = builder.build();
        nodeBuilder = currentNodeState.builder();

        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.includedPaths("/a", "/b");

        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.includedPaths("/a", "/c");
        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.getBuilderTree().removeProperty(PathFilter.PROP_INCLUDED_PATHS);
        currentNodeState = builder.build();

        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
    }

    @Test
    public void renidexIfFacetsNodeAddedwithSomeNewPropThatReqIndexing() throws Exception {
        builder.indexRule("nt:base")
                .property("foo1").facets();

        NodeState currentNodeState  = builder.build();
        nodeBuilder = currentNodeState.builder();

        //Unset the reindex flag first because first build would have set it .
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().addChild(PROP_FACETS);

        //Add foo prop on facets node
        builder.getBuilderTree().getChild(PROP_FACETS).setProperty("foo","bar");
        currentNodeState = builder.build();

        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertFalse(currentNodeState.getBoolean(PROP_REFRESH_DEFN));
        nodeBuilder = currentNodeState.builder();

        //Now test with changing the value - this too should set the reindexing flag
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild(PROP_FACETS).setProperty("foo","bar2");
        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertFalse(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        //now deleting the node
        nodeBuilder = currentNodeState.builder();
        nodeBuilder.removeProperty(PROP_REFRESH_DEFN);
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().getChild(PROP_FACETS).remove();
        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertFalse(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

        nodeBuilder = currentNodeState.builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.getBuilderTree().addChild(PROP_FACETS);

        //Add foo prop on facets node
        builder.getBuilderTree().getChild(PROP_FACETS).setProperty("foo","bar");
        builder.getBuilderTree().getChild(PROP_FACETS).setProperty(FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE,200);

        currentNodeState = builder.build();
        assertTrue(currentNodeState.getBoolean(REINDEX_PROPERTY_NAME));
        assertFalse(currentNodeState.getBoolean(PROP_REFRESH_DEFN));

    }

    @Test
    public void propRuleCustomName() throws Exception{
        builder.indexRule("nt:base").property("foo").property("bar");
        builder.indexRule("nt:base").property("fooProp", "foo2");
        builder.indexRule("nt:base").property("fooProp", "foo2");

        Root idx = new ImmutableRoot(builder.build());
        assertTrue(idx.getTree("/indexRules/nt:base/properties/fooProp").exists());
        assertTrue(idx.getTree("/indexRules/nt:base/properties/bar").exists());
        assertTrue(idx.getTree("/indexRules/nt:base/properties/foo").exists());
    }

    @Test
    public void typeNotChangedIfSet() throws Exception{
        NodeState state = builder.build();
        assertEquals("lucene", state.getString("type"));

        NodeBuilder updated = state.builder();
        updated.setProperty("type", "disabled");
        IndexDefinitionBuilder newBuilder = new IndexDefinitionBuilder(updated);

        NodeState updatedState = newBuilder.build();
        assertEquals("disabled", updatedState.getString("type"));

        //Type other than 'disabled' would be reset
        updated.setProperty("type", "foo");
        assertEquals("lucene", new IndexDefinitionBuilder(updated).build().getString("type"));
    }

    @Test
    public void nodeTypeIndex() throws Exception{
        builder.nodeTypeIndex();
        builder.indexRule("nt:file");

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        NodeState ntFileRule = state.getChildNode("indexRules").getChildNode("nt:file");
        assertTrue(ntFileRule.exists());
        assertTrue(state.getBoolean(FulltextIndexConstants.PROP_INDEX_NODE_TYPE));
        assertFalse(ntFileRule.getBoolean(FulltextIndexConstants.PROP_SYNC));
    }

    @Test
    public void nodeTypeIndexSync() throws Exception{
        builder.nodeTypeIndex();
        builder.indexRule("nt:file").sync();

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        NodeState ntFileRule = state.getChildNode("indexRules").getChildNode("nt:file");
        assertTrue(ntFileRule.exists());
        assertTrue(state.getBoolean(FulltextIndexConstants.PROP_INDEX_NODE_TYPE));
        assertTrue(ntFileRule.getBoolean(FulltextIndexConstants.PROP_SYNC));
    }

    @Test
    public void noPropertiesNodeForEmptyRule() throws Exception{
        builder.nodeTypeIndex();
        builder.indexRule("nt:file").sync();

        NodeState state = builder.build();
        assertFalse(NodeStateUtils.getNode(state, "/indexRules/nt:file/properties").exists());
    }

    @Test
    public void deprecated() {
        NodeState state = builder.build();
        assertFalse("By default index isn't deprecated", state.getBoolean(INDEX_DEPRECATED));

        state = builder.deprecated().build();
        assertTrue("Index must be deprecated if marked so", state.getBoolean(INDEX_DEPRECATED));
    }

    @Test
    public void boost() {
        builder.indexRule("nt:base")
                .property("foo1").boost(1.0f).enclosingRule()
                .property("foo2").boost(2.0f);

        NodeState state = builder.build();

        NodeState foo1 = NodeStateUtils.getNode(state, "indexRules/nt:base/properties/foo1");
        assertTrue(foo1.exists());
        assertEquals("Incorrectly set boost",
                1.0f, foo1.getProperty(FIELD_BOOST).getValue(Type.DOUBLE).floatValue(), 0.0001);

        NodeState foo2 = NodeStateUtils.getNode(state, "indexRules/nt:base/properties/foo2");
        assertTrue(foo2.exists());
        assertEquals("Incorrectly set boost",
                2.0f, foo2.getProperty(FIELD_BOOST).getValue(Type.DOUBLE).floatValue(), 0.0001);
    }

    @Test
    public void facets() {
        builder.indexRule("nt:base")
                .property("foo1").facets().enclosingRule()
                .property("foo2").propertyIndex();

        NodeState state = builder.build();

        NodeState foo1 = NodeStateUtils.getNode(state, "indexRules/nt:base/properties/foo1");
        assertTrue(foo1.exists());
        assertTrue("Incorrectly set facets property",
                foo1.getBoolean(PROP_FACETS));

        NodeState foo2 = NodeStateUtils.getNode(state, "indexRules/nt:base/properties/foo2");
        assertTrue(foo2.exists());
        assertFalse("Incorrectly existing facets property",
                foo2.hasProperty(PROP_FACETS));
    }
}