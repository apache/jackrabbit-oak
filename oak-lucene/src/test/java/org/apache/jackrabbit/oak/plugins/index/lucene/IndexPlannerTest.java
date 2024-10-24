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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import static java.util.Objects.requireNonNull;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_FUNCTION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexStatistics.SYNTHETICALLY_FALLIABLE_FIELD;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.NT_TEST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.registerTestNodeType;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.REP_FACET;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexSelectionPolicy;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PropertyIndexResult;
import org.apache.jackrabbit.oak.plugins.index.search.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;

public class IndexPlannerTest {
    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @After
    public void cleanup(){
        FulltextIndexPlanner.setUseActualEntryCount(true);
    }

    @Test
    public void planForSortField() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", createFilter("nt:base"),
            ImmutableList.of(new OrderEntry("foo", Type.LONG, OrderEntry.Order.ASCENDING)));
        assertNotNull(planner.getPlan());
        assertTrue(pr(planner.getPlan()).isUniquePathsRequired());
    }

    @Test
    public void noPlanForSortOnlyByScore() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", createFilter("nt:file"),
            ImmutableList.of(new OrderEntry("jcr:score", Type.LONG, OrderEntry.Order.ASCENDING)));
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQueryNonFulltextIndex() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void noApplicableRule() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.DECLARING_NODE_TYPES, Set.of("nt:folder"), STRINGS));
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());

        filter = createFilter("nt:folder");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void nodeTypeInheritance() throws Exception{
        //Index if for nt:hierarchyNode and query is for nt:folder
        //as nt:folder extends nt:hierarchyNode we should get a plan
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.DECLARING_NODE_TYPES, Set.of("nt:hierarchyNode"), STRINGS));
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:folder");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void noMatchingProperty() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }
    @Test
    public void matchingProperty() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);
        assertNotNull(pr(plan));
        assertTrue(pr(plan).evaluateNonFullTextConstraints());
    }

    @Test
    public void purePropertyIndexAndPathRestriction() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void fulltextIndexAndPathRestriction() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        //For case when a full text property is present then path restriction can be
        //evaluated
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fulltextIndexAndNodeTypeRestriction() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        defn.setProperty(IndexConstants.DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:file/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(),"/foo"));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        //For case when a full text property is present then path restriction can be
        //evaluated
        assertNotNull(planner.getPlan());
    }

    @Test
    public void pureNodeTypeWithEvaluatePathRestrictionEnabled() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defn = newLuceneIndexDefinition(index, "lucene",
            Set.of(TYPENAME_STRING));
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        TestUtil.useV2(defn);

        FilterImpl filter = createFilter("nt:file");
        filter.restrictPath("/", Filter.PathRestriction.ALL_CHILDREN);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        // /jcr:root//element(*, nt:file)
        //For queries like above Fulltext index should not return a plan
        assertNull(planner.getPlan());
    }

    @Test
    public void purePropertyIndexAndNodeTypeRestriction() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        defn.setProperty(IndexConstants.DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        assertNull(planner.getPlan());
    }

    @Test
    public void purePropertyIndexAndNodeTypeRestriction2() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        //No plan should be result for a index with just a rule for nt:base
        assertNull(planner.getPlan());
    }

    @Test
    public void purePropertyIndexAndNodeTypeRestriction3() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        defn.setProperty(IndexConstants.DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:file/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);
        assertNotNull(pr(plan));
        assertTrue(pr(plan).evaluateNodeTypeRestriction());
    }

    @Test
    public void worksWithIndexFormatV2Onwards() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinition(index, "lucene",
            Set.of(TYPENAME_STRING));
        //Dummy data node to ensure that LuceneIndexDefinition does not consider it
        //as a fresh indexing case
        nb.child(INDEX_DATA_CHILD_NAME);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, nb.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void propertyIndexCost() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 1000;

        FulltextIndexPlanner.setUseActualEntryCount(false);
        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn.getNodeState(), "/foo");
        LuceneIndexNode node = createIndexNode(idxDefn, numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //For propertyIndex if entry count (default to IndexDefinition.DEFAULT_ENTRY_COUNT) is
        //less than numOfDoc then that would be preferred
        assertEquals(idxDefn.getEntryCount(), plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);
    }

    @Test
    public void propertyIndexCost2() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.COST_PER_ENTRY, 2.0);
        defn.setProperty(FulltextIndexConstants.COST_PER_EXECUTION, 3.0);

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT - 100;
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(documentsPerValue(numofDocs), plan.getEstimatedEntryCount());
        assertEquals(3.0, plan.getCostPerExecution(), 0);
        assertEquals(2.0, plan.getCostPerEntry(), 0);
        assertNotNull(plan);
    }

    @Test
    public void propertyIndexCostActualOverriddenByEntryCount() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        long entryCount = IndexDefinition.DEFAULT_ENTRY_COUNT - 100;
        defn.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, entryCount);

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 100;

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(entryCount, plan.getEstimatedEntryCount());
    }

    @Test
    public void propertyIndexCostActualByDefault() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 100;

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(documentsPerValue(numofDocs), plan.getEstimatedEntryCount());
    }

    @Test
    public void fulltextIndexCost() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defn = newLuceneIndexDefinition(index, "lucene",
            Set.of(TYPENAME_STRING));
        TestUtil.useV2(defn);

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 1000;
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);
        assertEquals(numofDocs, plan.getEstimatedEntryCount());
    }

    @Test
    public void nullPropertyCheck() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, null);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("For null checks no plan should be returned", plan);
    }

    @Test
    public void nullPropertyCheck2() throws Exception{
        root = registerTestNodeType(builder).getNodeState();
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        NodeBuilder rules = defn.child(INDEX_RULES);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
            .setProperty(FulltextIndexConstants.PROP_NAME, "foo")
            .setProperty(FulltextIndexConstants.PROP_NULL_CHECK_ENABLED, true)
            .setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);

        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, builder.getNodeState().getChildNode("test"), "/foo");
        LuceneIndexNode node = createIndexNode(idxDefn);

        FilterImpl filter = createFilter(NT_TEST);
        filter.restrictProperty("foo", Operator.EQUAL, null);

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull("For null checks plan should be returned with nullCheckEnabled", plan);
        FulltextIndexPlanner.PlanResult pr =
            (FulltextIndexPlanner.PlanResult) plan.getAttribute(FulltextIndex.ATTR_PLAN_RESULT);
        assertNotNull(pr.getPropDefn(filter.getPropertyRestriction("foo")));
    }

    @Test
    public void noPathRestHasQueryPath() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.QUERY_PATHS, Set.of("/test/a"), Type.STRINGS));
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void hasPathRestHasMatchingQueryPaths() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.QUERY_PATHS, Set.of("/test/a", "/test/b"), Type.STRINGS));
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));

        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/test/a", Filter.PathRestriction.ALL_CHILDREN);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void hasPathRestHasNoExplicitQueryPaths() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));

        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void noPlanForFulltextQueryAndOnlyAnalyzedProperties() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull(plan);
    }

    @Test
    public void noPlanForNodeTypeQueryAndOnlyAnalyzedProperties() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        defn.setProperty(IndexConstants.DECLARING_NODE_TYPES, Set.of("nt:file"), NAMES);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:file/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FilterImpl filter = createFilter("nt:file");
        filter.restrictPath("/foo", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull(plan);
    }

    @Test
    public void indexedButZeroWeightProps() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("bar").propertyIndex().weight(0);

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("a"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        //Even though foo is indexed it would not be considered for a query involving just foo
        assertNull(planner.getPlan());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("a"));
        planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan1 = planner.getPlan();
        assertNotNull(plan1);

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("a"));
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("a"));
        planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan2 = planner.getPlan();
        assertNotNull(plan2);


        // Since, the index has no entries for "bar", estimated entry count for plan2 would be 0
        assertEquals(0, plan2.getEstimatedEntryCount());
        assertThat(plan2.getEstimatedEntryCount(), lessThan(plan1.getEstimatedEntryCount()));

        assertTrue(pr(plan2).hasProperty("foo"));
        assertTrue(pr(plan2).hasProperty("bar"));
    }


    //------ Suggestion/spellcheck plan tests
    @Test
    public void nonSuggestIndex() throws Exception {
        //An index which doesn't define any property to support suggestions shouldn't turn up in plan.
        String indexNodeType = "nt:base";
        String queryNodeType = "nt:base";
        boolean enableSuggestionIndex = false;
        boolean enableSpellcheckIndex = false;
        boolean queryForSugggestion = true;

        LuceneIndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNull(plan);
    }

    @Test
    public void nonSpellcheckIndex() throws Exception {
        //An index which doesn't define any property to support spell check shouldn't turn up in plan.
        String indexNodeType = "nt:base";
        String queryNodeType = "nt:base";
        boolean enableSuggestionIndex = false;
        boolean enableSpellcheckIndex = false;
        boolean queryForSugggestion = false;

        LuceneIndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNull(plan);
    }

    @Test
    public void simpleSuggestIndexPlan() throws Exception {
        //An index defining a property for suggestions should turn up in plan.
        String indexNodeType = "nt:base";
        String queryNodeType = "nt:base";
        boolean enableSuggestionIndex = true;
        boolean enableSpellcheckIndex = false;
        boolean queryForSugggestion = true;

        LuceneIndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNotNull(plan);
        assertFalse(pr(plan).isUniquePathsRequired());
    }

    @Test
    public void simpleSpellcheckIndexPlan() throws Exception {
        //An index defining a property for spellcheck should turn up in plan.
        String indexNodeType = "nt:base";
        String queryNodeType = "nt:base";
        boolean enableSuggestionIndex = false;
        boolean enableSpellcheckIndex = true;
        boolean queryForSugggestion = false;

        LuceneIndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNotNull(plan);
        assertFalse(pr(plan).isUniquePathsRequired());
    }

    @Test
    public void suggestionIndexingRuleHierarchy() throws Exception {
        //An index defining a property for suggestion on a base type shouldn't turn up in plan.
        String indexNodeType = "nt:base";
        String queryNodeType = "nt:unstructured";
        boolean enableSuggestionIndex = true;
        boolean enableSpellcheckIndex = false;
        boolean queryForSugggestion = true;

        LuceneIndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNull(plan);
    }

    @Test
    public void spellcheckIndexingRuleHierarchy() throws Exception {
        //An index defining a property for spellcheck on a base type shouldn't turn up in plan.
        String indexNodeType = "nt:base";
        String queryNodeType = "nt:unstructured";
        boolean enableSuggestionIndex = false;
        boolean enableSpellcheckIndex = true;
        boolean queryForSugggestion = false;

        LuceneIndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNull(plan);
    }

    @Test
    public void fullTextQuery_RelativePath1() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(), FullTextParser.parse("bar", "mountain"));

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("allProps"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/allProps");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        foob.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        FullTextExpression exp = FullTextParser.parse("bar", "mountain OR valley");
        exp = new FullTextContains("bar", "mountain OR valley", exp);
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(), exp);

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps_NodePathQuery() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("allProps"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/allProps");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        foob.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
            FullTextParser.parse("jcr:content/*", "mountain OR valley"));

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps_AggregatedNodePathQuery() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("allProps"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder agg = defn.child(FulltextIndexConstants.AGGREGATES).child("nt:base").child("include0");
        agg.setProperty(FulltextIndexConstants.AGG_PATH, "jcr:content");
        agg.setProperty(FulltextIndexConstants.AGG_RELATIVE_NODE, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
            FullTextParser.parse("jcr:content/*", "mountain OR valley"));

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps_NodePathQuery_NoPlan() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
            FullTextParser.parse("jcr:content/*", "mountain OR valley"));

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_NonAnalyzedProp_NoPlan() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo", "bar"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");

        NodeBuilder barb = getNode(defn, "indexRules/nt:base/properties/bar");
        barb.setProperty(FulltextIndexConstants.PROP_NAME, "bar");
        barb.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
            FullTextParser.parse("foo", "mountain OR valley"));

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_RelativePropertyPaths() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo", "bar"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        NodeBuilder barb = getNode(defn, "indexRules/nt:base/properties/bar");
        barb.setProperty(FulltextIndexConstants.PROP_NAME, "bar");
        barb.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);


        //where contains('jcr:content/bar', 'mountain OR valley') and contains('jcr:content/foo', 'mountain OR valley')
        //above query can be evaluated by index which indexes foo and bar with restriction that both belong to same node
        //by displacing the query path to evaluate on contains('bar', ...) and filter out those parents which do not
        //have jcr:content as parent
        FullTextExpression fooExp = FullTextParser.parse("jcr:content/bar", "mountain OR valley");
        FullTextExpression barExp = FullTextParser.parse("jcr:content/foo", "mountain OR valley");
        FullTextExpression exp = new FullTextAnd(Arrays.asList(fooExp, barExp));
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),exp);

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_DisjointPropertyPaths() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo", "bar"), "async");

        //Index all props and then perform fulltext
        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        NodeBuilder barb = getNode(defn, "indexRules/nt:base/properties/bar");
        barb.setProperty(FulltextIndexConstants.PROP_NAME, "bar");
        barb.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        FullTextExpression fooExp = FullTextParser.parse("metadata/bar", "mountain OR valley");
        FullTextExpression barExp = FullTextParser.parse("jcr:content/foo", "mountain OR valley");
        FullTextExpression exp = new FullTextAnd(Arrays.asList(fooExp, barExp));
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),exp);

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void valuePattern_Equals() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base")
            .property("foo")
            .propertyIndex()
            .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNotNull(planner.getPlan());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("/jobs/a"));
        planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void valuePattern_StartsWith() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base")
            .property("foo")
            .propertyIndex()
            .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("foo", Operator.LESS_OR_EQUAL, PropertyValues.newString("/bar0"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNotNull(planner.getPlan());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newString("/jobs"));
        filter.restrictProperty("foo", Operator.LESS_OR_EQUAL, PropertyValues.newString("/jobs0"));

        planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void relativeProperty_Basics() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("jcr:content/bar").propertyIndex();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertTrue(pr.isPathTransformed());
        assertEquals("/a/b", pr.transformPath("/a/b/jcr:content"));
        assertEquals("/a/b", pr.transformPath("/a/b/c"));

        assertTrue(pr.hasProperty("jcr:content/foo"));
        assertFalse(pr.hasProperty("bar"));

        Filter.PropertyRestriction r = new Filter.PropertyRestriction();
        r.propertyName = "jcr:content/foo";
        assertEquals("foo", pr.getPropertyName(r));
    }

    @Test
    public void relativeProperty_Non_NtBase() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:unstructured").property("foo").propertyIndex();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:unstructured");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //Should not return a plan for index rule other than nt:base
        assertNull(plan);
    }

    @Test
    public void relativeProperty_WithFulltext() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("bar").analyzed();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.setFullTextConstraint(FullTextParser.parse("jcr:content/bar", "mountain"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertTrue(pr.isPathTransformed());
        assertFalse(pr.evaluateNonFullTextConstraints());
        assertEquals("/a/b", pr.transformPath("/a/b/jcr:content"));
        assertNull(pr.transformPath("/a/b/c"));
    }

    @Test
    public void relativeProperty_FullText() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        FullTextExpression ft = FullTextParser.parse("jcr:content/*", "mountain OR valley");
        filter.setFullTextConstraint(ft);

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertFalse(pr.hasProperty("jcr:content/foo"));
    }

    @Test
    public void relativeProperty_MultipleMatch() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("bar").propertyIndex();
        defnb.indexRule("nt:base").property("baz").propertyIndex();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("jcr:content/bar", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("metadata/baz", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertTrue(pr.hasProperty("jcr:content/foo"));
        assertTrue(pr.hasProperty("jcr:content/bar"));
        assertFalse(pr.hasProperty("metadata/baz"));
    }

    @Test
    public void evaluatePathRestrictionExposesSupportCorrectly() throws Exception{
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        // Evaluates path restriction
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async")
            .setProperty(EVALUATE_PATH_RESTRICTION, true);
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertTrue(plan.getSupportsPathRestriction());

        // Doesn't evaluate path restriction
        defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async")
            .setProperty(EVALUATE_PATH_RESTRICTION, false);
        node = createIndexNode(new LuceneIndexDefinition(root, defn.getNodeState(), "/foo1"));
        planner = new FulltextIndexPlanner(node, "/foo1", filter, Collections.<OrderEntry>emptyList());
        plan = planner.getPlan();
        assertFalse(plan.getSupportsPathRestriction());
    }


    //~------------------------------< sync indexes >

    @Test
    public void syncIndex_uniqueIndex() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        assertEquals(1, plan.getEstimatedEntryCount());
        PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

        assertNotNull(hr);
        assertEquals("foo", hr.propertyName);
        assertEquals("foo", hr.pr.propertyName);
    }

    @Test
    public void syncIndex_uniqueAndRelative() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        assertEquals(1, plan.getEstimatedEntryCount());
        PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

        assertNotNull(hr);
        assertEquals("foo", hr.propertyName);
        assertEquals("jcr:content/foo", hr.pr.propertyName);
    }

    @Test
    public void syncIndex_nonUnique() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        assertEquals(documentsPerValue(100), plan.getEstimatedEntryCount());
        PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

        assertNotNull(hr);
        assertEquals("foo", hr.propertyName);
        assertEquals("foo", hr.pr.propertyName);
    }

    /**
     * If both non unique and unique indexes are found then unique should be picked
     */
    @Test
    public void syncIndex_nonUniqueAndUniqueBoth() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();
        defnb.indexRule("nt:base").property("bar").propertyIndex().sync();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("foo"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        assertEquals(1, plan.getEstimatedEntryCount());
        PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

        assertNotNull(hr);
        assertEquals("foo", hr.propertyName);
        assertEquals("foo", hr.pr.propertyName);
    }

    @Test
    public void syncIndex_NotUsedWithSort() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        defnb.indexRule("nt:base").property("bar").propertyIndex().ordered();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter,
            ImmutableList.of(new OrderEntry("bar", Type.LONG, OrderEntry.Order.ASCENDING)));
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        assertEquals(documentsPerValue(100), plan.getEstimatedEntryCount());
        PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

        assertNull(hr);
    }

    @Test
    public void syncIndex_NotUsedWithFulltext() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        defnb.indexRule("nt:base").property("bar").analyzed();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.setFullTextConstraint(FullTextParser.parse("bar", "mountain"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter,
            ImmutableList.of(new OrderEntry("bar", Type.LONG, OrderEntry.Order.ASCENDING)));
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        assertEquals(documentsPerValue(100), plan.getEstimatedEntryCount());
        PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

        assertNull(hr);
    }

    //~----------------------------------------< nodetype >

    String testNodeTypeDefn = "[oak:TestMixA]\n" +
        "  mixin\n" +
        "\n" +
        "[oak:TestSuperType]\n" +
        "- * (UNDEFINED) multiple\n" +
        "\n" +
        "[oak:TestTypeA] > oak:TestSuperType\n" +
        "- * (UNDEFINED) multiple\n" +
        "\n" +
        "[oak:TestTypeB] > oak:TestSuperType, oak:TestMixA\n" +
        "- * (UNDEFINED) multiple";

    @Test
    public void nodetype_primaryType() throws Exception{
        TestUtil.registerNodeType(builder, testNodeTypeDefn);
        root = builder.getNodeState();

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestSuperType");

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("oak:TestSuperType");

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());

        //As oak:TestSuperType is parent of oak:TestTypeA the child nodetypes should
        //also be indexed
        filter = createFilter("oak:TestTypeA");
        planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        plan = planner.getPlan();

        assertNotNull(plan);
        r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());
    }

    @Test
    public void nodetype_mixin() throws Exception{
        TestUtil.registerNodeType(builder, testNodeTypeDefn);
        root = builder.getNodeState();

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestMixA");

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("oak:TestMixA");

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());
    }

    @Test
    public void syncNodeTypeIndex() throws Exception{
        TestUtil.registerNodeType(builder, testNodeTypeDefn);
        root = builder.getNodeState();

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestSuperType").sync();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("oak:TestSuperType");

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());
        assertTrue(r.evaluateSyncNodeTypeRestriction());
    }

    private FulltextIndexPlanner createPlannerForFulltext(NodeState defn, FullTextExpression exp) throws IOException {
        LuceneIndexNode node = createIndexNode(new LuceneIndexDefinition(root, defn, "/foo"));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(exp);
        return new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
    }

    private LuceneIndexNode createSuggestionOrSpellcheckIndex(String nodeType,
                                                              boolean enableSuggestion,
                                                              boolean enableSpellcheck) throws Exception {
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(DECLARING_NODE_TYPES, nodeType);

        defn = LuceneIndexDefinition.updateDefinition(defn.getNodeState().builder());
        NodeBuilder foob = getNode(defn, "indexRules/" + nodeType + "/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        if (enableSuggestion) {
            foob.setProperty(FulltextIndexConstants.PROP_USE_IN_SUGGEST, true);
        } if (enableSpellcheck) {
            foob.setProperty(FulltextIndexConstants.PROP_USE_IN_SPELLCHECK, true);
        }

        LuceneIndexDefinition indexDefinition = new LuceneIndexDefinition(root, defn.getNodeState(), "/foo");
        return createIndexNode(indexDefinition);
    }

    private QueryIndex.IndexPlan getSuggestOrSpellcheckIndexPlan(LuceneIndexNode indexNode, String nodeType,
                                                                 boolean forSugggestion) throws Exception {
        FilterImpl filter = createFilter(nodeType);
        filter.restrictProperty(indexNode.getDefinition().getFunctionName(), Operator.EQUAL,
            PropertyValues.newString((forSugggestion?"suggest":"spellcheck") + "?term=foo"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(indexNode, "/foo", filter, Collections.<OrderEntry>emptyList());

        return planner.getPlan();
    }
    //------ END - Suggestion/spellcheck plan tests

    //------ Cost via doc count per field plan tests
    @Test
    public void noRestrictionWithSingleSortableField() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo"), "async");
        defn.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo"), STRINGS));
        LuceneIndexDefinition definition = new LuceneIndexDefinition(root, defn.getNodeState(), "/test");
        LuceneIndexNode node = createIndexNode(definition);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/test", createFilter("nt:base"),
            ImmutableList.of(new OrderEntry("foo", Type.LONG, OrderEntry.Order.ASCENDING),
                new OrderEntry("bar", Type.LONG, OrderEntry.Order.ASCENDING)));

        assertNotNull(planner.getPlan());
        assertEquals(1, planner.getPlan().getEstimatedEntryCount());
        assertEquals(definition.getCostPerEntry()/2, planner.getPlan().getCostPerEntry(), 0.0001);
    }

    @Test
    public void noRestrictionWithTwoSortableFields() throws Exception{
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo", "bar"), "async");
        defn.setProperty(createProperty(ORDERED_PROP_NAMES, Set.of("foo", "bar"), STRINGS));
        LuceneIndexDefinition definition = new LuceneIndexDefinition(root, defn.getNodeState(), "/test");
        LuceneIndexNode node = createIndexNode(definition);
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/test", createFilter("nt:base"),
            ImmutableList.of(new OrderEntry("foo", Type.LONG, OrderEntry.Order.ASCENDING),
                new OrderEntry("bar", Type.LONG, OrderEntry.Order.ASCENDING)));

        assertNotNull(planner.getPlan());
        assertEquals(1, planner.getPlan().getEstimatedEntryCount());
        assertEquals(definition.getCostPerEntry()/3, planner.getPlan().getCostPerEntry(), 0.0001);
    }

    @Test
    public void useNumDocsOnFieldForCost() throws Exception {
        NodeBuilder defn = newLucenePropertyIndexDefinition(builder, "test", Set.of("foo", "foo1", "foo2"), "async");
        long numOfDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 1000;

        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn.getNodeState(), "/test");
        Document doc = new Document();
        doc.add(new StringField("foo1", "bar1", Field.Store.NO));
        Directory sampleDirectory = createSampleDirectory(numOfDocs, doc);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // Query on "foo"
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(documentsPerValue(numOfDocs), plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

        // Query on "foo" is not null
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.NOT_EQUAL, null);
        planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        plan = planner.getPlan();
        assertEquals(numOfDocs, plan.getEstimatedEntryCount());

        // Query on "foo" like x
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.LIKE, PropertyValues.newString("bar%"));
        planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        plan = planner.getPlan();
        // weight of 3
        assertEquals(numOfDocs / 3 + 1, plan.getEstimatedEntryCount());

        // Query on "foo" > x
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        plan = planner.getPlan();
        // weight of 3
        assertEquals(numOfDocs / 3 + 1, plan.getEstimatedEntryCount());

        // Query on "foo1"
        filter = createFilter("nt:base");
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar1"));
        planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(1, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

        // Query on "foo" and "foo1" should use minimum
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar1"));
        planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(1, plan.getEstimatedEntryCount());

        // Query on "foo" and "foo1" and "foo2" should give 0 as foo3 isn't there in any document
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar1"));
        filter.restrictProperty("foo2", Operator.EQUAL, PropertyValues.newString("bar2"));
        planner = new FulltextIndexPlanner(node, "/test", filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(0, plan.getEstimatedEntryCount());
    }

    @Test
    public void weightedPropDefs() throws Exception {
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex().weight(500)
            .enclosingRule().property("foo1").propertyIndex().weight(20)
            .enclosingRule().property("foo2").propertyIndex().weight(0)
            .enclosingRule().property("foo3").propertyIndex()
        ;
        NodeState defn = idxBuilder.build();

        List<Document> docs = new ArrayList<>();
        Document doc;
        for (int i = 0; i < 60; i++) {
            doc = new Document();
            doc.add(new StringField("foo1", "bar1" + i, Field.Store.NO));
            docs.add(doc);
        }
        doc = new Document();
        doc.add(new StringField("foo2", "bar2", Field.Store.NO));
        docs.add(doc);
        Directory sampleDirectory = createSampleDirectory(1000, docs);
        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // Query on "foo"
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //scale down 1000 by 500 = 2
        assertEquals(2, plan.getEstimatedEntryCount());

        // Query on "foo1"
        filter = createFilter("nt:base");
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //scale down 60 by 20 = 2
        assertEquals(3, plan.getEstimatedEntryCount());

        // Query on "foo" and "foo1"
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //min(2, 3)
        assertEquals(2, plan.getEstimatedEntryCount());

        // Query on "foo1" and "foo2"
        filter = createFilter("nt:base");
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo2", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //don't scale down 1 by 0 (foo1 would estimate 3)
        assertEquals(1, plan.getEstimatedEntryCount());

        // Query on "foo1" and "foo3"
        filter = createFilter("nt:base");
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo3", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //min(0, 3)
        assertEquals(0, plan.getEstimatedEntryCount());
    }

    @Test
    public void weightedRegexPropDefs() throws Exception {
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex()
            .enclosingRule().property("bar", "bar.*", true).propertyIndex().weight(20)
        ;
        NodeState defn = idxBuilder.build();

        List<Document> docs = new ArrayList<>();
        Document doc;
        for (int i = 0; i < 60; i++) {
            doc = new Document();
            doc.add(new StringField("bar1", "foo1" + i, Field.Store.NO));
            docs.add(doc);
        }
        for (int i = 0; i < 40; i++) {
            doc = new Document();
            doc.add(new StringField("bar2", "foo2" + i, Field.Store.NO));
            docs.add(doc);
        }
        Directory sampleDirectory = createSampleDirectory(1000, docs);
        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // Query on and "bar1"
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("bar1", Operator.EQUAL, PropertyValues.newString("foo1"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //scale down 60 by 20 = 3
        assertEquals(3, plan.getEstimatedEntryCount());

        // Query on and "bar1" and "bar2"
        filter = createFilter("nt:base");
        filter.restrictProperty("bar1", Operator.EQUAL, PropertyValues.newString("foo1"));
        filter.restrictProperty("bar2", Operator.EQUAL, PropertyValues.newString("foo2"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //min(3, 2)
        assertEquals(2, plan.getEstimatedEntryCount());
    }

    @Test
    public void overflowingWeight() throws Exception {
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex().weight(Integer.MAX_VALUE/2)
            .enclosingRule().property("foo1").propertyIndex()
        ;
        NodeState defn = idxBuilder.build();

        List<Document> docs = new ArrayList<>();
        Document doc;
        for (int i = 0; i < 60; i++) {
            doc = new Document();
            doc.add(new StringField("foo1", "bar1" + i, Field.Store.NO));
            docs.add(doc);
        }
        Directory sampleDirectory = createSampleDirectory(1000, docs);
        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // Query on and "foo"
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("foo1"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //scale down 1000 by INT_MAX/2 and ceil ~= 1.
        assertEquals(1, plan.getEstimatedEntryCount());

        // Query on and "foo" and "foo1"
        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar1"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //min(1, 60)
        assertEquals(1, plan.getEstimatedEntryCount());
    }

    @Test
    public void functionPropDef() throws Exception {
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        Tree fooPD = idxBuilder.getBuilderTree().getChild("indexRules").getChild("nt:base")
            .getChild("properties").getChild("foo");
        fooPD.setProperty(PROP_FUNCTION, "lower([foo])");
        NodeState defn = idxBuilder.build();

        Document doc = new Document();
        doc.add(new StringField(
            FunctionIndexProcessor.convertToPolishNotation("lower([foo])"), "bar1", Field.Store.NO));
        Directory sampleDirectory = createSampleDirectory(2, doc);
        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // Query on and "foo"
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(
            FunctionIndexProcessor.convertToPolishNotation("lower([foo])"), Operator.EQUAL,
            PropertyValues.newString("foo1"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(1, plan.getEstimatedEntryCount());
    }

    @Test
    public void fullTextWithPropRestriction() throws Exception{
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").nodeScopeIndex()
            .enclosingRule().property("foo1").propertyIndex()
            .enclosingRule().property("foo2").analyzed();
        NodeState defn = idxBuilder.build();

        long numOfDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 1000;

        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        Document doc = new Document();
        doc.add(new StringField("foo1", "bar1", Field.Store.NO));
        Directory sampleDirectory = createSampleDirectory(numOfDocs, doc);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // contains(., 'mountain') AND contains('foo2', 'hill')
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        filter.setFullTextConstraint(FullTextParser.parse("foo2", "hill"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(numOfDocs + 1, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

        // contains(., 'mountain') AND [foo1]='bar' AND contains('foo2', 'hill')
        filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.setFullTextConstraint(FullTextParser.parse("foo2", "hill"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(1, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);
    }

    @Test
    public void unableToIterateFields() throws Exception {
        try {
            LuceneIndexStatistics.failReadingFields = true;
            String indexPath = "/test";
            LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
            idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
            idxBuilder.indexRule("nt:base").property("bar").propertyIndex();
            NodeState defn = idxBuilder.build();

            long numOfDocs = 100;

            LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
            LuceneIndexNode node = createIndexNode(idxDefn, numOfDocs);

            FilterImpl filter = createFilter("nt:base");
            filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
            FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();

            assertEquals(numOfDocs, plan.getEstimatedEntryCount());

            filter = createFilter("nt:base");
            filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("bar"));
            planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            plan = planner.getPlan();

            assertEquals(numOfDocs, plan.getEstimatedEntryCount());

            filter = createFilter("nt:base");
            filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
            filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("bar"));
            planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            plan = planner.getPlan();

            assertEquals(numOfDocs, plan.getEstimatedEntryCount());
        } finally {
            LuceneIndexStatistics.failReadingFields = false;
        }
    }

    @Test
    public void unableToReadCountForJcrTitle() throws Exception {
        try {
            LuceneIndexStatistics.failReadingSyntheticallyFalliableField = true;
            String indexPath = "/test";
            LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
            idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
            idxBuilder.indexRule("nt:base").property("foo1").propertyIndex();
            idxBuilder.indexRule("nt:base").property(SYNTHETICALLY_FALLIABLE_FIELD).propertyIndex();
            idxBuilder.indexRule("nt:base").property("bar").propertyIndex();
            NodeState defn = idxBuilder.build();

            long numOfDocs = 100;

            LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
            Document doc = new Document();
            doc.add(new StringField("foo1", "bar1", Field.Store.NO));
            doc.add(new StringField(SYNTHETICALLY_FALLIABLE_FIELD, "failingField", Field.Store.NO));
            Directory sampleDirectory = createSampleDirectory(numOfDocs, doc);
            LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

            FilterImpl filter = createFilter("nt:base");
            filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
            FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();

            assertEquals(documentsPerValue(numOfDocs), plan.getEstimatedEntryCount());

            filter = createFilter("nt:base");
            filter.restrictProperty(SYNTHETICALLY_FALLIABLE_FIELD, Operator.EQUAL, PropertyValues.newString("bar"));
            planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            plan = planner.getPlan();

            // falliable field's count couldn't be read - so, fallback to numDocs
            assertEquals(numOfDocs + 1, plan.getEstimatedEntryCount());

            filter = createFilter("nt:base");
            filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
            filter.restrictProperty(SYNTHETICALLY_FALLIABLE_FIELD, Operator.EQUAL, PropertyValues.newString("bar"));
            planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            plan = planner.getPlan();

            // min() still comes into play even when one field's count couldn't be read
            assertEquals(1, plan.getEstimatedEntryCount());

            filter = createFilter("nt:base");
            filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("bar"));
            filter.restrictProperty(SYNTHETICALLY_FALLIABLE_FIELD, Operator.EQUAL, PropertyValues.newString("bar"));
            planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
            plan = planner.getPlan();

            // min() still comes into play even when one field's count couldn't be read
            assertEquals(0, plan.getEstimatedEntryCount());
        } finally {
            LuceneIndexStatistics.failReadingSyntheticallyFalliableField = false;
        }
    }

    @Test
    public void costForPathTransformation() throws Exception {
        LuceneIndexStatistics.failReadingSyntheticallyFalliableField = true;
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.indexRule("nt:base").property("foo1").propertyIndex();
        idxBuilder.indexRule("nt:base").property("foo2").propertyIndex();
        Tree fooPD = idxBuilder.getBuilderTree().getChild("indexRules").getChild("nt:base")
            .getChild("properties").getChild("foo2");
        fooPD.setProperty(PROP_FUNCTION, "lower([foo])");
        NodeState defn = idxBuilder.build();

        long numOfDocs = 100;

        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        LuceneIndexNode node = createIndexNode(idxDefn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("a/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(documentsPerValue(numOfDocs), plan.getEstimatedEntryCount());

        filter = createFilter("nt:base");
        filter.restrictProperty("a/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        // there is no doc with foo1
        assertEquals(0, plan.getEstimatedEntryCount());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("a/foo1", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        //Because path transormation comes into play only when direct prop defs don't match
        assertEquals(documentsPerValue(numOfDocs), plan.getEstimatedEntryCount());

        filter = createFilter("nt:base");
        filter.restrictProperty("a/foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty(
            FunctionIndexProcessor.convertToPolishNotation("lower([foo])"), Operator.EQUAL,
            PropertyValues.newString("foo1"));
        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        // there is no doc with lower([foo])
        assertEquals(0, plan.getEstimatedEntryCount());

    }
    //------ END - Cost via doc count per field plan tests

    @Test
    public void facetGetsPlanned() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("facet").getBuilderTree().setProperty(FACETS, true);

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull("Index supporting facet should participate", plan);
    }

    @Test
    public void facetGetsPlanned2() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("facet1").getBuilderTree().setProperty(FACETS, true);
        defnb.indexRule("nt:base").property("rel/facet2").getBuilderTree().setProperty(FACETS, true);

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet1)"));
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(rel/facet2)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull("Index supporting all facets should participate", plan);
    }

    @Test
    public void noFacetPropIndexed() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index supporting none of the facets mustn't participate", plan);
    }

    @Test
    public void someFacetPropIndexed() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("facet").getBuilderTree().setProperty(FACETS, true);

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet)"));
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(rel/facet)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index supporting some of the facets mustn't participate", plan);
    }

    @Test
    public void selectionPolicyWithTags() throws Exception {
        // query without specifying index tag

        // case 1: tags are defined in the index definition
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.selectionPolicy(IndexSelectionPolicy.TAG);
        defnb.tags("bar", "baz");

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index specifying a tag selection policy is not selected", plan);
    }

    @Test
    public void selectionPolicyWithoutTags() throws Exception {
        // query without specifying index tag

        // case 2: tags are not defined in index definition
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.selectionPolicy(IndexSelectionPolicy.TAG);

        LuceneIndexDefinition defn = new LuceneIndexDefinition(root, defnb.build(), "/foo");
        LuceneIndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, "/foo", filter, Collections.<OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index specifying a tag selection policy is not selected", plan);
    }

    private LuceneIndexNode createIndexNode(LuceneIndexDefinition  defn, long numOfDocs) throws IOException {
        return new LuceneIndexNodeManager("foo", defn, new TestReaderFactory(createSampleDirectory(numOfDocs)).createReaders(defn, EMPTY_NODE, "foo"), null).acquire();
    }

    private LuceneIndexNode createIndexNode(LuceneIndexDefinition  defn) throws IOException {
        return new LuceneIndexNodeManager("foo", defn, new TestReaderFactory(createSampleDirectory()).createReaders(defn, EMPTY_NODE, "foo"), null).acquire();
    }

    private LuceneIndexNode createIndexNode(LuceneIndexDefinition  defn, Directory sampleDirectory) throws IOException {
        return new LuceneIndexNodeManager("foo", defn, new TestReaderFactory(sampleDirectory).createReaders(defn, EMPTY_NODE, "foo"), null).acquire();
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private static Directory createSampleDirectory() throws IOException {
        return createSampleDirectory(1);
    }

    private static Directory createSampleDirectory(long numOfDocs) throws IOException {
        return createSampleDirectory(numOfDocs, Collections.emptyList());
    }

    private static Directory createSampleDirectory(long numOfDocs, @NotNull Document doc) throws IOException {
        return createSampleDirectory(numOfDocs, Collections.singletonList(doc));
    }

    private static Directory createSampleDirectory(long numOfDocs, Iterable<Document> docs) throws IOException {
        Directory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(VERSION, LuceneIndexConstants.ANALYZER);
        IndexWriter writer = new  IndexWriter(dir, config);
        for (int i = 0; i < numOfDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar" + i, Field.Store.NO));
            writer.addDocument(doc);
        }
        for (Document doc : docs) {
            writer.addDocument(doc);
        }
        writer.close();
        return dir;
    }

    private static FulltextIndexPlanner.PlanResult pr(QueryIndex.IndexPlan plan) {
        return (FulltextIndexPlanner.PlanResult) plan.getAttribute(FulltextIndex.ATTR_PLAN_RESULT);
    }

    @NotNull
    private static NodeBuilder getNode(@NotNull NodeBuilder node, @NotNull String path) {
        for (String name : PathUtils.elements(requireNonNull(path))) {
            node = node.getChildNode(requireNonNull(name));
        }
        return node;
    }

    private static class TestReaderFactory implements LuceneIndexReaderFactory {
        final Directory directory;

        private TestReaderFactory(Directory directory) {
            this.directory = directory;
        }

        @Override
        public List<LuceneIndexReader> createReaders(LuceneIndexDefinition definition, NodeState definitionState,
                                                     String indexPath) throws IOException {
            List<LuceneIndexReader> readers = new ArrayList<>();
            readers.add(new DefaultIndexReader(directory, null, definition.getAnalyzer()));
            return readers;
        }

        @Override
        public MountInfoProvider getMountInfoProvider() {
            return Mounts.defaultMountInfoProvider();
        }
    }

    /**
     * The estimated number of documents per unique value.
     *
     * @param numofDocs the total number of documents
     * @return the estimated number of documents
     */
    public static long documentsPerValue(long numofDocs) {
        // OAK-7379: divide the number of documents by the number of unique entries
        return Math.max(1, numofDocs / FulltextIndexPlanner.DEFAULT_PROPERTY_WEIGHT);
    }


}
