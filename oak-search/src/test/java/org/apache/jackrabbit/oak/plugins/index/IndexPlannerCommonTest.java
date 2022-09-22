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
package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.index.TestUtil.registerTestNodeType;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.REP_FACET;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class IndexPlannerCommonTest {

    protected NodeState root = INITIAL_CONTENT;
    protected NodeBuilder builder = root.builder();
    public static final String NT_TEST = "oak:TestNode";
    protected IndexOptions indexOptions;
    protected String indexName;

    @After
    public void cleanup() {
        FulltextIndexPlanner.setUseActualEntryCount(true);
    }

    @Before
    public void setup() {
        indexName = generateRandomIndexName("test3123");
    }

    @Test
    public void planForSortField() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, createFilter("nt:base"),
                ImmutableList.of(new QueryIndex.OrderEntry("foo", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING)));
        assertNotNull(planner.getPlan());
        assertTrue(pr(planner.getPlan()).isUniquePathsRequired());
    }

    @Test
    public void noPlanForSortOnlyByScore() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, createFilter("nt:file"),
                ImmutableList.of(new QueryIndex.OrderEntry("jcr:score", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING)));
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQueryNonFulltextIndex() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void noApplicableRule() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:folder"), STRINGS));
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());

        filter = createFilter("nt:folder");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void nodeTypeInheritance() throws Exception {
        //Index if for nt:hierarchyNode and query is for nt:folder
        //as nt:folder extends nt:hierarchyNode we should get a plan
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:hierarchyNode"), STRINGS));
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:folder");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void noMatchingProperty() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void matchingProperty() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);
        assertNotNull(pr(plan));
        assertTrue(pr(plan).evaluateNonFullTextConstraints());
    }

    @Test
    public void purePropertyIndexAndPathRestriction() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void fulltextIndexAndPathRestriction() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        //For case when a full text property is present then path restriction can be
        //evaluated
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fulltextIndexAndNodeTypeRestriction() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        builder.getChildNode("oak:index").getChildNode(indexName).setProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:file"), NAMES)
                .setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));

        NodeBuilder foob = getNode(defn, "indexRules/nt:file/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        //For case when a full text property is present then path restriction can be
        //evaluated
        assertNotNull(planner.getPlan());
    }

    @Test
    public void pureNodeTypeWithEvaluatePathRestrictionEnabled() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defn = getIndexDefinitionNodeBuilder(index, indexName,
                of(TYPENAME_STRING));
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        TestUtil.useV2(defn);

        FilterImpl filter = createFilter("nt:file");
        filter.restrictPath("/", Filter.PathRestriction.ALL_CHILDREN);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        // /jcr:root//element(*, nt:file)
        //For queries like above Fulltext index should not return a plan
        assertNull(planner.getPlan());
    }

    @Test
    public void purePropertyIndexAndNodeTypeRestriction() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        defn.setProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:file"), NAMES);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        assertNull(planner.getPlan());
    }

    @Test
    public void purePropertyIndexAndNodeTypeRestriction2() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");

        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        //No plan should be result for a index with just a rule for nt:base
        assertNull(planner.getPlan());
    }

    @Test
    public void purePropertyIndexAndNodeTypeRestriction3() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        builder.getChildNode("oak:index").getChildNode(indexName).setProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:file"), NAMES)
                .setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:file/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:file");
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);
        assertNotNull(pr(plan));
        assertTrue(pr(plan).evaluateNodeTypeRestriction());
    }

    @Test
    public void worksWithIndexFormatV2Onwards() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = getIndexDefinitionNodeBuilder(index, indexName,
                of(TYPENAME_STRING));
        //Dummy data node to ensure that LuceneIndexDefinition does not consider it
        //as a fresh indexing case
        nb.child(INDEX_DATA_CHILD_NAME);

        IndexNode node = createIndexNode(getIndexDefinition(root, nb.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void propertyIndexCost() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 1000;

        FulltextIndexPlanner.setUseActualEntryCount(false);
        IndexDefinition idxDefn = getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(idxDefn, numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));


        //For propertyIndex if entry count (default to IndexDefinition.DEFAULT_ENTRY_COUNT) is
        //less than numOfDoc then that would be preferred
        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertEquals(idxDefn.getEntryCount(), plan.getEstimatedEntryCount());
            assertEquals(1.0, plan.getCostPerExecution(), 0);
            assertEquals(1.0, plan.getCostPerEntry(), 0);
        }, 4500 * 5);
    }

    @Test
    public void propertyIndexCost2() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(FulltextIndexConstants.COST_PER_ENTRY, 2.0);
        defn.setProperty(FulltextIndexConstants.COST_PER_EXECUTION, 3.0);

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT - 100;
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(documentsPerValue(numofDocs), plan.getEstimatedEntryCount());
            assertEquals(3.0, plan.getCostPerExecution(), 0);
            assertEquals(2.0, plan.getCostPerEntry(), 0);
        }, 4500 * 5);
    }

    @Test
    public void propertyIndexCostActualOverriddenByEntryCount() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        long entryCount = IndexDefinition.DEFAULT_ENTRY_COUNT - 100;
        defn.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, entryCount);

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 100;

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(entryCount, plan.getEstimatedEntryCount());
        }, 4500 * 5);
    }

    @Test
    public void propertyIndexCostActualByDefault() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 100;

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(documentsPerValue(numofDocs), plan.getEstimatedEntryCount());
        }, 4500 * 5);
    }

    @Test
    public void fulltextIndexCost() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defn = getIndexDefinitionNodeBuilder(index, indexName,
                of(TYPENAME_STRING));
        TestUtil.useV2(defn);

        long numofDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 1000;
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName), numofDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(numofDocs, plan.getEstimatedEntryCount());
        }, 4500 * 5);

    }

    @Test
    public void nullPropertyCheck() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, null);
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("For null checks no plan should be returned", plan);
    }

    @Test
    public void nullPropertyCheck2() throws Exception {
        root = registerTestNodeType(builder).getNodeState();
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        NodeBuilder rules = defn.child(INDEX_RULES);
        child(rules, "oak:TestNode/properties/prop2")
                .setProperty(FulltextIndexConstants.PROP_NAME, "foo")
                .setProperty(FulltextIndexConstants.PROP_NULL_CHECK_ENABLED, true)
                .setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);

        IndexDefinition idxDefn = getIndexDefinition(root, builder.getNodeState().getChildNode("oak:index").getChildNode(indexName), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(idxDefn);

        FilterImpl filter = createFilter(NT_TEST);
        filter.restrictProperty("foo", Operator.EQUAL, null);

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull("For null checks plan should be returned with nullCheckEnabled", plan);
        FulltextIndexPlanner.PlanResult pr =
                (FulltextIndexPlanner.PlanResult) plan.getAttribute(FulltextIndex.ATTR_PLAN_RESULT);
        assertNotNull(pr.getPropDefn(filter.getPropertyRestriction("foo")));
    }

    @Test
    public void noPathRestHasQueryPath() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.QUERY_PATHS, of("/test/a"), Type.STRINGS));
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void hasPathRestHasMatchingQueryPaths() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(createProperty(IndexConstants.QUERY_PATHS, of("/test/a", "/test/b"), Type.STRINGS));
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));

        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/test/a", Filter.PathRestriction.ALL_CHILDREN);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void hasPathRestHasNoExplicitQueryPaths() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));

        FilterImpl filter = createFilter("nt:base");
        filter.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNotNull(planner.getPlan());
    }

    @Test
    public void noPlanForFulltextQueryAndOnlyAnalyzedProperties() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");

        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        defn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull(plan);
    }

    @Test
    public void noPlanForNodeTypeQueryAndOnlyAnalyzedProperties() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        builder.getChildNode("oak:index").getChildNode(indexName).setProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:file"), NAMES)
                .setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:file/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:file");
        filter.restrictPath("/foo", Filter.PathRestriction.ALL_CHILDREN);
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull(plan);
    }

    @Test
    public void indexedButZeroWeightProps() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("bar").propertyIndex().weight(0);
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("a"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        //Even though foo is indexed it would not be considered for a query involving just foo
        assertNull(planner.getPlan());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("a"));
        planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan1 = planner.getPlan();
        assertNotNull(plan1);

        final FilterImpl filter2 = createFilter("nt:base");
        filter2.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("a"));
        filter2.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("a"));


        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner2 = getIndexPlanner(node, "/oak:index/" + indexName, filter2, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan2 = planner2.getPlan();
            assertNotNull(plan2);
            // Since, the index has no entries for "bar", estimated entry count for plan2 would be 0
            assertEquals(0, plan2.getEstimatedEntryCount());
            assertThat(plan2.getEstimatedEntryCount(), lessThan(plan1.getEstimatedEntryCount()));
            assertTrue(pr(plan2).hasProperty("foo"));
            assertTrue(pr(plan2).hasProperty("bar"));
        }, 4500 * 5);
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

        IndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
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

        IndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
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

        IndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
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

        IndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
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

        IndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
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

        IndexNode node = createSuggestionOrSpellcheckIndex(indexNodeType, enableSuggestionIndex, enableSpellcheckIndex);
        QueryIndex.IndexPlan plan = getSuggestOrSpellcheckIndexPlan(node, queryNodeType, queryForSugggestion);

        assertNull(plan);
    }

    @Test
    public void fullTextQuery_RelativePath1() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");

        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(), FullTextParser.parse("bar", "mountain"), "/oak:index/" + indexName);

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("allProps"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/allProps");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        foob.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        FullTextExpression exp = FullTextParser.parse("bar", "mountain OR valley");
        exp = new FullTextContains("bar", "mountain OR valley", exp);
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(), exp, "/oak:index/" + indexName);

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps_NodePathQuery() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("allProps"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/allProps");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        foob.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        foob.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
                FullTextParser.parse("jcr:content/*", "mountain OR valley"), "/oak:index/" + indexName);

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps_AggregatedNodePathQuery() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("allProps"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder agg = defn.child(FulltextIndexConstants.AGGREGATES).child("nt:base").child("include0");
        agg.setProperty(FulltextIndexConstants.AGG_PATH, "jcr:content");
        agg.setProperty(FulltextIndexConstants.AGG_RELATIVE_NODE, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
                FullTextParser.parse("jcr:content/*", "mountain OR valley"), "/oak:index/" + indexName);

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_IndexAllProps_NodePathQuery_NoPlan() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
                FullTextParser.parse("jcr:content/*", "mountain OR valley"), "/oak:index/" + indexName);

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_NonAnalyzedProp_NoPlan() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo", "bar"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");

        NodeBuilder barb = getNode(defn, "indexRules/nt:base/properties/bar");
        barb.setProperty(FulltextIndexConstants.PROP_NAME, "bar");
        barb.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        //where contains('jcr:content/*', 'mountain OR valley') can be evaluated by index
        //on nt:base by evaluating on '.' and then checking if node name is 'jcr:content'
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(),
                FullTextParser.parse("foo", "mountain OR valley"), "/oak:index/" + indexName);

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_RelativePropertyPaths() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo", "bar"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
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
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(), exp, "/oak:index/" + indexName);

        //No plan for unindex property
        assertNotNull(planner.getPlan());
    }

    @Test
    public void fullTextQuery_DisjointPropertyPaths() throws Exception {
        getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo", "bar"), "async");

        //Index all props and then perform fulltext
        builder = builder.getNodeState().builder();
        NodeBuilder defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/nt:base/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_NAME, "foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        NodeBuilder barb = getNode(defn, "indexRules/nt:base/properties/bar");
        barb.setProperty(FulltextIndexConstants.PROP_NAME, "bar");
        barb.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);

        FullTextExpression fooExp = FullTextParser.parse("metadata/bar", "mountain OR valley");
        FullTextExpression barExp = FullTextParser.parse("jcr:content/foo", "mountain OR valley");
        FullTextExpression exp = new FullTextAnd(Arrays.asList(fooExp, barExp));
        FulltextIndexPlanner planner = createPlannerForFulltext(defn.getNodeState(), exp, "/oak:index/" + indexName);

        //No plan for unindex property
        assertNull(planner.getPlan());
    }

    @Test
    public void valuePattern_Equals() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .valueExcludedPrefixes("/jobs");
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNotNull(planner.getPlan());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("/jobs/a"));
        planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void valuePattern_StartsWith() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .valueExcludedPrefixes("/jobs");
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("foo", Operator.LESS_OR_EQUAL, PropertyValues.newString("/bar0"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNotNull(planner.getPlan());

        filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newString("/jobs"));
        filter.restrictProperty("foo", Operator.LESS_OR_EQUAL, PropertyValues.newString("/jobs0"));

        planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        assertNull(planner.getPlan());
    }

    @Test
    public void relativeProperty_Basics() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("jcr:content/bar").propertyIndex();

        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
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
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:unstructured").property("foo").propertyIndex();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:unstructured");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        //Should not return a plan for index rule other than nt:base
        assertNull(plan);
    }

    @Test
    public void relativeProperty_WithFulltext() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("bar").analyzed();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.setFullTextConstraint(FullTextParser.parse("jcr:content/bar", "mountain"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertTrue(pr.isPathTransformed());
        assertFalse(pr.evaluateNonFullTextConstraints());
        assertEquals("/a/b", pr.transformPath("/a/b/jcr:content"));
        assertNull(pr.transformPath("/a/b/c"));
    }

    @Test
    public void relativeProperty_FullText() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        FullTextExpression ft = FullTextParser.parse("jcr:content/*", "mountain OR valley");
        filter.setFullTextConstraint(ft);

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertFalse(pr.hasProperty("jcr:content/foo"));
    }

    @Test
    public void relativeProperty_MultipleMatch() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("bar").propertyIndex();
        defnb.indexRule("nt:base").property("baz").propertyIndex();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("jcr:content/bar", Operator.EQUAL, PropertyValues.newString("/bar"));
        filter.restrictProperty("metadata/baz", Operator.EQUAL, PropertyValues.newString("/bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult pr = pr(plan);
        assertTrue(pr.hasProperty("jcr:content/foo"));
        assertTrue(pr.hasProperty("jcr:content/bar"));
        assertFalse(pr.hasProperty("metadata/baz"));
    }

    @Test
    public void evaluatePathRestrictionExposesSupportCorrectly() throws Exception {
        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        // Evaluates path restriction
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async")
                .setProperty(EVALUATE_PATH_RESTRICTION, true);
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertTrue(plan.getSupportsPathRestriction());

        // Doesn't evaluate path restriction
        defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async")
                .setProperty(EVALUATE_PATH_RESTRICTION, false);
        node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName));
        planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        plan = planner.getPlan();
        assertFalse(plan.getSupportsPathRestriction());
    }


    //~------------------------------< sync indexes >

    @Test
    public void syncIndex_uniqueIndex() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(1, plan.getEstimatedEntryCount());
            FulltextIndexPlanner.PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

            assertNotNull(hr);
            assertEquals("foo", hr.propertyName);
            assertEquals("foo", hr.pr.propertyName);
        }, 4500 * 5);
    }

    @Test
    public void syncIndex_uniqueAndRelative() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("jcr:content/foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(1, plan.getEstimatedEntryCount());
            FulltextIndexPlanner.PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

            assertNotNull(hr);
            assertEquals("foo", hr.propertyName);
            assertEquals("jcr:content/foo", hr.pr.propertyName);
        }, 4500 * 5);
    }

    @Test
    public void syncIndex_nonUnique() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(documentsPerValue(100), plan.getEstimatedEntryCount());
            FulltextIndexPlanner.PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

            assertNotNull(hr);
            assertEquals("foo", hr.propertyName);
            assertEquals("foo", hr.pr.propertyName);
        }, 4500 * 5);
    }

    /**
     * If both non unique and unique indexes are found then unique should be picked
     */
    @Test
    public void syncIndex_nonUniqueAndUniqueBoth() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();
        defnb.indexRule("nt:base").property("bar").propertyIndex().sync();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.restrictProperty("bar", Operator.EQUAL, PropertyValues.newString("foo"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertEquals(1, plan.getEstimatedEntryCount());
            FulltextIndexPlanner.PropertyIndexResult hr = pr(plan).getPropertyIndexResult();
            assertNotNull(hr);
            assertEquals("foo", hr.propertyName);
            assertEquals("foo", hr.pr.propertyName);
        }, 4500 * 5);

    }

    @Test
    public void syncIndex_NotUsedWithSort() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        defnb.indexRule("nt:base").property("bar").propertyIndex().ordered();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter,
                    ImmutableList.of(new QueryIndex.OrderEntry("bar", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING)));
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);

            assertEquals(documentsPerValue(100), plan.getEstimatedEntryCount());
            FulltextIndexPlanner.PropertyIndexResult hr = pr(plan).getPropertyIndexResult();
            assertNull(hr);
        }, 4500 * 5);
    }

    @Test
    public void syncIndex_NotUsedWithFulltext() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();
        defnb.indexRule("nt:base").property("bar").analyzed();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn, 100);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        filter.setFullTextConstraint(FullTextParser.parse("bar", "mountain"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter,
                    ImmutableList.of(new QueryIndex.OrderEntry("bar", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING)));
            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);

            assertEquals(documentsPerValue(100), plan.getEstimatedEntryCount());
            FulltextIndexPlanner.PropertyIndexResult hr = pr(plan).getPropertyIndexResult();

            assertNull(hr);
        }, 4500 * 5);
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
    public void nodetype_primaryType() throws Exception {
        TestUtil.registerNodeType(builder, testNodeTypeDefn);
        root = builder.getNodeState();

        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestSuperType");
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("oak:TestSuperType");

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());

        //As oak:TestSuperType is parent of oak:TestTypeA the child nodetypes should
        //also be indexed
        filter = createFilter("oak:TestTypeA");
        planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        plan = planner.getPlan();

        assertNotNull(plan);
        r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());
    }

    @Test
    public void nodetype_mixin() throws Exception {
        TestUtil.registerNodeType(builder, testNodeTypeDefn);
        root = builder.getNodeState();

        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestMixA");
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("oak:TestMixA");

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());
    }

    @Test
    public void syncNodeTypeIndex() throws Exception {
        TestUtil.registerNodeType(builder, testNodeTypeDefn);
        root = builder.getNodeState();

        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestSuperType").sync();
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("oak:TestSuperType");

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull(plan);

        FulltextIndexPlanner.PlanResult r = pr(plan);
        assertTrue(r.evaluateNodeTypeRestriction());
        assertTrue(r.evaluateSyncNodeTypeRestriction());
    }

    private FulltextIndexPlanner createPlannerForFulltext(NodeState defn, FullTextExpression exp, String indexPath) throws IOException {
        IndexNode node = createIndexNode(getIndexDefinition(root, defn, indexPath));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(exp);
        return getIndexPlanner(node, indexPath, filter, Collections.<QueryIndex.OrderEntry>emptyList());
    }

    private IndexNode createSuggestionOrSpellcheckIndex(String nodeType,
                                                        boolean enableSuggestion,
                                                        boolean enableSpellcheck) throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(DECLARING_NODE_TYPES, nodeType);

        builder = builder.getNodeState().builder();
        defn = IndexDefinition.updateDefinition(builder.getChildNode("oak:index").getChildNode(indexName));
        NodeBuilder foob = getNode(defn, "indexRules/" + nodeType + "/properties/foo");
        foob.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        if (enableSuggestion) {
            foob.setProperty(FulltextIndexConstants.PROP_USE_IN_SUGGEST, true);
        }
        if (enableSpellcheck) {
            foob.setProperty(FulltextIndexConstants.PROP_USE_IN_SPELLCHECK, true);
        }

        IndexDefinition indexDefinition = getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName);
        return createIndexNode(indexDefinition);
    }

    private QueryIndex.IndexPlan getSuggestOrSpellcheckIndexPlan(IndexNode indexNode, String nodeType,
                                                                 boolean forSugggestion) throws Exception {
        FilterImpl filter = createFilter(nodeType);
        filter.restrictProperty(indexNode.getDefinition().getFunctionName(), Operator.EQUAL,
                PropertyValues.newString((forSugggestion ? "suggest" : "spellcheck") + "?term=foo"));
        FulltextIndexPlanner planner = getIndexPlanner(indexNode, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

        return planner.getPlan();
    }
    //------ END - Suggestion/spellcheck plan tests

    //------ Cost via doc count per field plan tests
    @Test
    public void noRestrictionWithSingleSortableField() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo"), "async");
        defn.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        IndexDefinition definition = getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(definition);

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, createFilter("nt:base"),
                    ImmutableList.of(new QueryIndex.OrderEntry("foo", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING),
                            new QueryIndex.OrderEntry("bar", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING)));
            assertNotNull(planner.getPlan());
            assertEquals(1, planner.getPlan().getEstimatedEntryCount());
            assertEquals(definition.getCostPerEntry() / 2, planner.getPlan().getCostPerEntry(), 0.0001);
        }, 4500 * 5);

    }

    @Test
    public void noRestrictionWithTwoSortableFields() throws Exception {
        NodeBuilder defn = getPropertyIndexDefinitionNodeBuilder(builder, indexName, of("foo", "bar"), "async");
        defn.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo", "bar"), STRINGS));
        IndexDefinition definition = getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(definition);

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, createFilter("nt:base"),
                    ImmutableList.of(new QueryIndex.OrderEntry("foo", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING),
                            new QueryIndex.OrderEntry("bar", Type.LONG, QueryIndex.OrderEntry.Order.ASCENDING)));

            assertNotNull(planner.getPlan());

            assertEquals(1, planner.getPlan().getEstimatedEntryCount());
            assertEquals(definition.getCostPerEntry() / 3, planner.getPlan().getCostPerEntry(), 0.0001);
        }, 4500 * 5);
    }


    @Test
    public void facetGetsPlanned() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("facet").getBuilderTree().setProperty(FACETS, true);
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull("Index supporting facet should participate", plan);
    }

    @Test
    public void facetGetsPlanned2() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("facet1").getBuilderTree().setProperty(FACETS, true);
        defnb.indexRule("nt:base").property("rel/facet2").getBuilderTree().setProperty(FACETS, true);
        builder.getChildNode("oak:index").getChildNode(indexName).removeProperty("async");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet1)"));
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(rel/facet2)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNotNull("Index supporting all facets should participate", plan);
    }

    @Test
    public void noFacetPropIndexed() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index supporting none of the facets mustn't participate", plan);
    }

    @Test
    public void someFacetPropIndexed() throws Exception {
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.indexRule("nt:base").property("facet").getBuilderTree().setProperty(FACETS, true);

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(facet)"));
        filter.restrictProperty(REP_FACET, Operator.EQUAL, PropertyValues.newString(REP_FACET + "(rel/facet)"));

        // just so that the index can be picked..
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index supporting some of the facets mustn't participate", plan);
    }

    @Test
    public void selectionPolicyWithTags() throws Exception {
        // query without specifying index tag

        // case 1: tags are defined in the index definition
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.selectionPolicy(IndexSelectionPolicy.TAG);
        defnb.tags("bar", "baz");

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index specifying a tag selection policy is not selected", plan);
    }

    @Test
    public void selectionPolicyWithoutTags() throws Exception {
        // query without specifying index tag

        // case 2: tags are not defined in index definition
        IndexDefinitionBuilder defnb = getIndexDefinitionBuilder(builder.child("oak:index").child(indexName));
        defnb.selectionPolicy(IndexSelectionPolicy.TAG);

        IndexDefinition defn = getIndexDefinition(root, defnb.build(), "/oak:index/" + indexName);
        IndexNode node = createIndexNode(defn);

        FilterImpl filter = createFilter("nt:base");

        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();
        assertNull("Index specifying a tag selection policy is not selected", plan);
    }


    protected FilterImpl createFilter(String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private static FulltextIndexPlanner.PlanResult pr(QueryIndex.IndexPlan plan) {
        return (FulltextIndexPlanner.PlanResult) plan.getAttribute(FulltextIndex.ATTR_PLAN_RESULT);
    }

    @NotNull
    private static NodeBuilder getNode(@NotNull NodeBuilder node, @NotNull String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            node = node.getChildNode(checkNotNull(name));
        }
        return node;
    }

    private static String generateRandomIndexName(String prefix) {
        return prefix + RandomStringUtils.random(5, true, false);
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


    protected abstract IndexNode createIndexNode(IndexDefinition defn) throws IOException;

    protected abstract IndexNode createIndexNode(IndexDefinition defn, long numOfDocs) throws IOException;

    protected abstract IndexDefinition getIndexDefinition(NodeState root, NodeState defn, String indexPath);

    protected abstract NodeBuilder getPropertyIndexDefinitionNodeBuilder(@NotNull NodeBuilder builder, @NotNull String name,
                                                                         @NotNull Set<String> includes,
                                                                         @NotNull String async);

    protected abstract NodeBuilder getIndexDefinitionNodeBuilder(@NotNull NodeBuilder index, @NotNull String name,
                                                                 @Nullable Set<String> propertyTypes);

    protected abstract FulltextIndexPlanner getIndexPlanner(IndexNode indexNode,
                                                            String indexPath,
                                                            Filter filter, List<QueryIndex.OrderEntry> sortOrder);

    protected abstract IndexDefinitionBuilder getIndexDefinitionBuilder();

    protected abstract IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder);


}
