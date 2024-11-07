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

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexPlannerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
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
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexStatistics.SYNTHETICALLY_FALLIABLE_FIELD;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_FUNCTION;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

public class LuceneIndexPlannerCommonTest extends IndexPlannerCommonTest {

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
    public void isNotNullAndIsNull() throws Exception{
        String indexPath = "/test";
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder(child(builder, indexPath));
        idxBuilder.indexRule("nt:unstructured").property("foo")
                .enclosingRule().property("foo").propertyIndex().nullCheckEnabled()
                .enclosingRule().property("foo2").propertyIndex().nullCheckEnabled();
        NodeState defn = idxBuilder.build();

        LuceneIndexDefinition idxDefn = new LuceneIndexDefinition(root, defn, indexPath);
        List<Document> list = new ArrayList<>();
        // 10 documents have "foo" (foo = 1)
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("foo", "1", Field.Store.NO));
            list.add(doc);
        }
        // 20 documents have "foo2 is null" (:nullProps = foo2)
        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new StringField(FieldNames.NULL_PROPS, "foo2", Field.Store.NO));
            list.add(doc);
        }
        Directory sampleDirectory = createSampleDirectory(0, list);
        LuceneIndexNode node = createIndexNode(idxDefn, sampleDirectory);

        // foo is null
        // that can be at most 20, because there are only
        // that many documents with ":nullProps"
        FilterImpl filter = createFilter("nt:unstructured");
        filter.restrictProperty("foo", Operator.EQUAL, null);

        FulltextIndexPlanner planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        QueryIndex.IndexPlan plan = planner.getPlan();

        assertEquals(20, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

        // foo2 is null:
        // that can be at most 20, because there are only 10 documents with this property
        filter = createFilter("nt:unstructured");
        filter.restrictProperty("foo2", Operator.EQUAL, null);

        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(20, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

        // foo is not null:
        // that can be at most 10, because there are only 10 documents with this property
        filter = createFilter("nt:unstructured");
        filter.restrictProperty("foo", Operator.NOT_EQUAL, null);

        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(10, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

        // foo2 is not null:
        // that can be at most 0, because there are no documents wit this property
        filter = createFilter("nt:unstructured");
        filter.restrictProperty("foo2", Operator.NOT_EQUAL, null);

        planner = new FulltextIndexPlanner(node, indexPath, filter, Collections.emptyList());
        plan = planner.getPlan();

        assertEquals(0, plan.getEstimatedEntryCount());
        assertEquals(1.0, plan.getCostPerExecution(), 0);
        assertEquals(1.0, plan.getCostPerEntry(), 0);

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
    public void unableToIterateFields() throws Exception {
        try {
            LuceneIndexStatistics.failReadingFields = true;
            String indexPath = "/test";
            IndexDefinitionBuilder idxBuilder = getIndexDefinitionBuilder(org.apache.jackrabbit.oak.plugins.index.TestUtil.child(builder, indexPath));
            idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
            idxBuilder.indexRule("nt:base").property("bar").propertyIndex();
            NodeState defn = idxBuilder.build();

            long numOfDocs = 100;

            IndexDefinition idxDefn = getIndexDefinition(root, defn, indexPath);
            IndexNode node = createIndexNode(idxDefn, numOfDocs);

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
    public void costForPathTransformation() throws Exception {
        LuceneIndexStatistics.failReadingSyntheticallyFalliableField = true;
        String indexPath = "/test";
        IndexDefinitionBuilder idxBuilder = getIndexDefinitionBuilder(org.apache.jackrabbit.oak.plugins.index.TestUtil.child(builder, indexPath));
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
        idxBuilder.indexRule("nt:base").property("foo1").propertyIndex();
        idxBuilder.indexRule("nt:base").property("foo2").propertyIndex();
        Tree fooPD = idxBuilder.getBuilderTree().getChild("indexRules").getChild("nt:base")
                .getChild("properties").getChild("foo2");
        fooPD.setProperty(PROP_FUNCTION, "lower([foo])");
        NodeState defn = idxBuilder.build();

        long numOfDocs = 100;

        IndexDefinition idxDefn = getIndexDefinition(root, defn, indexPath);
        IndexNode node = createIndexNode(idxDefn, 100);

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






    @Override
    protected IndexNode createIndexNode(IndexDefinition defn) throws IOException {
        return new LuceneIndexNodeManager("foo", (LuceneIndexDefinition) defn, new TestReaderFactory(createSampleDirectory()).createReaders((LuceneIndexDefinition) defn, EMPTY_NODE, defn.getIndexPath()), null).acquire();
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition defn, long numOfDocs) throws IOException {
        return new LuceneIndexNodeManager("foo", (LuceneIndexDefinition) defn, new TestReaderFactory(createSampleDirectory(numOfDocs)).createReaders((LuceneIndexDefinition) defn, EMPTY_NODE, defn.getIndexPath()), null).acquire();
    }

    private LuceneIndexNode createIndexNode(LuceneIndexDefinition  defn, Directory sampleDirectory) throws IOException {
        return new LuceneIndexNodeManager("foo", defn, new TestReaderFactory(sampleDirectory).createReaders(defn, EMPTY_NODE, "foo"), null).acquire();
    }

    @Override
    protected IndexDefinition getIndexDefinition(NodeState root, NodeState defn, String indexPath) {
        return new LuceneIndexDefinition(root, defn, indexPath);
    }

    @Override
    protected NodeBuilder getPropertyIndexDefinitionNodeBuilder(@NotNull NodeBuilder builder, @NotNull String name, @NotNull Set<String> includes, @NotNull String async) {
        NodeBuilder oakIndex = builder.child("oak:index");
        return newLucenePropertyIndexDefinition(oakIndex, name, includes, async);
    }

    @Override
    protected NodeBuilder getIndexDefinitionNodeBuilder(@NotNull NodeBuilder index, @NotNull String name, @Nullable Set<String> propertyTypes) {
        return newLuceneIndexDefinition(index, name, propertyTypes);
    }

    @Override
    protected FulltextIndexPlanner getIndexPlanner(IndexNode indexNode, String indexPath, Filter filter, List<QueryIndex.OrderEntry> sortOrder) {
        return new FulltextIndexPlanner(indexNode, indexPath, filter, sortOrder);
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder() {
        return new LuceneIndexDefinitionBuilder();
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder) {
        return new LuceneIndexDefinitionBuilder(builder);
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
}
