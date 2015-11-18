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

import java.util.Collections;

import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.TokenizerChain;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.codecs.Codec;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_LONG;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANALYZERS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TIKA;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.registerTestNodeType;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.tree.impl.TreeConstants.OAK_CHILD_ORDER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexDefinitionTest {
    private Codec oakCodec = new OakCodec();

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void defaultConfig() throws Exception{
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertTrue(idxDefn.saveDirListing());
    }

    @Test
    public void fullTextEnabled() throws Exception{
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = idxDefn.getApplicableIndexingRule(NT_BASE);
        assertTrue("By default fulltext is enabled", idxDefn.isFullTextEnabled());
        assertTrue("By default everything is indexed", rule.isIndexed("foo"));
        assertTrue("Property types need to be defined", rule.includePropertyType(PropertyType.DATE));
        assertTrue("For fulltext storage is enabled", rule.getConfig("foo").stored);

        assertFalse(rule.getConfig("foo").skipTokenization("foo"));
        assertTrue(rule.getConfig("jcr:uuid").skipTokenization("jcr:uuid"));
    }

    @Test
    public void propertyTypes() throws Exception{
        builder.setProperty(createProperty(INCLUDE_PROPERTY_TYPES, of(TYPENAME_LONG), STRINGS));
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar"), STRINGS));
        builder.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = idxDefn.getApplicableIndexingRule(NT_BASE);
        assertFalse(idxDefn.isFullTextEnabled());
        assertFalse("If fulltext disabled then nothing stored", rule.getConfig("foo").stored);

        assertTrue(rule.includePropertyType(PropertyType.LONG));
        assertFalse(rule.includePropertyType(PropertyType.STRING));

        assertTrue(rule.isIndexed("foo"));
        assertTrue(rule.isIndexed("bar"));
        assertFalse(rule.isIndexed("baz"));

        assertTrue(rule.getConfig("foo").skipTokenization("foo"));
    }

    @Test
    public void propertyDefinition() throws Exception{
        builder.child(PROP_NODE).child("foo").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar"), STRINGS));
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = idxDefn.getApplicableIndexingRule(NT_BASE);

        assertTrue(rule.isIndexed("foo"));
        assertTrue(rule.isIndexed("bar"));

        assertEquals(PropertyType.DATE, rule.getConfig("foo").getType());
    }

    @Test
    public void propertyDefinitionWithExcludes() throws Exception{
        builder.child(PROP_NODE).child("foo").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = idxDefn.getApplicableIndexingRule(NT_BASE);
        assertTrue(rule.isIndexed("foo"));
        assertTrue(rule.isIndexed("bar"));

        assertEquals(PropertyType.DATE, rule.getConfig("foo").getType());
    }

    @Test
    public void codecConfig() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        assertNotNull(defn.getCodec());
        assertEquals(oakCodec.getName(), defn.getCodec().getName());

        builder.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        defn = new IndexDefinition(root, builder.getNodeState());
        assertNull(defn.getCodec());

        Codec simple = Codec.getDefault();
        builder.setProperty(LuceneIndexConstants.CODEC_NAME, simple.getName());
        defn = new IndexDefinition(root, builder.getNodeState());
        assertNotNull(defn.getCodec());
        assertEquals(simple.getName(), defn.getCodec().getName());
    }

    @Test
    public void relativePropertyConfig() throws Exception{
        builder.child(PROP_NODE).child("foo1").child("bar").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        builder.child(PROP_NODE).child("foo2").child("bar2").child("baz").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo", "foo1/bar", "foo2/bar2/baz"), STRINGS));
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule.getConfig("foo1/bar"));
        assertEquals(PropertyType.DATE, rule.getConfig("foo1/bar").getType());
        assertEquals(PropertyType.LONG, rule.getConfig("foo2/bar2/baz").getType());
        assertTrue(rule.getConfig("foo1/bar").relative);
        assertArrayEquals(new String[]{"foo2", "bar2"}, rule.getConfig("foo2/bar2/baz").ancestors);
    }

    @Test
    public void indexRuleSanity() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0)
                .setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_BOOLEAN);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:base"))));

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);
        assertEquals(2.0f, rule1.boost, 0);

        assertTrue(rule1.isIndexed("prop1"));
        assertFalse(rule1.isIndexed("prop2"));

        PropertyDefinition pd = rule1.getConfig("prop1");
        assertEquals(3.0f, pd.boost, 0);
        assertEquals(PropertyType.BOOLEAN, pd.getType());
    }

    @Test
    public void indexRuleInheritance() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        builder.setProperty(PROP_NAME, "testIndex");
        rules.child("nt:hierarchyNode").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:base"))));
        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:hierarchyNode"))));
        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:folder"))));
    }

    @Test
    public void indexRuleMixin() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("mix:title");
        TestUtil.child(rules, "mix:title/properties/jcr:title")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:folder", "mix:title"))));
        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:folder"))));
    }

    @Test
    public void indexRuleMixinInheritance() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("mix:mimeType");
        TestUtil.child(rules, "mix:mimeType/properties/jcr:mimeType")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:folder", "mix:mimeType"))));
        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:folder"))));

        //nt:resource > mix:mimeType
        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:resource"))));
    }

    @Test
    public void indexRuleInheritanceDisabled() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        builder.setProperty(PROP_NAME, "testIndex");
        rules.child("nt:hierarchyNode")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0)
                .setProperty(LuceneIndexConstants.RULE_INHERITED, false);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:base"))));
        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:hierarchyNode"))));
        assertNull("nt:folder should not be index as rule is not inheritable",
                defn.getApplicableIndexingRule(newTree(newNode("nt:folder"))));
    }

    @Test
    public void indexRuleInheritanceOrdering() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.setProperty(OAK_CHILD_ORDER, ImmutableList.of("nt:hierarchyNode", "nt:base"),NAMES);
        rules.child("nt:hierarchyNode").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);
        rules.child("nt:base").setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertEquals(3.0, getRule(defn, "nt:base").boost, 0);
        assertEquals(2.0, getRule(defn, "nt:hierarchyNode").boost, 0);
        assertEquals(3.0, getRule(defn, "nt:query").boost, 0);
    }
    @Test
    public void indexRuleInheritanceOrdering2() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.setProperty(OAK_CHILD_ORDER, ImmutableList.of("nt:base", "nt:hierarchyNode"),NAMES);
        rules.child("nt:hierarchyNode").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);
        rules.child("nt:base").setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        //As nt:base is defined earlier it would supercede everything
        assertEquals(3.0, getRule(defn, "nt:base").boost, 0);
        assertEquals(3.0, getRule(defn, "nt:hierarchyNode").boost, 0);
        assertEquals(3.0, getRule(defn, "nt:file").boost, 0);
    }

    @Test
    public void indexRuleWithPropertyRegEx() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);
        TestUtil.child(rules, "nt:folder/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo.*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 4.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);

        assertTrue(rule1.isIndexed("prop1"));
        assertFalse(rule1.isIndexed("prop2"));
        assertTrue(rule1.isIndexed("fooProp"));

        PropertyDefinition pd = rule1.getConfig("fooProp2");
        assertEquals(4.0f, pd.boost, 0);
    }

    @Test
    public void indexRuleWithPropertyRegEx2() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, ".*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
        TestUtil.child(rules, "nt:folder/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "metadata/.*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 4.0);


        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);

        assertTrue(rule1.isIndexed("prop1"));
        assertTrue(rule1.isIndexed("prop2"));
        assertFalse(rule1.isIndexed("jcr:content/prop1"));

        assertTrue(rule1.isIndexed("metadata/foo"));
        assertFalse(rule1.isIndexed("metadata/foo/bar"));
    }

    @Test
    public void indexRuleWithPropertyOrdering() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo.*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);
        TestUtil.child(rules, "nt:folder/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, ".*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 4.0);

        rules.child("nt:folder").child(PROP_NODE).setProperty(OAK_CHILD_ORDER, ImmutableList.of("prop2", "prop1"), NAMES);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);

        assertTrue(rule1.isIndexed("prop1"));
        assertTrue(rule1.isIndexed("fooProp"));

        assertEquals(4.0f, rule1.getConfig("bazProp2").boost, 0);
        //As prop2 is ordered before prop1 its regEx is evaluated first
        //hence even with a specific regex of foo.* the defn used is from .*
        assertEquals(4.0f, rule1.getConfig("fooProp").boost, 0);

        //Order it correctly to get expected result
        rules.child("nt:folder").child(PROP_NODE).setProperty(OAK_CHILD_ORDER, ImmutableList.of("prop1", "prop2"), NAMES);
        defn = new IndexDefinition(root, builder.getNodeState());
        rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertEquals(3.0f, rule1.getConfig("fooProp").boost, 0);
    }

    @Test
    public void propertyConfigCaseInsensitive() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/foo")
                .setProperty(LuceneIndexConstants.PROP_NAME, "Foo")
                .setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        TestUtil.child(rules, "nt:folder/properties/bar")
                .setProperty(LuceneIndexConstants.PROP_NAME, "BAR")
                .setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);

        assertTrue(rule1.isIndexed("Foo"));
        assertTrue(rule1.isIndexed("foo"));
        assertTrue(rule1.isIndexed("fOO"));
        assertTrue(rule1.isIndexed("bar"));
        assertFalse(rule1.isIndexed("baz"));
    }

    @Test
    public void skipTokenization() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, ".*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.PROP_ANALYZED, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertFalse(rule.getConfig("foo").skipTokenization("foo"));
        assertTrue(rule.getConfig(JcrConstants.JCR_UUID).skipTokenization(JcrConstants.JCR_UUID));
    }

    @Test
    public void versionFullTextIsV1() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));

        //Simulate condition that index exists
        defnb.child(INDEX_DATA_CHILD_NAME);

        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(IndexFormatVersion.V1, defn.getVersion());
    }

    @Test
    public void versionDefnUpdateFulltextIsV1() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));

        //Simulate condition that index exists
        defnb.child(INDEX_DATA_CHILD_NAME);
        defnb = defnb.getNodeState().builder();
        IndexDefinition.updateDefinition(defnb);

        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(IndexFormatVersion.V1, defn.getVersion());
    }

    @Test
    public void versionPropertyIsV2() throws Exception{
        NodeBuilder defnb = newLucenePropertyIndexDefinition(builder, "test", of("foo"), "async");

        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(IndexFormatVersion.V2, defn.getVersion());
    }

    @Test
    public void versionFreshIsCurrent() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));

        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(IndexFormatVersion.getDefault(), defn.getVersion());
    }

    @Test
    public void versionFreshCompateMode() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));
        defnb.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());

        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(IndexFormatVersion.V1, defn.getVersion());
    }

    @Test
    public void formatUpdate() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING), of("foo", "Bar"), "async");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertTrue(defn.isOfOldFormat());

        NodeBuilder updated = IndexDefinition.updateDefinition(defnb.getNodeState().builder());
        IndexDefinition defn2 = new IndexDefinition(root, updated.getNodeState());

        assertFalse(defn2.isOfOldFormat());
        IndexingRule rule = defn2.getApplicableIndexingRule(newTree(newNode("nt:base")));
        assertNotNull(rule);
        assertFalse(rule.getConfig("foo").index);
        assertFalse(rule.getConfig("Bar").index);
    }

    @Test
    public void propertyRegExAndRelativeProperty() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING), of("foo"), "async");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertTrue(defn.isOfOldFormat());

        NodeBuilder updated = IndexDefinition.updateDefinition(defnb.getNodeState().builder());
        IndexDefinition defn2 = new IndexDefinition(root, updated.getNodeState());

        IndexingRule rule = defn2.getApplicableIndexingRule(newTree(newNode("nt:base")));
        assertNotNull(rule.getConfig("foo"));
        assertNull("Property regex used should not allow relative properties", rule.getConfig("foo/bar"));
    }

    @Test
    public void fulltextEnabledAndAggregate() throws Exception{
        NodeBuilder defnb = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of("foo"), "async");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertFalse(defn.isFullTextEnabled());

        NodeBuilder aggregates = defnb.child(LuceneIndexConstants.AGGREGATES);
        NodeBuilder aggFolder = aggregates.child("nt:base");
        aggFolder.child("i1").setProperty(LuceneIndexConstants.AGG_PATH, "*");

        defn = new IndexDefinition(root, defnb.getNodeState());
        assertTrue(defn.isFullTextEnabled());
    }

    @Test
    public void costConfig() throws Exception{
        NodeBuilder defnb = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of("foo"), "async");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(1.0, defn.getCostPerEntry(), 0);
        assertEquals(1.0, defn.getCostPerExecution(), 0);
        assertEquals(IndexDefinition.DEFAULT_ENTRY_COUNT, defn.getEntryCount());
        assertFalse(defn.isEntryCountDefined());

        defnb.setProperty(LuceneIndexConstants.COST_PER_ENTRY, 2.0);
        defnb.setProperty(LuceneIndexConstants.COST_PER_EXECUTION, 3.0);
        defnb.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 500);

        IndexDefinition defn2 = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(2.0, defn2.getCostPerEntry(), 0);
        assertEquals(3.0, defn2.getCostPerExecution(), 0);
        assertEquals(500, defn2.getEntryCount());
    }

    @Test
    public void fulltextCost() throws Exception{
        NodeBuilder defnb = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of("foo"), "async");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(300, defn.getFulltextEntryCount(300));
        assertEquals(IndexDefinition.DEFAULT_ENTRY_COUNT + 100,
                defn.getFulltextEntryCount(IndexDefinition.DEFAULT_ENTRY_COUNT + 100));

        //Once count is explicitly defined then it would influence the cost
        defnb.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 100);
        defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(100, defn.getFulltextEntryCount(300));
        assertEquals(50, defn.getFulltextEntryCount(50));
    }

    @Test
    public void customAnalyzer() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));

        //Set this to -1 to avoid wrapping by LimitAnalyzer
        defnb.setProperty(LuceneIndexConstants.MAX_FIELD_LENGTH, -1);
        defnb.child(ANALYZERS).child(ANL_DEFAULT)
                .child(LuceneIndexConstants.ANL_TOKENIZER)
                .setProperty(LuceneIndexConstants.ANL_NAME, "whitespace");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(TokenizerChain.class.getName(), defn.getAnalyzer().getClass().getName());
    }

    @Test
    public void customTikaConfig() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertFalse(defn.hasCustomTikaConfig());

        defnb.child(LuceneIndexConstants.TIKA)
                .child(LuceneIndexConstants.TIKA_CONFIG)
                .child(JcrConstants.JCR_CONTENT)
                .setProperty(JcrConstants.JCR_DATA, "hello".getBytes());
        defn = new IndexDefinition(root, defnb.getNodeState());
        assertTrue(defn.hasCustomTikaConfig());
    }

    @Test
    public void maxExtractLength() throws Exception{
        NodeBuilder defnb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", of(TYPENAME_STRING));
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(-IndexDefinition.DEFAULT_MAX_EXTRACT_LENGTH * IndexDefinition.DEFAULT_MAX_FIELD_LENGTH,
                defn.getMaxExtractLength());


        defnb.child(TIKA).setProperty(LuceneIndexConstants.TIKA_MAX_EXTRACT_LENGTH, 1000);

        defn = new IndexDefinition(root, defnb.getNodeState());
        assertEquals(1000, defn.getMaxExtractLength());
    }

    @Test(expected = IllegalStateException.class)
    public void nullCheckEnabledWithNtBase() throws Exception{
        builder.child(PROP_NODE).child("foo").setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
    }

    @Test(expected = IllegalStateException.class)
    public void nullCheckEnabledWithRegex() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child(TestUtil.NT_TEST);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, ".*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);
        root = registerTestNodeType(builder).getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
    }

    @Test
    public void nullCheckEnabledWithTestNode() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo")
                .setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);
        root = registerTestNodeType(builder).getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertTrue(!idxDefn.getApplicableIndexingRule(TestUtil.NT_TEST).getNullCheckEnabledProperties().isEmpty());
    }

    @Test
    public void notNullCheckEnabledWithTestNode() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo")
                .setProperty(LuceneIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true);
        root = registerTestNodeType(builder).getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertTrue(!idxDefn.getApplicableIndexingRule(TestUtil.NT_TEST).getNotNullCheckEnabledProperties().isEmpty());
    }

    @Test
    public void testSuggestEnabledOnNamedProp() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo")
                .setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true);
        root = registerTestNodeType(builder).getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertTrue(idxDefn.isSuggestEnabled());
    }

    @Test
    public void testSuggestEnabledOnRegexProp() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child(TestUtil.NT_TEST);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, ".*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true);
        root = registerTestNodeType(builder).getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertTrue(idxDefn.isSuggestEnabled());
    }

    @Test
    public void testSuggestDisabled() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        TestUtil.child(rules, "oak:TestNode/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo");
        root = registerTestNodeType(builder).getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertFalse(idxDefn.isSuggestEnabled());
    }

    @Test
    public void analyzedEnabledForBoostedField() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0)
                .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        TestUtil.child(rules, "nt:folder/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_ANALYZED, true)
                .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        TestUtil.child(rules, "nt:folder/properties/prop3")
                .setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true)
                .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);

        PropertyDefinition pd = rule1.getConfig("prop1");
        assertEquals(3.0f, pd.boost, 0);
        assertTrue("Analyzed should be assumed to be true for boosted fields", pd.analyzed);
        assertFalse(rule1.getConfig("prop3").analyzed);

        assertEquals(2, rule1.getNodeScopeAnalyzedProps().size());
    }

    @Test
    public void nodeFullTextIndexed_Regex() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, ".*")
                .setProperty(LuceneIndexConstants.PROP_ANALYZED, true)
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule);
        assertFalse(rule.isNodeFullTextIndexed());

        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        defn = new IndexDefinition(root, builder.getNodeState());
        rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertTrue(rule.isNodeFullTextIndexed());
        assertTrue(rule.indexesAllNodesOfMatchingType());
    }

    @Test
    public void nodeFullTextIndexed_Simple() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo")
                .setProperty(LuceneIndexConstants.PROP_ANALYZED, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule);
        assertFalse(rule.isNodeFullTextIndexed());

        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        defn = new IndexDefinition(root, builder.getNodeState());
        rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertTrue(rule.isNodeFullTextIndexed());
        assertTrue(rule.indexesAllNodesOfMatchingType());
    }

    @Test
    public void nodeFullTextIndexed_Aggregates() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo")
                .setProperty(LuceneIndexConstants.PROP_ANALYZED, true);

        NodeBuilder aggregates = builder.child(LuceneIndexConstants.AGGREGATES);
        NodeBuilder aggFolder = aggregates.child("nt:folder");
        aggFolder.child("i1").setProperty(LuceneIndexConstants.AGG_PATH, "*");

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule);
        assertTrue(rule.isNodeFullTextIndexed());
        assertTrue(rule.indexesAllNodesOfMatchingType());
    }

    @Test
    public void nonIndexPropShouldHaveAllOtherConfigDisabled() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        TestUtil.child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo")
                .setProperty(LuceneIndexConstants.PROP_INDEX, false)
                .setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true)
                .setProperty(LuceneIndexConstants.PROP_USE_IN_SPELLCHECK, true)
                .setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true)
                .setProperty(LuceneIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true)
                .setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true)
                .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true)
                .setProperty(LuceneIndexConstants.PROP_ORDERED, true)
                .setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexingRule rule = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule);

        PropertyDefinition pd = rule.getConfig("foo");
        //Assert that all other config is false if the index=false for any property
        assertFalse(pd.index);
        assertFalse(pd.nodeScopeIndex);
        assertFalse(pd.useInSuggest);
        assertFalse(pd.useInSpellcheck);
        assertFalse(pd.nullCheckEnabled);
        assertFalse(pd.notNullCheckEnabled);
        assertFalse(pd.stored);
        assertFalse(pd.ordered);
        assertFalse(pd.analyzed);

    }

    //TODO indexesAllNodesOfMatchingType - with nullCheckEnabled

    private static IndexingRule getRule(IndexDefinition defn, String typeName){
        return defn.getApplicableIndexingRule(newTree(newNode(typeName)));
    }

    private static Tree newTree(NodeBuilder nb){
        return TreeFactory.createReadOnlyTree(nb.getNodeState());
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static NodeBuilder newNode(String typeName, String mixins){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName);
        builder.setProperty(JcrConstants.JCR_MIXINTYPES, Collections.singleton(mixins), Type.NAMES);
        return builder;
    }

}
