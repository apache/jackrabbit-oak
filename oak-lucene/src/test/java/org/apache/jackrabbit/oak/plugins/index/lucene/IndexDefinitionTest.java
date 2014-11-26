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

import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.codecs.Codec;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_LONG;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
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
    }

    @Test
    public void indexRuleSanity() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);
        child(rules, "nt:folder/properties/prop1")
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

        //TODO Inheritance and mixin
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
        child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);
        child(rules, "nt:folder/properties/prop2")
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
    public void indexRuleWithPropertyOrdering() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo.*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);
        child(rules, "nt:folder/properties/prop2")
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
    public void skipTokenization() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/prop2")
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
                "lucene", of(TYPENAME_STRING), of("foo"), "async");
        IndexDefinition defn = new IndexDefinition(root, defnb.getNodeState());
        assertTrue(defn.isOfOldFormat());

        NodeBuilder updated = IndexDefinition.updateDefinition(defnb.getNodeState().builder());
        IndexDefinition defn2 = new IndexDefinition(root, updated.getNodeState());

        assertFalse(defn2.isOfOldFormat());
        IndexingRule rule = defn2.getApplicableIndexingRule(newTree(newNode("nt:base")));
        assertNotNull(rule);
        assertFalse(rule.getConfig("foo").index);
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


    private static IndexingRule getRule(IndexDefinition defn, String typeName){
        return defn.getApplicableIndexingRule(newTree(newNode(typeName)));
    }

    private static Tree newTree(NodeBuilder nb){
        return new ImmutableTree(nb.getNodeState());
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }
}
