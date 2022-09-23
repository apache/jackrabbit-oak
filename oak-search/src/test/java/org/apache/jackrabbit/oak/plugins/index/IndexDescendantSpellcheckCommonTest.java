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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_USE_IN_SPELLCHECK;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;

public abstract class IndexDescendantSpellcheckCommonTest extends AbstractJcrTest {
    protected TestRepository repositoryOptionsUtil;
    protected Node indexNode;
    protected IndexOptions indexOptions;

    protected JackrabbitSession session = null;
    protected Node root = null;

    @Override
    protected void initialize() {
        session = (JackrabbitSession) adminSession;
        try {
            root = adminSession.getRootNode();
            createContent();
            adminSession.save();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected void createContent() throws Exception {
        /*
        Make content with following structure:
        * sugg-idx is a simple index to suggest node names on type "oak:Unstructured"
        * test[1-6] nodes would be "oak:Unstructured".
        * all other nodes, unless required are "nt:unstructured"
        * Index on one sub-tree is on nt:base so that we can do sub-tree suggestion test with unambiguous indices
        */

        //  /oak:index/spellcheck-idx, /test1
        createSuggestIndex(root, "spellcheck-idx", NT_OAK_UNSTRUCTURED, PROPDEF_PROP_NODE_NAME);
        root.addNode("test1", NT_OAK_UNSTRUCTURED);

        /*
            /content1
                /tree1
                    /test2
                /tree2
                    /test3
         */
        Node content1 = root.addNode("content1", NT_UNSTRUCTURED);
        Node tree1 = content1.addNode("tree1", NT_UNSTRUCTURED);
        tree1.addNode("test2", NT_OAK_UNSTRUCTURED);
        Node tree2 = content1.addNode("tree2", NT_UNSTRUCTURED);
        tree2.addNode("test3", NT_OAK_UNSTRUCTURED);

        //  /content2/oak:index/spellcheck-idx, /content2/test4
        Node content2 = root.addNode("content2", NT_UNSTRUCTURED);
        createSuggestIndex(content2, "spellcheck-idx", NT_OAK_UNSTRUCTURED, PROPDEF_PROP_NODE_NAME);
        content2.addNode("test4", NT_OAK_UNSTRUCTURED);

        //  /content3/oak:index/spellcheck-idx, /content3/test5, /content3/sC/test6
        Node content3 = root.addNode("content3", NT_UNSTRUCTURED);
        createSuggestIndex(content3, "spellcheck-idx", NT_BASE, PROPDEF_PROP_NODE_NAME);
        content3.addNode("test5", NT_OAK_UNSTRUCTURED);
        Node subChild = content3.addNode("sC", NT_UNSTRUCTURED);
        subChild.addNode("test6", NT_OAK_UNSTRUCTURED);
    }

    private void createSuggestIndex(Node rootNode, String name, String indexedNodeType, String indexedPropertyName)
            throws Exception {
        Node def = JcrUtils.getOrAddNode(rootNode, INDEX_DEFINITIONS_NAME)
                .addNode(name, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", name);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        def.setProperty(EVALUATE_PATH_RESTRICTION, true);

        Node propertyIdxDef = def.addNode(INDEX_RULES, JcrConstants.NT_UNSTRUCTURED)
                .addNode(indexedNodeType, JcrConstants.NT_UNSTRUCTURED)
                .addNode(FulltextIndexConstants.PROP_NODE, JcrConstants.NT_UNSTRUCTURED)
                .addNode("indexedProperty", JcrConstants.NT_UNSTRUCTURED);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty(PROP_USE_IN_SPELLCHECK, true);
        propertyIdxDef.setProperty("name", indexedPropertyName);
    }

    private String createSpellcheckQuery(String nodeTypeName, String suggestFor, String rootPath) {
        return "SELECT [rep:spellcheck()] as spellcheck, [jcr:score] as score  FROM [" + nodeTypeName + "]" +
                " WHERE spellcheck('" + suggestFor + "')" +
                (rootPath == null ? "" : " AND ISDESCENDANTNODE([" + rootPath + "])");

    }

    private Set<String> getSpellchecks(String query) throws Exception {
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(query, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        Set<String> suggestions = newHashSet();
        while (rows.hasNext()) {
            suggestions.add(rows.nextRow().getValue("spellcheck").getString());
        }

        return suggestions;
    }

    private void validateSpellchecks(String query, Set<String> expected) {
        assertEventually(() -> {
            Set<String> suggestions;
            try {
                suggestions = getSpellchecks(query);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertEquals("Incorrect suggestions", expected, suggestions);
        });
    }

    @Test
    public void noDescendantSuggestsAll() {
        validateSpellchecks(
                createSpellcheckQuery(NT_OAK_UNSTRUCTURED, "taste", null),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

    //OAK-3994
    @Test
    public void rootIndexWithDescendantConstraint() {
        validateSpellchecks(
                createSpellcheckQuery(NT_OAK_UNSTRUCTURED, "taste", "/content1"),
                newHashSet("test2", "test3"));
    }

    @Test
    public void descendantSuggestionRequirePathRestrictionIndex() throws Exception {
        Node rootIndexDef = root.getNode("oak:index/spellcheck-idx");
        rootIndexDef.getProperty(EVALUATE_PATH_RESTRICTION).remove();
        rootIndexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        session.save();

        //Without path restriction indexing, descendant clause shouldn't be respected
        validateSpellchecks(
                createSpellcheckQuery(NT_OAK_UNSTRUCTURED, "taste", "/content1"),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

    //OAK-3994
    @Test
    public void unambiguousSubtreeIndexWithDescendantConstraint() {
        validateSpellchecks(
                createSpellcheckQuery(NT_BASE, "taste", "/content3"),
                newHashSet("test5", "test6"));
    }

    //OAK-3994
    @Test
    public void unambiguousSubtreeIndexWithSubDescendantConstraint() {
        validateSpellchecks(
                createSpellcheckQuery(NT_BASE, "taste", "/content3/sC"),
                newHashSet("test6"));
    }

    private static void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, 3000 * 3);
    }
}
