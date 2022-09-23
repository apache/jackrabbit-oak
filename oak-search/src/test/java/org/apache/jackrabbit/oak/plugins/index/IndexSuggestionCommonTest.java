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
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import javax.jcr.security.Privilege;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class IndexSuggestionCommonTest extends AbstractJcrTest {
    protected TestRepository repositoryOptionsUtil;
    protected Node indexNode;
    protected IndexOptions indexOptions;

    private JackrabbitSession session = null;
    private Node root = null;

    @Before
    public void settingup() throws RepositoryException {
        session = (JackrabbitSession) adminSession;
        root = session.getRootNode();
    }

    private Node createSuggestIndex(String name, String indexedNodeType, String indexedPropertyName)
            throws Exception {
        return createSuggestIndex(name, indexedNodeType, indexedPropertyName, false, false);
    }

    private Node createSuggestIndex(String name, String indexedNodeType, String indexedPropertyName, boolean addFullText, boolean suggestAnalyzed)
            throws Exception {
        Node def = root.getNode(INDEX_DEFINITIONS_NAME)
                .addNode(name, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", name);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        if (suggestAnalyzed) {
            def.addNode(FulltextIndexConstants.SUGGESTION_CONFIG).setProperty("suggestAnalyzed", suggestAnalyzed);
        }
        addPropertyDefinition(def, indexedNodeType, indexedPropertyName, addFullText);
        return def;
    }

    private Node getOrCreate(Node parent, String childName, String type) throws RepositoryException {
        if (parent.hasNode(childName)) {
            return parent.getNode(childName);
        }
        return parent.addNode(childName, type);
    }

    private void addPropertyDefinition(Node indexDefNode, String indexedNodeType, String indexedPropertyName, boolean addFullText) throws Exception {
        Node rulesNode = getOrCreate(indexDefNode, INDEX_RULES, JcrConstants.NT_UNSTRUCTURED);
        Node nodeTypeNode = getOrCreate(rulesNode, indexedNodeType, JcrConstants.NT_UNSTRUCTURED);
        Node propertiesNode = getOrCreate(nodeTypeNode, FulltextIndexConstants.PROP_NODE, JcrConstants.NT_UNSTRUCTURED);
        Node propertyIdxDef = getOrCreate(propertiesNode, Text.escapeIllegalJcrChars(indexedPropertyName),
                JcrConstants.NT_UNSTRUCTURED);
        propertyIdxDef.setProperty("propertyIndex", true);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty("useInSuggest", true);
        if (addFullText) {
            propertyIdxDef.setProperty("nodeScopeIndex", true);
        }
        propertyIdxDef.setProperty("name", indexedPropertyName);
    }

    /**
     * Utility method to check suggestion over {@code nodeType} when the index definition also created for
     * the same type
     */
    private void checkSuggestions(final String nodeType,
                                  final String indexPropName, final String indexPropValue,
                                  final boolean addFullText, final boolean useUserSession,
                                  final String suggestQueryText, final boolean shouldSuggest, final boolean suggestAnalyzed)
            throws Exception {
        checkSuggestions(nodeType, nodeType,
                indexPropName, indexPropValue,
                addFullText, useUserSession,
                suggestQueryText, shouldSuggest, suggestAnalyzed);
    }

    /**
     * Utility method to check suggestion over {@code queryNodeType} when the index definition is created for
     * {@code indexNodeType}
     */
    private void checkSuggestions(final String indexNodeType, final String queryNodeType,
                                  final String indexPropName, final String indexPropValue,
                                  final boolean addFullText, final boolean useUserSession,
                                  final String suggestQueryText, final boolean shouldSuggest,
                                  final boolean suggestAnalyzed)
            throws Exception {
        createSuggestIndex("lucene-suggest", indexNodeType, indexPropName, addFullText, suggestAnalyzed);

        Node indexedNode = root.addNode("indexedNode1", queryNodeType);

        if (indexPropValue != null) {
            indexedNode.setProperty(indexPropName, indexPropValue + " 1");
            indexedNode = root.addNode("indexedNode2", queryNodeType);
            indexedNode.setProperty(indexPropName, indexPropValue + " 2");
        }

        session.save();

        Session userSession = session;

        if (useUserSession) {
            allow(indexedNode);
            session.save();
            userSession = anonymousSession;//repository.login(new SimpleCredentials(TEST_USER_NAME, TEST_USER_NAME.toCharArray()));
        }

        String suggQuery = createSuggestQuery(queryNodeType, suggestQueryText);
        QueryManager queryManager = userSession.getWorkspace().getQueryManager();
        if (shouldSuggest) {
            assertEventually(() -> {
                try {
                    assertTrue("There should be some suggestion", getAllResults(queryManager, suggQuery).size() > 0);
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            });

        } else {
            assertEventually(() -> {
                try {
                    assertEquals("There shouldn't be any suggestion", 0, getAllResults(queryManager, suggQuery).size());
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            });

        }
        userSession.logout();
    }

    private List<String> getAllResults(QueryManager queryManager, String suggQuery) throws RepositoryException {

        QueryResult result = queryManager.createQuery(suggQuery, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        List<String> value = new ArrayList<>();
        while (rows.hasNext()) {
            Row firstRow = rows.nextRow();
            value.add(firstRow.getValue("suggestion").getString());
        }
        return value;
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    private String createSuggestQuery(String nodeTypeName, String suggestFor) {
        return "SELECT [rep:suggest()] as suggestion, [jcr:score] as score  FROM [" + nodeTypeName + "] WHERE suggest('" + suggestFor + "')";
    }

    @Test
    public void suggestNodeName() throws Exception {
        final String nodeType = "nt:unstructured";

        createSuggestIndex("lucene-suggest", nodeType, FulltextIndexConstants.PROPDEF_PROP_NODE_NAME);

        root.addNode("indexedNode", nodeType);
        adminSession.save();

        String suggQuery = createSuggestQuery(nodeType, "indexedn");
        QueryManager queryManager = adminSession.getWorkspace().getQueryManager();
        assertEventually(() -> {
            try {
                List<String> results = getAllResults(queryManager, suggQuery);
                assertTrue("Node name should be suggested", results.size() == 1 && "indexedNode".equals(results.get(0)));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    //OAK-3157
    @Test
    public void testSuggestQuery() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "th";
        final boolean shouldSuggest = true;

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                false, false,
                suggestQueryText, shouldSuggest, false);
    }

    //OAK-4126
    @Test
    public void testSuggestQuerySpecialChars() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "DD~@#$%^&*()_+{}\":?><`1234567890-=[]";
        final String suggestQueryText = "dd";
        final boolean shouldSuggest = true;

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                false, false,
                suggestQueryText, shouldSuggest, false);
    }

    @Test
    public void avoidInfiniteSuggestions() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String higherRankPropValue = "DD DD DD DD";
        final String exceptionThrowingPropValue = "DD~@#$%^&*()_+{}\":?><`1234567890-=[]";
        final String suggestQueryText = "dd";

        createSuggestIndex("lucene-suggest", nodeType, indexPropName);

        root.addNode("higherRankNode", nodeType).setProperty(indexPropName, higherRankPropValue);
        root.addNode("exceptionThrowingNode", nodeType).setProperty(indexPropName, exceptionThrowingPropValue);
        adminSession.save();

        String suggQuery = createSuggestQuery(nodeType, suggestQueryText);
        QueryManager queryManager = adminSession.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(suggQuery, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        int count = 0;
        while (count < 3 && rows.hasNext()) {
            count++;
            rows.nextRow();
        }

        assertTrue("There must not be more than 2 suggestions", count <= 2);
    }

    //OAK-3156
    @Test
    public void testSuggestQueryWithUserAccess() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "this is jus";
        final boolean shouldSuggest = true;

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                false, true,
                suggestQueryText, shouldSuggest, false);
    }

    //OAK-3156
    @Test
    public void testSuggestQueryFromMoreGeneralNodeType() throws Exception {
        final String indexNodeType = "nt:base";
        final String queryNodeType = "oak:Unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "th";
        final boolean shouldSuggest = false;

        checkSuggestions(indexNodeType, queryNodeType,
                indexPropName, indexPropValue,
                true, false,
                suggestQueryText, shouldSuggest, false);
    }

    //OAK-3156
    @Test
    public void testSuggestQueryOnNonNtBase() throws Exception {
        final String nodeType = "oak:Unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "th";
        final boolean shouldSuggest = true;

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                true, false,
                suggestQueryText, shouldSuggest, false);
    }

    //OAK-3509
    @Test
    public void testMultipleSuggestions() throws Exception {
        final String nodeType = "oak:Unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "th";
        final boolean shouldSuggest = true;

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                true, false,
                suggestQueryText, shouldSuggest, false);
    }

    //OAK-3407
    @Test
    public void testSuggestQueryAnalyzed() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "sa";

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                true, true,
                suggestQueryText, true, true);
    }

    //OAK-3149
    @Test
    public void testSuggestQueryInfix() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "sa";

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                true, true,
                suggestQueryText, true, false);
    }

    //OAK-4067
    @Test
    public void emptySuggestWithNothingIndexed() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = null;
        final String suggestQueryText = null;

        checkSuggestions(nodeType,
                indexPropName, indexPropValue,
                true, true,
                suggestQueryText, false, false);
    }

    @Test
    public void testMultipleSuggestionProperties() throws Exception {
        String nodeType = JcrConstants.NT_UNSTRUCTURED;
        String suggestProp1 = "shortDes";
        String suggestProp2 = "longDes";

        Node indexDefNode = createSuggestIndex("index-suggest", nodeType, suggestProp1);
        addPropertyDefinition(indexDefNode, nodeType, suggestProp2, false);

        Node indexedNode = root.addNode("indexedNode", nodeType);
        indexedNode.setProperty(suggestProp1, "car there");
        indexedNode.setProperty(suggestProp2, "bike there");
        indexedNode = root.addNode("indexedNode2", nodeType);
        indexedNode.setProperty(suggestProp1, "bike here");
        indexedNode.setProperty(suggestProp2, "car here");

        session.save();

        QueryManager queryManager = session.getWorkspace().getQueryManager();
        assertEventually(() -> {
            try {
                List<String> results = getAllResults(queryManager, createSuggestQuery(nodeType, "car"));
                assertEquals("There should be some suggestion",2, results.size());
                assertTrue(results.stream().allMatch(r -> r.startsWith("car")));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
        assertEventually(() -> {
            try {
                List<String> results = getAllResults(queryManager, createSuggestQuery(nodeType, "bike"));
                assertEquals("There should be some suggestion",2, results.size());
                assertTrue(results.stream().allMatch(r -> r.startsWith("bike")));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testDuplicateSuggestionProperties() throws Exception {
        String nodeType = JcrConstants.NT_UNSTRUCTURED;
        String suggestProp1 = "shortDes";
        String suggestProp2 = "longDes";

        Node indexDefNode = createSuggestIndex("index-suggest", nodeType, suggestProp1);
        addPropertyDefinition(indexDefNode, nodeType, suggestProp2, false);

        Node indexedNode = root.addNode("indexedNode", nodeType);
        indexedNode.setProperty(suggestProp1, "car here");
        indexedNode.setProperty(suggestProp2, "car here");
        indexedNode = root.addNode("indexedNode2", nodeType);
        indexedNode.setProperty(suggestProp1, "car there");
        indexedNode.setProperty(suggestProp2, "car there");

        session.save();

        String suggQuery = createSuggestQuery(nodeType, "car");
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        assertEventually(() -> {
            try {
                assertEquals("There should be some suggestion",2, getAllResults(queryManager, suggQuery).size());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testDuplicateSuggestions() throws Exception {
        String nodeType = JcrConstants.NT_UNSTRUCTURED;
        String suggestProp1 = "des";

        createSuggestIndex("index-suggest", nodeType, suggestProp1);

        Node indexedNode = root.addNode("indexedNode", nodeType);
        indexedNode.setProperty(suggestProp1, "Hello World");
        indexedNode = root.addNode("indexedNode2", nodeType);
        indexedNode.setProperty(suggestProp1, "Hello World");
        indexedNode = root.addNode("indexedNode3", nodeType);
        indexedNode.setProperty(suggestProp1, "Hello Oak");

        session.save();

        String suggQuery = createSuggestQuery(nodeType, "Hello");
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        assertEventually(() -> {
            try {
                List<String> resultSet = getAllResults(queryManager, suggQuery);
                assertEquals("There should not be any duplicate suggestions.",
                        2, resultSet.size());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, 3000 * 3);
    }
}
