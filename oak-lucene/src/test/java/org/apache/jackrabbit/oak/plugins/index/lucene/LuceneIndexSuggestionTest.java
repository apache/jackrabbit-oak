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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import javax.jcr.security.Privilege;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("ConstantConditions")
public class LuceneIndexSuggestionTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String TEST_USER_NAME = "testUserName";

    private Repository repository = null;
    private JackrabbitSession session = null;
    private Node root = null;

    @Before
    public void before() throws Exception {
        LuceneIndexProvider provider = new LuceneIndexProvider();

        Jcr jcr = new Jcr()
                .with(((QueryIndexProvider) provider))
                .with((Observer) provider)
                .with(new LuceneIndexEditorProvider());

        repository = jcr.createRepository();
        session = (JackrabbitSession)repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        root = session.getRootNode();
    }

    private void createSuggestIndex(String name, String indexedNodeType, String indexedPropertyName, boolean addFullText, boolean suggestAnalyzed)
            throws Exception {
        Node def = root.getNode(INDEX_DEFINITIONS_NAME)
                .addNode(name, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", name);
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        if (suggestAnalyzed) {
            def.setProperty("suggestAnalyzed", suggestAnalyzed);
        }


        Node propertyIdxDef = def.addNode(INDEX_RULES, JcrConstants.NT_UNSTRUCTURED)
                .addNode(indexedNodeType, JcrConstants.NT_UNSTRUCTURED)
                .addNode(LuceneIndexConstants.PROP_NODE, JcrConstants.NT_UNSTRUCTURED)
                .addNode("indexedProperty", JcrConstants.NT_UNSTRUCTURED);
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
        indexedNode.setProperty(indexPropName, indexPropValue + " 1");
        indexedNode = root.addNode("indexedNode2", queryNodeType);
        indexedNode.setProperty(indexPropName, indexPropValue + " 2");
        if (useUserSession) {
            session.getUserManager().createUser(TEST_USER_NAME, TEST_USER_NAME);
        }
        session.save();

        Session userSession = session;

        if (useUserSession) {
            AccessControlUtils.allow(indexedNode, TEST_USER_NAME, Privilege.JCR_READ);
            session.save();
            userSession = repository.login(new SimpleCredentials(TEST_USER_NAME, TEST_USER_NAME.toCharArray()));
        }

        String suggQuery = createSuggestQuery(queryNodeType, suggestQueryText);
        QueryManager queryManager = userSession.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(suggQuery, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        String value = null;
        while (rows.hasNext()) {
            Row firstRow = rows.nextRow();
            value = firstRow.getValue("suggestion").getString();
        }

        if (shouldSuggest) {
            assertNotNull("There should be some suggestion", value);
        } else {
            assertNull("There shouldn't be any suggestion", value);
        }
    }

    private String createSuggestQuery(String nodeTypeName, String suggestFor) {
        return "SELECT [rep:suggest()] as suggestion, [jcr:score] as score  FROM [" + nodeTypeName + "] WHERE suggest('" + suggestFor + "')";
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

    //OAK-3156
    @Test
    public void testSuggestQueryWithUserAccess() throws Exception {
        final String nodeType = "nt:unstructured";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "th";
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
        final boolean shouldSuggest = true;

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
}
