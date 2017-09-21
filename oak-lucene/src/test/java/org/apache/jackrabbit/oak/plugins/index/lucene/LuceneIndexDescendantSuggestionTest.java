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
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;
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
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_USE_IN_SUGGEST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LuceneIndexDescendantSuggestionTest {
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
        session = (JackrabbitSession) repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        root = session.getRootNode();

        createContent();
        session.save();
    }

    @After
    public void after() {
        session.logout();
        shutdown(repository);
    }

    private void createContent() throws Exception {
        /*
        Make content with following structure:
        * sugg-idx is a simple index to suggest node names on type "oak:Unstructured"
        * test[1-6] nodes would be "oak:Unstructured".
        * all other nodes, unless required are "nt:unstructured"
        * Index on one sub-tree is on nt:base so that we can do sub-tree suggestion test with unambiguous indices
        */

        //  /oak:index/sugg-idx, /test1
        createSuggestIndex(root, "sugg-idx", NT_OAK_UNSTRUCTURED, PROPDEF_PROP_NODE_NAME);
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

        //  /content2/oak:index/sugg-idx, /content2/test4
        Node content2 = root.addNode("content2", NT_UNSTRUCTURED);
        createSuggestIndex(content2, "sugg-idx", NT_OAK_UNSTRUCTURED, PROPDEF_PROP_NODE_NAME);
        content2.addNode("test4", NT_OAK_UNSTRUCTURED);

        //  /content3/oak:index/sugg-idx, /content3/test5, /content3/sC/test6
        Node content3 = root.addNode("content3", NT_UNSTRUCTURED);
        createSuggestIndex(content3, "sugg-idx", NT_BASE, PROPDEF_PROP_NODE_NAME);
        content3.addNode("test5", NT_OAK_UNSTRUCTURED);
        Node subChild = content3.addNode("sC", NT_UNSTRUCTURED);
        subChild.addNode("test6", NT_OAK_UNSTRUCTURED);
    }

    private void createSuggestIndex(Node rootNode, String name, String indexedNodeType, String indexedPropertyName)
            throws Exception {
        Node def = JcrUtils.getOrAddNode(rootNode, INDEX_DEFINITIONS_NAME)
                .addNode(name, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", name);
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        def.setProperty(EVALUATE_PATH_RESTRICTION, true);

        Node propertyIdxDef = def.addNode(INDEX_RULES, JcrConstants.NT_UNSTRUCTURED)
                .addNode(indexedNodeType, JcrConstants.NT_UNSTRUCTURED)
                .addNode(LuceneIndexConstants.PROP_NODE, JcrConstants.NT_UNSTRUCTURED)
                .addNode("indexedProperty", JcrConstants.NT_UNSTRUCTURED);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty(PROP_USE_IN_SUGGEST, true);
        propertyIdxDef.setProperty("name", indexedPropertyName);
    }

    private String createSuggestQuery(String nodeTypeName, String suggestFor, String rootPath) {
        return "SELECT [rep:suggest()] as suggestion, [jcr:score] as score  FROM [" + nodeTypeName + "]" +
                " WHERE suggest('" + suggestFor + "')" +
                (rootPath==null?"":" AND ISDESCENDANTNODE([" + rootPath + "])");

    }

    private Set<String> getSuggestions(String query) throws Exception {
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(query, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        Set<String> suggestions = newHashSet();
        while (rows.hasNext()) {
            suggestions.add(rows.nextRow().getValue("suggestion").getString());
        }

        return suggestions;
    }

    private void validateSuggestions(String query, Set<String> expected) throws Exception {
        Set<String> suggestions = getSuggestions(query);
        assertEquals("Incorrect suggestions", expected, suggestions);
    }

    //Don't break suggestions :)
    @Test
    public void noDescendantSuggestsAll() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", null),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

    //OAK-3994
    @Test
    public void rootIndexWithDescendantConstraint() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1"),
                newHashSet("test2", "test3"));
    }

    //OAK-3994
    @Test
    public void descendantSuggestionRequirePathRestrictionIndex() throws Exception {
        Node rootIndexDef = root.getNode("oak:index/sugg-idx");
        rootIndexDef.getProperty(EVALUATE_PATH_RESTRICTION).remove();
        rootIndexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        session.save();

        //Without path restriction indexing, descendant clause shouldn't be respected
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1"),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

    @Ignore("OAK-3992")
    @Test
    public void ambiguousSubtreeIndexWithDescendantConstraint() throws Exception {
        String query = createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content2");
        String explainQuery = "EXPLAIN " + createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content2");

        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(explainQuery, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        String explanation = rows.nextRow().toString();
        assertTrue("Subtree index should get picked", explanation.contains("lucene:sugg-idx(/content2/oak:index/sugg-idx)"));

        validateSuggestions(query, newHashSet("test4"));
    }

    //OAK-3994
    @Test
    public void unambiguousSubtreeIndexWithDescendantConstraint() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_BASE, "te", "/content3"),
                newHashSet("test5", "test6"));
    }

    //OAK-3994
    @Test
    public void unambiguousSubtreeIndexWithSubDescendantConstraint() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_BASE, "te", "/content3/sC"),
                newHashSet("test6"));
    }

    @Ignore("OAK-3993")
    @Test
    public void unionOnTwoDescendants() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1") +
                        " UNION " +
                        createSuggestQuery(NT_BASE, "te", "/content3"),
                newHashSet("test2", "test3", "test5"));
    }
}
