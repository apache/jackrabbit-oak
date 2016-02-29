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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;
import static org.junit.Assert.assertEquals;

public class SuggestionIntervalTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Repository repository = null;
    private JackrabbitSession session = null;
    private Node root = null;

    private Clock clock = null;

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

        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        LuceneIndexEditorContext.setClock(clock);
    }

    @After
    public void after() {
        LuceneIndexEditorContext.setClock(Clock.SIMPLE);

        session.logout();
        shutdown(repository);
    }

    private void createSuggestIndex(String indexedNodeType)
            throws Exception {
        String indexName = "lucene-suggest";
        Node def = root.getNode(INDEX_DEFINITIONS_NAME)
                .addNode(indexName, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", indexName);
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Node propertyIdxDef = def.addNode(INDEX_RULES, JcrConstants.NT_UNSTRUCTURED)
                .addNode(indexedNodeType, JcrConstants.NT_UNSTRUCTURED)
                .addNode(LuceneIndexConstants.PROP_NODE, JcrConstants.NT_UNSTRUCTURED)
                .addNode("indexedProperty", JcrConstants.NT_UNSTRUCTURED);
        propertyIdxDef.setProperty("propertyIndex", true);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty("useInSuggest", true);
        propertyIdxDef.setProperty("name", LuceneIndexConstants.PROPDEF_PROP_NODE_NAME);
    }

    Set<String> getSuggestions(String nodeType, String suggestFor) throws Exception {
        Set<String> ret = Sets.newHashSet();

        String suggQuery = createSuggestQuery(nodeType, suggestFor);
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(suggQuery, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        while (rows.hasNext()) {
            Row firstRow = rows.nextRow();
            ret.add(firstRow.getValue("suggestion").getString());
        }

        return ret;
    }

    private String createSuggestQuery(String nodeType, String suggestFor) {
        return "SELECT [rep:suggest()] as suggestion, [jcr:score] as score  FROM [" + nodeType + "] WHERE suggest('" + suggestFor + "')";
    }

    //OAK-4068
    @Test
    public void defaultSuggestInterval() throws Exception {
        final String nodeType = "nt:unstructured";

        createSuggestIndex(nodeType);
        session.save();

        //wait for documented time before suggestions are refreshed
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(10));
        clock.getTime();//get one more tick

        root.addNode("indexedNode", nodeType);
        session.save();

        Set<String> suggestions = getSuggestions(nodeType, "indexedn");

        assertEquals(1, suggestions.size());
        assertEquals("indexedNode", suggestions.iterator().next());
    }
}
