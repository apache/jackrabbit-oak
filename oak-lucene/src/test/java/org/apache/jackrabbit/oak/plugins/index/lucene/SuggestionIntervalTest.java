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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;

import javax.jcr.query.Query;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SuggestionIntervalTest extends AbstractQueryTest {

    private Clock clock = null;

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexProvider provider = new LuceneIndexProvider();

        ContentRepository repository = new Oak()
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(new LuceneIndexEditorProvider())
                .createContentRepository();

        clock = new Clock.Virtual();
        try {
            clock.waitUntil(System.currentTimeMillis());
        } catch (Exception e) {
            //eat exception if clock couldn't wait... that was just courteous anyway!
        }
        LuceneIndexEditorContext.setClock(clock);

        return repository;
    }

    @After
    public void after() throws Exception {
        LuceneIndexEditorContext.setClock(Clock.SIMPLE);
    }

    private Tree createSuggestIndex(String indexedNodeType)
            throws Exception {
        String indexName = "lucene-suggest";
        Tree def = root.getTree("/" + INDEX_DEFINITIONS_NAME)
                .addChild(indexName);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", indexName);
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Tree propertyIdxDef = def.addChild(INDEX_RULES)
                .addChild(indexedNodeType)
                .addChild(LuceneIndexConstants.PROP_NODE)
                .addChild("indexedProperty");
        propertyIdxDef.setProperty("propertyIndex", true);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty("useInSuggest", true);
        propertyIdxDef.setProperty("name", LuceneIndexConstants.PROPDEF_PROP_NODE_NAME);

        return def;
    }

    Set<String> getSuggestions(String nodeType, String suggestFor) throws Exception {
        Set<String> ret = Sets.newHashSet();

        String suggQuery = createSuggestQuery(nodeType, suggestFor);
        QueryEngine qe = root.getQueryEngine();
        Result result = qe.executeQuery(suggQuery, Query.JCR_SQL2, null, null);

        for (ResultRow row : result.getRows()) {
            ret.add(row.getValue("suggestion").toString());
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

        //initial data
        createSuggestIndex(nodeType);
        root.commit();

        //wait for documented time before suggestions are refreshed
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(10));
        clock.getTime();//get one more tick

        //add a node... this should kick in a suggestions udpate too as enough time has passed
        root.getTree("/").addChild("indexedNode")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
        root.commit();

        Set<String> suggestions = getSuggestions(nodeType, "indexedn");

        assertEquals(1, suggestions.size());
        assertEquals("indexedNode", suggestions.iterator().next());
    }

    //OAK-4066
    @Test
    public void suggestionUpdateWithoutIndexChange() throws Exception {
        final String nodeType = "nt:unstructured";

        createSuggestIndex(nodeType);
        root.commit();

        long currTime = clock.getTime();
        long toTime = currTime + TimeUnit.MINUTES.toMillis(IndexDefinition.DEFAULT_SUGGESTER_UPDATE_FREQUENCY_MINUTES);

        //add a node that get part in the index
        root.getTree("/").addChild("indexedNode")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
        root.commit();

        //wait for suggestions refresh time
        clock.waitUntil(toTime);
        clock.getTime();//get one more tick

        //push a change which should not make any change in the index but yet should help update suggestions
        root.getTree("/").addChild("some-non-index-change")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:Unstructured", Type.NAME);
        root.commit();

        Set<String> suggestions = getSuggestions(nodeType, "indexedn");

        assertEquals(1, suggestions.size());
        assertEquals("indexedNode", suggestions.iterator().next());
    }

    //OAK-4066
    @Test
    public void avoidRedundantSuggestionBuildOnNonIndexUpdate() throws Exception {
        final String nodeType = "nt:unstructured";

        //initial content which also updates suggestions with "indexedNode"
        Tree indexDef = createSuggestIndex(nodeType);
        root.getTree("/").addChild("indexedNode")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType);
        root.commit();
        String suggUpdateTime1 = getSuggestionLastUpdated(indexDef);

        assertNotNull("Suggestions update timestamp couldn't be read", suggUpdateTime1);

        long currTime = clock.getTime();
        long toTime = currTime + TimeUnit.MINUTES.toMillis(IndexDefinition.DEFAULT_SUGGESTER_UPDATE_FREQUENCY_MINUTES);

        //wait for suggestions refresh time
        clock.waitUntil(toTime);
        clock.getTime();//get one more tick

        //push a change which should not make any change in the index but would kick in suggestion logic
        //YET, suggestion logic shouldn't do any change
        root.getTree("/").addChild("some-non-index-change")
                .setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:Unstructured");
        root.commit();

        String suggUpdateTime2 = getSuggestionLastUpdated(indexDef);

        assertEquals("Suggestions shouldn't rebuild un-necessarily. Update times are different",
                suggUpdateTime1, suggUpdateTime2);
    }

    private String getSuggestionLastUpdated(Tree indexDef) {
        Tree suggStat = root.getTree(indexDef.getPath() + "/" + LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME);
        if (!suggStat.hasProperty("lastUpdated")) {
            return null;
        }
        return suggStat.getProperty("lastUpdated").toString();
    }
}
