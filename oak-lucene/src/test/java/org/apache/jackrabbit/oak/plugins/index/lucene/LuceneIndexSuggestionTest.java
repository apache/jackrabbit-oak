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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("ConstantConditions")
public class LuceneIndexSuggestionTest extends AbstractQueryTest {

    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private LuceneIndexEditorProvider editorProvider;

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        editorProvider = new LuceneIndexEditorProvider(createIndexCopier());
        LuceneIndexProvider provider = new LuceneIndexProvider();
        return new Oak()
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void shutdownExecutor(){
        executorService.shutdown();
    }

    private Tree createSuggestIndex(String name, String indexedNodeType, String indexedPropertyName, boolean addFullText)
            throws CommitFailedException {
        Tree index = root.getTree("/");

        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", name);
        def.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Tree indexRules = def.addChild("indexRules");
        indexRules.setProperty(JcrConstants.JCR_PRIMARYTYPE,
        		JcrConstants.NT_UNSTRUCTURED, Type.NAME);

        Tree nodeType = indexRules.addChild(indexedNodeType);
        nodeType.setProperty(JcrConstants.JCR_PRIMARYTYPE,
        		JcrConstants.NT_UNSTRUCTURED, Type.NAME);

        Tree properties = nodeType.addChild(LuceneIndexConstants.PROP_NODE);
        properties.setProperty(JcrConstants.JCR_PRIMARYTYPE,
        		JcrConstants.NT_UNSTRUCTURED, Type.NAME);

        Tree propertyIdxDef = properties.addChild("indexedProperty");
        propertyIdxDef.setProperty(JcrConstants.JCR_PRIMARYTYPE,
        		JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        propertyIdxDef.setProperty("propertyIndex", true);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty("useInSuggest", true);
        if (addFullText) {
            propertyIdxDef.setProperty("nodeScopeIndex", true);
        }
        propertyIdxDef.setProperty("name", indexedPropertyName);

        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    /**
     * Utility method to check suggestion over {@code nodeType} when the index definition also created for
     * the same type
     */
    private void checkSuggestions(final String nodeType,
                                  final String indexPropName, final String indexPropValue,
                                  final String suggestQueryText, final boolean shouldSuggest)
            throws CommitFailedException, ParseException {
        checkSuggestions(nodeType, nodeType, indexPropName, false, indexPropValue, suggestQueryText, shouldSuggest);
    }

    /**
     * Utility method to check suggestion over {@code queryNodeType} when the index definition is created for
     * {@code indexNodeType}
     */
    private void checkSuggestions(final String indexNodeType, final String queryNodeType,
                                  final String indexPropName, final boolean addFullText, final String indexPropValue,
                                  final String suggestQueryText, final boolean shouldSuggest)
            throws CommitFailedException, ParseException {
        createSuggestIndex("lucene-suggest", indexNodeType, indexPropName, addFullText);

        Tree test = root.getTree("/").addChild("indexedNode");
        test.setProperty(indexPropName, indexPropValue);
        root.commit();

        String suggQuery = createSuggestQuery(queryNodeType, suggestQueryText);
        Result result = executeQuery(suggQuery, SQL2, QueryEngine.NO_BINDINGS);
        Iterator<? extends ResultRow> rows = result.getRows().iterator();

        ResultRow firstRow = rows.next();
        String value = firstRow.getValue("suggestion").getValue(Type.STRING);
        Suggestion suggestion = Suggestion.fromString(value);

        if (shouldSuggest) {
            assertNotNull("There should be some suggestion", suggestion.getSuggestion());
        } else {
            assertNull("There shouldn't be any suggestion", suggestion.getSuggestion());
        }
    }

    private String createSuggestQuery(String nodeTypeName, String suggestFor) {
        return "SELECT [rep:suggest()] as suggestion  FROM [" + nodeTypeName + "] WHERE suggest('" + suggestFor + "')";
    }

    static class Suggestion {
        private final long weight;
        private final String suggestion;
        Suggestion(String suggestion, long weight) {
            this.suggestion = suggestion;
            this.weight = weight;
        }

        long getWeight() {
            return weight;
        }

        String getSuggestion() {
            return suggestion;
        }

        static Suggestion fromString(String suggestionResultStr) {
            if (suggestionResultStr==null || "".equals(suggestionResultStr) || "[]".equals(suggestionResultStr)) {
                return new Suggestion(null, -1);
            }

            String [] parts = suggestionResultStr.split(",weight=", 2);
            int endWeightIdx = parts[1].lastIndexOf("}");
            long weight = Long.parseLong(parts[1].substring(0, endWeightIdx));
            String suggestion = parts[0].split("=", 2)[1];

            return new Suggestion(suggestion, weight);
        }
    }

    //OAK-3157
    @Test
    public void testSuggestQuery() throws Exception {
        final String nodeType = "nt:base";
        final String indexPropName = "description";
        final String indexPropValue = "this is just a sample text which should get some response in suggestions";
        final String suggestQueryText = "th";
        final boolean shouldSuggest = true;

        checkSuggestions(nodeType, indexPropName, indexPropValue, suggestQueryText, shouldSuggest);
    }
}
