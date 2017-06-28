/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

import java.util.Properties;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class LuceneOakRepositoryStub extends OakSegmentTarRepositoryStub {

    public LuceneOakRepositoryStub(Properties settings)
            throws RepositoryException {
        super(settings);
    }

    @Override
    protected void preCreateRepository(Jcr jcr) {
        LuceneIndexProvider provider = new LuceneIndexProvider().with(getNodeAggregator());
        jcr.with(
                new LuceneCompatModeInitializer("luceneGlobal", (Set<String>) null))
                .with((QueryIndexProvider)provider)
                .with((Observer) provider)
                .withFastQueryResultSize(true)
                .with(new LuceneIndexEditorProvider());
    }

    private static QueryIndex.NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator()
            .newRuleWithName(NT_FILE, newArrayList(JCR_CONTENT, JCR_CONTENT + "/*"));
    }

    private static class LuceneCompatModeInitializer extends LuceneInitializerHelper {
        private final String name;

        public LuceneCompatModeInitializer(String name, Set<String> propertyTypes) {
            super(name, propertyTypes);
            this.name = name;
        }

        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)
                    && builder.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name)) {
                // do nothing
            } else {
                NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME).child(name);
                index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                        .setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE)
                        .setProperty(REINDEX_PROPERTY_NAME, true)
                        .setProperty(LuceneIndexConstants.TEST_MODE, true)
                        .setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);
                index.child(LuceneIndexConstants.SUGGESTION_CONFIG)
                        .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", NAME)
                        .setProperty(LuceneIndexConstants.SUGGEST_UPDATE_FREQUENCY_MINUTES, 10);

                NodeBuilder rules = index.child(LuceneIndexConstants.INDEX_RULES);
                rules.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", NAME);
                NodeBuilder ntBase = rules.child("nt:base");
                ntBase.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", NAME);

                //Enable nodeName index support
                ntBase.setProperty(LuceneIndexConstants.INDEX_NODE_NAME, true);
                NodeBuilder props = ntBase.child(LuceneIndexConstants.PROP_NODE);
                props.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", NAME);

                // Enable function-based indexes: upper+lower(name+localname+prop1)
                functionBasedIndex(props, "upper(name())");
                functionBasedIndex(props, "lower(name())");
                functionBasedIndex(props, "upper(localname())");
                functionBasedIndex(props, "lower(localname())");
                functionBasedIndex(props, "upper([prop1])");
                functionBasedIndex(props, "lower([prop1])");

                enableFulltextIndex(props.child("allProps"));
            }
        }

        private void enableFulltextIndex(NodeBuilder propNode){
            propNode.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", NAME)
                    .setProperty(LuceneIndexConstants.PROP_ANALYZED, true)
                    .setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true)
                    .setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true)
                    .setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true)
                    .setProperty(LuceneIndexConstants.PROP_USE_IN_SPELLCHECK, true)
                    .setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true)
                    .setProperty(LuceneIndexConstants.PROP_NAME, LuceneIndexConstants.REGEX_ALL_PROPS)
                    .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
        }
        
        private static void functionBasedIndex(NodeBuilder props, String function) {
            props.child(function).
                setProperty(JCR_PRIMARYTYPE, "nt:unstructured", NAME).
                setProperty(LuceneIndexConstants.PROP_FUNCTION, function);
        }

    }
}
