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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexAugmentorFactory {

    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(IndexAugmentorFactory.class.getName() + ".perf"));

    private final Tracker<IndexFieldProvider> indexFieldProviderTracker;
    private final Tracker<FulltextQueryTermsProvider> fulltextQueryTermsProviderTracker;

    private volatile Map<String, CompositeIndexFieldProvider> indexFieldProviderMap;
    private volatile Map<String, CompositeFulltextQueryTermsProvider> fulltextQueryTermsProviderMap;

    public IndexAugmentorFactory(Whiteboard whiteboard) {
        indexFieldProviderTracker = whiteboard.track(IndexFieldProvider.class);
        fulltextQueryTermsProviderTracker = whiteboard.track(FulltextQueryTermsProvider.class);

        indexFieldProviderMap = Maps.newHashMap();
        fulltextQueryTermsProviderMap = Maps.newHashMap();
    }

    @Nonnull
    public IndexFieldProvider getIndexFieldProvider(String nodeType) {
        IndexFieldProvider provider = indexFieldProviderMap.get(nodeType);
        return (provider != null) ? provider : IndexFieldProvider.DEFAULT;
    }

    @Nonnull
    public FulltextQueryTermsProvider getFulltextQueryTermsProvider(String nodeType) {
        FulltextQueryTermsProvider provider = fulltextQueryTermsProviderMap.get(nodeType);
        return (provider != null) ? provider : FulltextQueryTermsProvider.DEFAULT;
    }

    public void refreshServices() {
        refreshIndexFieldProviderServices();
        refreshFulltextQueryTermsProviderServices();
    }

    private void refreshIndexFieldProviderServices() {
        ListMultimap<String, IndexFieldProvider> indexFieldProviderListMultimap =
                LinkedListMultimap.create();
        for (IndexFieldProvider provider : indexFieldProviderTracker.getServices()) {
            Set<String> supportedNodeTypes = provider.getSupportedTypes();
            for (String nodeType : supportedNodeTypes) {
                indexFieldProviderListMultimap.put(nodeType, provider);
            }
        }

        Map<String, CompositeIndexFieldProvider> tempMap = Maps.newHashMap();
        for (String nodeType : indexFieldProviderListMultimap.keySet()) {
            List<IndexFieldProvider> providers = indexFieldProviderListMultimap.get(nodeType);
            CompositeIndexFieldProvider compositeIndexFieldProvider =
                    new CompositeIndexFieldProvider(nodeType, providers);
            tempMap.put(nodeType, compositeIndexFieldProvider);
        }

        indexFieldProviderMap = tempMap;
    }

    private void refreshFulltextQueryTermsProviderServices() {
        ListMultimap<String, FulltextQueryTermsProvider> fulltextQueryTermsProviderLinkedListMultimap =
                LinkedListMultimap.create();
        for (FulltextQueryTermsProvider provider : fulltextQueryTermsProviderTracker.getServices()) {
            Set<String> supportedNodeTypes = provider.getSupportedTypes();
            for (String nodeType : supportedNodeTypes) {
                fulltextQueryTermsProviderLinkedListMultimap.put(nodeType, provider);
            }
        }

        Map<String, CompositeFulltextQueryTermsProvider> tempMap = Maps.newHashMap();
        for (String nodeType : fulltextQueryTermsProviderLinkedListMultimap.keySet()) {
            List<FulltextQueryTermsProvider> providers = fulltextQueryTermsProviderLinkedListMultimap.get(nodeType);
            CompositeFulltextQueryTermsProvider compositeFulltextQueryTermsProvider =
                    new CompositeFulltextQueryTermsProvider(nodeType, providers);
            tempMap.put(nodeType, compositeFulltextQueryTermsProvider);
        }

        fulltextQueryTermsProviderMap = tempMap;
    }

    class CompositeIndexFieldProvider implements IndexFieldProvider {

        private final String nodeType;
        private final List<IndexFieldProvider> providers;

        CompositeIndexFieldProvider(String nodeType, List<IndexFieldProvider> providers) {
            this.nodeType = nodeType;
            this.providers = providers;
        }

        @Nonnull
        @Override
        public List<Field> getAugmentedFields(final String path,
                                              final NodeState document, final NodeState indexDefinition) {
            List<Field> fields = Lists.newArrayList();
            for (IndexFieldProvider indexFieldProvider : providers) {
                final long start = PERFLOG.start();
                Iterable<Field> providedFields = indexFieldProvider.getAugmentedFields(path, document, indexDefinition);
                PERFLOG.end(start, 1, "indexFieldProvider: {}, path: {}, doc: {}, indexDef: {}",
                        indexFieldProvider, path, document, indexDefinition);
                for (Field f : providedFields) {
                    fields.add(f);
                }
            }
            return fields;
        }

        @Nonnull
        @Override
        public Set<String> getSupportedTypes() {
            return Collections.singleton(nodeType);
        }
    }

    class CompositeFulltextQueryTermsProvider implements FulltextQueryTermsProvider {

        private final String nodeType;
        private final List<FulltextQueryTermsProvider> providers;

        CompositeFulltextQueryTermsProvider(String nodeType, List<FulltextQueryTermsProvider> providers) {
            this.nodeType = nodeType;
            this.providers = providers;
        }

        @Override
        public Query getQueryTerm(final String text, final Analyzer analyzer, NodeState indexDefinition) {
            List<Query> subQueries = Lists.newArrayList();
            for (FulltextQueryTermsProvider fulltextQueryTermsProvider : providers) {
                final long start = PERFLOG.start();
                Query subQuery = fulltextQueryTermsProvider.getQueryTerm(text, analyzer, indexDefinition);
                PERFLOG.end(start, 1, "fulltextQueryTermsProvider: {}, text: {}", fulltextQueryTermsProvider, text);
                if (subQuery != null) {
                    subQueries.add(subQuery);
                }
            }

            Query ret;
            if (subQueries.size() == 0) {
                ret = null;
            } else if (subQueries.size() == 1) {
                ret = subQueries.get(0);
            } else {
                BooleanQuery query = new BooleanQuery();
                for ( Query subQuery : subQueries ) {
                    query.add(subQuery, BooleanClause.Occur.SHOULD);
                }
                ret = query;
            }

            return ret;
        }

        @Nonnull
        @Override
        public Set<String> getSupportedTypes() {
            return Collections.singleton(nodeType);
        }
    }
}
