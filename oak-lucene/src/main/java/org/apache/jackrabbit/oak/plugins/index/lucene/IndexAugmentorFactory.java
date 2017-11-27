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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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

@SuppressWarnings("UnusedDeclaration")
@Component
@Service(value = IndexAugmentorFactory.class)
@References({
        @Reference(name = "IndexFieldProvider",
                policy = ReferencePolicy.DYNAMIC,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                referenceInterface = IndexFieldProvider.class),
        @Reference(name = "FulltextQueryTermsProvider",
                policy = ReferencePolicy.DYNAMIC,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                referenceInterface = FulltextQueryTermsProvider.class)
})
public class IndexAugmentorFactory {

    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(IndexAugmentorFactory.class.getName() + ".perf"));

    private final Set<IndexFieldProvider> indexFieldProviders;
    private final Set<FulltextQueryTermsProvider> fulltextQueryTermsProviders;

    private volatile Map<String, CompositeIndexFieldProvider> indexFieldProviderMap;
    private volatile Map<String, CompositeFulltextQueryTermsProvider> fulltextQueryTermsProviderMap;

    public IndexAugmentorFactory() {
        indexFieldProviders = Sets.newIdentityHashSet();
        fulltextQueryTermsProviders = Sets.newIdentityHashSet();

        resetState();
    }

    @Deactivate
    private synchronized void deactivate() {
        resetState();
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

    synchronized void bindIndexFieldProvider(IndexFieldProvider indexFieldProvider) {
        indexFieldProviders.add(indexFieldProvider);
        refreshIndexFieldProviders();
    }

    synchronized void unbindIndexFieldProvider(IndexFieldProvider indexFieldProvider) {
        indexFieldProviders.remove(indexFieldProvider);
        refreshIndexFieldProviders();
    }

    synchronized void bindFulltextQueryTermsProvider(FulltextQueryTermsProvider fulltextQueryTermsProvider) {
        fulltextQueryTermsProviders.add(fulltextQueryTermsProvider);
        refreshFulltextQueryTermsProviders();
    }

    synchronized void unbindFulltextQueryTermsProvider(FulltextQueryTermsProvider fulltextQueryTermsProvider) {
        fulltextQueryTermsProviders.remove(fulltextQueryTermsProvider);
        refreshFulltextQueryTermsProviders();
    }

    private void refreshIndexFieldProviders() {
        ListMultimap<String, IndexFieldProvider> providerMultimap =
                LinkedListMultimap.create();
        for (IndexFieldProvider provider : indexFieldProviders) {
            Set<String> supportedNodeTypes = provider.getSupportedTypes();
            for (String nodeType : supportedNodeTypes) {
                providerMultimap.put(nodeType, provider);
            }
        }

        Map<String, CompositeIndexFieldProvider> providerMap = Maps.newHashMap();
        for (String nodeType : providerMultimap.keySet()) {
            List<IndexFieldProvider> providers = providerMultimap.get(nodeType);
            CompositeIndexFieldProvider compositeIndexFieldProvider =
                    new CompositeIndexFieldProvider(nodeType, providers);
            providerMap.put(nodeType, compositeIndexFieldProvider);
        }

        indexFieldProviderMap = ImmutableMap.copyOf(providerMap);
    }

    private void refreshFulltextQueryTermsProviders() {
        ListMultimap<String, FulltextQueryTermsProvider> providerMultimap =
                LinkedListMultimap.create();
        for (FulltextQueryTermsProvider provider : fulltextQueryTermsProviders) {
            Set<String> supportedNodeTypes = provider.getSupportedTypes();
            for (String nodeType : supportedNodeTypes) {
                providerMultimap.put(nodeType, provider);
            }
        }

        Map<String, CompositeFulltextQueryTermsProvider> providerMap = Maps.newHashMap();
        for (String nodeType : providerMultimap.keySet()) {
            List<FulltextQueryTermsProvider> providers = providerMultimap.get(nodeType);
            CompositeFulltextQueryTermsProvider compositeFulltextQueryTermsProvider =
                    new CompositeFulltextQueryTermsProvider(nodeType, providers);
            providerMap.put(nodeType, compositeFulltextQueryTermsProvider);
        }

        fulltextQueryTermsProviderMap = ImmutableMap.copyOf(providerMap);
    }

    private void resetState() {
        indexFieldProviders.clear();
        fulltextQueryTermsProviders.clear();

        indexFieldProviderMap = Collections.EMPTY_MAP;
        fulltextQueryTermsProviderMap = Collections.EMPTY_MAP;
    }

    boolean isStateEmpty() {
        return indexFieldProviders.size() == 0 &&
                indexFieldProviderMap.size() == 0 &&
                fulltextQueryTermsProviders.size() == 0 &&
                fulltextQueryTermsProviderMap.size() == 0;
    }

    class CompositeIndexFieldProvider implements IndexFieldProvider {

        private final String nodeType;
        private final List<IndexFieldProvider> providers;

        CompositeIndexFieldProvider(String nodeType, List<IndexFieldProvider> providers) {
            this.nodeType = nodeType;
            this.providers = ImmutableList.copyOf(providers);
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
            this.providers = ImmutableList.copyOf(providers);
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
