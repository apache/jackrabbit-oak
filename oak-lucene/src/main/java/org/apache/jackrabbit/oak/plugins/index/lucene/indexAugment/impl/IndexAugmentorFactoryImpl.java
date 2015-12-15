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
package org.apache.jackrabbit.oak.plugins.index.lucene.indexAugment.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.lucene.indexAugment.IndexAugmentorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

@Component(metatype = false, immediate = true)
@Service(value = IndexAugmentorFactory.class)
@References({
        @Reference(name = "IndexFieldProvider",
                policy = ReferencePolicy.DYNAMIC,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                referenceInterface = IndexFieldProvider.class,
                bind = "bindIndexFieldProvider",
                unbind = "unbindIndexFieldProvider"),
        @Reference(name = "FulltextQueryTermsProvider",
                policy = ReferencePolicy.DYNAMIC,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                referenceInterface = FulltextQueryTermsProvider.class,
                bind = "bindFulltextQueryTermsProvider",
                unbind = "unbindFulltextQueryTermsProvider")
})
public class IndexAugmentorFactoryImpl implements IndexAugmentorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAugmentorFactoryImpl.class);
    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(IndexAugmentorFactoryImpl.class.getName() + ".perf"));

    private Set<IndexFieldProvider> indexFieldProviders =
            Sets.newConcurrentHashSet();

    private Set<FulltextQueryTermsProvider> fulltextQueryTermsProviders =
            Sets.newConcurrentHashSet();

    private IndexFieldProvider indexFieldProvider = new CompositeIndexFieldProvider();
    private FulltextQueryTermsProvider fulltextQueryTermsProvider = new CompositeFulltextQueryTermsProvider();

    @Deactivate
    private void deactivate() {
        indexFieldProviders.clear();
        fulltextQueryTermsProviders.clear();
    }

    public IndexFieldProvider getIndexFieldProvider() {
        return indexFieldProvider;
    }

    public FulltextQueryTermsProvider getFulltextQueryTermsProvider() {
        return fulltextQueryTermsProvider;
    }

    class CompositeIndexFieldProvider implements IndexFieldProvider {
        @Override
        public List<Field> getAugmentedFields(final String path, final String propertyName,
                                              final NodeState document, final PropertyState property,
                                              final NodeState indexDefinition) {
            List<Field> fields = Lists.newArrayList();
            for (IndexFieldProvider indexFieldProvider : indexFieldProviders) {
                final long start = PERFLOG.start();
                Iterable<Field> providedFields = indexFieldProvider.getAugmentedFields(path, propertyName,
                        document, property,
                        indexDefinition);
                PERFLOG.end(start, 1, "indexFieldProvider: {}, path: {}, propertyName: {}",
                        indexFieldProvider, path, propertyName);
                if (providedFields != null) {
                    for (Field f : providedFields) {
                        fields.add(f);
                    }
                }
            }
            return fields;
        }
    }

    class CompositeFulltextQueryTermsProvider implements FulltextQueryTermsProvider {
        @Override
        public Query getQueryTerm(final String text, final Analyzer analyzer) {
            List<Query> subQueries = Lists.newArrayList();
            for (FulltextQueryTermsProvider fulltextQueryTermsProvider : fulltextQueryTermsProviders) {
                final long start = PERFLOG.start();
                Query subQuery = fulltextQueryTermsProvider.getQueryTerm(text, analyzer);
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
    }

    private void bindIndexFieldProvider(IndexFieldProvider indexFieldProvider) {
        indexFieldProviders.add(indexFieldProvider);
        LOG.info("bindIndexFieldProvider: {}", indexFieldProvider);
    }

    private void unbindIndexFieldProvider(IndexFieldProvider indexFieldProvider) {
        indexFieldProviders.remove(indexFieldProvider);
        LOG.info("unbindIndexFieldProvider: {}", indexFieldProvider);
    }

    private void bindFulltextQueryTermsProvider(FulltextQueryTermsProvider fulltextQueryTermsProvider) {
        fulltextQueryTermsProviders.add(fulltextQueryTermsProvider);
        LOG.info("bindFulltextQueryTermsProvider: {}", fulltextQueryTermsProvider);
    }

    private void unbindFulltextQueryTermsProvider(FulltextQueryTermsProvider fulltextQueryTermsProvider) {
        fulltextQueryTermsProviders.remove(fulltextQueryTermsProvider);
        LOG.info("unbindFulltextQueryTermsProvider: {}", fulltextQueryTermsProvider);
    }
}
