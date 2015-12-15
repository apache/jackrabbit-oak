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
package org.apache.jackrabbit.oak.plugins.index.lucene.indexAugment;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.IndexFieldProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;

import java.util.Collections;

public interface IndexAugmentorFactory {
    IndexAugmentorFactory DEFAULT = new IndexAugmentorFactory() {
        @Override
        public IndexFieldProvider getIndexFieldProvider() {
            return new IndexFieldProvider() {
                @Override
                public Iterable<Field> getAugmentedFields(String path, String propertyName,
                                                      NodeState document, PropertyState property,
                                                      NodeState indexDefinition) {
                    return Collections.emptyList();
                }
            };
        }

        @Override
        public FulltextQueryTermsProvider getFulltextQueryTermsProvider() {
            return new FulltextQueryTermsProvider() {
                @Override
                public Query getQueryTerm(String text, Analyzer analyzer) {
                    return null;
                }
            };
        }
    };

    IndexFieldProvider getIndexFieldProvider();

    FulltextQueryTermsProvider getFulltextQueryTermsProvider();
}
