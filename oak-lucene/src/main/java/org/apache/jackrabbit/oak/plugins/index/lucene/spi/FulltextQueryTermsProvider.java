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
package org.apache.jackrabbit.oak.plugins.index.lucene.spi;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Implementations of this interface would get callbacks while forming lucene full text queries.
 */
public interface FulltextQueryTermsProvider {
    /**
     * Implementation which doesn't do anything useful... yet, abides with the contract.
     */
    FulltextQueryTermsProvider DEFAULT = new FulltextQueryTermsProvider() {
        @Override
        public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
            return null;
        }

        @Override
        public Set<String> getSupportedTypes() {
            return Collections.EMPTY_SET;
        }
    };
    /**
     * This method would get called while forming full text clause for full text clause not constrained on a particular
     * field.
     * @param text full text term
     * @param analyzer {@link Analyzer} being used while forming the query. Can be used to analyze text consistently.
     * @param indexDefinition {@link NodeState} of index definition
     * @return {@link Query} object to be OR'ed with query being prepared. {@code null}, if nothing is to be added.
     */
    @CheckForNull
    Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition);

    /**
     * This method is used to find which node types are supported by the implementation. Based, on the index
     * definition being used to query the document, only those implementations would get callback to
     * {@link FulltextQueryTermsProvider#getQueryTerm} which declare a matching node type. Note, node types are
     * exact matches and do not support inheritance.
     * @return {@link Set} of types supported by the implementation
     */
    @Nonnull
    Set<String> getSupportedTypes();
}
