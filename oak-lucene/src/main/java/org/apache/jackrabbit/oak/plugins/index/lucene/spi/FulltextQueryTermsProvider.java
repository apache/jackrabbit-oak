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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

/**
 * Implementations of this interface would get callbacks while forming lucene full text queries.
 */
public interface FulltextQueryTermsProvider {
    /**
     * This method would get called while forming full text clause for full text clause not constrained on a particular
     * field.
     * @param text full text term
     * @param analyzer {@link Analyzer} being used while forming the query. Can be used to analyze text consistently.
     * @return {@link Query} object to be OR'ed with query being prepared. {@code null}, if nothing is to be added.
     */
    Query getQueryTerm(final String text, final Analyzer analyzer);
}
