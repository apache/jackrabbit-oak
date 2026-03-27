/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.FacetCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;

import javax.jcr.Repository;

/**
 * Runs {@link FacetCommonTest} against Lucene 9 ({@code lucene9}) indexes so facet behaviour matches
 * legacy Lucene and Elastic facet scenarios.
 */
public class LuceneNgFacetCommonTest extends FacetCommonTest {

    @Override
    protected Repository createJcrRepository() {
        indexOptions = new LuceneNgIndexOptions();
        repositoryOptionsUtil = new LuceneNgTestRepositoryBuilder().build();
        Oak oak = repositoryOptionsUtil.getOak();
        return new Jcr(oak).createRepository();
    }

    @Override
    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, (repositoryOptionsUtil.isAsync()
                ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) * 5);
    }
}
