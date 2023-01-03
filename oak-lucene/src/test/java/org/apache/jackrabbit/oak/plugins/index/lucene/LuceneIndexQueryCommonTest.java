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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexQueryCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests the query engine using the default index implementation: the
 * {@link LuceneIndexProvider}
 */
public class LuceneIndexQueryCommonTest extends IndexQueryCommonTest {

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Override
    protected ContentRepository createRepository() {
        LuceneTestRepositoryBuilder luceneTestRepositoryBuilder = new LuceneTestRepositoryBuilder(executorService, temporaryFolder);
        luceneTestRepositoryBuilder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        repositoryOptionsUtil = luceneTestRepositoryBuilder.build();
        indexOptions = new LuceneIndexOptions();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    @Override
    public String getContainsValueForEqualityQuery_native() {
        return "+:ancestors:/test +propa:bar";
    }

    @Override
    public String getContainsValueForInequalityQuery_native() {
        return "+:ancestors:/test +propa:[* TO *] -propa:bar";
    }

    @Override
    public String getContainsValueForInequalityQueryWithoutAncestorFilter_native() {
        return "+propa:[* TO *] -propa:bar";
    }

    @Override
    public String getContainsValueForEqualityInequalityCombined_native() {
        return "+:ancestors:/test +propb:world +propa:[* TO *] -propa:bar";
    }

    @Override
    public String getContainsValueForNotNullQuery_native() {
        return "+:ancestors:/test +propa:[* TO *]";
    }

    @Override
    public String getExplainValueForDescendantTestWithIndexTagExplain() {
        return "[nt:base] as [nt:base] /* lucene:test-index(/oak:index/test-index) :ancestors:/test" +
                " where isdescendantnode([nt:base], [/test]) */";
    }
}
