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
package org.apache.jackrabbit.oak.benchmark;


import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

import javax.jcr.Repository;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LuceneFacetSearchTest extends FacetSearchTest {

    private final boolean disableCopyOnRead = Boolean.getBoolean("disableCopyOnRead");
    protected ExecutorService executorService = Executors.newFixedThreadPool(2);
    protected String indexName = "luceneFacetTest";

    protected File indexCopierDir;

    public LuceneFacetSearchTest(Boolean storageEnabled) {
        super(storageEnabled);
        this.indexCopierDir = createTemporaryFolder(null);
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        Map<String, Boolean> propMap = new LinkedHashMap();
        propMap.put(SEARCH_PROP, false);
        propMap.put(FACET_PROP_1, true);
        propMap.put(FACET_PROP_2, true);
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = createLuceneIndexProvider();
                    oak.with((QueryIndexProvider) provider)
                            .with((Observer) provider)
                            .with(new LuceneIndexEditorProvider())
                            .with(new FacetSearchTest.FacetIndexInitializer(indexName, propMap,
                                    LuceneIndexConstants.TYPE_LUCENE, getFacetMode()));
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

    private LuceneIndexProvider createLuceneIndexProvider() {
        if (!disableCopyOnRead) {
            try {
                IndexCopier copier = new IndexCopier(executorService, indexCopierDir, true);
                return new LuceneIndexProvider(copier);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new LuceneIndexProvider();
    }

    private File createTemporaryFolder(File parentFolder) {
        File createdFolder;
        try {
            createdFolder = File.createTempFile("oak", "", parentFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        createdFolder.delete();
        createdFolder.mkdir();
        return createdFolder;
    }

    @Override
    protected void afterSuite() throws Exception {
        super.afterSuite();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        FileUtils.deleteDirectory(indexCopierDir);
    }
}
