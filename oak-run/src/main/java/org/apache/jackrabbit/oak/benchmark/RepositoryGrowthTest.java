/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

import static org.apache.jackrabbit.oak.fixture.OakFixture.SegmentFixture;

public class RepositoryGrowthTest extends WikipediaImport {
    private final boolean luceneIndexOnFS;
    private final File basedir;
    private final Map<RepositoryFixture, File> indexDirs = Maps.newHashMap();


    public RepositoryGrowthTest(File dump, File basedir, boolean luceneIndexOnFS) {
        super(dump, true, true);
        this.luceneIndexOnFS = luceneIndexOnFS;
        this.basedir = basedir;
    }

    @Override
    protected Repository[] setupCluster(final RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    String path = null;
                    if(luceneIndexOnFS){
                        File indexDir = new File(basedir, "lucene-"+System.currentTimeMillis());
                        path = indexDir.getAbsolutePath();
                        indexDirs.put(fixture, indexDir);
                    }
                    oak.with((QueryIndexProvider) provider)
                            .with((Observer) provider)
                            .with(new LuceneIndexEditorProvider())
                            .with(new LuceneInitializerHelper("luceneGlobal", LuceneIndexHelper.JR_PROPERTY_INCLUDES,
                                    null, path, null));
                    return new Jcr(oak);
                }
            });
        }
        return super.setupCluster(fixture);
    }

    @Override
    protected void tearDown(RepositoryFixture fixture) throws IOException {
        if (fixture instanceof OakRepositoryFixture) {
            OakFixture oakFixture = ((OakRepositoryFixture) fixture).getOakFixture();
            if(oakFixture instanceof SegmentFixture){
                SegmentFixture sf = (SegmentFixture) oakFixture;
                long size = sf.getStores()[0].size();

                if(sf.getBlobStoreFixtures().length > 0) {
                    size = sf.getBlobStoreFixtures()[0].size();
                }

                File indexDir = indexDirs.get(fixture);
                if(indexDir != null){
                    size += FileUtils.sizeOfDirectory(indexDir);
                }
                System.out.printf("Repository size %s %s %n", fixture, IOUtils.humanReadableByteCount(size));
            }
        }
        super.tearDown(fixture);
    }

    @Override
    protected void batchDone(Session session, long start, int count) throws RepositoryException {
        session.save();
        super.batchDone(session, start, count);
    }
}
