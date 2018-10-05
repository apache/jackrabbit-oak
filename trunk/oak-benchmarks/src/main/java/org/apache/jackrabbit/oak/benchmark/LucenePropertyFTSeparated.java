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
package org.apache.jackrabbit.oak.benchmark;

import static com.google.common.collect.ImmutableSet.of;

import java.io.File;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

/**
 * same as {@link LucenePropertyFullTextTest} but will initialise a repository where the global
 * full-text runs on a separate thread from lucene property.
 */
public class LucenePropertyFTSeparated extends LucenePropertyFullTextTest {

    public LucenePropertyFTSeparated(final File dump, 
                                     final boolean flat, 
                                     final boolean doReport,
                                     final Boolean storageEnabled) {
        super(dump, flat, doReport, storageEnabled);
        currentTest = this.getClass().getSimpleName();
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            currentFixture = fixture.toString();
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    oak.with((QueryIndexProvider) provider)
                       .with((Observer) provider)
                       .with(new LuceneIndexEditorProvider())
                        .with(
                            (new LuceneInitializerHelper("luceneGlobal", storageEnabled))
                                .async("async-slow"))
                       // the WikipediaImporter set a property `title`
                       .with(new LucenePropertyInitialiser("luceneTitle", of("title")))
                       .withAsyncIndexing("async", 5)
                       .withAsyncIndexing("async-slow", 5);
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }
}
