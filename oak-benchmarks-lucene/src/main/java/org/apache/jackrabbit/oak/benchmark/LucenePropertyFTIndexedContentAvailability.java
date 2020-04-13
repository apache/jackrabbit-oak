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
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Perform a benchmark on how long it takes for an ingested item to be available in a Lucene
 * Property index when indexed in conjunction with a Global full-text lucene (same thread). It makes
 * use of the {@link WikipediaImport} to use a Wikipedia dump for content injestion.
 * </p>
 * <p>
 * Suggested dump: 
 * <a href="https://dumps.wikimedia.org/enwiki/20150403/enwiki-20150403-pages-articles.xml.bz2">https://dumps.wikimedia.org/enwiki/20150403/enwiki-20150403-pages-articles.xml.bz2</a>
 * </p>
 * <p>
 * Usage example:
 * </p>
 * 
 * <pre>
 * java -Druntime=900 -Dlogback.configurationFile=logback-benchmark.xml \
 *      -jar ~/.m2/repository/org/apache/jackrabbit/oak-run/1.4-SNAPSHOT/oak-run-1.4-SNAPSHOT.jar \
 *      benchmark --wikipedia enwiki-20150403-pages-articles.xml.bz2 \
 *      --base ~/tmp/oak/ LucenePropertyFTIndexedContentAvailability Oak-Tar Oak-Mongo
 * </pre>
 * <p>
 * it will run the benchmark for 15 minutes against TarNS and MongoNS.
 * </p>
 */
public class LucenePropertyFTIndexedContentAvailability extends PropertyFullTextTest {
    private static final Logger LOG = LoggerFactory.getLogger(LucenePropertyFTIndexedContentAvailability.class);
    private String currentFixtureName;

    @Override
    public String getCurrentFixtureName() {
        return currentFixtureName;
    }

    @Override
    public String getCurrentTest() {
        return this.getClass().getSimpleName();
    }


    public LucenePropertyFTIndexedContentAvailability(final File dump,
                                                      final boolean flat,
                                                      final boolean doReport,
                                                      final Boolean storageEnabled) {
        super(dump, flat, doReport, storageEnabled);
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            currentFixtureName = fixture.toString();
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    oak.with((QueryIndexProvider) provider)
                       .with((Observer) provider)
                       .with(new LuceneIndexEditorProvider())
                       .with((new LuceneInitializerHelper("luceneGlobal", storageEnabled)).async())
                       // the WikipediaImporter set a property `title`
                       .with(new FullTextPropertyInitialiser("luceneTitle", of("title"), LuceneIndexConstants.TYPE_LUCENE).async())
                       .withAsyncIndexing("async", 5);
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

}
