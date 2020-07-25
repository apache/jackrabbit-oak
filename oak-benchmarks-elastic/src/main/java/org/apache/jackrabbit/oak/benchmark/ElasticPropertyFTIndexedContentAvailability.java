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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.benchmark.util.ElasticGlobalInitializer;
import org.apache.jackrabbit.oak.benchmark.util.TestHelper;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Repository;
import java.io.File;

import static com.google.common.collect.ImmutableSet.of;

/**
 * <p>
 * Perform a benchmark on how long it takes for an ingested item to be available in a Elastic
 * Property index when indexed in conjunction with a Global full-text Elastic (same thread). It makes
 * use of the {@link WikipediaImport} to use a Wikipedia dump for content injestion.
 * <p>
 * Extend this class in lucene and elastic benchmarks and override the createRepository method to include respective
 * Index Editor providers.
 * <p>
 * Suggested dump:
 * <a href="https://dumps.wikimedia.org/enwiki/20150403/enwiki-20150403-pages-articles.xml.bz2">https://dumps.wikimedia.org/enwiki/20150403/enwiki-20150403-pages-articles.xml.bz2</a>
 * <p>
 * Usage example:
 * <p>
 * <pre>
 * java -Druntime=900 -Dlogback.configurationFile=logback-benchmark.xml \
 *      -jar ~/.m2/repository/org/apache/jackrabbit/oak-run/1.4-SNAPSHOT/oak-run-1.4-SNAPSHOT.jar \
 *      benchmark --wikipedia enwiki-20150403-pages-articles.xml.bz2 \
 *      --base ~/tmp/oak/ &lt;Test Extending this class&gt;&lt;/&gt; Oak-Tar Oak-Mongo
 * </pre>
 * <p>
 * it will run the benchmark for 15 minutes against TarNS and MongoNS.
 */
public class ElasticPropertyFTIndexedContentAvailability extends PropertyFullTextTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticPropertyFTIndexedContentAvailability.class);
    private String currentFixtureName;
    private ElasticConnection coordinate;
    private String elasticGlobalIndexName;
    private String elasticTitleIndexName;

    @Override
    public String getCurrentFixtureName() {
        return currentFixtureName;
    }

    @Override
    public String getCurrentTest() {
        return this.getClass().getSimpleName();
    }


    ElasticPropertyFTIndexedContentAvailability(final File dump,
                                                final boolean flat,
                                                final boolean doReport,
                                                final Boolean storageEnabled, ElasticConnection coordinate) {
        super(dump, flat, doReport, storageEnabled);
        this.coordinate = coordinate;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        elasticGlobalIndexName = TestHelper.getUniqueIndexName("elasticGlobal");
        elasticTitleIndexName = TestHelper.getUniqueIndexName("elasticTitle");
        if (fixture instanceof OakRepositoryFixture) {
            currentFixtureName = fixture.toString();
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(coordinate,
                            new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
                    ElasticIndexProvider indexProvider = new ElasticIndexProvider(coordinate);
                    oak.with(editorProvider)
                            .with((Observer) indexProvider)
                            .with((QueryIndexProvider) indexProvider)
                            .with((new ElasticGlobalInitializer(elasticGlobalIndexName, storageEnabled)).async())
                            // the WikipediaImporter set a property `title`
                            .with(new FullTextPropertyInitialiser(elasticTitleIndexName, of("title"),
                                    ElasticIndexDefinition.TYPE_ELASTICSEARCH).async())
                            .withAsyncIndexing("async", 5);
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

    @Override
    protected void afterSuite() throws Exception {
        super.afterSuite();
        TestHelper.cleanupRemoteElastic(coordinate, elasticGlobalIndexName);
        TestHelper.cleanupRemoteElastic(coordinate, elasticTitleIndexName);
    }

}
