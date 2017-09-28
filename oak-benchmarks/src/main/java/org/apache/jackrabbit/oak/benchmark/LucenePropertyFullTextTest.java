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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.COMPAT_MODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
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
 *      --base ~/tmp/oak/ LucenePropertyFullTextTest Oak-Tar Oak-Mongo
 * </pre>
 * <p>
 * it will run the benchmark for 15 minutes against TarNS and MongoNS.
 * </p>
 */
public class LucenePropertyFullTextTest extends AbstractTest<LucenePropertyFullTextTest.TestContext> {
    private static final Logger LOG = LoggerFactory.getLogger(LucenePropertyFullTextTest.class);
    private WikipediaImport importer;    
    private Thread asyncImporter;
    private boolean benchmarkCompleted, importerCompleted;
    Boolean storageEnabled;
    String currentFixture, currentTest;
    
    /**
     * context used across the tests
     */
    class TestContext {
        final Session session = loginWriter();
        final String title;
        
        public TestContext(@Nonnull final String title) {
            this.title = checkNotNull(title);
        }
    }

    /**
     * helper class to initialise the Lucene Property index definition
     */
    static class LucenePropertyInitialiser implements RepositoryInitializer {
        private String name;
        private Set<String> properties;
        
        public LucenePropertyInitialiser(@Nonnull final String name, 
                                         @Nonnull final Set<String> properties) {
            this.name = checkNotNull(name);
            this.properties = checkNotNull(properties);
        }
                
        private boolean isAlreadyThere(@Nonnull final NodeBuilder root) {
            return checkNotNull(root).hasChildNode(INDEX_DEFINITIONS_NAME) &&
                root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name);
        }
        
        @Override
        public void initialize(final NodeBuilder builder) {
            if (!isAlreadyThere(builder)) {
                Tree t = TreeFactory.createTree(builder.child(INDEX_DEFINITIONS_NAME));
                t = t.addChild(name);
                t.setProperty("jcr:primaryType", INDEX_DEFINITIONS_NODE_TYPE, NAME);
                t.setProperty(COMPAT_MODE, 2L, LONG);
                t.setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE, STRING);
                t.setProperty(ASYNC_PROPERTY_NAME, "async", STRING);
                t.setProperty(REINDEX_PROPERTY_NAME, true);
                
                t = t.addChild(INDEX_RULES);
                t.setOrderableChildren(true);
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);
                
                t = t.addChild("nt:base");
                
                Tree propnode = t.addChild(PROP_NODE);
                propnode.setOrderableChildren(true);
                propnode.setProperty("jcr:primaryType", "nt:unstructured", NAME);
                
                for (String p : properties) {
                    Tree t1 = propnode.addChild(PathUtils.getName(p));
                    t1.setProperty(PROP_PROPERTY_INDEX, true, BOOLEAN);
                    t1.setProperty(PROP_NAME, p);
                }
            }
        }
    }
    
    /**
     * reference to the last added title. Used for looking up with queries.
     */
    private AtomicReference<String> lastTitle = new AtomicReference<String>();
    
    public LucenePropertyFullTextTest(final File dump, 
                                      final boolean flat, 
                                      final boolean doReport, 
                                      final Boolean storageEnabled) {
        this.importer = new WikipediaImport(dump, flat, doReport) {

            @Override
            protected void pageAdded(String title, String text) {
                LOG.trace("Setting title: {}", title);
                lastTitle.set(title);
            }
        };
        this.storageEnabled = storageEnabled;
        this.currentTest = this.getClass().getSimpleName();
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
                       .with((new LuceneInitializerHelper("luceneGlobal", storageEnabled)).async())
                       // the WikipediaImporter set a property `title`
                       .with(new LucenePropertyInitialiser("luceneTitle", of("title")))
                       .withAsyncIndexing("async", 5);
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

    @Override
    protected void beforeSuite() throws Exception {
        LOG.debug("beforeSuite() - {} - {}", currentFixture, currentTest);
        benchmarkCompleted = false;
        importerCompleted = false;
        asyncImporter = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    importer.importWikipedia(loginWriter());
                } catch (Exception e) {
                    LOG.error("Error while importing the dump. Trying to halt everything.", e);
                    importerCompleted = true;
                } finally {
                    if (!benchmarkCompleted) {
                        importerCompleted = true;
                        issueHaltRequest("Wikipedia import completed.");
                    }
                }
            }
        });
        asyncImporter.start();

        // allowing the async index to catch up. 
        TimeUnit.SECONDS.sleep(10);
    }

    @Override
    protected void afterSuite() throws Exception {
        LOG.debug("afterSuite() - {} - {}", currentFixture, currentTest);
        asyncImporter.join();
    }
    
    @Override
    protected void runTest() throws Exception {
        if (lastTitle.get() == null) {
            return;
        }
        runTest(new TestContext(lastTitle.get()));
    }

    @Override
    protected void runTest(final TestContext ec) throws Exception {
        if (importerCompleted) {
            return;
        }
        final long maxWait = TimeUnit.MINUTES.toMillis(5);
        final long waitUnit = 50;
        long sleptSoFar = 0;
        
        while (!performQuery(ec) && sleptSoFar < maxWait) {
            LOG.trace("title '{}' not found. Waiting and retry. sleptSoFar: {}ms", ec.title,
                sleptSoFar);
            sleptSoFar += waitUnit;
            TimeUnit.MILLISECONDS.sleep(waitUnit);
        }
        
        if (sleptSoFar < maxWait) {
            // means we exited the loop as we found it.
            LOG.info("{} - {} - title '{}' found with a wait/try of {}ms", currentFixture,
                currentTest, ec.title, sleptSoFar);
        } else {
            LOG.warn("{} - {} - title '{}' timed out with a way/try of {}ms.", currentFixture,
                currentTest, ec.title, sleptSoFar);
        }
    }
    
    private boolean performQuery(@Nonnull final TestContext ec) throws RepositoryException {
        QueryManager qm = ec.session.getWorkspace().getQueryManager();
        ValueFactory vf = ec.session.getValueFactory();
        Query q = qm.createQuery("SELECT * FROM [nt:base] WHERE [title] = $title", Query.JCR_SQL2);
        q.bindValue("title", vf.createValue(ec.title));
        LOG.trace("statement: {} - title: {}", q.getStatement(), ec.title);        
        RowIterator rows = q.execute().getRows();
        if (rows.hasNext()) {
            rows.nextRow().getPath();
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void issueHaltChildThreads() {
        if (!importerCompleted) {
            LOG.info("benchmark completed. Issuing an halt for the importer");
            benchmarkCompleted = true;
            this.importer.issueHaltImport();
        }
    }
}
