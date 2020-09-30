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


import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.RowIterator;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.COMPAT_MODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_ANALYZED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE_SCOPE_INDEX;

/**
 * <p>
 * Perform a benchmark on how long it takes for an ingested item to be available in a Lucene
 * Property index when indexed in conjunction with a Global full-text lucene (same thread). It makes
 * use of the {@link WikipediaImport} to use a Wikipedia dump for content injestion.
 * <p>
 * Extend this class in lucene and elastic benchmarks and override the createRepository method to include respective
 * Index Editor providers.
 * <p>
 * Suggested dump:
 * <a href="https://dumps.wikimedia.org/enwiki/20150403/enwiki-20150403-pages-articles.xml.bz2">https://dumps.wikimedia.org/enwiki/20150403/enwiki-20150403-pages-articles.xml.bz2</a>
 * <p>
 * Usage example:
 * <pre>
 * java -Druntime=900 -Dlogback.configurationFile=logback-benchmark.xml \
 *      -jar ~/.m2/repository/org/apache/jackrabbit/oak-run/1.4-SNAPSHOT/oak-run-1.4-SNAPSHOT.jar \
 *      benchmark --wikipedia enwiki-20150403-pages-articles.xml.bz2 \
 *      --base ~/tmp/oak/ &lt;Test Extending this class&gt;&lt;/&gt; Oak-Tar Oak-Mongo
 * </pre>
 * <p>
 * it will run the benchmark for 15 minutes against TarNS and MongoNS.
 */
public class PropertyFullTextTest extends AbstractTest<PropertyFullTextTest.TestContext> {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyFullTextTest.class);
    private WikipediaImport importer;
    private Thread asyncImporter;
    private boolean benchmarkCompleted, importerCompleted;
    Boolean storageEnabled;
    String currentFixtureName, currentTest;
    /*
    Reference to the count of pages ingested and committed to repo by WikiPediaImport
     */
    private AtomicReference<Integer> count = new AtomicReference<Integer>();

    public String getCurrentFixtureName() {
        return currentFixtureName;
    }

    public String getCurrentTest() {
        return currentTest;
    }

    public PropertyFullTextTest(final File dump,
                                final boolean flat,
                                final boolean doReport,
                                final Boolean storageEnabled) {
        this.importer = new WikipediaImport(dump, flat, doReport) {

            @Override
            protected int getBatchCount() {
                return 1000;
            }

            @Override
            protected void pageAdded(String title, String text) {
                count.set(count.get() == null ? 1 : count.get()  + 1);
                // We save session in batches of 1000 in wiki import
                // So doesn't make sense to change the last set title before that
                // because then we might be querying on a title for a node that hasn't
                // yet been committed.
                if (count.get() % getBatchCount() == 0) {
                    LOG.trace("Setting title: {}, current page count {}", title, count.get());
                    lastTitle.set(title);
                }

            }
        };
        this.storageEnabled = storageEnabled;
        this.currentTest = this.getClass().getSimpleName();
    }


    /**
     * helper class to initialise the Lucene/Elastic Property index definition
     */
    static class FullTextPropertyInitialiser implements RepositoryInitializer {
        private String name;
        private Set<String> properties;
        private String type;
        private boolean nodeScope;
        private boolean async;
        private boolean analyzed;

        public FullTextPropertyInitialiser(@NotNull final String name,
                                           @NotNull final Set<String> properties,
                                           @NotNull final String type) {
            this.name = name;
            this.properties = properties;
            this.type = type;
            this.async = false;
            this.nodeScope = false;
            this.analyzed = false;
        }

        public FullTextPropertyInitialiser async() {
            this.async = true;
            return this;
        }

        public FullTextPropertyInitialiser nodeScope() {
            this.nodeScope = true;
            return this;
        }

        public FullTextPropertyInitialiser analyzed() {
            this.analyzed = true;
            return this;
        }

        private boolean isAlreadyThere(final @NotNull NodeBuilder root) {
            return root.hasChildNode(INDEX_DEFINITIONS_NAME) &&
                    root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name);
        }

        @Override
        public void initialize(final @NotNull NodeBuilder builder) {
            if (!isAlreadyThere(builder)) {
                Tree t = TreeFactory.createTree(builder.child(INDEX_DEFINITIONS_NAME));
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                NodeBuilder uuid = IndexUtils.createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "uuid", true, true,
                        ImmutableList.<String>of("jcr:uuid"), null);
                uuid.setProperty("info",
                        "Oak index for UUID lookup (direct lookup of nodes with the mixin 'mix:referenceable').");

                t = t.addChild(name);
                t.setProperty("jcr:primaryType", INDEX_DEFINITIONS_NODE_TYPE, NAME);
                t.setProperty(COMPAT_MODE, 2L, LONG);
                t.setProperty(TYPE_PROPERTY_NAME, type, STRING);
                if (async) {
                    t.setProperty(ASYNC_PROPERTY_NAME, "async", STRING);
                }
                t.setProperty(REINDEX_PROPERTY_NAME, true);

                t = t.addChild(INDEX_RULES);
                t.setOrderableChildren(true);
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                t = t.addChild("nt:base");
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                Tree propnode = t.addChild(PROP_NODE);
                propnode.setOrderableChildren(true);
                propnode.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                for (String p : properties) {
                    Tree t1 = propnode.addChild(PathUtils.getName(p));
                    t1.setProperty(PROP_PROPERTY_INDEX, true, BOOLEAN);
                    if (nodeScope) {
                        t1.setProperty(PROP_NODE_SCOPE_INDEX, true, BOOLEAN);
                    }

                    if (analyzed) {
                        t1.setProperty(PROP_ANALYZED, true, BOOLEAN);
                    }
                    t1.setProperty(PROP_NAME, p);
                    t1.setProperty("jcr:primaryType", "nt:unstructured", NAME);
                }
            }
        }
    }


    /**
     * context used across the tests
     */
    class TestContext {
        final Session session = loginWriter();
        final String title;

        public TestContext(@NotNull final String title) {
            LOG.trace("Setting title - {} for test context", title);
            this.title = checkNotNull(title);
        }
    }

    /**
     * reference to the last added title. Used for looking up with queries.
     */
    private AtomicReference<String> lastTitle = new AtomicReference<String>();


    @Override
    protected void beforeSuite() throws Exception {
        LOG.debug("beforeSuite() - {} - {}", getCurrentFixtureName(), getCurrentTest());
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
        LOG.debug("afterSuite() - {} - {}", getCurrentFixtureName(), getCurrentTest());
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
        LOG.trace("Starting test execution");
        if (importerCompleted) {
            LOG.trace("Import completed....");
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
            LOG.info("{} - {} - title '{}' found with a wait/try of {}ms", getCurrentFixtureName(),
                    getCurrentTest(), ec.title, sleptSoFar);
        } else {
            LOG.warn("{} - {} - title '{}' timed out with a way/try of {}ms.", getCurrentFixtureName(),
                    getCurrentTest(), ec.title, sleptSoFar);
        }
    }

    private boolean performQuery(@NotNull final TestContext ec) throws RepositoryException {
        // refresh the session first to get the current view of the
        // persisted state.
        ec.session.refresh(true);
        QueryManager qm = ec.session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("SELECT * FROM [nt:base] WHERE [title] = \"" + ec.title + "\"", Query.JCR_SQL2);
        LOG.trace("statement: {} - title: {}", q.getStatement(), ec.title);
        RowIterator rows = q.execute().getRows();
        if (rows.hasNext()) {
            LOG.trace("Path ->" + rows.nextRow().getPath());
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
