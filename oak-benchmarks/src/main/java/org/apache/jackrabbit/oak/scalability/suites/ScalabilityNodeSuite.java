/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.scalability.suites;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.scalability.ScalabilitySuite;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;
import org.apache.jackrabbit.oak.scalability.util.NodeTypeUtils;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The suite test will incrementally increase the load and execute searches.
 * Each test run thus adds nodes and executes different benchmarks. This way we measure time taken for
 * benchmark execution.
 *
 * <p>
 * The following system JVM properties can be defined to configure the suite.
 * <ul>
 * <li>
 *     <code>loaders</code> - Controls the number of concurrent threads for loading blobs initially.
 *     Defaults to 1.
 * </li>
 * <li>
 *     <code>testers</code> - Controls the number of concurrent tester threads. Defaults to 1.
 * </li>
 * <li>
 *     <code>nodeLevels</code> - Comma separated string property that governs the depth and the number of
 *     nodes in the hierarchy. Defaults to 10, 5, 2.
 * </li>
 * <li>
 *     <code>densityLevel</code> - Controls the percentage of root nodes which will have sub nodes created.
 *     Defaults to 100.
 * </li>
 * <li>
 *     <code>index</code> - Controls if the index definitions are to be created. Defaults to false.
 * </li>
 * <li>
 *      <code>asyncIndex</code> - Controls whether the indexing is async. Defaults to false.
 * </li>
 * <li>
 *     <code>noFullIndex</code> - Controls whether fulltext indexing is enabled or disabled. Defaults to false.
 * </li>
 * <li>
 *     <code>randDate</code> - Controls whether to generate random dates in a range. Defaults to false.
 * </li>
 * <li>
 *     <code>customType</code> - Controls if nodes created in the load have a custom node type. Defaults to false.
 * </li>
 * </ul>
 *
 */
public class ScalabilityNodeSuite extends ScalabilityAbstractSuite {
    protected static final Logger LOG = LoggerFactory.getLogger(ScalabilityNodeSuite.class);

    /**
     * Controls the number of concurrent threads for loading blobs initially
     */
    protected static final int LOADERS = Integer.getInteger("loaders", 1);

    /**
     * Controls the number of nodes at each level
     */
    protected static final List<String> NODE_LEVELS = Splitter.on(",").trimResults()
            .omitEmptyStrings().splitToList(System.getProperty("nodeLevels", "10,5,2"));

    /**
     * Controls the number of concurrent tester threads
     */
    protected static final int TESTERS = Integer.getInteger("testers", 1);

    /**
     * Controls the percentage of root nodes which will have sub nodes created.
     * Value ranges from [0, 100]
     */
    protected static final int DENSITY_LEVEL = Integer.getInteger("densityLevel", 100);

    /**
     * Controls if the index definitions are to be created.
     */
    protected static final boolean INDEX = Boolean.getBoolean("index");

    /**
     * Controls whether the indexing is async
     */
    protected static final String ASYNC_INDEX = System.getProperty("asyncIndex");

    /**
     * Controls whether fulltext indexing is enabled or disabled. Enabled by default.
     */
    protected static final boolean FULL_TEXT = !Boolean.getBoolean("noFullIndex");

    /**
     * Controls whether to generate random dates in a range
     */
    protected static final boolean RAND_DATE = Boolean.getBoolean("randDate");

    /**
     * Controls if a customType is to be created
     */
    protected static final boolean CUSTOM_TYPE = Boolean.getBoolean("customType");

    public static final String CTX_DESC_SEARCH_PATHS_PROP = "descPaths";

    public static final String CTX_ROOT_NODE_NAME_PROP = "rootNodeName";

    public static final String CTX_ACT_NODE_TYPE_PROP = "rootType";

    public static final String CTX_REL_NODE_TYPE_PROP = "descendantType";

    public static final String CUSTOM_ROOT_NODE_TYPE = "ParentType";

    public static final String CUSTOM_DESC_NODE_TYPE = "DescendantType";

    public static final String DATE_PROP = "added";

    public static final String CTX_PAGINATION_KEY_PROP = DATE_PROP;

    public static final String FILTER_PROP = "filter";

    public static final String SORT_PROP = "viewed";

    public static final String TITLE_PROP = "title";

    public static final String ROOT_NODE_NAME =
            "LongevitySearchAssets" + TEST_ID;

    public enum Index {
        PROPERTY, ORDERED, LUCENE, LUCENE_DOC, LUCENE_FILE, LUCENE_FILE_DOC
    }

    /** Type of index to be created */
    public final Index INDEX_TYPE =
        Index.valueOf(System.getProperty("indexType", Index.PROPERTY.toString()));

    protected final Boolean storageEnabled;

    protected Whiteboard whiteboard;

    protected final List<String> nodeTypes;

    private final Random random = new Random(29);

    private List<String> searchRootPaths;

    private List<String> searchDescPaths;

    public ScalabilityNodeSuite(Boolean storageEnabled) {
        this.storageEnabled = storageEnabled;
        this.nodeTypes = newArrayList();
    }

    @Override
    public ScalabilitySuite addBenchmarks(ScalabilityBenchmark... tests) {
        for (ScalabilityBenchmark test : tests) {
            benchmarks.put(test.toString(), test);
        }
        return this;
    }

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode();
        root.addNode(ROOT_NODE_NAME);
        session.save();

        if (CUSTOM_TYPE) {
            NodeTypeUtils.createNodeType(session, CUSTOM_DESC_NODE_TYPE,
                    new String[] {DATE_PROP, SORT_PROP, FILTER_PROP, TITLE_PROP},
                    new int[] {PropertyType.DATE, PropertyType.BOOLEAN, PropertyType.STRING,
                            PropertyType.STRING},
                    new String[0], new String[] {CUSTOM_DESC_NODE_TYPE}, null, false);
            NodeTypeUtils.createNodeType(session, CUSTOM_ROOT_NODE_TYPE,
                    new String[] {DATE_PROP, SORT_PROP, FILTER_PROP, TITLE_PROP},
                    new int[] {PropertyType.DATE, PropertyType.BOOLEAN, PropertyType.STRING,
                            PropertyType.STRING},
                    new String[0], new String[] {CUSTOM_DESC_NODE_TYPE}, null, false);
            nodeTypes.add(CUSTOM_ROOT_NODE_TYPE);
            nodeTypes.add(CUSTOM_DESC_NODE_TYPE);
        }

        if (INDEX) {
            createIndexes(session);
        }
    }

    protected void createIndexes(Session session) throws RepositoryException {
        Map<String, Map<String, String>> orderedMap = Maps.newHashMap();
        String persistencePath = "";

        switch (INDEX_TYPE) {
            case ORDERED:
                // define ordered indexes on properties
                OakIndexUtils.orderedIndexDefinition(session, "customIndexParent", ASYNC_INDEX,
                    new String[] {DATE_PROP}, false,
                    (nodeTypes.isEmpty() ? new String[0] : new String[] {nodeTypes.get(0)}),
                    OrderedIndex.OrderDirection.DESC.getDirection());
                OakIndexUtils.orderedIndexDefinition(session, "customIndexDescendant", ASYNC_INDEX,
                        new String[]{DATE_PROP}, false,
                        (nodeTypes.isEmpty() ? new String[0]: new String[] {nodeTypes.get(1)}),
                        OrderedIndex.OrderDirection.DESC.getDirection());
                break;
            // define lucene index on properties
            case LUCENE_FILE:
                persistencePath =
                    "target" + StandardSystemProperty.FILE_SEPARATOR.value() + "lucene" + String
                        .valueOf(System.currentTimeMillis());
                OakIndexUtils.luceneIndexDefinition(session, "customIndex", ASYNC_INDEX,
                    new String[] {FILTER_PROP, DATE_PROP},
                    new String[] {PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_DATE},
                    null, persistencePath);
                break;
            case LUCENE_FILE_DOC:
                persistencePath =
                    "target" + StandardSystemProperty.FILE_SEPARATOR.value() + "lucene" + String
                        .valueOf(System.currentTimeMillis());
            case LUCENE_DOC:
                Map<String, String> propMap = Maps.newHashMap();
                propMap.put(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
                orderedMap.put(DATE_PROP, propMap);
            case LUCENE:
                OakIndexUtils.luceneIndexDefinition(session, "customIndex", ASYNC_INDEX,
                    new String[] {FILTER_PROP, DATE_PROP},
                    new String[] {PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_DATE},
                    orderedMap, persistencePath);
                break;
            case PROPERTY:
                break;
        }
    }

    /**
     * Executes before each test run
     */
    @Override
    public void beforeIteration(ExecutionContext context) throws RepositoryException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Started beforeIteration()");
        }

        // Contextualize the node types being used
        if (nodeTypes != null && !nodeTypes.isEmpty()) {
            context.getMap().put(CTX_ACT_NODE_TYPE_PROP, nodeTypes.get(0));
            context.getMap().put(CTX_REL_NODE_TYPE_PROP, nodeTypes.get(1));
        }

        // recreate paths created in this run
        searchRootPaths = newArrayList();
        searchDescPaths = newArrayList();

        // create the blob load for this iteration
        createLoad(context);
        long loadFinish = System.currentTimeMillis();

        context.getMap().put(CTX_ROOT_NODE_NAME_PROP, ROOT_NODE_NAME);
        context.getMap().put(CTX_SEARCH_PATHS_PROP, searchRootPaths);
        context.getMap().put(CTX_DESC_SEARCH_PATHS_PROP, searchDescPaths);

        waitBeforeIterationFinish(loadFinish);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished beforeIteration()");
        }
    }

    protected void waitBeforeIterationFinish(long loadFinish) {
        IndexStatsMBean indexStatsMBean = WhiteboardUtils.getService(whiteboard, IndexStatsMBean.class);

        if (indexStatsMBean != null) {
            String lastIndexedTime = indexStatsMBean.getLastIndexedTime();
            while (((lastIndexedTime == null)
                || ISO8601.parse(lastIndexedTime).getTimeInMillis() < loadFinish)) {
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Waiting for async indexing to finish");
                    }
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LOG.error("Error waiting for async index to finish", e);
                }
                lastIndexedTime = indexStatsMBean.getLastIndexedTime();
            }

            LOG.info("Execution Count {}", indexStatsMBean.getExecutionCount());
            LOG.info("Execution Time {}", indexStatsMBean.getExecutionTime());
            LOG.info("Consolidated Execution Stats {}", indexStatsMBean.getConsolidatedExecutionStats());
        }
    }

    /**
     * Creates the load for the search.
     *
     * @param context the context
     * @throws RepositoryException the repository exception
     */
    protected void createLoad(ExecutionContext context) throws RepositoryException {
        // Creates assets for this run

        SynchronizedDescriptiveStatistics writeStats = new SynchronizedDescriptiveStatistics();

        List<Thread> loadThreads = newArrayList();
        for (int idx = 0; idx < LOADERS; idx++) {
            /* Each loader will write to a directory of the form load-idx */
            Thread t =
                    new Thread(getWriter(context, writeStats, idx),
                            "LoadThread-" + idx);
            loadThreads.add(t);
            t.start();
        }

        // wait for the load threads to finish
        for (Thread t : loadThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.error("Exception waiting for join ", e);
            }
        }
        
        LOG.info("Write stats");
        LOG.info(String.format(
            "# min     10%%     50%%     90%%     max       N%n"));
        LOG.info(String.format(
            "%6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d%n",
            writeStats.getMin(),
            writeStats.getPercentile(10.0),
            writeStats.getPercentile(50.0),
            writeStats.getPercentile(90.0),
            writeStats.getMax(),
            writeStats.getN()));
    }

    protected Writer getWriter(ExecutionContext context,
            SynchronizedDescriptiveStatistics writeStats, int idx) throws RepositoryException {
        return new Writer((context.getIncrement() + "-" + idx),
                (context.getIncrement() * Integer.parseInt(NODE_LEVELS.get(0)))
                        / LOADERS,
                writeStats);
    }

    @Override
    protected void executeBenchmark(final ScalabilityBenchmark benchmark,
            final ExecutionContext context) throws Exception {

        LOG.info("Started pre benchmark hook : {}", benchmark);
        benchmark.beforeExecute(getRepository(), CREDENTIALS, context);

        LOG.info("Started execution : {}", benchmark);
        if (PROFILE) {
            context.startProfiler();
        }
        //Execute the benchmark with the number threads configured 
        List<Thread> threads = newArrayListWithCapacity(TESTERS);
        for (int idx = 0; idx < TESTERS; idx++) {
            Thread t = new Thread("Tester-" + idx) {
                @Override
                public void run() {
                    try {
                        benchmark.execute(getRepository(), CREDENTIALS, context);
                    } catch (Exception e) {
                        LOG.error("Exception in benchmark execution ", e);
                    }
                }
            };
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                LOG.error("Exception in search thread join ", e);
            }
        }
        context.stopProfiler();

        LOG.info("Started post benchmark hook : {}", benchmark);
        benchmark.afterExecute(getRepository(), CREDENTIALS, context);
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    oak.with((QueryIndexProvider) provider)
                            .with((Observer) provider)
                            .with(new LuceneIndexEditorProvider());

                    if (!Strings.isNullOrEmpty(ASYNC_INDEX) && ASYNC_INDEX
                        .equals(IndexConstants.ASYNC_PROPERTY_NAME)) {
                        oak.withAsyncIndexing();
                    }

                    if (FULL_TEXT) {
                        oak.with(new LuceneInitializerHelper("luceneGlobal", storageEnabled));
                    }

                    whiteboard = oak.getWhiteboard();
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

    private synchronized void addRootSearchPath(String path) {
        int limit = 1000;
        if (searchRootPaths.size() < limit) {
            searchRootPaths.add(path);
        } else if (random.nextDouble() < 0.5) {
            searchRootPaths.set(random.nextInt(limit), path);
        }
    }

    private synchronized void addDescSearchPath(String path) {
        int limit = 1000;
        if (searchDescPaths.size() < limit) {
            searchDescPaths.add(path);
        } else if (random.nextDouble() < 0.5) {
            searchDescPaths.set(random.nextInt(limit), path);
        }
    }

    /**
     * Creates a node hierarchy as follows:
     *
     * <pre>
     * {@code
     *  /LongevitySearchAssets<ID>
     *      /writer<ID>
     *          /Node<ID>
     *              jcr:primaryType : <oak:Unstructured|rootType|nt:unstructured>
     *              added : <DATE>
     *              viewed : <true|false>
     *              filter : <true|false>
     *              title : Node<ID>
     *              /SubNode<ID>
     *                  jcr:primaryType : <oak:Unstructured|descendantTypeType|nt:unstructured>
     *                  added : <DATE>
     *                  viewed : <true|false>
     *                  filter : <true|false>
     *                  title : SubNode<ID>
     * }
     * </pre>
     */
    class Writer implements Runnable {

        final Node parent;

        final Session session;

        final String id;

        final SynchronizedDescriptiveStatistics stats;

        long counter;

        int secsIn2Years = 31622400;

        Calendar start;

        long startMillis;

        Timer timer;

        /** The maximum number of assets to be written by this thread. */
        final int maxAssets;

        Writer(String id, int maxAssets, SynchronizedDescriptiveStatistics writeStats)
                throws RepositoryException {
            this.id = id;
            this.maxAssets = maxAssets;
            this.stats = writeStats;
            this.session = loginWriter();
            this.parent = session
                    .getRootNode()
                    .getNode(ROOT_NODE_NAME)
                    .addNode("writer-" + id);
            start = Calendar.getInstance();
            start.add(Calendar.YEAR, -2);
            start.setTimeZone(TimeZone.getTimeZone("GMT"));
            startMillis = start.getTimeInMillis();

            session.save();

            timer = new Timer(writeStats);
        }

        protected Calendar generateDate() {
            if (RAND_DATE) {
                start.setTimeInMillis(startMillis + random.nextInt(secsIn2Years));
            } else {
                start.add(Calendar.SECOND, 1);
            }
            return start;
        }

        @Override
        public void run() {
            try {
                int count = 1;
                while (count <= maxAssets) {
                    session.refresh(false);

                    // skip creation of child nodes based on the defined DENSITY_LEVEL
                    Node node =
                            createParent(parent, (random.nextInt(100) <= DENSITY_LEVEL), "Node"
                                    + count);

                    // record for searching and reading
                    addRootSearchPath(node.getPath());

                    if ((counter + 1) % 1000 == 0) {
                        LOG.info("Thread " + id + " - Added nodes : " + (counter));
                    }
                    count++;
                }
            } catch (Exception e) {
                LOG.error("Exception in load creation ", e);
            }
            LOG.info("Max Assets created by " + id + " - " + counter);
        }

        private Node createParent(Node parent, boolean createChildren, String name) throws Exception {
            Node node = createNode(parent, 0, name);

            if (createChildren) {
                createChildren(node, 1);
            }

            return node;
        }

        private void createChildren(Node parent, int levelIdx) throws Exception {
            if (levelIdx > NODE_LEVELS.size() - 1) {
                return;
            }

            // Recursively create sub nodes
            for (int idx = 0; idx < Integer.parseInt(NODE_LEVELS.get(levelIdx)); idx++) {
                Node subNode =
                        createNode(parent, levelIdx, "SubNode-" + levelIdx + "-" + idx);
                addDescSearchPath(subNode.getPath());

                createChildren(subNode, (levelIdx + 1));
            }
        }

        /**
         * Creates the node.
         *
         * @param parent the parent
         * @param levelIdx the level idx
         * @param name the name
         * @return the node
         * @throws Exception the exception
         */
        private Node createNode(Node parent, int levelIdx, String name)
                throws Exception {

            timer.start();
            Node node =
                    JcrUtils.getOrAddNode(parent, name, getType(levelIdx));
            // Add relevant properties
            node.setProperty(DATE_PROP, generateDate());
            node.setProperty(SORT_PROP, toss());
            node.setProperty(FILTER_PROP, toss());
            node.setProperty(TITLE_PROP, name);

            session.save();
            counter++;
            if (LOG.isDebugEnabled()) {
                LOG.debug(node.getPath());
            }

            // Record time taken for creation
            timer.stop();

            return node;
        }

        /**
         * Order of precedence is customNodeType, oak:Unstructured, nt:unstructured.
         *
         * @param levelIdx the hierarchy level of node (root or descendant)
         * @return the type
         * @throws RepositoryException the repository exception
         */
        protected String getType(int levelIdx) throws RepositoryException {
            String typeOfNode = (levelIdx == 0 ? CTX_ACT_NODE_TYPE_PROP : CTX_REL_NODE_TYPE_PROP);

            String type = NodeTypeConstants.NT_UNSTRUCTURED;
            if (context.getMap().containsKey(typeOfNode)) {
                type = (String) context.getMap().get(typeOfNode);
            } else if (parent.getSession().getWorkspace().getNodeTypeManager().hasNodeType(
                    NodeTypeConstants.NT_OAK_UNSTRUCTURED)) {
                type = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
                context.getMap().put(typeOfNode, type);
            }
            return type;
        }

        private boolean toss() {
            int tossOutcome = random.nextInt(2);
            return tossOutcome == 0;
        }
    }

    static class Timer {
        private final Stopwatch watch;
        private final SynchronizedDescriptiveStatistics stats;

        public Timer(SynchronizedDescriptiveStatistics stats) {
            watch = Stopwatch.createUnstarted();
            this.stats = stats;
        }

        public void start() {
            if (watch.isRunning()) {
                watch.stop();
                watch.reset();
            }
            watch.start();
        }

        public void stop() {
            watch.stop();
            stats.addValue(watch.elapsed(TimeUnit.MILLISECONDS));
            watch.reset();
        }
    }
}

