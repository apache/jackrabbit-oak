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
package org.apache.jackrabbit.oak.scalability;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.util.Calendar;
import java.util.List;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFormatException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.VersionException;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.benchmark.TestInputStream;
import org.apache.jackrabbit.oak.benchmark.util.Date;
import org.apache.jackrabbit.oak.benchmark.util.MimeType;
import org.apache.jackrabbit.oak.fixture.JcrCustomizer;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * The suite test will incrementally increase the load and execute searches.
 * Each test run thus adds blobs and executes different searches. This way we measure time taken for
 * search(es) execution.
 * 
 */
public class ScalabilityBlobSearchSuite extends ScalabilityAbstractSuite {
    private static final int FILE_SIZE = Integer.getInteger("fileSize", 1);

    /**
     * Controls the number of concurrent threads for loading blobs initially
     */
    private static final int LOADERS = Integer.getInteger("loaders", 1);

    /**
     * Controls the number of concurrent threads for writing blobs
     */
    private static final int WRITERS = Integer.getInteger("fileWriters", 0);

    /**
     * Controls the number of concurrent thread for reading blobs
     */
    private static final int READERS = Integer.getInteger("fileReaders", 0);

    /**
     * Controls the number of concurrent thread for searching
     */
    private static final int SEARCHERS = Integer.getInteger("fileSearchers", 1);

    /**
     * Controls the max child nodes created under a node.
     */
    private static final int MAX_ASSETS_PER_LEVEL = Integer.getInteger("maxAssets", 500);

    public static final String CTX_SEARCH_PATHS_PROP = "searchPaths";
    
    public static final String CTX_ROOT_NODE_NAME_PROP = "rootNodeName";
    
    public static final String CTX_FILE_NODE_TYPE_PROP = "nodeType";

    private static final String CUSTOM_PATH_PROP = "contentPath";

    private static final String CUSTOM_REF_PROP = "references";

    public static final String FORMAT_PROP = "format";

    protected static final String ROOT_NODE_NAME =
            "LongevitySearchAssets" + TEST_ID;

    private final Random random = new Random(29);

    private List<String> searchPaths;

    private List<String> readPaths;

    private final Boolean storageEnabled;

    public ScalabilityBlobSearchSuite(Boolean storageEnabled) {
        this.storageEnabled = storageEnabled;
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

        // defining indexes
        createIndexDefinition(root, FORMAT_PROP, PropertyIndexEditorProvider.TYPE,
            new String[] { FORMAT_PROP }, false);

        session.save();
    }

    /**
     * create a new index definition
     * 
     * @param root the root node of the repository
     * @param indexDefinitionName the name of the node for the index definition
     * @param indexType the type of the index. Eg {@code property} or {@code ordered}
     * @param propertyNames the list of properties to index
     * @param unique if unique or not
     * @return the node just created
     * @throws RepositoryException
     */
    private static Node createIndexDefinition(final Node root, final String indexDefinitionName,
            final String indexType, final String[] propertyNames,
            final boolean unique) throws RepositoryException {
        Node indexDefRoot = JcrUtils.getOrAddNode(root, IndexConstants.INDEX_DEFINITIONS_NAME,
            NodeTypeConstants.NT_UNSTRUCTURED);
        Node indexDef = JcrUtils.getOrAddNode(indexDefRoot, indexDefinitionName,
            IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, indexType);
        indexDef.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        indexDef.setProperty(IndexConstants.PROPERTY_NAMES, propertyNames,
            PropertyType.NAME);
        indexDef.setProperty(IndexConstants.UNIQUE_PROPERTY_NAME, unique);
        // TODO declaring nodeTypes
        // TODO any additional properties
        return indexDef;
    }
    
    /**
     * Executes before each test run
     */
    @Override
    public void beforeIteration(ExecutionContext context) throws RepositoryException {
        if (DEBUG) {
            System.out.println("Started beforeIteration()");
        }

        // recreate paths created in this run
        searchPaths = newArrayList();
        readPaths = newArrayListWithCapacity(READERS);
        
        // create the blob load for this iteration
        createLoad(context);
        
        // Add background jobs to simulate workload
        for (int i = 0; i < WRITERS; i++) {
            /* Each writer will write to a directory of the form load-b-i */
            addBackgroundJob(new Writer(String.valueOf(context.getIncrement() + "-b-" + i), 1));
        }
        for (int i = 0; i < READERS; i++) {
            addBackgroundJob(new Reader());
        }
        
        if (DEBUG) {
            System.out.println("Finish beforeIteration()");
        }
        
        context.getMap().put(CTX_ROOT_NODE_NAME_PROP, ROOT_NODE_NAME);
        context.getMap().put(CTX_SEARCH_PATHS_PROP, searchPaths);
    }

    /**
     * Creates the load for the search.
     *
     * @param context the context
     * @throws RepositoryException the repository exception
     */
    private void createLoad(ExecutionContext context) throws RepositoryException {
        // Creates assets for this run
        List<Thread> loadThreads = newArrayList();
        for(int idx = 0; idx < LOADERS; idx++) {
            /* Each loader will write to a directory of the form load-idx */
            Thread t = new Thread(new Writer((context.getIncrement() + "-" + idx), 
                                        context.getIncrement()/LOADERS), 
                                    "LoadThread-" + idx);
            loadThreads.add(t);
            t.start();
        }
        
        // wait for the load threads to finish
        for(Thread t : loadThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void executeBenchmark(final ScalabilityBenchmark benchmark, final ExecutionContext context) throws Exception {
        //Execute the benchmark with the number threads configured 
        List<Thread> threads = newArrayListWithCapacity(SEARCHERS);
        for (int idx = 0; idx < SEARCHERS;idx++) {
            Thread t = new Thread("Search-" + idx) {
                @Override
                public void run() {
                    try {
                        benchmark.execute(getRepository(), CREDENTIALS, context);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            threads.add(t);
            t.start();
        }
        
        for(Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCustomizer() {
                @Override
                public Jcr customize(Jcr jcr) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    jcr.with((QueryIndexProvider) provider)
                            .with((Observer) provider)
                            .with(new LuceneIndexEditorProvider())
                            .with(new LuceneInitializerHelper("luceneGlobal", storageEnabled));
                    return jcr;
                }
            });
        }
        return super.createRepository(fixture);
    }

    private synchronized String getRandomReadPath() {
        if (readPaths.isEmpty()) {
            return "";
        } else {
            return readPaths.get(random.nextInt(readPaths.size()));
        }
    }

    private synchronized void addReadPath(String file) {
        // Limit the number of paths added to be no more than the number of readers to limit the
        // heap used.
        int limit = 1000;
        if (readPaths.size() < limit) {
            readPaths.add(file);
        } else if (random.nextDouble() < 0.5) {
            readPaths.set(random.nextInt(limit), file);
        }
    }

    private synchronized void addSearchPath(String path) {
        if (!searchPaths.contains(path)) {
            searchPaths.add(path);
        }
    }
    
    private class Reader implements Runnable {

        private final Session session = loginWriter();

        @Override
        public void run() {
            try {
                String path = getRandomReadPath();
                session.refresh(false);
                JcrUtils.readFile(
                        session.getNode(path), new NullOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private class Writer implements Runnable {

        private final Node parent;
        
        private final Session session;
        
        private long counter;
        
        private final String id;
        
        /** The maximum number of assets to be written by this thread. */
        private final int maxAssets;

        Writer(String id, int maxAssets) throws RepositoryException {
            this.id = id;
            this.maxAssets = maxAssets;
            this.session = loginWriter();
            this.parent = session
                    .getRootNode()
                    .getNode(ROOT_NODE_NAME)
                    .addNode("writer-" + id);
            session.save();
        }

        @Override
        public void run() {
            try {
                int count = 0;
                while (count < maxAssets) {
                    session.refresh(false);

                    List<String> levels = Lists.newArrayList();
                    getParentLevels(count, maxAssets, levels);

                    String fileNamePrefix = getFileNamePrefix(levels);
                    String parentDir = getParentSuffix(levels);

                    Node file = putFile(fileNamePrefix, parentDir);

                    session.save();

                    // record for searching and reading
                    addReadPath(file.getPath());
                    addSearchPath(fileNamePrefix);

                    if (DEBUG && (count + 1) % 1000 == 0) {
                        System.out.println("Thread " + id + " - Added assets : " + (count + 1));
                    }
                    count++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private Node putFile(String fileNamePrefix, String parentDir) throws RepositoryException,
                UnsupportedRepositoryOperationException, ValueFormatException, VersionException,
                LockException, ConstraintViolationException {
            String type = getType();

            Node filepath = JcrUtils.getOrAddNode(parent, parentDir, type);
            Node file =
                    JcrUtils.getOrAddNode(filepath,
                            (fileNamePrefix + "File" + counter++),
                            type);
            
            // adding a custom format/mime-type for later querying.
            file.setProperty(FORMAT_PROP, MimeType.randomMimeType().getValue());
            // adding a last modified for later querying
            file.setProperty(Property.JCR_LAST_MODIFIED, Date.randomDate().getCalendar());
            
            Binary binary =
                    parent.getSession().getValueFactory().createBinary(
                            new TestInputStream(FILE_SIZE * 1024));
            try {
                Node content =
                        JcrUtils.getOrAddNode(file, Node.JCR_CONTENT, NodeType.NT_RESOURCE);

                content.setProperty(Property.JCR_MIMETYPE, "application/octet-stream");
                content.setProperty(Property.JCR_LAST_MODIFIED, Calendar.getInstance());
                content.setProperty(Property.JCR_DATA, binary);

                file.setProperty(CUSTOM_PATH_PROP, file.getPath());
                String reference = getRandomReadPath();
                if (!Strings.isNullOrEmpty(reference)) {
                    file.setProperty(CUSTOM_REF_PROP, reference);
                }
            } finally {
                binary.dispose();
            }
            return file;
        }

        private String getType() throws RepositoryException {
            String type = NodeTypeConstants.NT_UNSTRUCTURED;
            if (!context.getMap().containsKey(CTX_FILE_NODE_TYPE_PROP)) {
                if (parent.getSession().getWorkspace().getNodeTypeManager().hasNodeType(
                        NodeTypeConstants.NT_OAK_UNSTRUCTURED)) {
                    type = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
                }
                context.getMap().put(CTX_FILE_NODE_TYPE_PROP, type);
            } else {
                type = (String) context.getMap().get(CTX_FILE_NODE_TYPE_PROP);
            }
            return type;
        }


        /**
         * Create a handy filename to search known files.
         * 
         * @param levels
         * @return
         */
        private String getFileNamePrefix(List<String> levels) {
            String name = "";
            for (String level : levels) {
                name = name + "Level" + level;
            }
            return name;
        }

        private String getParentSuffix(List<String> levels) {
            String parentSuffix = "";
            for (String level : levels) {
                parentSuffix = parentSuffix + level + "/";
            }
            return parentSuffix;
        }

        /**
         * Assigns the asset to it appropriate folder. The folder hierarchy is constructed such that
         * each
         * folder has only MAX_ASSETS_PER_LEVEL children.
         * 
         * @param assetNum
         * @param maxAssets
         * @param levels
         */
        private void getParentLevels(long assetNum, long maxAssets,
                List<String> levels) {

            int maxAssetsNextLevel =
                    (int) Math.ceil((double) maxAssets / (double) MAX_ASSETS_PER_LEVEL);
            long nextAssetBucket = assetNum / maxAssetsNextLevel;

            levels.add(String.valueOf(nextAssetBucket));
            if (maxAssetsNextLevel > MAX_ASSETS_PER_LEVEL) {
                getParentLevels((assetNum - nextAssetBucket * maxAssetsNextLevel),
                        maxAssetsNextLevel,
                        levels);
            }
        }
    }
}

