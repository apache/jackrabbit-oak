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
package org.apache.jackrabbit.oak.scalability.suites;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFormatException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.VersionException;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.benchmark.TestInputStream;
import org.apache.jackrabbit.oak.benchmark.util.Date;
import org.apache.jackrabbit.oak.benchmark.util.MimeType;
import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.scalability.util.NodeTypeUtils;


/**
 * The suite test will incrementally increase the load and execute searches.
 * Each test run thus adds blobs and executes different searches. This way we measure time taken for
 * search(es) execution.
 *
 * <p>
 * The following system JVM properties can be defined to configure the suite.
 * <ul>
 * <li>
 *     <code>fileWriters</code> - Controls the number of concurrent background threads for writing blobs.
 *     Defaults to 0.
 * </li>
 * <li>
 *     <code>fileReaders</code> - Controls the number of concurrent background threads for reading blobs.
 *     Defaults to 1.
 * </li>
 * <li>
 *     <code>fileSize</code> - Controls the size in KB of the blobs. Defaults to 1.
 * </li>
 * <li>
 *     <code>maxAssets</code> - Controls the max child nodes created under a node. Defaults to 500.
 * </li>
 * </ul>
 */
public class ScalabilityBlobSearchSuite extends ScalabilityNodeSuite {
    private static final int FILE_SIZE = Integer.getInteger("fileSize", 1);

    /**
     * Controls the number of concurrent threads for writing blobs
     */
    private static final int WRITERS = Integer.getInteger("fileWriters", 0);

    /**
     * Controls the number of concurrent thread for reading blobs
     */
    private static final int READERS = Integer.getInteger("fileReaders", 0);

    /**
     * Controls the max child nodes created under a node.
     */
    private static final int MAX_ASSETS_PER_LEVEL = Integer.getInteger("maxAssets", 500);

    public static final String CTX_FILE_NODE_TYPE_PROP = "nodeType";

    private static final String CUSTOM_PATH_PROP = "contentPath";

    private static final String CUSTOM_REF_PROP = "references";

    private static final String CUSTOM_NODE_TYPE = "Asset";

    private static final String CUSTOM_INDEX_TYPE = "AssetIndex";

    private final Random random = new Random(29);

    private List<String> searchPaths;

    private List<String> readPaths;
    private String nodeType;
    private String indexType;

    public ScalabilityBlobSearchSuite(Boolean storageEnabled) {
        super(storageEnabled);
    }


    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode();
        root.addNode(ROOT_NODE_NAME);
        session.save();

        if (CUSTOM_TYPE) {
            indexType =
                    NodeTypeUtils.createNodeType(session, CUSTOM_INDEX_TYPE, null, null, null,
                            null, null, true);
            setNodeType(NodeTypeUtils.createNodeType(
                    session, CUSTOM_NODE_TYPE,
                    new String[] {CUSTOM_PATH_PROP, CUSTOM_REF_PROP},
                    new int[] {PropertyType.STRING, PropertyType.STRING},                    
                    new String[] {indexType}, null, NodeTypeConstants.NT_FILE, false));
        } else {
            String type = NodeTypeConstants.NT_UNSTRUCTURED;
            if (session.getWorkspace().getNodeTypeManager().hasNodeType(
                    NodeTypeConstants.NT_OAK_UNSTRUCTURED)) {
                type = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
            }
            setNodeType(type);
        }

        // defining indexes
        if (INDEX) {
            OakIndexUtils.propertyIndexDefinition(session, NodeTypeConstants.JCR_MIMETYPE,
                new String[] {NodeTypeConstants.JCR_MIMETYPE}, false,
                (Strings.isNullOrEmpty(indexType) ? new String[0] : new String[] {indexType}));
            OakIndexUtils
                .orderedIndexDefinition(session, NodeTypeConstants.JCR_LASTMODIFIED, ASYNC_INDEX,
                    new String[] {NodeTypeConstants.JCR_LASTMODIFIED}, false,
                    (Strings.isNullOrEmpty(indexType) ? new String[0] : new String[] {indexType}),
                    null);
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

        // recreate paths created in this run
        searchPaths = newArrayList();
        readPaths = newArrayListWithCapacity(READERS);

        // create the blob load for this iteration
        createLoad(context);

        // Add background jobs to simulate workload
        for (int i = 0; i < WRITERS; i++) {
            /* Each writer will write to a directory of the form load-b-i */
            addBackgroundJob(new BlobWriter(String.valueOf(context.getIncrement() + "-b-" + i), 1,
                    null));
        }
        for (int i = 0; i < READERS; i++) {
            addBackgroundJob(new Reader());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Finish beforeIteration()");
        }

        context.getMap().put(CTX_ROOT_NODE_NAME_PROP, ROOT_NODE_NAME);
        context.getMap().put(CTX_SEARCH_PATHS_PROP, searchPaths);
    }

    @Override
    protected Writer getWriter(ExecutionContext context,
            SynchronizedDescriptiveStatistics writeStats, int idx) throws RepositoryException {
        return new BlobWriter((context.getIncrement() + "-" + idx),
                context.getIncrement() / LOADERS, writeStats);
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

    public String getNodeType() {
        return nodeType;
    }

    protected void setNodeType(String nodeType) {
        this.nodeType = nodeType;
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
                LOG.error("Exception in reader execution ", e);
            }
        }

    }

    /**
     * Creates a node hierarchy similar to the below structure. Here a file Level0Level1Level2File0 is created
     * which has a reference to Leve0Level1Level3File10:
     *
     * <pre>
     * {@code
     *  /LongevitySearchAssets<ID>
     *      /writer<ID>
     *          /0
     *              /1
     *                  /2
     *                      /Level0Level1Level2File0
     *                          jcr:primaryType : <oak:Unstructured|Asset|nt:unstructured>
     *                          /jcr:content
     *                              jcr:mimeType : <MIMETYPE>
     *                              jcr:lastModified : <DATE>
     *                              jcr:data : <BINARY>
     *                              customPathProp : /LongevitySearchAssets<ID>/writer<ID>/0/1/2/Leve0Level1Level2File0
     *                              references : /LongevitySearchAssets<ID>/writer<ID>/0/1/3/Leve0Level1Level3File10
     * }
     * </pre>
     */
    private class BlobWriter extends Writer implements Runnable {
        BlobWriter(String id, int maxAssets, SynchronizedDescriptiveStatistics writeStats)
                throws RepositoryException {
            super(id, maxAssets, writeStats);
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

                    Stopwatch watch = Stopwatch.createStarted();

                    Node file = putFile(fileNamePrefix, parentDir);
                    session.save();

                    if (stats != null) {
                        stats.addValue(watch.elapsed(TimeUnit.MILLISECONDS));
                    }

                    // record for searching and reading
                    addReadPath(file.getPath());
                    addSearchPath(fileNamePrefix);

                    if (LOG.isDebugEnabled() && (count + 1) % 1000 == 0) {
                        LOG.debug("Thread " + id + " - Added assets : " + (count + 1));
                    }
                    count++;
                }
            } catch (Exception e) {
                LOG.error("Exception in load creation ", e);
            }
        }

        /**
         * Puts the file at the given path with the given prefix.
         * 
         * @param fileNamePrefix the prefix for the filename
         * @param parentDir the parent dir of the file
         * @return the node
         * @throws RepositoryException
         * @throws UnsupportedRepositoryOperationException
         * @throws ValueFormatException
         * @throws VersionException
         * @throws LockException
         * @throws ConstraintViolationException
         */
        private Node putFile(String fileNamePrefix, String parentDir) throws RepositoryException {
            Node filepath = JcrUtils.getOrAddNode(parent, parentDir, getParentType());
            Node file =
                    JcrUtils.getOrAddNode(filepath,
                            (fileNamePrefix + "File" + counter++),
                            getType());

            Binary binary =
                    parent.getSession().getValueFactory().createBinary(
                            new TestInputStream(FILE_SIZE * 1024));
            try {
                Node content =
                        JcrUtils.getOrAddNode(file, Node.JCR_CONTENT, NodeType.NT_RESOURCE);
                if (indexType != null) {
                    content.addMixin(CUSTOM_INDEX_TYPE);
                    file.addMixin(CUSTOM_INDEX_TYPE);
                }
                content.setProperty(Property.JCR_MIMETYPE, MimeType.randomMimeType().getValue());
                content.setProperty(Property.JCR_LAST_MODIFIED, Date.randomDate().getCalendar());
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

        /**
         * Gets the node type of the parent.
         * 
         * @return the parent type
         * @throws RepositoryException the repository exception
         */
        protected String getParentType() throws RepositoryException {
            String type = NodeTypeConstants.NT_UNSTRUCTURED;
            if (parent.getSession().getWorkspace().getNodeTypeManager().hasNodeType(
                    NodeTypeConstants.NT_OAK_UNSTRUCTURED)) {
                type = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
            }
            return type;
        }

        /**
         * Order of precedence is customNodeType, oak:Unstructured, nt:unstructured
         * 
         * @return the type
         * @throws RepositoryException
         */
        protected String getType() throws RepositoryException {
            String type = NodeTypeConstants.NT_UNSTRUCTURED;
            if (!context.getMap().containsKey(CTX_FILE_NODE_TYPE_PROP)) {
                if (getNodeType() != null) {
                    type = getNodeType();
                } else if (parent.getSession().getWorkspace().getNodeTypeManager().hasNodeType(
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
         * @param levels the levels for the file
         * @return the prefix
         */
        private String getFileNamePrefix(List<String> levels) {
            StringBuilder name = new StringBuilder();
            for (String level : levels) {
                name.append("Level").append(level);
            }
            return name.toString();
        }

        private String getParentSuffix(List<String> levels) {
            StringBuilder parentSuffix = new StringBuilder();
            for (String level : levels) {
                parentSuffix.append(level).append("/");
            }
            return parentSuffix.toString();
        }

        /**
         * Assigns the asset to it appropriate folder. The folder hierarchy is constructed such that
         * each
         * folder has only MAX_ASSETS_PER_LEVEL children.
         * 
         * @param assetNum the asset number
         * @param maxAssets the max no. of assets to be created
         * @param levels the no. of levels to create
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

