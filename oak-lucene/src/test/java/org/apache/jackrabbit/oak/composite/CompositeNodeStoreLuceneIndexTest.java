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
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@SuppressWarnings("ConstantConditions")
public class CompositeNodeStoreLuceneIndexTest extends CompositeNodeStoreQueryTestBase {

    private static String READ_ONLY_MOUNT_V1_NAME = "readOnlyV1";
    private static String READ_ONLY_MOUNT_V2_NAME = "readOnlyV2";
    // JCR repository
    private CompositeRepo repoV1;
    private CompositeRepo repoV2;
    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;

    public CompositeNodeStoreLuceneIndexTest(NodeStoreKind root, NodeStoreKind mounts) {
        super(root, mounts);
    }

    @Override
    @Before
    public void initStore() throws Exception {
        globalStore = register(nodeStoreRoot.create(null));
        repoV1 = new CompositeRepo(READ_ONLY_MOUNT_V1_NAME);
        repoV1.initCompositeRepo();
    }

    @After
    public void tearDown() throws Exception {
        repoV1.cleanup();
        if (repoV2 != null) {
            repoV2.cleanup();
        }
    }

    /**
     * Steps overview -
     * Add a new index to composite repo and reindex
     * Add the index def in global read write part of composite repo (read write part)
     * Add the index def in read only repo
     * Add content served by this index in both parts
     * Query using composite repo session to see if index is used and results from both parts are returned correctly
     * Reindex on composite session
     */
    @Test
    public void addLuceneIndexAndReindex() throws Exception {
        // create indexes in read-only and read write parts of repo 
        // and content covered by them and check if index is used properly
        repoV1.setupIndexAndContentInRepo("luceneTest", "foo", false, VERSION_1);

        Session s1 = repoV1.getSession();
        Node luceneTest = s1.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode("luceneTest");
        long reindexCount = luceneTest.getProperty(REINDEX_COUNT).getValue().getLong();

        // Now we reindex and see everything works fine
        luceneTest.setProperty(REINDEX_PROPERTY_NAME, true);
        s1.save();

        long reindexCount2 = luceneTest.getProperty(REINDEX_COUNT).getValue().getLong();
        assertEquals(reindexCount2, reindexCount + 1);
    }

    /**
     * Given a composite node store , create an index in read-write part
     * with the same index node already existing in the read-only part already.
     */
    @Test
    public void addIndexInReadWriteWithIndexExistinginReadOnly() throws Exception {
        repoV1.setupIndexAndContentInRepo("luceneTest", "foo", true, VERSION_1);
    }

    @Test
    public void reindexCounterIndex() throws Exception {
        Session s = repoV1.getSession();
        Node c = s.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode("counter");

        c.setProperty("async", (String) null);
        c.setProperty("resolution", 1);
        c.setProperty("reindex", true);
        s.save();

        assertFalse(c.getProperty("reindex").getBoolean());
    }

    /**
     * Given a composite jcr repo with a lucene index with indexed data from both read only and read write parts
     * We create a V2 of this repo which will have the lucene index removed -
     * Expected behaviour - The same query that returned results from both readonly
     * and readwrite in V1 should now return
     * results - but it would be a traversal query and not use the index .
     */
    @Test
    public void removeLuceneIndex() throws Exception {
        repoV1.setupIndexAndContentInRepo("luceneTest", "foo", false, VERSION_1);

        repoV2 = new CompositeRepo(READ_ONLY_MOUNT_V2_NAME);

        repoV2.getReadOnlyRoot().getNode("libs").addNode("node-foo-0").setProperty("foo", "bar");
        repoV2.getReadOnlySession().save();

        repoV2.initCompositeRepo();

        // Now we  have a read only V2 setup that doesn't have the lucene index and also doesn't have the
        // USE_IF_EXISTS node that would enable the index .
        // So now composite repo V2 - this will act as 2nd version of composite app
        // This uses same global read write store as used in V1

        // Check query traverses - and doesn't use the index available in read-write part
        // since it is now disabled as path corresponding to useIfExists property is not present in new read only lib
        QueryResult result = repoV2.executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("allDescendents: /"));

        // Check that proper nodes are returned by the query 
        // even after traversal from both readonly version 2 and global read write parts
        result = repoV2.executeQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo/node-0, /content-foo/node-1, " +
                "/libs/node-foo-0", getResult(result, "jcr:path"));

        // Now just for sake of completeness - check that the index is still used if we use V1 of composite app.
        result = repoV1.executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("/oak:index/luceneTest"));

        result = repoV1.executeQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo/node-0, " +
                "/content-foo/node-1, " +
                "/libs/node-luceneTest-0, " +
                "/libs/node-luceneTest-1, " +
                "/libs/node-luceneTest-2", getResult(result, "jcr:path"));

        // At this point we have a V2 of app up and runnin with the index removed . 
        // We can now close the V1 of the app
        // and remove the index defintion from the global repo

        repoV1.cleanup();

        repoV2.getRoot().getNode(INDEX_DEFINITIONS_NAME).getNode("luceneTest").remove();

        result = repoV2.executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("allDescendents: /"));

        // Check that proper nodes are returned by the query 
        // even after traversal from both readonly version 2 and global read write parts
        result = repoV2.executeQuery("/jcr:root//*[@foo = 'bar'] order by jcr:path", "xpath");
        assertEquals("/content-foo/node-0, " +
                "/content-foo/node-1, " +
                "/libs/node-foo-0", getResult(result, "jcr:path"));

        repoV2.cleanup();

    }

    /**
     * Steps Overview -
     * Create 2 indexes A and B and corresponding content in V1 app - in both global/composite  and readonly v1 repos
     * Create 2 indexes B2 and C in read only v2 - B2 is effective replacement of B and C is a new index .
     * Create respective content and new indexes in global repo as well.
     * Create a version 2 of composite repo using the new read only repo V2
     * Check that B2 and C are NOT used in composite repo V1 and are correctly used in composite repo V2
     * Check that A and B are NOT used in composite repo V2 .
     */
    @Test
    public void updateLuceneIndex() throws Exception {

        // Create 2 index defintions in Version 1
        // A
        repoV1.setupIndexAndContentInRepo("luceneTest", "foo", false, VERSION_1);
        // B
        repoV1.setupIndexAndContentInRepo("luceneTest2", "foo2", false, VERSION_1);

        // Now initialize V2 composite Repo
        repoV2 = new CompositeRepo(READ_ONLY_MOUNT_V2_NAME);
        repoV2.initCompositeRepo();

        // Now Create 2 index definitons in Version 2 - one supposed to replace B and one entirely new
        // C
        repoV2.setupIndexAndContentInRepo("luceneTest3", "foo3", false, VERSION_2);
        // B2
        repoV2.setupIndexAndContentInRepo("luceneTest2_V2", "foo2", false, VERSION_2);

        // Check V2 now uses luceneTest2_V2 for foo2 and no index for foo i.e traversal
        QueryResult result = repoV2.executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("allDescendents: /"));

        // Check that proper nodes are returned by the query 
        // even after traversal from both readonly version 2 and global read write parts
        result = repoV2.executeQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo/node-0, /content-foo/node-1", getResult(result, "jcr:path"));

        // Checking for prop foo2 now
        result = repoV2.executeQuery("explain /jcr:root//*[@foo2 = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("/oak:index/luceneTest2_V2"));

        result = repoV2.executeQuery("/jcr:root//*[@foo2 = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo2/node-0, /content-foo2/node-1, " +
                "/libs/node-luceneTest2_V2-0, " +
                "/libs/node-luceneTest2_V2-1, " +
                "/libs/node-luceneTest2_V2-2", getResult(result, "jcr:path"));

        // Checking for foo3 now - new index on V2
        result = repoV2.executeQuery("explain /jcr:root//*[@foo3 = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("/oak:index/luceneTest3"));

        // Check that proper nodes are returned by the query 
        // even after traversal from both readonly version 2 and global read write parts
        result = repoV2.executeQuery("/jcr:root//*[@foo3 = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo3/node-0, " +
                "/content-foo3/node-1, " +
                "/libs/node-luceneTest3-0, " +
                "/libs/node-luceneTest3-1, " +
                "/libs/node-luceneTest3-2", getResult(result, "jcr:path"));

        // Now check that the V1 instance still uses B for foo2 , A for foo and traverses for foo3
        result = repoV1.executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("/oak:index/luceneTest"));

        result = repoV1.executeQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo/node-0, " +
                "/content-foo/node-1, " +
                "/libs/node-luceneTest-0, " +
                "/libs/node-luceneTest-1, " +
                "/libs/node-luceneTest-2", getResult(result, "jcr:path"));

        // foo 2 check
        result = repoV1.executeQuery("explain /jcr:root//*[@foo2 = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("/oak:index/luceneTest2"));

        result = repoV1.executeQuery("/jcr:root//*[@foo2 = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo2/node-0, " +
                "/content-foo2/node-1, " +
                "/libs/node-luceneTest2-0, " +
                "/libs/node-luceneTest2-1, " +
                "/libs/node-luceneTest2-2", getResult(result, "jcr:path"));

        // foo 3 check
        repoV1.login();
        result = repoV1.executeQuery("explain /jcr:root//*[@foo3 = 'bar']", "xpath");
        assertThat(result.getRows().next().toString(),
                containsString("allDescendents: /"));

        result = repoV1.executeQuery("/jcr:root//*[@foo3 = 'bar'] order by @jcr:path", "xpath");
        assertEquals("/content-foo3/node-0, " +
                "/content-foo3/node-1", getResult(result, "jcr:path"));
    }

    private static String getResult(QueryResult result, String propertyName) throws RepositoryException {
        StringBuilder buff = new StringBuilder();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            buff.append(it.nextRow().getValue(propertyName).getString());
        }
        return buff.toString();
    }

    private static Node createLuceneIndex(JackrabbitSession session, String name, String property) throws Exception {
        Node n = session.getRootNode().getNode(INDEX_DEFINITIONS_NAME);
        n = n.addNode(name);
        n.setPrimaryType(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        n.setProperty("compatVersion", 2);
        n.setProperty(TYPE_PROPERTY_NAME, "lucene");
        n.setProperty(REINDEX_PROPERTY_NAME, true);
        n.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        n.setProperty("includePropertyNames", new String[]{property});
        session.save();
        return n;
    }

    private static Node addOrGetNode(Node parent, String path, String primaryType) throws Exception {
        return parent.hasNode(path) ? parent.getNode(path) : parent.addNode(path, primaryType);
    }

    private class CompositeRepo {
        private Repository compositeRepository;
        private JackrabbitSession compositeSession;
        private Node compositeRoot;
        private QueryManager compositeQueryManager;

        private NodeStore readOnlyStore;
        private Repository readOnlyRepository;
        private CompositeNodeStore store;
        private MountInfoProvider mip;
        private JackrabbitSession readOnlySession;
        private Node readOnlyRoot;
        private String readOnlyMountName;

        private boolean cleanedUp;

        public Node getReadOnlyRoot() {
            return readOnlyRoot;
        }

        public JackrabbitSession getReadOnlySession() {
            return readOnlySession;
        }

        public JackrabbitSession getSession() {
            return compositeSession;
        }

        public Node getRoot() {
            return compositeRoot;
        }

        public QueryResult executeQuery(String statement, String language) throws RepositoryException {
            return compositeQueryManager.createQuery(statement, language).execute();
        }

        CompositeRepo(String readOnlyMountName) throws Exception {
            this.readOnlyMountName = readOnlyMountName;
            this.readOnlyStore = register(mounts.create(readOnlyMountName));

            this.mip = Mounts.newBuilder().readOnlyMount(readOnlyMountName, "/libs").build();

            initReadOnlySeedRepo();
            List<MountedNodeStore> nonDefaultStores = new ArrayList<>();
            nonDefaultStores.add(new MountedNodeStore(this.mip.getMountByName(readOnlyMountName), readOnlyStore));
            this.store = new CompositeNodeStore(this.mip, globalStore, nonDefaultStores);

        }

        private void initCompositeRepo() throws Exception {
            compositeRepository = createJCRRepository(this.store, this.mip);
            compositeSession = (JackrabbitSession) compositeRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            compositeRoot = compositeSession.getRootNode();
            compositeQueryManager = compositeSession.getWorkspace().getQueryManager();
        }

        private void initReadOnlySeedRepo() throws Exception {
            readOnlyRepository = createJCRRepository(readOnlyStore, this.mip);
            readOnlySession = (JackrabbitSession) readOnlyRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            readOnlyRoot = readOnlySession.getRootNode();
            Node libs = readOnlyRoot.addNode("libs", NT_UNSTRUCTURED);
            libs.setPrimaryType(NT_UNSTRUCTURED);
        }

        private void setupIndexAndContentInRepo(String indexName, String indexedProperty, boolean createIndexInReadOnlyFirst, int version) throws Exception {
            String versionProp = (version == VERSION_1) ? "@v1" : "@v2";

            if (createIndexInReadOnlyFirst) {
                setupIndexAndContentInReadOnly(indexName, indexedProperty, version);
            }

            Node indexNode = createLuceneIndex(compositeSession, indexName, indexedProperty);
            indexNode.setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/" + indexName + "/" + versionProp);
            indexNode.setProperty(FulltextIndexConstants.PROP_REFRESH_DEFN, true);

            // Add some content to read write now

            Node n1 = addOrGetNode(compositeRoot, "content-" + indexedProperty, NT_UNSTRUCTURED);
            for (int i = 0; i < 2; i++) {
                Node n2 = addOrGetNode(n1, "node-" + i, NT_UNSTRUCTURED);
                n2.setProperty(indexedProperty, "bar");
                n2.setPrimaryType(NT_UNSTRUCTURED);
            }
            compositeSession.save();

            if (!createIndexInReadOnlyFirst) {
                setupIndexAndContentInReadOnly(indexName, indexedProperty, version);
            }
            ///////////////////////////////////////////////////////////////////////////////////////////
            // Need to restart the composite repo after changes done to the read only part - needed for segement node store
            // Otherwise the changes in read only repo are not visible to the cached reader.
            restartRepo();
            ///////////////////////////////////////////////////////////////////////////////////////////

            // Need to save the composite session and login again for query to return results frorm read only part
            compositeSession.save();

            login();
            QueryResult result = executeQuery("explain /jcr:root//*[@" + indexedProperty + " = 'bar']", "xpath");

            assertThat(result.getRows().next().toString(),
                    containsString("/oak:index/" + indexName));

            result = executeQuery("/jcr:root//*[@" + indexedProperty + " = 'bar'] order by @jcr:path", "xpath");
            assertEquals("/content-" + indexedProperty + "/node-0, " +
                    "/content-" + indexedProperty + "/node-1, " +
                    "/libs/node-" + indexName + "-0, " +
                    "/libs/node-" + indexName + "-1, " +
                    "/libs/node-" + indexName + "-2", getResult(result, "jcr:path"));
        }

        private void setupIndexAndContentInReadOnly(String indexName, String indexedProperty, int version) throws Exception {
            String versionProp = (version == 1) ? "@v1" : "@v2";
            // Add some content to read-only  repo
            for (int i = 0; i < 3; i++) {
                Node b = readOnlyRoot.getNode("libs").addNode("node-" + indexName + "-" + i, NT_UNSTRUCTURED);
                b.setProperty(indexedProperty, "bar");
                b.setPrimaryType(NT_UNSTRUCTURED);
            }

            // index creation
            Node readOnlyIndex = createLuceneIndex(readOnlySession, indexName, indexedProperty);
            readOnlyIndex.setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/" + indexName + "/@" + versionProp);
            readOnlyIndex.setProperty(FulltextIndexConstants.PROP_REFRESH_DEFN, true);

            // Add /libs/indexes/luceneTest/@v1 to make this index usable/enabled
            addOrGetNode(readOnlyRoot.getNode("libs"), "indexes", NT_UNSTRUCTURED).
                    addNode(indexName, NT_UNSTRUCTURED).setProperty(versionProp.replace("@", ""), true);
            readOnlySession.save();
        }

        private void restartRepo() throws Exception {
            compositeSession.logout();
            shutdown(compositeRepository);

            List<MountedNodeStore> nonDefaultStores = new ArrayList<>();
            nonDefaultStores.add(new MountedNodeStore(this.mip.getMountByName(readOnlyMountName), readOnlyStore));
            this.store = new CompositeNodeStore(this.mip, globalStore, nonDefaultStores);
            compositeRepository = createJCRRepository(this.store, this.mip);
            login();
        }

        private void login() throws Exception {
            if (compositeSession != null) {
                compositeSession.logout();
            }
            compositeSession = (JackrabbitSession) compositeRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            compositeRoot = compositeSession.getRootNode();
            compositeQueryManager = compositeSession.getWorkspace().getQueryManager();
        }

        private void cleanup() {
            if (!cleanedUp) {
                compositeSession.logout();
                shutdown(compositeRepository);
                readOnlySession.logout();
                shutdown(readOnlyRepository);
            }
            cleanedUp = true;
        }

    }
}
