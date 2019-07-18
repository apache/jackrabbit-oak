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


import com.google.common.collect.Lists;
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
import static org.junit.Assert.assertThat;

@SuppressWarnings("ConstantConditions")
public class CompositeNodeStoreLuceneIndexTest extends CompositeNodeStoreQueryTestBase{

    // version 1
    private NodeStore readOnlyStoreV1;
    private Repository readOnlyRepositoryV1;
    private CompositeNodeStore storeV1;
    private MountInfoProvider mipV1;
    private JackrabbitSession readOnlyV1Session;
    private Node readOnly1Root;


    // version 2
    private NodeStore readOnlyStoreV2;
    private Repository readOnlyRepositoryV2;
    private CompositeNodeStore storeV2;
    private MountInfoProvider mipV2;
    private JackrabbitSession readOnlyV2Session;
    private Node readOnly2Root;

    // JCR repository

    private CompositeRepo compositeRepoV1;
    private CompositeRepo compositeRepoV2;
    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;



    List<JackrabbitSession> sessions = new ArrayList<>();
    List<Repository> repositories = new ArrayList<>();

    public CompositeNodeStoreLuceneIndexTest(NodeStoreKind root, NodeStoreKind mounts) {
        super(root, mounts);
    }

    @Override
    @Before
    public void initStore() throws Exception {
        globalStore = register(nodeStoreRoot.create(null));
        readOnlyStoreV1 = register(mounts.create("readOnlyv1"));
        readOnlyStoreV2 = register(mounts.create("readOnlyv2"));

        initMounts();
        readOnlyRepositoryV1 = createJCRRepository(readOnlyStoreV1, mipV1);
        readOnlyV1Session = (JackrabbitSession) readOnlyRepositoryV1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        readOnly1Root = readOnlyV1Session.getRootNode();

        Node libs = readOnly1Root.addNode("libs", NT_UNSTRUCTURED);
        libs.setPrimaryType(NT_UNSTRUCTURED);

        readOnlyRepositoryV2 = createJCRRepository(readOnlyStoreV2, mipV2);
        readOnlyV2Session = (JackrabbitSession) readOnlyRepositoryV2.login(new SimpleCredentials("admin", "admin".toCharArray()));
        readOnly2Root = readOnlyV2Session.getRootNode();
        libs = readOnly2Root.addNode("libs", NT_UNSTRUCTURED);
        libs.setPrimaryType(NT_UNSTRUCTURED);
        readOnlyV2Session.save();

        repositories.add(readOnlyRepositoryV1);
        repositories.add(readOnlyRepositoryV2);
        sessions.add(readOnlyV1Session);
        sessions.add(readOnlyV2Session);

    }

    void initMounts() throws Exception {

        mipV1 = Mounts.newBuilder().readOnlyMount("readOnlyv1", "/libs").build();
        List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();
        nonDefaultStores.add(new MountedNodeStore(mipV1.getMountByName("readOnlyv1"), readOnlyStoreV1));

        // This will be V1
        storeV1 = new CompositeNodeStore(mipV1, globalStore, nonDefaultStores);

        compositeRepoV1 =  new CompositeRepo(createJCRRepository(storeV1, mipV1), VERSION_1);

        // Now Setting up mount info for V2 - and creating stoveV2 - the corresponding JCR repo can be created in individual tests later on
        mipV2 = Mounts.newBuilder().readOnlyMount("readOnlyv2", "/libs").build();
        List<MountedNodeStore> nonDefaultStores2 = Lists.newArrayList();
                nonDefaultStores2.add(new MountedNodeStore(mipV2.getMountByName("readOnlyv2"), readOnlyStoreV2));
        storeV2 = new CompositeNodeStore(mipV2, globalStore, nonDefaultStores2);
    }

    /*
    Steps overview -
    Add a new index to composite repo and reindex
    Add the index def in global read write part of composite repo (read write part)
    Add the index def in read only repo
    Add content served by this index in both parts
    Query using composite repo session to see if index is used and results from both parts are returned correctly
    Reindex on composite session
     */
    @Test
    public void luceneIndexAddAndReindex() throws Exception {
        setupIndexAndContentInComposite("luceneTest", "foo", compositeRepoV1); // This should create indexes in readonly and read write parts of repo and content convered by them and check if index is used properly

        // Now we reindex and see everything works fine
        long reindexCount = compositeRepoV1.getCompositeRoot().getNode(INDEX_DEFINITIONS_NAME).getNode("luceneTest").getProperty(REINDEX_COUNT).getValue().getLong();
        compositeRepoV1.getCompositeRoot().getNode(INDEX_DEFINITIONS_NAME).getNode("luceneTest").setProperty(REINDEX_PROPERTY_NAME,true);
        compositeRepoV1.getCompositeSession().save();
        assertEquals(compositeRepoV1.getCompositeRoot().getNode(INDEX_DEFINITIONS_NAME).getNode("luceneTest").getProperty(REINDEX_COUNT).getValue().getLong(), reindexCount + 1);
        compositeRepoV1.cleanup();
    }

    /*
    Given a composite jcr repo with a lucene index with indexed data from both read only and read write parts
    We create a V2 of this repo which will have the lucene index removed -
    Expected behaviour - The same query that returned resutls from both readonly and readwrite in V1 should now return
    results - but it would be a traversal query and not use the index .
     */
    @Test
    public void luceneRemoveIndex() throws Exception {
        setupIndexAndContentInComposite("luceneTest", "foo", compositeRepoV1);

        readOnly2Root.getNode("libs").addNode("node-foo-0").setProperty("foo","bar");
        readOnlyV2Session.save();

        // Now we already have a read only V2 setup in the before method that doesn't have the lucene index and also doesn't have the
        // USE_IF_EXISTS node that would enable the index .
        // So now we create a composite store repository with this read only version - this will act as 2nd version of composite app
        // We use the same global read write store as used in V1

        compositeRepoV2 =  new CompositeRepo(createJCRRepository(storeV2, mipV2), VERSION_2);


        QueryResult result = compositeRepoV2.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo = 'bar']", "xpath").execute();

        // Check query traverses - and doesn't use the index available in read-write part
        // since it is now disabled as path corresponding to useIfExists property is not present in new read only lib
        assertThat(result.getRows().next().toString(),
                containsString("/* traverse \"//*\" where ([a].[foo] = 'bar'"));

        result = compositeRepoV2.getCompositeQueryManager().createQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath").execute();

        // Check that proper nodes are returned by the query even after traversal from both readonly version 2 and global read write parts
        assertEquals("/content-foo/node-0, /content-foo/node-1, " +
                "/libs/node-foo-0", getResult(result,"jcr:path"));

        // Now just for sake of completeness - check that the index is still used if we use V1 of composite app.
        result = compositeRepoV1.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* lucene:luceneTest(/oak:index/luceneTest) foo:bar"));

        result = compositeRepoV1.getCompositeQueryManager().createQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath").execute();


        assertEquals("/content-foo/node-0, /content-foo/node-1, " +
                "/libs/node-luceneTest-0, /libs/node-luceneTest-1, /libs/node-luceneTest-2", getResult(result,"jcr:path"));


        // At this point we have a V2 of app up and runnin with the index removed . We can now close the V1 of the app
        // and remove the index defintion from the global repo

        compositeRepoV1.cleanup();

        compositeRepoV2.getCompositeRoot().getNode(INDEX_DEFINITIONS_NAME).getNode("luceneTest").remove();

        result = compositeRepoV2.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* traverse \"//*\" where ([a].[foo] = 'bar'"));
        result = compositeRepoV2.getCompositeQueryManager().createQuery("/jcr:root//*[@foo = 'bar'] order by jcr:path", "xpath").execute();

        // Check that proper nodes are returned by the query even after traversal from both readonly version 2 and global read write parts
        assertEquals("/content-foo/node-0, /content-foo/node-1, " +
                "/libs/node-foo-0", getResult(result,"jcr:path"));
        compositeRepoV2.cleanup();

    }
    /*
    Steps Overview -
    Create 2 indexes A and B and corresponding content in V1 app - in both global/composite  and readonly v1 repos
    Create 2 indexes B2 and C in read only v2 - B2 is effective replacement of B and C is a new index . Create respective content
    and new indexes in global repo as well.
    Create a version 2 of composite repo using the new read only repo V2
    Check that B2 and C are NOT used in composite repo V1 and are correctly used in composite repo V2
    Check that A and B are NOT used in composite repo V2 .
     */
    @Test
    public void updateLuceneIndex() throws Exception {

        // Create 2 index defintions in Version 1
        setupIndexAndContentInComposite("luceneTest", "foo", compositeRepoV1); // A
        setupIndexAndContentInComposite("luceneTest2", "foo2", compositeRepoV1); //B

        // Now initialize V2 composite Repo

        compositeRepoV2 =  new CompositeRepo(createJCRRepository(storeV2, mipV2), VERSION_2);

        // Now Create 2 index definitons in Version 2 - one supposed to replace B and one entirely new
        setupIndexAndContentInComposite("luceneTest3", "foo3", compositeRepoV2); // C
        setupIndexAndContentInComposite("luceneTest2_V2", "foo2", compositeRepoV2); //B2

        // Check V2 now uses luceneTest2_V2 for foo2 and no index for foo i.e traversal

        QueryResult result = compositeRepoV2.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* traverse \"//*\" where ([a].[foo] = 'bar'"));

        result = compositeRepoV2.getCompositeQueryManager().createQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath").execute();

        // Check that proper nodes are returned by the query even after traversal from both readonly version 2 and global read write parts
        assertEquals("/content-foo/node-0, /content-foo/node-1", getResult(result,"jcr:path"));

        // Checking for prop foo2 now

        result = compositeRepoV2.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo2 = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* lucene:luceneTest2_V2(/oak:index/luceneTest2_V2) foo2:bar"));

        result = compositeRepoV2.getCompositeQueryManager().createQuery("/jcr:root//*[@foo2 = 'bar'] order by @jcr:path", "xpath").execute();

        assertEquals("/content-foo2/node-0, /content-foo2/node-1, " +
                "/libs/node-luceneTest2_V2-0, /libs/node-luceneTest2_V2-1, /libs/node-luceneTest2_V2-2", getResult(result,"jcr:path"));


        // Checking for foo3 now - new index on V2
        result = compositeRepoV2.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo3 = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* lucene:luceneTest3(/oak:index/luceneTest3) foo3:bar"));

        result = compositeRepoV2.getCompositeQueryManager().createQuery("/jcr:root//*[@foo3 = 'bar'] order by @jcr:path", "xpath").execute();

        // Check that proper nodes are returned by the query even after traversal from both readonly version 2 and global read write parts
        assertEquals("/content-foo3/node-0, /content-foo3/node-1, " +
                "/libs/node-luceneTest3-0, /libs/node-luceneTest3-1, /libs/node-luceneTest3-2", getResult(result,"jcr:path"));



        // Now check that the V1 instance still uses B for foo2 , A for foo and traverses for foo3

        result = compositeRepoV1.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* lucene:luceneTest(/oak:index/luceneTest) foo:bar"));

        result = compositeRepoV1.getCompositeQueryManager().createQuery("/jcr:root//*[@foo = 'bar'] order by @jcr:path", "xpath").execute();

        assertEquals("/content-foo/node-0, /content-foo/node-1, " +
                "/libs/node-luceneTest-0, /libs/node-luceneTest-1, /libs/node-luceneTest-2", getResult(result,"jcr:path"));

        // foo 2 check

        result = compositeRepoV1.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo2 = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* lucene:luceneTest2(/oak:index/luceneTest2) foo2:bar"));

        result = compositeRepoV1.getCompositeQueryManager().createQuery("/jcr:root//*[@foo2 = 'bar'] order by @jcr:path", "xpath").execute();

        assertEquals("/content-foo2/node-0, /content-foo2/node-1, " +
                "/libs/node-luceneTest2-0, /libs/node-luceneTest2-1, /libs/node-luceneTest2-2", getResult(result,"jcr:path"));

        // foo 3 check
        compositeRepoV1.login();
        result = compositeRepoV1.getCompositeQueryManager().createQuery("explain /jcr:root//*[@foo3 = 'bar']", "xpath").execute();
        assertThat(result.getRows().next().toString(),
                containsString("/* traverse \"//*\" where ([a].[foo3] = 'bar'"));

        result = compositeRepoV1.getCompositeQueryManager().createQuery("/jcr:root//*[@foo3 = 'bar'] order by @jcr:path", "xpath").execute();

        assertEquals("/content-foo3/node-0, /content-foo3/node-1", getResult(result,"jcr:path"));

        compositeRepoV1.cleanup();
        compositeRepoV2.cleanup();
    }

    @After
    public void cleanup() throws Exception {
        for (JackrabbitSession session : sessions) session.logout();
        for (Repository repo : repositories) shutdown(repo);
    }

    private void setupIndexAndContentInComposite(String indexName, String indexedProperty, CompositeRepo compositeRepo) throws Exception {

        // Add Index def to both read only (V1) and read-write(global/shared) parts

        Repository compositeRepository;
        JackrabbitSession compositeSession;
        Node compositeRoot;
        QueryManager queryManager;

        String versionProp = (compositeRepo.getVersion() == VERSION_1) ? "@v1" : "@v2";

        createLuceneIndex(compositeRepo.getCompositeSession(), indexName, indexedProperty);
        compositeRepo.getCompositeRoot().getNode(INDEX_DEFINITIONS_NAME).getNode(indexName).setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/" + indexName + "/" + versionProp);
        compositeRepo.getCompositeRoot().getNode(INDEX_DEFINITIONS_NAME).getNode(indexName).setProperty(FulltextIndexConstants.PROP_REFRESH_DEFN, true);

        // Add some content to read write now

        Node n1 = addOrGetNode(compositeRepo.getCompositeRoot(), "content-" + indexedProperty, NT_UNSTRUCTURED);
        for (int i = 0; i < 2; i++) {
            Node n2 = addOrGetNode(n1, "node-" + i, NT_UNSTRUCTURED);
            n2.setProperty(indexedProperty, "bar");
            n2.setPrimaryType(NT_UNSTRUCTURED);
        }

        compositeRepo.getCompositeSession().save();
        if (compositeRepo.getVersion() == VERSION_1) {
            setupIndexAndContentInReadOnlyV1(indexName, indexedProperty, compositeRepo.getVersion());
        } else {
            setupIndexAndContentInReadOnlyV2(indexName, indexedProperty, compositeRepo.getVersion());
        }
        ///////////////////////////////////////////////////////////////////////////////////////////
        // Need to restart the composite repo after changes done to the read only part - needed for segement node store
        // Other wise the changes in read only repo are not visible to the cached reader.
        compositeRepo = restartCompositeRepo(compositeRepo.getVersion());
        ///////////////////////////////////////////////////////////////////////////////////////////

        // Need to save the composite session and login again for query to return results frorm read only part
        compositeRepo.getCompositeSession().save();

        compositeRepo.login();
        QueryResult result = compositeRepo.getCompositeQueryManager().createQuery("explain /jcr:root//*[@" + indexedProperty + " = 'bar']", "xpath").execute();


        assertThat(result.getRows().next().toString(),
                containsString("/* lucene:" + indexName + "(/oak:index/" + indexName + ") " + indexedProperty + ":bar"));

        result = compositeRepo.getCompositeQueryManager().createQuery("/jcr:root//*[@" + indexedProperty + " = 'bar'] order by @jcr:path", "xpath").execute();


        assertEquals("/content-" + indexedProperty + "/node-0, /content-" + indexedProperty + "/node-1, " +
        "/libs/node-" + indexName + "-0, /libs/node-" + indexName + "-1, /libs/node-" + indexName + "-2", getResult(result,"jcr:path"));

        // ****This point we have V1 of our app with index properly configured in read only V1 and and global read write****
    }

    void setupIndexAndContentInReadOnlyV1(String indexName, String indexedProperty, int version) throws Exception {
        String versionProp = (version == VERSION_1) ? "@v1" : "@v2";
        // Add some content to read-only v1 repo
        for (int i = 0; i < 3; i++) {
            Node b = readOnly1Root.getNode("libs").addNode("node-" + indexName + "-" + i, NT_UNSTRUCTURED);
            b.setProperty(indexedProperty, "bar");
            b.setPrimaryType(NT_UNSTRUCTURED);
        }

        // index creation
        createLuceneIndex(readOnlyV1Session, indexName, indexedProperty);
        readOnly1Root.getNode(INDEX_DEFINITIONS_NAME).getNode(indexName).setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/" + indexName + "/@" + versionProp);
        readOnly1Root.getNode(INDEX_DEFINITIONS_NAME).getNode(indexName).setProperty(FulltextIndexConstants.PROP_REFRESH_DEFN,true);

        // Add /libs/indexes/luceneTest/@v1 to make this index usable/enabled
        addOrGetNode(readOnly1Root.getNode("libs"), "indexes", NT_UNSTRUCTURED).addNode(indexName, NT_UNSTRUCTURED).setProperty(versionProp.replace("@", ""), true);
        readOnlyV1Session.save();
    }

    void setupIndexAndContentInReadOnlyV2(String indexName, String indexedProperty, int version) throws Exception {
        String versionProp = (version == 1) ? "@v1" : "@v2";
        // Add some content to read-only v1 repo
        for (int i = 0; i < 3; i++) {
            Node b = readOnly2Root.getNode("libs").addNode("node-" + indexName + "-" + i, NT_UNSTRUCTURED);
            b.setProperty(indexedProperty, "bar");
            b.setPrimaryType(NT_UNSTRUCTURED);
        }

        // index creation
        createLuceneIndex(readOnlyV2Session, indexName, indexedProperty);
        readOnly2Root.getNode(INDEX_DEFINITIONS_NAME).getNode(indexName).setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/" + indexName + "/@" + versionProp);
        readOnly2Root.getNode(INDEX_DEFINITIONS_NAME).getNode(indexName).setProperty(FulltextIndexConstants.PROP_REFRESH_DEFN,true);

        // Add /libs/indexes/luceneTest/@v1 to make this index usable/enabled
        addOrGetNode(readOnly2Root.getNode("libs"), "indexes", NT_UNSTRUCTURED).addNode(indexName, NT_UNSTRUCTURED).setProperty(versionProp.replace("@", ""), true);
        readOnlyV2Session.save();
    }

    private String getResult(QueryResult result, String propertyName) throws RepositoryException {
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

    private void createLuceneIndex(JackrabbitSession session, String name, String property) throws Exception {

        Node n = session.getRootNode().getNode(INDEX_DEFINITIONS_NAME);
        n = n.addNode(name);
        n.setPrimaryType(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        n.setProperty("compatVersion", 2);
        n.setProperty(TYPE_PROPERTY_NAME, "lucene");
        n.setProperty(REINDEX_PROPERTY_NAME, true);
        n.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        n.setProperty("includePropertyNames", new String[]{property});
        session.save();
    }

    private Node addOrGetNode(Node parent, String path, String primaryType) throws Exception{
        return parent.hasNode(path) ? parent.getNode(path) : parent.addNode(path, primaryType);
    }

    private CompositeRepo restartCompositeRepo(int version) throws Exception{
        List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();
        if (version == VERSION_1) {
            compositeRepoV1.cleanup();
            nonDefaultStores.add(new MountedNodeStore(mipV1.getMountByName("readOnlyv1"), readOnlyStoreV1));
            storeV1 = new CompositeNodeStore(mipV1, globalStore, nonDefaultStores);
            return compositeRepoV1 =  new CompositeRepo(createJCRRepository(storeV1, mipV1), VERSION_1);
        } else {
            compositeRepoV2.getClass();
            nonDefaultStores.add(new MountedNodeStore(mipV2.getMountByName("readOnlyv2"), readOnlyStoreV2));
            storeV2 = new CompositeNodeStore(mipV2, globalStore, nonDefaultStores);
            return compositeRepoV2 =  new CompositeRepo(createJCRRepository(storeV2, mipV2), VERSION_2);
        }
    }

    private class CompositeRepo {
        private Repository compositeRepository;
        private JackrabbitSession compositeSession;
        private Node compositeRoot;
        private QueryManager queryManager;
        private int version;

        public Repository getCompositeRepository() {
            return compositeRepository;
        }

        public int getVersion() {
            return version;
        }

        public JackrabbitSession getCompositeSession() {
            return compositeSession;
        }

        public Node getCompositeRoot() {
            return compositeRoot;
        }

        public QueryManager getCompositeQueryManager() {
            return queryManager;
        }

        CompositeRepo(Repository compositeRepository, int version) throws Exception{
            this.compositeRepository = compositeRepository;
            this.version = version;
            compositeSession = (JackrabbitSession) compositeRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            compositeRoot = compositeSession.getRootNode();
            queryManager = compositeSession.getWorkspace().getQueryManager();
        }

        private void login() throws Exception{
            compositeSession = (JackrabbitSession) compositeRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            compositeRoot = compositeSession.getRootNode();
            queryManager = compositeSession.getWorkspace().getQueryManager();
        }

        private void cleanup() {
            this.compositeSession.logout();
            shutdown(this.compositeRepository);
        }
    }
}
