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
package org.apache.jackrabbit.oak.run.osgi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.sql.DataSource;
import org.apache.commons.io.IOUtils;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreStatsMBean;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

public class DocumentNodeStoreConfigTest extends AbstractRepositoryFactoryTest {

    private PojoServiceRegistry registry;
    private final MongoConnection mongoConn = MongoUtils.getConnection();

    @Override
    protected PojoServiceRegistry getRegistry() {
        return registry;
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (mongoConn != null) {
            MongoUtils.dropCollections(mongoConn.getDatabase());
        }
    }

    @Test
    public void testRDBDocumentStore() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDB;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(), ds, new Hashtable<>(Map.of("datasource.name", "oak")));

        //2. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = Map.of(
                "org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService",
                Map.of("documentStoreType", "RDB"));
        createConfig(map);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);
        // OAK-9668: H2 2.0.206 has a limit of 1MB for BINARY VARYING
        AbstractBlobStore blobStore = (AbstractBlobStore) getServiceWithWait(BlobStore.class);
        blobStore.setBlockSize(1024 * 1024);

        //3. Check that DS contains tables from both RDBBlobStore and RDBDocumentStore
        assertTrue(getExistingTables(ds).containsAll(new ArrayList<>(Arrays.asList("NODES", "DATASTORE_META"))));

        //4. Check that only one cluster node was instantiated
        assertEquals(1, getIdsOfClusterNodes(ds).size());
        testBlobStoreStats(ns);
        testDocumentStoreStats(ns);
    }

    @Test
    public void testRDBDocumentStore2Datasources() throws Exception {
        // see https://issues.apache.org/jira/browse/OAK-5098
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDB;DB_CLOSE_DELAY=-1");
        ServiceRegistration fds = registry.registerService(DataSource.class.getName(),
                ds,
                new Hashtable<>(Map.of("datasource.name", "oak")));

        //2. Register another DataSource as a service with the same name
        DataSource ds2 = createDS("jdbc:h2:mem:testRDB;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(), ds2, new Hashtable<>(Map.of("datasource.name", "oak")));

        //3. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(1);
        map1.put("documentStoreType", "RDB");
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        //4. unregister first DS
        fds.unregister();

        //5. check that nodestore is gone
        TimeUnit.MILLISECONDS.sleep(500);
        assertNoService(NodeStore.class);
    }

    @Test
    public void testRDBDocumentStoreRestart() {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDBrestart;DB_CLOSE_DELAY=-1");
        ServiceRegistration<?> srds = registry.registerService(DataSource.class.getName(),
                ds,
                new Hashtable<>(Map.of("datasource.name", "oak")));

        //2. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(1);
        map1.put("documentStoreType", "RDB");
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        //3. Shut down ds
        // Wait for service to be unregistered after at most 5s.
        // Previously, we waited only 500ms; this was extended due to
        // occasional test failures on Jenkins (see OAK-5612). If 5s
        // are not sufficient, we should investigate some more.
        awaitServiceEvent(srds::unregister,
                AbstractRepositoryFactoryTest.classNameFilter(NodeStore.class.getName()),
                ServiceEvent.UNREGISTERING,
                5,
                TimeUnit.SECONDS);
        assertNull(registry.getServiceReference(NodeStore.class.getName()));

        //4. Restart ds, service should still be down
        registry.registerService(DataSource.class.getName(),
                ds,
                new Hashtable<>(Map.of("datasource.name", "oak")));

        assertNoService(NodeStore.class);
    }

    @Test
    public void testRDBDocumentStoreLateDataSource() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(1);
        map1.put("documentStoreType", "RDB");
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        //2. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDBlateds;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(), ds, new Hashtable<>(Map.of("datasource.name", "oak")));

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        //3. Check that DS contains tables from both RDBBlobStore and RDBDocumentStore
        assertTrue(getExistingTables(ds).containsAll(new ArrayList<>(Arrays.asList("NODES", "DATASTORE_META"))));

        //4. Check that only one cluster node was instantiated
        assertEquals(1, getIdsOfClusterNodes(ds).size());
    }

    @Test
    public void testRDBDocumentStore_CustomBlobDataSource() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds1 = createDS("jdbc:h2:mem:testRDB1;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(), ds1, new Hashtable<>(Map.of("datasource.name", "oak")));

        //2. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(1);
        map1.put("documentStoreType", "RDB");
        map1.put("blobDataSource.target", "(datasource.name=oak-blob)");
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        DataSource ds2 = createDS("jdbc:h2:mem:testRDB2;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(),
                ds2,
                new Hashtable<>(Map.of("datasource.name", "oak-blob")));

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        //3. Check that DS contains tables from RDBBlobStore and RDBDocumentStore
        //in there respective DataStores
        List<String> ds1Tables = getExistingTables(ds1);
        List<String> ds2Tables = getExistingTables(ds2);

        //DS1 should contain RDBDocumentStore tables
        assertTrue(ds1Tables.contains("NODES"));
        assertFalse(ds1Tables.contains("DATASTORE_META"));

        //DS2 should contain only RDBBlobStore tables
        assertFalse(ds2Tables.contains("NODES"));
        assertTrue(ds2Tables.contains("DATASTORE_META"));

        //4. Check that only one cluster node was instantiated
        assertEquals(1, getIdsOfClusterNodes(ds1).size());
    }

    @Test
    public void testRDBDocumentStore_CustomBlobStore() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds1 = createDS("jdbc:h2:mem:testRDB3;DB_CLOSE_DELAY=-1");
        ServiceRegistration sdsds = registry.registerService(DataSource.class.getName(),
                ds1,
                new Hashtable<>(Map.of("datasource.name", "oak")));

        DataSource ds2 = createDS("jdbc:h2:mem:testRDB3b;DB_CLOSE_DELAY=-1");
        ServiceRegistration sdsbs = registry.registerService(DataSource.class.getName(),
                ds2,
                new Hashtable<>(Map.of("datasource.name", "oak-blob")));

        //2. Create config for DocumentNodeStore with RDB enabled
        // (supply blobDataSource which should be ignored because customBlob takes precedence)
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(3);
        map1.put("documentStoreType", "RDB");
        map1.put("blobDataSource.target", "(datasource.name=oak-blob)");
        map1.put("customBlobStore", true);
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        registry.registerService(BlobStore.class.getName(), new MemoryBlobStore(), null);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        //3. Check that DS1 contains tables only from both RDBDocumentStore
        List<String> ds1Tables = getExistingTables(ds1);

        //DS1 should contain RDBDocumentStore tables only
        assertTrue(ds1Tables.contains("NODES"));
        assertFalse(ds1Tables.contains("DATASTORE_META"));

        //4. Check that DS2 is empty
        List<String> ds2Tables = getExistingTables(ds2);
        assertFalse(ds2Tables.contains("NODES"));
        assertFalse(ds2Tables.contains("DATASTORE_META"));

        //5. Check that only one cluster node was instantiated
        assertEquals(1, getIdsOfClusterNodes(ds1).size());

        //6. Unregister the data sources to test resilience wrt
        //multiple deregistrations (OAK-3383)
        sdsds.unregister();
        sdsbs.unregister();
    }

    @Test
    public void testMongoDocumentStore_CustomBlobStore() throws Exception {
        mongoCheck();

        registry = repositoryFactory.initializeServiceRegistry(config);

        Map<String, Map<String, Object>> map = Map.of(
                "org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService",
                Map.of(
                        "mongouri", MongoUtils.URL,
                        "db", MongoUtils.DB,
                        "customBlobStore", true));
        createConfig(map);

        registry.registerService(BlobStore.class.getName(), new MemoryBlobStore(), null);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        Collection<String> colNames = getCollectionNames();
        assertTrue(colNames.contains("NODES"));
        assertFalse(colNames.contains("BLOBS"));
        assertNull(
                "BlobStoreStatsMBean should *NOT* be registered by DocumentNodeStoreService in case custom blobStore used",
                registry.getServiceReference(BlobStoreStatsMBean.class.getName()));
    }

    @Test
    public void testMongoDocumentStore() throws Exception {
        mongoCheck();

        registry = repositoryFactory.initializeServiceRegistry(config);

        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(3);
        map1.put("mongouri", MongoUtils.URL);
        map1.put("db", MongoUtils.DB);
        map1.put("blobCacheSize", 1);
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);

        Collection<String> colNames = getCollectionNames();
        assertTrue(colNames.containsAll(new ArrayList<>(Arrays.asList("NODES", "BLOBS"))));

        assertEquals(1 * 1024 * 1024, ((MongoBlobStore) ns.getBlobStore()).getBlobCacheSize());
        assertNotNull("BlobStoreStatsMBean should be registered by DocumentNodeStoreService in default blobStore used",
                getService(BlobStoreStatsMBean.class));

        assertNotNull("BlobStore service should be exposed for default setup", getService(BlobStore.class));
        testBlobStoreStats(ns);
        testDocumentStoreStats(ns);
    }

    @Test
    public void testBundlingEnabledByDefault() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDB;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(), ds, new Hashtable<>(Map.of("datasource.name", "oak")));

        //2. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(1);
        map1.put("documentStoreType", "RDB");
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);
        assertTrue(ns.getBundlingConfigHandler().isEnabled());
    }

    @Test
    public void testBundlingDisabled() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        //1. Register the DataSource as a service
        DataSource ds = createDS("jdbc:h2:mem:testRDB;DB_CLOSE_DELAY=-1");
        registry.registerService(DataSource.class.getName(), ds, new Hashtable<>(Map.of("datasource.name", "oak")));

        //2. Create config for DocumentNodeStore with RDB enabled
        Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
        Map<String, Object> map1 = new LinkedHashMap<>(2);
        map1.put("documentStoreType", "RDB");
        map1.put("bundlingDisabled", true);
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        createConfig(map);

        DocumentNodeStore ns = (DocumentNodeStore) getServiceWithWait(NodeStore.class);
        assertFalse(ns.getBundlingConfigHandler().isEnabled());
    }

    private void testDocumentStoreStats(DocumentNodeStore store) throws CommitFailedException {
        DocumentStoreStatsMBean stats = getService(DocumentStoreStatsMBean.class);

        long createdNodeCount = stats.getNodesCreateCount();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("testDocumentStoreStats").child("a");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(stats.getNodesCreateCount() - createdNodeCount >= 2);

    }

    private void testBlobStoreStats(DocumentNodeStore nodeStore) throws Exception {
        int size = 1024 * 1024 * 5;
        Blob blob = nodeStore.createBlob(testStream(size));
        BlobStoreStatsMBean stats = getService(BlobStoreStatsMBean.class);
        assertEquals(stats.getUploadTotalSize(), size);
        assertTrue(stats.getUploadCount() > 0);

        BlobStore bs = nodeStore.getBlobStore();
        assertTrue(bs instanceof GarbageCollectableBlobStore);
        ((GarbageCollectableBlobStore) bs).clearCache();

        assertEquals(size, IOUtils.toByteArray(blob.getNewStream()).length);

        assertTrue(stats.getDownloadCount() > 0);
        assertTrue(stats.getDownloadTotalSize() > 0);

        assertCacheStatsMBean(CachingBlobStore.MEM_CACHE_NAME);
    }

    private void mongoCheck() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    private Collection<String> getCollectionNames() {
        return StreamSupport.stream(
                        mongoConn.getDatabase()
                                .listCollectionNames()
                                .map(String::toUpperCase)
                                .spliterator(), false)
                .collect(Collectors.toList());
    }

    private List<String> getExistingTables(DataSource ds) throws Exception {
        Connection con = ds.getConnection();
        List<String> existing = new ArrayList<>();
        try {
            ResultSet rs = con.getMetaData().getTables(null, null, "%", null);
            while (rs.next()) {
                existing.add(rs.getString("TABLE_NAME").toUpperCase());
            }

        } finally {
            con.close();
        }

        return existing;
    }

    private List<String> getIdsOfClusterNodes(DataSource ds) throws Exception {
        Connection con = ds.getConnection();
        List<String> entries = new ArrayList<>();
        try {
            ResultSet rs = con.prepareStatement("SELECT ID FROM CLUSTERNODES").executeQuery();
            while (rs.next()) {
                entries.add(rs.getString(1));
            }

        } finally {
            con.close();
        }

        return entries;
    }

    private DataSource createDS(String url) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setUrl(url);
        return ds;
    }

    private InputStream testStream(int size) {
        //Cannot use NullInputStream as it throws exception upon EOF
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private void assertCacheStatsMBean(final String name) throws Exception {
        ServiceReference[] refs = registry.getServiceReferences(CacheStatsMBean.class.getName(), null);
        final ArrayList<Object> names = new ArrayList<>();
        Optional<ServiceReference> cacheStatsRef = Stream.of(refs).filter(sr -> {
            CacheStatsMBean mbean = (CacheStatsMBean) getRegistry().getService(sr);
            names.add(mbean.getName());
            return mbean.getName().equals(name);
        }).findAny();

        assertTrue("No CacheStat found for [" + name + "]. Registered cache stats " + names,
                cacheStatsRef.isPresent());
    }
}
